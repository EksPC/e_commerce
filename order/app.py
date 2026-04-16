from datetime import datetime
import logging
import os
import random
import asyncio
import uuid

import httpx
import psycopg  # type: ignore
from psycopg_pool import AsyncConnectionPool  # type: ignore
from psycopg.rows import dict_row  # type: ignore
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import services.utils as utils
from msgspec import json, Struct
import redis.asyncio as redis  # pip install redis
from producer import OutboxRelay
from contextlib import asynccontextmanager

import logging
logger = logging.getLogger("order-service") 


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SAGA_TIMEOUT_SECONDS: float = 30.0
DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:80")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8000")

# ---------------------------------------------------------------------------
# Startup/shutdown management
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app lifecycle: startup and shutdown."""
    # Startup
    db_pool = await get_db_pool()

    yield
    # Shutdown
    await close_resources()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="order-service", lifespan=lifespan)
kafka_producer = None
outbox_relay: OutboxRelay | None = None
service_name = "order"


conn_params = {
    'host': os.environ['POSTGRES_HOST'],
    'port': int(os.environ['POSTGRES_PORT']),
    'user': os.environ['POSTGRES_USER'],
    'password': os.environ['POSTGRES_PASSWORD'],
    'dbname': os.environ['POSTGRES_DB']
}

db_pool: AsyncConnectionPool | None = None


async def get_db_pool() -> AsyncConnectionPool:
    """Return the shared async pool, creating it on first call."""
    global db_pool
    if db_pool is None:
        db_pool = AsyncConnectionPool(
            conninfo=(
                f"host={conn_params['host']} port={conn_params['port']} "
                f"user={conn_params['user']} password={conn_params['password']} "
                f"dbname={conn_params['dbname']}"
            ),
            min_size=1,
            max_size=20,
            open=False,  # opened explicitly below
            kwargs={"connect_timeout": 10},
        )
        await db_pool.open()
    return db_pool



redis_client: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


async def close_resources() -> None:
    if db_pool is not None:
        await db_pool.close()
    await redis_client.aclose()


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------
class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def get_order_from_db(order_id: str) -> OrderValue:
    pool = await get_db_pool()
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT paid, items, user_id, total_cost FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                row = await cur.fetchone()
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    if row is None:
        raise HTTPException(status_code=404, detail=f"Order: {order_id} not found!")

    items = [(item["item_id"], item["quantity"]) for item in row["items"]]
    return OrderValue(
        paid=row["paid"],
        items=items,
        user_id=row["user_id"],
        total_cost=row["total_cost"],
    )


async def log_event(event: utils.BaseEvent) -> None:
    """Log an internal event to the DB."""
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO log (id, order_id, event_type, data)
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (
                    event.id,
                    event.order_id,
                    event.event_type,
                    json.encode(event.payload).decode(),
                ),
            )


async def write_outbox_and_log(event: utils.BaseEvent, topic: str) -> None:
    """Insert an event into the transactional outbox."""
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO outbox (id, topic, payload)
                VALUES (%s, %s, %s::jsonb)
                """,
                (event.id, topic, json.encode(event).decode()),
            )
            await cur.execute(
                """
                INSERT INTO log (id, order_id, event_type, data)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    event.id,
                    event.order_id,
                    event.event_type,
                    json.encode(event.payload).decode(),
                ),
            )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/create/{user_id}")
async def create_order(user_id: str):
    pool = await get_db_pool()
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    INSERT INTO orders (paid, items, user_id, total_cost)
                    VALUES (%s, %s::jsonb, %s, %s)
                    RETURNING order_id
                    """,
                    (False, "[]", user_id, 0),
                )
                row = await cur.fetchone()
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    if row is None:
        raise HTTPException(status_code=500, detail="Failed to create order")
    return JSONResponse(content={"order_id": row["order_id"]}, status_code=201)


@app.post("/batch_init/{n}/{n_items}/{n_users}/{item_price}")
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    if n <= 0 or n_items <= 0 or n_users <= 0 or item_price < 0:
        raise HTTPException(status_code=400, detail="Invalid batch_init parameters")

    pool = await get_db_pool()
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                # Generate all n orders in a single SQL statement with server-side randomization
                await cur.execute(
                    """
                    INSERT INTO orders (paid, items, user_id, total_cost)
                    SELECT
                        false,
                        jsonb_build_array(
                            jsonb_build_object('item_id', (random() * %s)::int::text, 'quantity', 1),
                            jsonb_build_object('item_id', (random() * %s)::int::text, 'quantity', 1)
                        ),
                        (random() * %s)::int::text,
                        %s
                    FROM generate_series(1, %s)
                    """,
                    (n_items - 1, n_items - 1, n_users - 1, item_price * 2, n),
                )
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    return JSONResponse(content={"msg": "Batch init for orders successful"}, status_code=201)


@app.get("/find/{order_id}")
async def find_order(order_id: str):
    order = await get_order_from_db(order_id)
    return JSONResponse(
        content={
            "order_id": order_id,
            "paid": order.paid,
            "items": order.items,
            "user_id": order.user_id,
            "total_cost": order.total_cost,
        },
        status_code=200
    )
    


@app.get("/routes")
def list_routes():
    return JSONResponse(content={"routes": [str(rule) for rule in app.routes]})


@app.get("/test/outbox")
async def test_outbox():
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT id, topic, payload, sent
                FROM outbox
                ORDER BY created_at ASC
                LIMIT 10
                """
            )
            rows = await cur.fetchall()
    return JSONResponse(content=[dict(row) for row in rows])

@app.get("/test/kafka")
async def test_kafka():
    event = utils.BaseEvent(
        id=str(uuid.uuid4()),
        event_type="test_event",
        order_id="test_order_id",
        saga_id="test_saga_id",
        payload=utils.StartPaymentCommandPayload(order_id="test_order_id", user_id="test_user_id", amount=100)
    )

    print(event.to_dict())
    
    return JSONResponse(content={"msg": "Test event written to outbox"}, status_code=200)


@app.post("/addItem/{order_id}/{item_id}/{quantity}")
async def add_item(order_id: str, item_id: str, quantity: int):
    logger.info("item=%s quantity=%d order=%s", item_id, quantity, order_id)
    order = await get_order_from_db(order_id)

    # Build the updated list before writing so we can return it accurately.
    updated_items = list(order.items) + [(item_id, quantity)]
    items_json = [{"item_id": i, "quantity": q} for i, q in updated_items]
    serialized = json.encode(items_json).decode()
    # get request to stock service to retrieve the price of the item
    item_price = await fetch_item_price(item_id)
    if item_price is None:
        raise HTTPException(status_code=400, detail=f"Item {item_id} not found in stock service")
    new_total_cost = order.total_cost + item_price * quantity
    pool = await get_db_pool()
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE orders SET items = %s::jsonb, total_cost = %s WHERE order_id = %s",
                    (serialized, new_total_cost, order_id),
                )
    except psycopg.Error as exc:
        logger.exception("DB error updating order %s", order_id)
        raise HTTPException(status_code=400, detail=f"Database error: {exc}")

    # Return the *updated* item list, not the stale pre-update snapshot.
    return JSONResponse(
        content={
            "order_id": order_id,
            "items": [{"item_id": i, "quantity": q} for i, q in updated_items],
            "user_id": order.user_id,
            "total_cost": new_total_cost,
        },
        status_code=200
    )


async def fetch_item_price(item_id: str) -> int | None:
    """Fetch the price of an item from the stock service via gateway."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{GATEWAY_URL}/stock/find/{item_id}")
            if resp.status_code == 200:
                data = resp.json()
                return data.get("price")
            else:
                logger.error("Failed to fetch price for item %s: HTTP %d", item_id, resp.status_code)
                return None
    except Exception as exc:
        logger.exception("Error fetching price for item %s", item_id)
        return None


@app.post("/checkout/{order_id}")
async def checkout(order_id: str):
    """
    Checkout via Redis pub/sub saga notification.

    Ordering matters:
      1. Subscribe FIRST — so a fast worker cannot publish before we listen.
      2. Dispatch the CHECKOUT_INITIATED event to the outbox.
      3. Block on the channel with a hard deadline via asyncio.wait_for.
      4. Always clean up the pub/sub connection in the finally block.
    """

    logging.info("TEST: order %s, TIME: %s", order_id, datetime.now().strftime("%H:%M:%S.%f"))


    order = await get_order_from_db(order_id)
    if order.paid:
        return JSONResponse(
            content={"status": "failed", "message": "Order already paid"},
            status_code=400
        )
    elif not order.items:
        return JSONResponse(
            content={"status": "failed", "message": "Cannot checkout an empty order"}, 
            status_code=400
        )
    saga_id = str(uuid.uuid4())
    # In 5 % of the cases generate a custom SAGA_ID which can be easily identified in the logs, to test the happy path and timeout scenarios.
    if random.random() < 0.01:
        saga_id = "TEST_SAGA_ID_" + str(uuid.uuid4())
        utils.print_test(
            location="ORDER_PRODUCER",
            step="CHECKOUT_INITIATED",
            saga_id=saga_id
        )

    channel = f"saga:{saga_id}"  # must match consumer subscription
    logger.info("Initiating checkout for order %s SAGA_ID: %s", order_id, saga_id)

    # 1. Subscribe before dispatching — no race window.
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)


    try:
        # 2. Persist the initiating event and enqueue it via outbox.
        event = utils.BaseEvent(
            id=str(uuid.uuid4()),
            event_type=utils.OrderIntegrationEvent.CHECKOUT_INITIATED,
            order_id=order_id,
            saga_id=saga_id,
            payload=utils.CheckoutPayload(
                user_id=order.user_id,
                items=order.items,
                amount=order.total_cost,
            )
        )
        print(f"Starting event {event.event_type} for order {order_id}")
        await write_outbox_and_log(event, topic="orchestrator.request")

        # 3. Wait for the saga worker to publish on the channel.
        try:
            result = await asyncio.wait_for(
                _wait_for_saga_message(pubsub),
                timeout=SAGA_TIMEOUT_SECONDS,
            )

            logger.info("Received saga result for order %s: %s", order_id, result)
            event: utils.DecodeResult = utils.decode_and_type_event(result)

            if isinstance(event, utils.Failure):
                logging.error("Failed to decode saga result for order %s: %s", order_id, event.error)
                return handle_decoding_error(event)
            
            await log_event(utils.BaseEvent(
                id=str(uuid.uuid4()),
                event_type=utils.OrderInternalEvent.ORDER_COMPLETED,
                order_id=order_id,
                saga_id=event.value.saga_id,
                payload=event.value.payload,
            ))

            if event.value.payload['status'] == "success":
                return JSONResponse(content={"status": "ok", "result": event.value.payload}, status_code=200)
            else:
                return JSONResponse(content={"status": "failed", "result": event.value.payload}, status_code=400)

            
        except asyncio.TimeoutError:
            return JSONResponse(content={"status": "timeout", "message": "No saga result received"}, status_code=408)

    finally:
        # 4. Always clean up — even on exception or timeout.
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()


async def _wait_for_saga_message(pubsub) -> str:
    """
    Iterate over pub/sub messages and return the data of the first real
    'message'-type frame, skipping subscribe confirmations.

    pubsub.listen() is an async generator that suspends properly between
    messages — no busy-polling, no manual sleep().
    """
    async for message in pubsub.listen():
        if message and message.get("type") == "message":
            return message["data"]
    raise RuntimeError("pub/sub connection closed before receiving a message")




def handle_decoding_error(result: utils.Failure):
    logger.error("Failed to decode saga response: %s", result.error)
    if result.error == "UNKNOWN_EVENT_TYPE":
        return JSONResponse(content={"status": "failed", "message": f"Unknown event type: {result.error}"}, status_code=500)
    if result.error == "EMPTY_MESSAGE":
        return JSONResponse(content={"status": "failed", "message": "Received tombstone (empty message)"}, status_code=500)
    else:
        return JSONResponse(content={"status": "failed", "message": f"Decoding error: {result.error}"}, status_code=500)


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    uvicorn_logger = logging.getLogger("uvicorn.error")
    logger.handlers = uvicorn_logger.handlers
    logger.setLevel(uvicorn_logger.level)