import asyncio
import logging
import os
import uuid

import psycopg # type: ignore
from psycopg.types.json import Jsonb # type: ignore
from psycopg_pool import AsyncConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore
from collections import defaultdict
import json as std_json
from msgspec import Struct, json
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
import services.utils as utils
from services.db_repository import StockRepository
from contextlib import asynccontextmanager


DB_ERROR_STR = "DB error"
import logging
logger = logging.getLogger("stock-service") 
# ---------------------------------------------------------------------------
# Startup/shutdown management
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app lifecycle: startup and shutdown."""
    # Startup
    global db_pool
    db_pool = init_db_pool()
    await db_pool.open()

    yield
    # Shutdown
    if db_pool:
        await db_pool.close()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="stock-service", lifespan=lifespan)
service_name = "stock"

kafka_producer = None
kafka_consumer = None


# Create connection pool
conn_params = {
    'host': os.environ['POSTGRES_HOST'],
    'port': int(os.environ['POSTGRES_PORT']),
    'user': os.environ['POSTGRES_USER'],
    'password': os.environ['POSTGRES_PASSWORD'],
    'dbname': os.environ['POSTGRES_DB']
}

db_pool: AsyncConnectionPool = None


def init_db_pool():
        
    return AsyncConnectionPool(
        conninfo=f"host={conn_params['host']} port={conn_params['port']} "
                f"user={conn_params['user']} password={conn_params['password']} "
                f"dbname={conn_params['dbname']}",
        min_size=1,
        max_size=10,
        open=False,
        # Reconnection policy
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10}
    )




class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                row = await repo.get_item_snapshot(item_id)
                if row is None:
                    return None
                return StockValue(stock=row['stock'], price=row['price'])

    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
   

async def dispatch_event(event: utils.BaseEvent):
    
    if event.event_type == utils.Commands.RESERVE_STOCK:
        # logger.info(f"DISPATCH: Received RESERVE_STOCK event_id={event.id}, saga_id={event.saga_id}, order_id={event.order_id}")
        payload = event.payload
        result = await subtract_stock_batch(event)
        
        if isinstance(result, utils.Success):
            logger.info(f"Stock reservation successful for order {payload.order_id}, total cost: {result.value}")
        elif isinstance(result, utils.Failure):
            logger.info(f"Stock reservation failed for order {payload.order_id}, reason: {result.error}")
        
    elif event.event_type == utils.Commands.FREE_STOCK:
        # logger.info(f"DISPATCH: Received FREE_STOCK event_id={event.id}, saga_id={event.saga_id}, order_id={event.order_id}")
        await handle_rollback(event)


async def handle_already_processed(event: utils.BaseEvent, result, cur: psycopg.AsyncCursor = None):
    """
    Handle events that have already been processed.
    """
    # Extract the result from the received_events table and resend the appropriate response based on the event type
    repo = StockRepository(cur)
    response_event = None

    if event.event_type == utils.Commands.RESERVE_STOCK:
        if result.get('status') == 'success':
            response_event = utils.build_stock_allocated_event(
                saga_id=event.saga_id,
                order_id=event.payload.order_id,
                amount=result.get('amount', 0),
            )
        else:
            response_event = utils.build_stock_unavailable_event(
                saga_id=event.saga_id,
                order_id=event.payload.order_id,
            )

        # write to outbox
        logger.info(f"Stock unavailable for item {event.saga_id}: {response_event.event_type}")
    
    elif event.event_type == utils.Commands.FREE_STOCK:
        if result.get('status') == 'compensated':
            response_event = utils.build_stock_freed_event(
                saga_id=event.saga_id,
                order_id=event.payload.order_id,
            )
            
            logger.info(f"Stock freed for item {event.saga_id}: {response_event.event_type}")
        else:
            logger.error(f"Unexpected status for already processed FREE_STOCK event {event.saga_id}: {result.get('status')}")
    else:
        logger.error(f"Unknown event type for already processed event {event.saga_id}: {event.event_type}")
        return
    await repo.insert_outbox_message('orchestrator.request', response_event)
            

async def handle_rollback(event: utils.BaseEvent):
    order_id = event.payload.order_id
    qty_by_item: dict[str, int] = defaultdict(int)
    for item_id, qty in event.payload.items:
        qty_by_item[item_id] += int(qty)

    items = sorted(qty_by_item.items(), key=lambda x: x[0])
    item_ids = [item_id for item_id, _ in items]

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                result = await repo.get_received_event_result(event.id)
                
                if result is not None:
                    logger.info(f"IDEMPOTENCY: Rollback event {event.id} already processed, returning cached result")
                    return await handle_already_processed(event, result, cur)
                
                logger.info(f"IDEMPOTENCY: Rollback event {event.id} is NEW, proceeding with stock addition")
                
                # Pessimistic locking: SELECT...FOR UPDATE to lock rows
                await cur.execute(
                    """
                    SELECT item_id, stock, version
                    FROM item_snapshots
                    WHERE item_id = ANY(%s)
                    ORDER BY item_id
                    FOR UPDATE
                    """,
                    (item_ids,),
                )
                locked_rows = await cur.fetchall()
                rows_dict = {row["item_id"]: row for row in locked_rows}

                # Check all items exist
                missing = [iid for iid, _ in items if iid not in rows_dict]
                if missing:
                    return await handle_already_processed(
                        event=event, 
                        result={"status": "failed", "reason": f"Item(s) not found: {', '.join(missing)}"}, 
                        cur=cur
                    )

                # Prepare batch update arrays with pessimistic lock held
                item_ids_arr = []
                new_stocks_arr = []
                new_versions_arr = []

                for item_id, qty in items:
                    row = rows_dict[item_id]
                    current_version = int(row["version"])
                    new_stock = int(row["stock"]) + int(qty)
                    new_version = current_version + 1

                    item_ids_arr.append(item_id)
                    new_stocks_arr.append(new_stock)
                    new_versions_arr.append(new_version)

                # Batch update snapshots (no version check needed, we hold the lock)
                await cur.execute(
                    """
                    UPDATE item_snapshots
                    SET stock = updates.new_stock,
                        version = updates.new_version
                    FROM unnest(
                        %s::text[],
                        %s::int[],
                        %s::int[]
                    ) AS updates(item_id, new_stock, new_version)
                    WHERE item_snapshots.item_id = updates.item_id
                    """,
                    (item_ids_arr, new_stocks_arr, new_versions_arr),
                )
                
                # Commit with outbox message
                outbox_msg = utils.build_stock_freed_event(
                    saga_id=event.saga_id,
                    order_id=order_id,
                )

                await StockRepository.write_outbox_and_update_event(
                    cur=cur,
                    event=event,
                    result={"status": "success"},
                    outbox_msg=outbox_msg,
                )
                logger.info("Stock rollback completed for order %s", order_id)
                return
                    
    except psycopg.Error:
        logger.exception("Stock rollback DB error for order %s", order_id)
        return utils.Failure("Database error during stock rollback")


async def subtract_stock_batch(event: utils.BaseEvent):

    items = event.payload.items
    order_id = event.order_id
    event_id = event.id
    saga_id = event.saga_id
    

    if not event.payload.items:
        return utils.Failure("No items to reserve")

    items = sorted(items, key=lambda x: x[0]) 
    item_ids = [item_id for item_id, _ in items]

    logger.info("Subtracting stock for order: %s", order_id)

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)

                result = await repo.get_received_event_result(event.id)
                
                if result is not None:
                    logger.info(f"IDEMPOTENCY: Event {event.id} already processed, returning cached result")
                    return await handle_already_processed(event, result, cur)

                # Pessimistic locking: SELECT...FOR UPDATE to lock rows
                await cur.execute(
                    """
                    SELECT item_id, stock, price, version
                    FROM item_snapshots
                    WHERE item_id = ANY(%s)
                    ORDER BY item_id
                    FOR UPDATE
                    """,
                    (item_ids,),
                )
                locked_rows = await cur.fetchall()
                items_snapshots = {row["item_id"]: row for row in locked_rows}

                # Check that all items exist
                missing = [iid for iid, _ in items if iid not in items_snapshots]
                if missing:
                    return await StockRepository.write_outbox_and_update_event(
                        event=event, 
                        result={"status": "failed", "reason": f"Item(s) not found: {', '.join(missing)}"},
                        cur=cur,
                        outbox_msg=utils.build_stock_failure(
                            saga_id=saga_id,
                            order_id=order_id,
                            data={"reason": f"Item(s) not found: {', '.join(missing)}"},
                        )
                    )
                    

                # Check availability before writing (with lock held)
                unavailable = [
                    iid for iid, qty in items
                    if int(items_snapshots[iid]['stock']) < int(qty)
                ]

                if unavailable:
                   return await StockRepository.write_outbox_and_update_event(
                        event=event, 
                        result={"status": "failed", "reason": f"Item(s) unavailable: {', '.join(unavailable)}"},
                        cur=cur,
                        outbox_msg=utils.build_stock_unavailable_event(
                            saga_id=saga_id,
                            order_id=order_id,
                        )
                    )

                # Write phase: Pessimistic locking (no retries needed)
                item_ids_arr = []
                new_stocks_arr = []
                new_versions_arr = []
                total_cost = 0

                for item_id, qty in items:
                    row = items_snapshots[item_id]
                    current_version = int(row["version"])
                    new_stock = int(row["stock"]) - int(qty)
                    new_version = current_version + 1
                    price = int(row["price"])

                    item_ids_arr.append(item_id)
                    new_stocks_arr.append(new_stock)
                    new_versions_arr.append(new_version)
                    total_cost += price * int(qty)

                # Batch update snapshots (no version check needed, we hold the lock)
                await cur.execute(
                    """
                    UPDATE item_snapshots
                    SET stock = updates.new_stock,
                        version = updates.new_version
                    FROM unnest(
                        %s::text[],
                        %s::int[],
                        %s::int[]
                    ) AS updates(item_id, new_stock, new_version)
                    WHERE item_snapshots.item_id = updates.item_id
                    """,
                    (item_ids_arr, new_stocks_arr, new_versions_arr),
                )
                
                logger.info(f"Stock subtraction successful for order {order_id}, total cost: {total_cost}")
                return await StockRepository.write_outbox_and_update_event(
                    event=event,
                    result={"status": "success", "amount": total_cost},
                    cur=cur,
                    outbox_msg=utils.build_stock_allocated_event(
                        saga_id=saga_id,
                        order_id=order_id,
                        amount=total_cost,
                    )
                )
                    
    except psycopg.Error:
        logger.exception("DB error during stock subtraction for order %s", order_id)
        return utils.Failure("Database error during stock subtraction")




@app.post('/item/create/{price}')
async def create_item(price: int):
    
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                repo = StockRepository(cur)
                item_id = await repo.insert_item_snapshot( 0, int(price), 1)
                
                
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return JSONResponse(content={'item_id': item_id}, status_code=201)


@app.get('/items')
async def get_items():
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            repo = StockRepository(cur)
            items = await repo.list_item_snapshots()
            return items
    

@app.post('/batch_init/{n}/{starting_stock}/{item_price}')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    try:
        n = int(n)
        starting_stock = int(starting_stock)
        item_price = int(item_price)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid batch_init parameters")

    if n <= 0 or starting_stock < 0 or item_price < 0:
        raise HTTPException(status_code=400, detail="Invalid batch_init parameters")

    created = 0
    skipped = 0

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Bulk initialize items in one SQL round-trip.
                # Keep API and semantics unchanged: existing items are skipped, new items get version=1 snapshot.
                await cur.execute(
                    """
                    WITH inserted_snapshots AS (
                        INSERT INTO item_snapshots (item_id, stock, price, version)
                        SELECT gs::text, %s, %s, 1
                        FROM generate_series(0, %s - 1) AS gs
                        ON CONFLICT (item_id) DO NOTHING
                        RETURNING item_id
                    )
                    SELECT count(*)::int AS created FROM inserted_snapshots
                    """,
                    (starting_stock, item_price, n),
                )
                row = await cur.fetchone()
                created = int(row['created']) if row is not None else 0
                skipped = n - created
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    return JSONResponse(
        content={
            "msg": "Batch init for stock successful",
            "created": created,
            "skipped": skipped
        },
        status_code=200
    )


@app.get('/find/{item_id}')
async def find_item(item_id: str):
    item_entry: StockValue = await get_item_from_db(item_id) # type: ignore
    if item_entry is None:
        raise HTTPException(status_code=404, detail=f"Item: {item_id} not found!")
    return JSONResponse(
        content={
            "stock": item_entry.stock,
            "price": item_entry.price
        },
        status_code=200
    )
    

@app.post('/add/{item_id}/{amount}')
async def add_stock(item_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = await repo.get_item_snapshot(item_id)
                    if row is None:
                        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")

                    current_version = int(row['version'])
                    new_stock = int(row['stock']) + int(amount)
                    new_version = current_version + 1

                    
                    if not await repo.update_snapshot_versioned(item_id, new_stock, new_version, current_version):
                        logger.warning(f"Version conflict for item {item_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    return PlainTextResponse(f"Item: {item_id} stock updated to: {new_stock}", status_code=200)

        except psycopg.Error:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    raise HTTPException(status_code=409, detail="Too many concurrent updates, please retry")


@app.post('/subtract/{item_id}/{amount}')
async def remove_stock(item_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = await repo.get_item_snapshot(item_id)
                    if row is None:
                        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")

                    current_stock = int(row['stock'])
                    current_version = int(row['version'])
                    new_stock = current_stock - int(amount)

                    if new_stock < 0:
                        raise HTTPException(status_code=400, detail=f"Item: {item_id} stock cannot get reduced below zero!")

                    new_version = current_version + 1

                    

                    if not await repo.update_snapshot_versioned(item_id, new_stock, new_version, current_version):
                        logger.warning(f"Version conflict for item {item_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    logger.debug(f"Item: {item_id} stock updated to: {new_stock}")
                    return PlainTextResponse(f"Item: {item_id} stock updated to: {new_stock}", status_code=200)

        except psycopg.Error:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    raise HTTPException(status_code=409, detail="Too many concurrent updates, please retry")


@app.get("/logs")
async def get_logs():
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                events = await repo.get_item_snapshots()
                return JSONResponse(content=events, status_code=200)

    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

@app.get("/outbox")
async def get_outbox():
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                events = await repo.get_outbox_events()
                print(events)
                return JSONResponse(content=events, status_code=200)

    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    uvicorn_logger = logging.getLogger("uvicorn.error")
    logger.handlers = uvicorn_logger.handlers
    logger.setLevel(uvicorn_logger.level)
