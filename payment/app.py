import asyncio
import logging
import os
import uuid
import psycopg # type: ignore
from psycopg_pool import AsyncConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore
from msgspec import Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
import services.utils as utils   

from services.db_repository import PaymentRepository
from contextlib import asynccontextmanager


DB_ERROR_STR = "DB error"
import logging
logger = logging.getLogger("payment-service") 

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
app = FastAPI(title="payment-service", lifespan=lifespan)
service_name = "payment"
kafka_producer = None
kafka_consumer = None



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
        max_size=30,
        open=False,
        # Reconnection policy
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10}
    )



#  FIXME Redundant
async def dispatch_event(event: utils.BaseEvent):
    if event.event_type == utils.Commands.START_PAYMENT:
        result = await remove_user_credit(event)
        if isinstance(result, utils.Success):
            # Log the message and result for debugging
            print(f"Payment succeeded for user: {event.payload.user_id}, order: {event.payload.order_id}. Remaining credit: {result.value}")

        else:
            print(f"Payment failed for user: {event.payload.user_id}, order: {event.payload.order_id}. Reason: {result.error}")
    elif event.event_type == utils.Commands.ROLLBACK_PAYMENT:
        result = await refund_user_credit(event)
        if isinstance(result, utils.Success):
            print(f"Payment refunded for user: {event.payload['user_id']}, order: {event.order_id}. New credit: {result.value}")
        else:
            print(f"Payment refund failed for user: {event.payload.get('user_id')}, order: {event.order_id}. Reason: {result.error}")
    else:
        print(f"Unknown event type: {event.event_type}")



    





async def close_db_connection():
    if db_pool:
        await db_pool.close()


class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = PaymentRepository(cur)
                row = await repo.get_user_snapshot(user_id)
                if row is not None:                    
                    return UserValue(credit=row['credit'])

                
                return UserValue(row['credit']) if row is not None else None
                
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    return UserValue(credit=row['credit'])


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                repo = PaymentRepository(cur)
                # Append creation event and create initial snapshot
                version = 1
                await repo.insert_user_snapshot(user_id=key, credit=0, version=version)
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return JSONResponse(content={'user_id': key}, status_code=201)


@app.post('/batch_init/{n}/{starting_money}')
async def batch_init_users(n: int, starting_money: int):
    try:
        n = int(n)
        starting_money = int(starting_money)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid batch_init parameters")

    if n <= 0 or starting_money < 0:
        raise HTTPException(status_code=400, detail="Invalid batch_init parameters")

    created = 0
    skipped = 0

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Bulk initialize users in one SQL round-trip.
                # Keep API and semantics unchanged: existing users are skipped, new users get
                # both a version=1 snapshot and a USER_CREATED event.
                await cur.execute(
                    """
                    WITH inserted_snapshots AS (
                        INSERT INTO user_snapshots (user_id, credit, version)
                        SELECT gs::text, %s, 1
                        FROM generate_series(0, %s - 1) AS gs
                        ON CONFLICT (user_id) DO NOTHING
                        RETURNING user_id
                    )
                    SELECT count(*)::int AS created
                    FROM inserted_snapshots
                    """,
                    (starting_money, n),
                )
                row = await cur.fetchone()
                created = int(row['created']) if row is not None else 0
                skipped = n - created
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    return JSONResponse(
        content={
            "msg": "Batch init for users successful",
            "created": created,
            "skipped": skipped
        },
        status_code=200
    )
    


@app.get('/find_user/{user_id}')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id) # type: ignore
    return JSONResponse(
        content={
            "user_id": user_id,
            "credit": user_entry.credit
        },
        status_code=200
    )


@app.post('/pay/{user_id}/{amount}')
async def http_remove_credit(user_id: str, amount: int):
    # Direct HTTP mode: update state and reply immediately without inbox/outbox writes.
    result = await remove_user_credit_direct(user_id, int(amount))
    if isinstance(result, utils.Success):
        return PlainTextResponse(f"User: {user_id} credit updated to: {result.value}", status_code=200)
    else:
        raise HTTPException(status_code=400, detail=result.error)

def load_aggregate_state():
    """Load aggregate state from database on startup"""
    # Extract all the events related to 


async def remove_user_credit_direct(user_id: str, amount: int):
    """Asynchronous debit path used by HTTP endpoint; no inbox/outbox side effects."""
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)
                    row = await repo.get_user_snapshot(user_id)
                    if row is None:
                        return utils.Failure(f"User: {user_id} not found")

                    current_credit = int(row['credit'])
                    current_version = int(row['version'])
                    if current_credit - int(amount) < 0:
                        return utils.Failure("Insufficient credit")

                    new_credit = current_credit - int(amount)
                    new_version = current_version + 1

                    if await repo.update_user_snapshot_versioned(user_id, new_credit, new_version, current_version):
                        logger.info(f"User: {user_id} credit updated to: {new_credit}")
                        return utils.Success(new_credit)

                    logger.warning(f"Version conflict user {user_id}, retry {attempt + 1}/{MAX_RETRIES}")

        except psycopg.Error as e:
            logger.error(f"Database error: {e}")
            return utils.Failure(DB_ERROR_STR)

    return utils.Failure("Too many concurrent updates, please retry")


async def remove_user_credit(event: utils.BaseEvent):
    user_id = event.payload.user_id
    order_id = event.payload.order_id
    amount = int(event.payload.amount)

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)

                    # 1. Idempotency check — must be first.
                    result = await repo.get_received_event_result(event.id)
                    if result is not None:
                        logger.info("IDEMPOTENCY: Event %s already processed", event.id)
                        if isinstance(result, dict) and result.get("status") == "success":
                            return utils.Success(result.get("remaining_credit"))
                        return utils.Failure(result.get("reason", "Previous attempt failed"))
                    await repo.insert_received_event(event.id)
                    # 2. Fetch snapshot.
                    user = await repo.get_user_snapshot(user_id)
                    if user is None:
                        logger.warning("User %s not found for order %s", user_id, order_id)
                        
                        await repo.set_received_event_result(
                            event.id, "PROCESSED",
                            {"status": "failed", "reason": "User not found"},
                        )
                        await repo.insert_outbox_message(
                            "orchestrator.request",
                            utils.build_payment_failed_event(
                                saga_id=event.saga_id,
                                order_id=event.payload.order_id,
                                user_id=event.payload.user_id,
                                amount=event.payload.amount,
                                reason="User not found"
                            ),
                        )
                        return utils.Failure("User not found")

                    # 3. Credit validation.
                    current_credit = int(user["credit"])
                    current_version = int(user["version"])

                    if current_credit < amount:
                        logger.info("Insufficient credit for user %s", user_id)
                        
                        await PaymentRepository.write_outbox_and_update_event(
                            cur=cur,
                            event=event,
                            result={"status": "failed", "reason": "Insufficient credit"},
                            outbox_msg=utils.build_payment_failed_event(
                                saga_id=event.saga_id,
                                order_id=event.payload.order_id,
                                user_id=event.payload.user_id,
                                amount=event.payload.amount,
                                reason="Insufficient credit"
                            )
                        )
                        logger.info("Payment failed for user %s due to insufficient credit", user_id)
                        return utils.Failure("Insufficient credit")

                    # 4. Optimistic locking write phase.
                    new_credit = current_credit - amount
                    new_version = current_version + 1

                   

                    if not await repo.update_user_snapshot_versioned(
                        user_id, new_credit, new_version, current_version
                    ):
                        # Version conflict — implicit rollback on context exit,
                        # then retry. Sleep OUTSIDE the connection block so we
                        # don't hold a pool connection during the wait.
                        logger.warning(
                            "Version conflict for user %s, attempt %d/%d",
                            user_id, attempt + 1, MAX_RETRIES,
                        )
                        # fall through to sleep below

                    else:
                        # Success — claim event and write outbox atomically.
                        await repo.set_received_event_result(
                            event.id, "PROCESSED",
                            {"status": "success", "remaining_credit": new_credit},
                        )
                        await repo.insert_outbox_message(
                            "orchestrator.request",
                            utils.build_payment_succeeded_event(
                                saga_id=event.saga_id,
                                order_id=event.payload.order_id,
                                user_id=event.payload.user_id,
                                amount=event.payload.amount,
                                remaining_credit=new_credit
                            ),
                        )
                        logger.info("User %s debited. New balance: %d", user_id, new_credit)
                        return utils.Success(new_credit)

            # Connection released back to pool before sleeping.
            wait_time = 0.01 * (2 ** attempt)
            logger.warning("Retrying in %.3fs...", wait_time)
            await asyncio.sleep(wait_time)

        except psycopg.Error:
            logger.exception("DB error during credit removal for user %s", user_id)
            return utils.Failure(DB_ERROR_STR)

    # All retries exhausted — fresh connection, guaranteed to commit.
    logger.error("Credit removal failed after %d retries for user %s", MAX_RETRIES, user_id)
    await handle_request_exhausted(event)
    return utils.Failure("Concurrent update limit reached")


async def refund_user_credit(event: utils.BaseEvent):
    """Refund credit to a user (add it back) when a saga is compensated."""
    user_id = event.payload.get("user_id") if isinstance(event.payload, dict) else event.payload["user_id"]
    order_id = event.order_id
    amount = int(event.payload.get("amount") if isinstance(event.payload, dict) else event.payload["amount"])

    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)

                    # 1. Idempotency check
                    result = await repo.get_received_event_result(event.id)
                    if result is not None:
                        logger.info("IDEMPOTENCY: Event %s already processed", event.id)
                        if isinstance(result, dict) and result.get("status") == "success":
                            return utils.Success(result.get("new_credit"))
                        return utils.Failure(result.get("reason", "Previous attempt failed"))
                    await repo.insert_received_event(event.id)

                    # 2. Fetch snapshot
                    user = await repo.get_user_snapshot(user_id)
                    if user is None:
                        logger.warning("User %s not found for refund on order %s", user_id, order_id)
                        await repo.set_received_event_result(
                            event.id, "PROCESSED",
                            {"status": "failed", "reason": "User not found"},
                        )
                        await repo.insert_outbox_message(
                            "orchestrator.request",
                            utils.build_payment_refund_event(
                                saga_id=event.saga_id,
                                order_id=order_id,
                                user_id=user_id,
                                amount=amount,
                            ),
                        )
                        return utils.Failure("User not found")

                    # 3. Add credit back (refund)
                    current_credit = int(user["credit"])
                    current_version = int(user["version"])
                    new_credit = current_credit + amount
                    new_version = current_version + 1

                    if not await repo.update_user_snapshot_versioned(
                        user_id, new_credit, new_version, current_version
                    ):
                        logger.warning(
                            "Version conflict for user %s during refund, attempt %d/%d",
                            user_id, attempt + 1, MAX_RETRIES,
                        )
                    else:
                        # Success — mark event as processed and send confirmation
                        await repo.set_received_event_result(
                            event.id, "PROCESSED",
                            {"status": "success", "new_credit": new_credit},
                        )
                        await repo.insert_outbox_message(
                            "orchestrator.request",
                            utils.build_payment_refund_event(
                                saga_id=event.saga_id,
                                order_id=order_id,
                                user_id=user_id,
                                amount=amount,
                            ),
                        )
                        logger.info("User %s refunded %d. New balance: %d", user_id, amount, new_credit)
                        return utils.Success(new_credit)

            wait_time = 0.01 * (2 ** attempt)
            logger.warning("Retrying refund in %.3fs...", wait_time)
            await asyncio.sleep(wait_time)

        except psycopg.Error:
            logger.exception("DB error during credit refund for user %s", user_id)
            return utils.Failure(DB_ERROR_STR)

    # All retries exhausted
    logger.error("Credit refund failed after %d retries for user %s", MAX_RETRIES, user_id)
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            repo = PaymentRepository(cur)
            await repo.set_received_event_result(
                event_id=event.id,
                status="PROCESSED",
                result={"status": "failed", "reason": "Too many retries"},
            )
            await repo.insert_outbox_message(
                topic="orchestrator.request",
                payload=utils.build_payment_refund_event(
                    saga_id=event.saga_id,
                    order_id=order_id,
                    user_id=user_id,
                    amount=amount,
                ),
            )
    return utils.Failure("Concurrent update limit reached")


async def handle_request_exhausted(event: utils.BaseEvent) -> None:
    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            repo = PaymentRepository(cur)
            await repo.set_received_event_result(
                event_id=event.id,
                status="PROCESSED",
                result={"status": "failed", "reason": "Too many retries"},
            )
            await repo.insert_outbox_message(
                topic="orchestrator.request",
                payload=utils.build_payment_failed_event(
                    saga_id=event.saga_id,
                    order_id=event.payload.order_id,
                    user_id=event.payload.user_id,
                    amount=event.payload.amount,
                    reason="Too many retries, please retry later",
                ),
            )

@app.post('/add_funds/{user_id}/{amount}')
async def add_credit(user_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            async with db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)
                    row = await repo.get_user_snapshot(user_id)
                    if row is None:
                        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")

                    new_credit = int(row['credit']) + int(amount)
                    current_version = int(row['version'])
                    new_version = current_version + 1

                    if not await repo.update_user_snapshot_versioned(user_id, new_credit, new_version, current_version):
                        logger.warning(f"Version conflict user {user_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    return PlainTextResponse(f"User: {user_id} credit updated to: {new_credit}", status_code=200)

        except psycopg.Error:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    raise HTTPException(status_code=409, detail="Too many concurrent updates, please retry")


@app.get('/users')
async def get_users():
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = PaymentRepository(cur)
                rows = await repo.list_user_snapshots()
                return rows
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    uvicorn_logger = logging.getLogger("uvicorn.error")
    logger.handlers = uvicorn_logger.handlers
    logger.setLevel(uvicorn_logger.level)