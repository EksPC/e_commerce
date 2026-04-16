import asyncio
import logging
from collections import defaultdict

import psycopg
from psycopg_pool import AsyncConnectionPool
import os

import services.utils as utils
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from services.db_repository import OrchestratorRepository

logging.basicConfig(level=logging.INFO)


conn_params = {
    'host': os.environ['POSTGRES_HOST'],
    'port': int(os.environ['POSTGRES_PORT']),
    'user': os.environ['POSTGRES_USER'],
    'password': os.environ['POSTGRES_PASSWORD'],
    'dbname': os.environ['POSTGRES_DB'],
}

kafka_consumer = None  # injected by gunicorn post_fork

# Created in lifespan, not at module level, to avoid the "no running loop"
# error that occurs when asyncio.Semaphore / asyncio.Lock are instantiated
# at import time under Gunicorn.
_global_sem: asyncio.Semaphore | None = None
_saga_locks: dict[str, asyncio.Lock] | None = None



def init_db_pool() -> AsyncConnectionPool:
    return AsyncConnectionPool(
        conninfo=(
            f"host={conn_params['host']} port={conn_params['port']} "
            f"user={conn_params['user']} password={conn_params['password']} "
            f"dbname={conn_params['dbname']}"
        ),
        min_size=1,
        max_size=50,
        open=False,
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10},
    )


def get_db_pool() -> AsyncConnectionPool:
    if db_pool is None:
        raise RuntimeError("DB pool is not initialised")
    return db_pool




@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, _global_sem, _saga_locks

    # Initialise the DB pool.
    db_pool = init_db_pool()
    await db_pool.open()

    # Asyncio primitives must be created here, inside the running event loop.
    _global_sem = asyncio.Semaphore(45)
    _saga_locks = defaultdict(asyncio.Lock)

    # Start the Kafka consumer loop as a background task.
    # kafka_consumer is injected by gunicorn post_fork before the worker
    # starts serving, so it is guaranteed to be set by the time lifespan runs.

    yield



    await db_pool.close()


app = FastAPI(title="orchestrator-service", lifespan=lifespan)
service_name = "orchestrator"


@app.get("/health")
async def health_check():
    return JSONResponse(content={"status": "ok"}, status_code=200)