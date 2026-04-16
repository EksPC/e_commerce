import asyncio
from datetime import datetime
import time
import logging
from collections import defaultdict
import os

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

import services.kafka_client as kafka_client
import services.utils as utils
from orchestrator import OrchestratorService
from services.db_repository import OrchestratorRepository
from app import init_db_pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator-consumer")


async def main() -> None:

    logger.info("Starting orchestrator consumer...")

    db_pool = init_db_pool()
    await db_pool.open()


    client = kafka_client.Client("orchestrator", ["orchestrator.request"])
    consumer = client.consumer
    orchestrator = OrchestratorService(db_pool)

    try:
        while True:
            message = await asyncio.to_thread(next, consumer)
            result = utils.decode_and_type_event(message)

            if isinstance(result, utils.Failure):
                logger.error("Kafka message dropped: %s", result.error)
                continue
            event = result.value
            logger.info("TEST: %s - order %s - saga %s - TIME: %s", event.event_type, event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))
            await orchestrator.handle_simple_event(event)  # Call the revised method name
    finally:
        await db_pool.close()
        logger.info("Orchestrator consumer shut down")


if __name__ == "__main__":
    asyncio.run(main())