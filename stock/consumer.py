import asyncio
from datetime import datetime
import logging
import psycopg
import services.kafka_client as kafka_client
import services.utils as utils
import app as stock_app

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    logging.info("Starting stock consumer...")

    stock_app.db_pool = stock_app.init_db_pool()
    await stock_app.db_pool.open()

    client = kafka_client.Client(stock_app.service_name, [f"{stock_app.service_name}.request"])
    consumer = client.consumer
    try:
        while True:
            # kafka-python consumer blocks; pull one message in a worker thread.
            message = await asyncio.to_thread(next, consumer)
            result = utils.decode_and_type_event(message)
            if isinstance(result, utils.Failure):
                logging.error("Kafka message dropped: %s", result.error)
                continue

            event = result.value
            logging.info("TEST: %s - order %s - saga %s - TIME: %s", event.event_type, event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))
            async with stock_app.db_pool.connection() as conn:
                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    repo = stock_app.StockRepository(cur)
                    await repo.insert_received_event(event.id)
            if event.saga_id.startswith("TEST_SAGA_ID_"):
                utils.print_test(
                    location="STOCK_CONSUMER",
                    step=event.event_type,
                    saga_id=event.saga_id
                )
            await stock_app.dispatch_event(event)
              
    finally:
        await stock_app.db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
