import asyncio
from datetime import datetime
import logging
from venv import logger
from psycopg import rows
import services.kafka_client as kafka_client
import services.utils as utils
import app as payment_app

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    logging.info("Starting payment consumer...")

    payment_app.db_pool = payment_app.init_db_pool()
    await payment_app.db_pool.open()

    client = kafka_client.Client(payment_app.service_name, [f"{payment_app.service_name}.request"])
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
            logging.info("TEST: %s - order %s - saga %s - TIME: %s",  event.event_type, event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))

            if event.saga_id.startswith("TEST_SAGA_ID_"):
                utils.print_test(
                    location="PAYMENT_CONSUMER",
                    step=event.event_type,
                    saga_id=event.saga_id
                )
            await payment_app.dispatch_event(event)
    finally:
        await payment_app.db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
