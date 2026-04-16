from datetime import datetime
import os
import asyncio
import logging

import redis.asyncio as redis
from msgspec import json
import services.utils as utils
import services.kafka_client as kafka_client

logging.basicConfig(level=logging.INFO)

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))


async def main():
    logging.info("Starting Order Service consumer...")
    service_name = "order"

    client = kafka_client.Client(service_name, [f"{service_name}.request"])
    consumer = client.consumer

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        while True:
            # KafkaConsumer is blocking; run next() in a thread
            message = await asyncio.to_thread(next, consumer)

            result = utils.decode_and_type_event(message)

            if isinstance(result, utils.Failure):
                logging.error("Kafka message dropped: %s", result.error)
                continue

            event = result.value
            logging.info("TEST received - order %s - saga %s - TIME: %s", event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))

            channel = f"saga:{event.saga_id}"  # must match checkout subscriber
            await redis_client.publish(channel, json.encode(event).decode())
            if event.saga_id.startswith("TEST_SAGA_ID_"):
                utils.print_test(
                    location="ORDER_CONSUMER",
                    step=event.event_type,
                    saga_id=event.saga_id
                )
            logging.info("Forwarded %s to Redis channel %s", event.event_type, channel)
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())