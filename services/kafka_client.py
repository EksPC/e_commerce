import os
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
import time
import msgspec 
from uuid import UUID

class Client:
    def __init__(self, service_name, topics_to_watch):

        self.service_name = service_name
        self.bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        self.producer = self._split_second_retry(
            lambda: KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: msgspec.json.encode(v),
                retries=5,
            )
        )


        # kafka_client.py
        self.consumer = self._split_second_retry(
            lambda: KafkaConsumer(
                *topics_to_watch,
                bootstrap_servers=self.bootstrap_servers,
                group_id=f'{self.service_name}-group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                session_timeout_ms=60000,         # how long before Kafka declares consumer dead
                heartbeat_interval_ms=10000,      # how often to send heartbeats
                max_poll_interval_ms=300000,      # max time between poll() calls
                fetch_max_wait_ms=10,             # reduce from 500ms default — return immediately if message available
                max_poll_records=1,               # fetch 1 message at a time, not batching
                connections_max_idle_ms=600000,   # 10 minutes (must be > request_timeout_ms)
            )
        )


        print("Kafka client started and subscribed to topics: ",topics_to_watch)

    def _split_second_retry(self, func):
        """Prevents crash if Kafka is still booting up in Docker."""
        for i in range(30):
            try:
                return func()
            except NoBrokersAvailable:
                logging.warning(f"Waiting for Kafka... attempt {i+1}")
                time.sleep(2)
            except Exception as exc:
                logging.warning("Kafka not ready yet (attempt %s): %s", i + 1, exc)
                time.sleep(2)
        raise ConnectionError("Could not connect to Kafka after 60 seconds.")


