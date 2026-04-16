from datetime import datetime
import logging
import os
import signal
import threading
import time
from typing import Any

import psycopg  # type: ignore
from msgspec import json
from psycopg.rows import dict_row  # type: ignore
from psycopg_pool import ConnectionPool  # type: ignore

import services.kafka_client as kafka_client
import services.utils as utils

logger = logging.getLogger(__name__)


_READY_BASE_DELAY = 1.0
_READY_MAX_DELAY  = 16.0
_READY_MAX_TRIES  = 20


def wait_until_ready(db_pool: ConnectionPool) -> None:
    """
    Block until the outbox table is reachable and exists.
    Retries with exponential back-off up to _READY_MAX_TRIES times.

    Distinguishes three cases:
      - DB unreachable (psycopg.OperationalError)    → retry
      - Table missing  (psycopg.errors.UndefinedTable) → retry
      - Any other error → re-raise immediately (bad creds, wrong DB name…)
    """
    delay = _READY_BASE_DELAY
    for attempt in range(1, _READY_MAX_TRIES + 1):
        try:
            with db_pool.connection() as conn:
                conn.execute("SELECT 1 FROM outbox LIMIT 1").fetchone()
            logger.info("Database ready (attempt %d)", attempt)
            return
        except psycopg.errors.UndefinedTable:
            logger.warning(
                "Outbox table not found yet (attempt %d/%d) — waiting %.1fs for migrations…",
                attempt, _READY_MAX_TRIES, delay,
            )
        except psycopg.OperationalError:
            logger.warning(
                "Database unreachable (attempt %d/%d) — retrying in %.1fs…",
                attempt, _READY_MAX_TRIES, delay,
            )
        except psycopg.Error as exc:
            raise RuntimeError(
                f"Unexpected database error during readiness check: {exc}"
            ) from exc

        time.sleep(delay)
        delay = min(delay * 2, _READY_MAX_DELAY)

    raise RuntimeError(
        f"Outbox table not ready after {_READY_MAX_TRIES} attempts. Giving up."
    )


class OutboxRelay:
    """Background worker that relays unsent outbox rows to Kafka."""

    def __init__(
        self,
        db_pool: ConnectionPool,
        kafka_producer,
        poll_interval: float = 0.1,    # consistent default across __init__ and from_env
        fetch_batch_size: int = 20,
    ):
        self.db_pool = db_pool
        self.kafka_producer = kafka_producer
        self.poll_interval = poll_interval
        self.fetch_batch_size = fetch_batch_size
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @classmethod
    def from_env(
        cls,
        service_name: str = "stock-outbox-relay",
        poll_interval: float = 0.1,    # was 0.5 — 100 DB queries/s on empty outbox
        fetch_batch_size: int = 20,
    ) -> tuple["OutboxRelay", ConnectionPool, Any]:
        """Build a standalone relay from environment variables."""
        conninfo = (
            f"host={os.environ['POSTGRES_HOST']} "
            f"port={os.environ['POSTGRES_PORT']} "
            f"user={os.environ['POSTGRES_USER']} "
            f"password={os.environ['POSTGRES_PASSWORD']} "
            f"dbname={os.environ['POSTGRES_DB']}"
        )
        db_pool = ConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=5,
            reconnect_timeout=30,
            kwargs={"connect_timeout": 10},
        )
        kafka = kafka_client.Client(service_name, [])
        relay = cls(
            db_pool=db_pool,
            kafka_producer=kafka.producer,
            poll_interval=poll_interval,
            fetch_batch_size=fetch_batch_size,
        )
        return relay, db_pool, kafka

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="outbox-relay"
        )
        self._thread.start()
        logger.info("Outbox relay started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        logger.info("Outbox relay stopped")

    def run_forever(self) -> None:
        """Run relay loop in the foreground, suitable for a dedicated container."""
        logger.info("Outbox relay running in foreground")
        self._run()

    def _run(self) -> None:
        logger.info(
            "Outbox relay loop started — poll_interval=%.2fs batch_size=%d",
            self.poll_interval, self.fetch_batch_size,
        )
        while not self._stop_event.is_set():
            sent = self.relay_oldest_unsent()
            if not sent:
               
                self._stop_event.wait(timeout=self.poll_interval)

    def relay_oldest_unsent(self) -> bool:
        """
        Read a batch of unsent outbox rows, publish each to Kafka, and mark
        successful ones as sent. Returns True if at least one row was relayed.

        FOR UPDATE SKIP LOCKED ensures multiple relay instances never process
        the same row concurrently.
        """
        if self.db_pool is None or self.kafka_producer is None:
            return False

        try:
            with self.db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        SELECT id, topic, payload
                        FROM outbox
                        WHERE sent = FALSE
                        ORDER BY created_at ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (self.fetch_batch_size,),
                    )
                    rows = cur.fetchall()

                    if not rows:
                        return False

                    logger.debug("Found %d unsent outbox row(s)", len(rows))

                    relayed_ids: list[str] = []
                    for row in rows:
                        try:
                            event = _payload_to_base_event(row["payload"])
                            future = self.kafka_producer.send(
                                topic=row["topic"], 
                                value=event,    
                                key=event.saga_id.encode()
                            )
                            # Block for broker ack before marking as sent —
                            # prevents marking a row sent if Kafka never received it.
                            future.get(timeout=10)
                            relayed_ids.append(row["id"])
                            # logger.info(
                            #     "Relayed outbox row %s → topic %s (key=%s)",
                            #     event.event_type, row["topic"], event.saga_id,
                            # )
                            logging.info("TEST: %s - order %s - saga %s - TIME: %s",event.event_type, event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))

                            if event.saga_id.startswith("TEST_SAGA_ID_"):
                                utils.print_test(
                                    location="STOCK_PRODUCER",
                                    step=event.event_type,
                                    saga_id=event.saga_id
                                )
                        except Exception:
                            # Leave the row unsent — next tick retries it.
                            logger.exception(
                                "Failed to relay outbox row %s → topic %s",
                                row.get("id"),  # ← was row.get("payload")
                                row.get("topic"),
                            )

                    # Single bulk UPDATE instead of one UPDATE per row.
                    if relayed_ids:
                        cur.execute(
                            "UPDATE outbox SET sent = TRUE WHERE id = ANY(%s)",
                            (relayed_ids,),
                        )

            return bool(relayed_ids)

        except Exception:
            logger.exception("Outbox relay loop error")
            return False



def _payload_to_base_event(payload: Any) -> utils.BaseEvent:
    """
    Normalise an outbox payload to BaseEvent.

    psycopg with dict_row deserialises JSONB columns as dicts, so the dict
    branch is the common path. Re-encoding through msgspec ensures nested
    struct types are validated correctly rather than constructed field-by-field.
    """
    if isinstance(payload, dict):
        return json.decode(json.encode(payload), type=utils.BaseEvent[dict])
    if isinstance(payload, str):
        return json.decode(payload.encode(), type=utils.BaseEvent[dict])
    if isinstance(payload, bytes):
        return json.decode(payload, type=utils.BaseEvent[dict])
    raise ValueError(f"Unsupported outbox payload type: {type(payload)}")


def _parse_poll_interval(raw: str) -> float:
    value = raw.strip().lower().removesuffix("s").strip()
    interval = float(value)
    if interval <= 0:
        raise ValueError("OUTBOX_POLL_INTERVAL must be > 0")
    return interval


def _parse_fetch_batch_size(raw: str) -> int:
    batch_size = int(raw)
    if batch_size <= 0:
        raise ValueError("OUTBOX_FETCH_BATCH_SIZE must be > 0")
    return batch_size



def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    # kafka-python is very chatty — keep it at WARNING unless explicitly overridden.
    logging.getLogger("kafka").setLevel(
        getattr(logging, os.getenv("KAFKA_LOG_LEVEL", "WARNING"))
    )

    relay, db_pool, kafka = OutboxRelay.from_env(
        poll_interval=_parse_poll_interval(
            os.getenv("OUTBOX_POLL_INTERVAL", "0.5s")
        ),
        fetch_batch_size=_parse_fetch_batch_size(
            os.getenv("OUTBOX_FETCH_BATCH_SIZE", "20")
        ),
    )

    def _shutdown(_signum, _frame):
        relay.stop()
        db_pool.close()
        try:
            kafka.producer.flush(timeout=5)
            kafka.producer.close()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        wait_until_ready(db_pool)
        relay.run_forever()
    finally:
        _shutdown(None, None)


if __name__ == "__main__":
    main()