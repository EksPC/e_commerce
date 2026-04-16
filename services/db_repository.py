import uuid
import json as std_json
from typing import Any
from msgspec import json as msgspec_json
import psycopg
from psycopg.types.json import Jsonb

from services import utils


class Repository:
    def __init__(self, cursor):
        self.cur = cursor

    @staticmethod
    def _to_jsonb( payload: Any) -> str:
        if isinstance(payload, dict):
            return std_json.dumps(payload)
        # Supports msgspec Structs like utils.BaseEvent
        return msgspec_json.encode(payload).decode("utf-8")

    @staticmethod

    async def write_outbox_and_update_event(
            event: utils.BaseEvent,
            result: dict,
            outbox_msg: utils.BaseEvent,
            cur: psycopg.AsyncCursor
            ):
        
        
        topic = 'orchestrator.request'
        
        await cur.execute(
            """
            WITH updated AS (
                UPDATE received_events
                SET status = %s, result = %s, updated_at = now()
                WHERE event_id = %s
                RETURNING 1
            )
            INSERT INTO outbox (id, topic, payload)
            SELECT %s, %s, %s
            WHERE EXISTS (SELECT 1 FROM updated)
            ON CONFLICT DO NOTHING;
            """,
            ('PROCESSED', Jsonb(result), event.id,
            str(uuid.uuid4()), topic, Jsonb(outbox_msg.to_dict())),
        )

        return utils.Failure(result.get('reason', 'Stock unavailable'))


    async def get_outbox_message(self, message_id: str) -> dict[str, Any] | None:
        await self.cur.execute("SELECT id, topic, payload FROM outbox WHERE id = %s", (message_id,))
        row = await self.cur.fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "topic": row["topic"],
            "payload": row["payload"],
        }
   
    async def insert_received_event(self, event_id: str) -> None:
        await self.cur.execute(
            "INSERT INTO received_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (event_id,),
        )

    async def get_outbox_events(self) -> list[dict[str, Any]]:
        await self.cur.execute("SELECT id, topic, payload FROM outbox ORDER BY created_at")
        return await self.cur.fetchall()

    async def list_outbox_messages(self) -> list[dict[str, Any]]:
        await self.cur.execute("SELECT id, topic, payload FROM outbox WHERE sent = FALSE ORDER BY created_at")
        return await self.cur.fetchall()

    async def get_received_event_result(self, event_id: str) -> dict[str, Any] | None:
        """
        Get the result of a received event if it has already been processed, to support idempotency and compensations.
        """
        await self.cur.execute(
            """
            SELECT result
            FROM received_events 
            WHERE event_id = %s 
            AND status = 'PROCESSED'
            """, (event_id,))
        row = await self.cur.fetchone()
        return row["result"] if row else None

    async def set_received_event_result(self, event_id: str, status: str, result: dict[str, Any]) -> None:
        await self.cur.execute(
            "UPDATE received_events SET status = %s, result = %s, updated_at = now() WHERE event_id = %s",
            (status, std_json.dumps(result), event_id),
        )

    async def insert_outbox_message(self, topic: str, payload: utils.BaseEvent) -> None:
        await self.cur.execute(
            "INSERT INTO outbox (id, topic, payload) VALUES (%s, %s, %s)"
            "ON CONFLICT DO NOTHING",
            (str(uuid.uuid4()), topic, self._to_jsonb(payload)),
        )


class StockRepository(Repository):
    def __init__(self, cursor):
        super().__init__(cursor)


    async def get_item_snapshot(self, item_id: str) -> dict[str, Any] | None:
        await self.cur.execute(
            "SELECT item_id, stock, price, version FROM item_snapshots WHERE item_id = %s",
            (item_id,),
        )
        return await self.cur.fetchone()

    async def get_item_snapshots(self) -> list[dict[str, Any]]:
        await self.cur.execute("SELECT item_id, stock, price FROM item_snapshots")
        return await self.cur.fetchall()
    
    # async def insert_log_event(self, item_id: str, event_type: str, payload: dict[str, Any], version: int) -> None:
    #     await self.cur.execute(
    #         "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
    #         (str(uuid.uuid4()), item_id, event_type, self._to_jsonb(payload), version),
    #     )


    async def list_item_snapshots(self) -> list[dict[str, Any]]:
        await self.cur.execute("SELECT item_id, stock, price FROM item_snapshots")
        return await self.cur.fetchall()


    async def get_item_snapshots_for_ids(self, item_ids: list[str], include_price: bool = False) -> dict[str, dict[str, Any]]:
        if not item_ids:
            return {}
        placeholders = ",".join(["%s"] * len(item_ids))
        cols = "item_id, stock, version"
        if include_price:
            cols = "item_id, stock, price, version"
        await self.cur.execute(
            f"SELECT {cols} FROM item_snapshots WHERE item_id IN ({placeholders})",
            item_ids,
        )
        return {row["item_id"]: row for row in await self.cur.fetchall()}

    # async def get_log_events_for_item(self, item_id: str) -> list[dict[str, Any]]:
    #     await self.cur.execute(
    #         "SELECT event_type, payload, item_id FROM log WHERE item_id = %s ORDER BY version",
    #         (item_id,),
    #     )
    #     return await self.cur.fetchall()


    async def update_snapshot_versioned(self, item_id: str, new_stock: int, new_version: int, current_version: int) -> bool:
        print(f"Attempting to update snapshot for item {item_id} from version {current_version} to {new_version} with stock {new_stock}")
        await self.cur.execute(
            """UPDATE item_snapshots
               SET stock = %s, version = %s
               WHERE item_id = %s AND version = %s""",
            (new_stock, new_version, item_id, current_version),
        )
        return self.cur.rowcount > 0


    
    async def insert_item_snapshot(self, stock: int, price: int, version: int) -> str:
        item_id = str(uuid.uuid4())
        await self.cur.execute(
            """
            INSERT INTO item_snapshots (item_id, stock, price, version)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (item_id) DO NOTHING
            RETURNING item_id
            """,
            (item_id, stock, price, version),
        )
        row = await self.cur.fetchone()
        # If insert was skipped due to conflict, still return the item_id
        return row[0] if row else item_id



class PaymentRepository(Repository):

    def __init__(self, cursor):
        self.cur = cursor
   

    async def get_user_snapshot(self, user_id: str) -> dict[str, Any] | None:
        await self.cur.execute(
            "SELECT user_id, credit, version FROM user_snapshots WHERE user_id = %s",
            (user_id,),
        )
        return await self.cur.fetchone()

    async def list_user_snapshots(self) -> list[dict[str, Any]]:
        await self.cur.execute("SELECT user_id, credit FROM user_snapshots")
        return await self.cur.fetchall()

    # async def get_events_for_user(self, user_id: str) -> list[dict[str, Any]]:
    #     await self.cur.execute(
    #         "SELECT event_type, payload FROM log WHERE user_id = %s ORDER BY id",
    #         (user_id,),
    #     )
    #     return await self.cur.fetchall()

    # async def insert_user_event(self, event_type: str, payload: dict[str, Any], version: int) -> None:
    #     # await self.cur.execute(
    #     #     "INSERT INTO log (id, event_type, payload, version) VALUES (%s, %s, %s, %s)",
    #     #     (str(uuid.uuid4()), event_type, self._to_jsonb(payload), version),
    #     # )
    #     pass

    async def insert_user_snapshot(self, user_id: str, credit: int, version: int) -> None:
        await self.cur.execute(
            "INSERT INTO user_snapshots (user_id, credit, version) VALUES (%s, %s, %s)",
            (user_id, credit, version),
        )

    async def upsert_user_snapshot(self, user_id: str, credit: int, version: int) -> None:
        await self.cur.execute(
            """
            INSERT INTO user_snapshots (user_id, credit, version)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET credit = EXCLUDED.credit, version = EXCLUDED.version
            """,
            (user_id, credit, version),
        )

    async def update_user_snapshot_versioned(self, user_id: str, credit: int, new_version: int, current_version: int) -> bool:
        await self.cur.execute(
            """UPDATE user_snapshots
               SET credit = %s, version = %s
               WHERE user_id = %s AND version = %s""",
            (credit, new_version, user_id, current_version),
        )
        return self.cur.rowcount > 0

    # async def get_log_events(self) -> list[dict[str, Any]]:
    #     await self.cur.execute(
    #         "SELECT event_type, payload, item_id FROM log ORDER BY version"
    #     )
    #     return await self.cur.fetchall()


class OrchestratorRepository(Repository):
    
    def __init__(self, cursor):
        self.cur = cursor
    
    async def get_saga(self, saga_id: str) -> dict[str, Any] | None:
        await self.cur.execute(
            "SELECT id, order_id, step, status, results FROM sagas WHERE id = %s",
            (saga_id,),
        )
        row = await self.cur.fetchone()
        if row is None:
            return None
        return {
            "saga_id": row["id"],
            "order_id": row["order_id"],
            "step": row["step"],
            "status": row["status"],
            "results": row["results"] or {},
            "version": row["version"],
        }
    
    async def list_sagas(self) -> list[dict[str, Any]]:
        await self.cur.execute(
            "SELECT id, order_id, step, status, results, version FROM sagas"
        )
        return await self.cur.fetchall()

    async def create_saga_if_not_exist(self, saga: dict[str, Any]) -> None:
        await self.cur.execute(
            """
            INSERT INTO sagas (id, order_id, step, status, results, version)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                saga["saga_id"],
                saga["order_id"],
                saga["step"],
                saga["status"],
                std_json.dumps(saga["results"]),
                saga["version"]
            ),
        )
        return self.cur.rowcount > 0

    async def cleanup_expired_sagas(self, timeout_minutes: int = 30) -> int:
        """
        Batch cleanup: Identify and mark expired sagas as FAILED in a single transaction.
        Returns the number of sagas marked as failed.
        """
        await self.cur.execute(
            """
            UPDATE sagas
            SET status = %s, updated_at = now()
            WHERE status NOT IN (%s, %s, %s)
              AND (now() - updated_at) > interval '%s minutes'
            """,
            ('FAILED', 'COMPLETED', 'FAILED', 'COMPENSATED', timeout_minutes),
        )
        return self.cur.rowcount

    
    # async def insert_log_event(self, saga_id: str, event_type: str, payload: dict[str, Any], version: int) -> None:
    #     await self.cur.execute(
    #         "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)"
    #         "ON CONFLICT DO NOTHING",
    #         (str(uuid.uuid4()), saga_id, event_type, self._to_jsonb(payload), version),
    #     )

