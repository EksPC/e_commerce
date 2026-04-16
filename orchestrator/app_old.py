from saga_core import SagaContext, SagaStatus, SagaStep
import services.utils as utils
import psycopg
from psycopg_pool import AsyncConnectionPool
import os
from typing import Optional
import asyncio


async def _transition_steps(event: utils.BaseEvent):
    # 1. Extract saga context from db using event.saga_id
    # 2. Compute new saga state based on event and current state
    # 3. Log new Saga state into table sagas
    # 4. Get the outbox messages to be emitted based on the new state, and write them to the outbox table
    # 5. Log the saga_advance internal event for event sourcing
    
    

async def _transition_steps(self, event: utils.BaseEvent) -> None:
    MAX_RETRIES = 3

    for attempt in range(MAX_RETRIES):
        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:

                    # 1. Load saga — include version for optimistic locking.
                    await cur.execute(
                        """
                        SELECT id, order_id, status, step, results, version
                        FROM sagas WHERE id = %s
                        """,
                        (event.saga_id,),
                    )
                    saga_row = await cur.fetchone()

                    # 2. Handle missing saga.
                    if not saga_row:
                        if event.event_type != utils.OrderIntegrationEvent.CHECKOUT_INITIATED:
                            await self._handle_saga_not_existing(event, cur)
                            return
                        context = SagaContext(
                            order_id=event.order_id,
                            saga_id=event.saga_id,
                            items=list(event.payload.items),
                        )
                        # New saga — version starts at 0, first write sets it to 1.
                        await self._on_checkout_initiated(event, context, cur, current_version=0)
                        return  # implicit commit

                    # 3. Skip terminal sagas.
                    if saga_row["status"] in ("completed", "failed", "compensated"):
                        await self._handle_already_processed(event, saga_row, cur)
                        return

                    # 4. Dispatch to the right handler, passing the cursor.
                    saga_obj = SagaContext.from_db(saga_row)
                    current_version = saga_row["version"]

                    handler = self.get_handler(
                        utils.IntegrationEvents(event.event_type), saga_obj.step
                    )
                    if handler is None:
                        logging.error(
                            "No handler for event %s in step %s — dropping",
                            event.event_type, saga_obj.step,
                        )
                        return

                    # 5. Run the handler — all writes go through the same cursor.
                    conflict = await handler(event, saga_obj, cur, current_version)

                    if not conflict:
                        return  # implicit commit on clean exit

            # Version conflict — retry with a fresh read.
            logging.warning(
                "Saga version conflict for %s (attempt %d/%d)",
                event.saga_id, attempt + 1, MAX_RETRIES,
            )

        except psycopg.Error:
            logging.exception("DB error in saga transition for %s", event.saga_id)
            return


async def _transition(
        # TODO Check out saga_step = CHECKOUT_COMPLETED
        self,
        context: SagaContext,
        new_status: SagaStatus,
        new_step: SagaStep | None,
        incoming_event: Optional[str] = None,
        outgoing_command: Optional[str] = None,
        incoming_payload: dict = {},
        outgoing_payload: dict = {},
        outbox_topic: Optional[str] = None,
        outbox_message: Optional[utils.BaseEvent] = None,
    ):
        """
        Atomically, in one transaction:
          1. Append incoming_event to log — what triggered this (past-tense fact)
          2. Append outgoing_command to log — what the saga decided (command)
          3. Upsert the sagas snapshot
          4. Write the outbox row (if a Kafka command needs to go out)

        The outbox relay (separate process) reads undelivered rows and
        sends them to Kafka, decoupling DB writes from Kafka availability.
        """
        context.status = new_status
        context.step = new_step if new_step else context.step  # only update if new_step is provided
        
        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # 1. Log the incoming trigger
                    if incoming_event:
                        await cur.execute(
                            """
                            INSERT INTO log (id, order_id, event_type, data, created_at)
                            VALUES (%s, %s, %s, %s, now())
                            """,
                            (
                                str(uuid.uuid4()),
                                context.order_id,
                                incoming_event,
                                std_json.dumps(incoming_payload) if incoming_payload else "{}",
                            ),
                        )
                    # 2. Log the saga decision / outgoing command
                    if outgoing_command:
                        await cur.execute(
                            """
                            INSERT INTO log (id, order_id, event_type, data, created_at)
                            VALUES (%s, %s, %s, %s, now())
                            """,
                            (
                                str(uuid.uuid4()),
                                context.order_id,
                                outgoing_command,
                                std_json.dumps(outgoing_payload) if outgoing_payload else "{}",
                            ),
                        )
                    # 3. Upsert saga snapshot
                    await cur.execute(
                        """
                        INSERT INTO sagas (id, order_id, status, step, results)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                            SET status = EXCLUDED.status,
                                step = EXCLUDED.step,
                                results = EXCLUDED.results
                        """,
                        (
                            context.saga_id,
                            context.order_id,
                            context.status.value,
                            context.step.value if context.step else None,
                            std_json.dumps({step.value: value for step, value in context.results.items()}),
                        ),
                    )
                    # 4. Outbox — written atomically so the command is never lost
                    if outbox_topic and outbox_message:
                        payload_str = json.encode(outbox_message).decode()
                        await cur.execute(
                            """
                            INSERT INTO outbox (id, topic, payload, created_at)
                            VALUES (%s, %s, %s::jsonb, now())
                            """,
                            (str(uuid.uuid4()), outbox_topic, payload_str),
                        )
                
        except psycopg.Error as e:
            logger.error(
                f"Transition [{incoming_event} -> {outgoing_command}] failed for saga [{context.saga_id}]: {e}"
            )