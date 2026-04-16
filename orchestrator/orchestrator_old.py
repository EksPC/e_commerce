# saga_orchestrator.py
#
# Encapsulates the entire checkout saga coordination logic.
# The orchestrator owns:
#   - Starting the saga (RESERVE_STOCK command to stock service)
#   - Listening on Redis for saga events
#   - Advancing the saga on success (trigger payment after stock allocated)
#   - Handling failures and timeouts
#   - Returning a final HTTP-ready result tuple to the caller
#
# Dependencies injected at construction time to keep this testable
# and decoupled from Flask globals.

import asyncio
from datetime import datetime
from itertools import count
import time
import logging
from typing import Optional, Callable, cast
import uuid
from msgspec import json
import psycopg # type: ignore
from psycopg.rows import dict_row # type: ignore

import json as std_json
import services.utils as utils

from saga_core import SagaStatus, SagaStep, SagaContext, OrchestratorState, Status, Step
from services.db_repository import OrchestratorRepository

logger = logging.getLogger(__name__)


class CheckoutSagaOrchestrator:
    """
    Coordinates the checkout saga across Stock and Payment services.

    Saga flow:
        1. Emit RESERVE_STOCK command → stock.request topic
        2. Wait on Redis for STOCK_ALLOCATED or STOCK_UNAVAILABLE
        3. On STOCK_ALLOCATED → emit START_PAYMENT command → payment.request topic
        4. Wait on Redis for PAYMENT_SUCCEEDED or PAYMENT_FAILED
        5. Return final result to the HTTP caller

    Compensation:
        - STOCK_UNAVAILABLE  → no compensation needed (nothing was committed)
        - PAYMENT_FAILED     → emit FREE_STOCK command → stock.request topic
    """

    
    

    def __init__(
        self,
        db_pool,
        order_id: str = ""  
    ):
        self.db_pool = db_pool
        self.order_id = order_id
    # ------------------------------------------------------------------
    # Public entry point — called directly from the checkout endpoint
    # ------------------------------------------------------------------
    HANDLERS: dict[tuple[utils.IntegrationEvents, SagaStep], str] = {
            (utils.StockIntegrationEvent.STOCK_ALLOCATED, SagaStep.STOCK_RESERVATION):    "_on_stock_allocated",
            (utils.StockIntegrationEvent.STOCK_UNAVAILABLE, SagaStep.STOCK_RESERVATION):  "_on_stock_unavailable",
            (utils.StockIntegrationEvent.STOCK_FAILED, SagaStep.STOCK_RESERVATION):       "_on_stock_failed",
            (utils.StockIntegrationEvent.STOCK_FREED, SagaStep.STOCK_COMPENSATION):       "_on_stock_freed",
            (utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED, SagaStep.PAYMENT): "_on_payment_succeeded",
            (utils.PaymentIntegrationEvent.PAYMENT_FAILED, SagaStep.PAYMENT):   "_on_payment_failed",
        }
    IDEMPOTENT_SKIP: set[tuple[utils.IntegrationEvents, SagaStep]] = {
        # STOCK_ALLOCATED redelivered after saga already advanced to PAYMENT
        (utils.StockIntegrationEvent.STOCK_ALLOCATED,  SagaStep.PAYMENT),
        # STOCK_FAILED redelivered after saga already moved past STOCK_RESERVATION
        (utils.StockIntegrationEvent.STOCK_FAILED,     SagaStep.PAYMENT),
        (utils.StockIntegrationEvent.STOCK_FAILED,     SagaStep.STOCK_COMPENSATION),
        (utils.StockIntegrationEvent.STOCK_FAILED,     SagaStep.FINISHED),
        # PAYMENT_FAILED redelivered after saga already started compensation
        (utils.PaymentIntegrationEvent.PAYMENT_FAILED, SagaStep.STOCK_COMPENSATION),
        # STOCK_FREED redelivered after saga already completed compensation
        (utils.StockIntegrationEvent.STOCK_FREED,      SagaStep.FINISHED),
    }



    def get_handler(self, event_type, step):
        method_name = self.HANDLERS.get((event_type, step))
        if method_name:
            return getattr(self, method_name)
        if (event_type, step) in self.IDEMPOTENT_SKIP:
            return self._idempotent_skip
        return None

    async def _idempotent_skip(self, event, context, cur):
        """Handler for idempotent redelivery—skip without writing."""
        return True




    async def handle_event(self, event: utils.BaseEvent, start: float) -> None:
        logging.info("Handling event %s for saga %s", event.event_type, event.saga_id)

        MAX_RETRIES = 3
        
        for attempt in range(MAX_RETRIES):
            try:
                async with self.db_pool.connection() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        repo = OrchestratorRepository(cur)

                        # If event is initiate checkout

                        if event.event_type == utils.OrderIntegrationEvent.CHECKOUT_INITIATED:
                            context = SagaContext(
                                saga_id=event.saga_id,
                                order_id=event.order_id,
                            )

                            context.set_result({"user_id": event.payload.user_id}, SagaStep.CREATED)  

                            logging.info("HANDLER: Created new saga context for saga_id %s", context.saga_id)    
                            await self._on_checkout_initiated(event, context, cur)
                            logging.info("Execution Time checkout_initiated: %.2f", time.time() - start_exec)
                            return  # implicit commit
                        
                        # Extract saga content

                        await cur.execute(
                            """
                            SELECT id, order_id, status, step, results, version
                            FROM sagas WHERE id = %s
                            """,
                            (event.saga_id,),
                        )

                        saga_row = await cur.fetchone()
                        logging.info("HANDLER: Fetched saga %s", event.saga_id)
                        
                        # If saga doesn't exist

                        if not saga_row:
                            await self._handle_saga_not_existing(event, cur)  # no writes, no conflict, no retry
                            logging.info("Execution time saga_not_existing: %.2f", time.time() - start_exec)
                            return
                            
                        # If saga already in terminal state, handle idempotently without retrying

                        if saga_row["status"] in (SagaStatus.COMPLETED.value, SagaStatus.FAILED.value, SagaStatus.COMPENSATED.value):
                            saga = SagaContext.from_db(saga_row)
                            await self._handle_already_processed(event, saga, cur)
                            return

                        saga_obj = SagaContext.from_db(saga_row)
                        handler = self.get_handler(utils.cast_integration_event(event.event_type), saga_obj.step)
                        if handler is None:
                            logging.error(
                                "No handler for event %s in step %s — dropping",
                                event.event_type, saga_obj.step,
                            )
                            # Mark event as processed even though handler not found
                            await repo.set_received_event_result(
                                event.id, "PROCESSED",
                                {"status": "dropped", "reason": "No handler for event type in step"}
                            )
                            return

                        # 5. Run the handler — all writes go through the same cursor. 
                        success = await handler(event, saga_obj, cur)
                        logging.debug("Handler for event %s in saga_id %s returned success=%s", event.event_type, event.saga_id, success)
                        if event.saga_id.startswith("TEST_SAGA_ID_"):
                            utils.print_test(
                                location=f"ORCHESTRATOR_HANDLER-END-retry{attempt+1}",
                                step=event.event_type,
                                saga_id=event.saga_id
                            )
                        if success:
                            logging.info("Execution Time success: %.2f", time.time() - start_exec)
                            return  

                
                logging.warning(
                    "Saga version conflict for %s (attempt %d/%d)",
                    event.saga_id, attempt + 1, MAX_RETRIES,
                )

            except psycopg.Error:
                logging.exception("DB error in saga transition for %s", event.saga_id)
                return
   

    async def _handle_saga_not_existing(self, event: utils.BaseEvent, cur: psycopg.AsyncCursor) -> None:
        logger.error(
            f"Saga [{event.saga_id}] does not exist for event {event.event_type}. Dropping event."
        )
        # create message and write on outbox
        repo = OrchestratorRepository(cur)
        outbox_msg = utils.build_unknown_saga_event(
            order_id=event.order_id or "unknown",
            saga_id=event.saga_id or "unknown",
        )
        topic = "order.request"
        await repo.insert_outbox_message(topic, outbox_msg)


    
    
    @staticmethod
    async def push_to_dead_letter_queue(
        db_pool,
        event,
        reason: Optional[str] = None
    ):
        # Write on the outbox specifying a dead letter queue
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                # repo = StockRepository(cur)
                error_event = utils.build_dead_letter_event(
                    order_id=event.order_id or "unknown",
                    saga_id=event.saga_id or "unknown",
                    reason=reason or f"unkown reason: {event.order_id}",
                    original_event=event
                )
                topic = "orchestrator.dead_letter"
                await cur.execute(
                    """
                    INSERT INTO outbox (id, topic, payload, created_at)
                    VALUES (%s, %s, %s::jsonb, now())
                    """,
                    (str(uuid.uuid4()), topic, std_json.dumps(json.encode(error_event))),
                )


    async def _flush_writes(
            self, 
            cur, 
            context, 
            event_id: str,
            result: dict, 
            outbox_topic, 
            outbox_msg
            ):

        """Atomically update saga, outbox, and received_events in a single transaction."""
        start_exec = time.time()
        new_version = context.version + 1
        async with cur.connection.transaction():  # ← Use explicit transaction
            await cur.execute(
                """
                WITH updated_saga AS (
                    UPDATE sagas
                    SET step = %s, status = %s, results = %s::jsonb, version = %s
                    WHERE id = %s AND version = %s
                    RETURNING 1
                ),
                updated_event AS (
                    UPDATE received_events
                    SET status = %s, result = %s::jsonb, updated_at = now()
                    WHERE event_id = %s
                    RETURNING 1
                )
                INSERT INTO outbox (id, topic, payload)
                SELECT %s, %s, %s
                WHERE (SELECT count(*) FROM updated_saga) > 0
                  AND (SELECT count(*) FROM updated_event) > 0
                RETURNING 1
                """,
                (
                    context.step.value,
                    context.status.value,
                    std_json.dumps(context.results),
                    new_version,
                    context.saga_id,
                    context.version,
                    'PROCESSED',
                    std_json.dumps(result),
                    event_id,
                    str(uuid.uuid4()),
                    outbox_topic,
                    OrchestratorRepository._to_jsonb(outbox_msg),
                ),
            )
            result = await cur.fetchone()
            logging.info("flush connection + execution: %.2f", time.time() - start_exec)
            return result is not None  # True if inserts succeeded, False if updates conflicted


    async def _on_checkout_initiated(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
        cur,                    # ← receives cursor from _transition_steps
    ) -> bool:
        """Returns False if there was a version conflict, True on success."""
        items_list = [(item_id, qty) for item_id, qty in event.payload.items]
        if not items_list:
            logging.error("INIT: Empty items list in checkout initiated for saga_id %s", context.saga_id)
            await self.push_to_dead_letter_queue(
                db_pool=self.db_pool, event=event,
                reason="Checkout initiated with empty items list",
            )
            return False
       
        
        context.advance()  # CREATED → STOCK_RESERVATION
        context.set_result({"items": items_list})  # store items for later steps
        
        topic = utils.ROUTING_TABLE[event.event_type]
        outbox_msg = utils.build_reserve_stock_command(
            saga_id=context.saga_id, order_id=context.order_id, items=items_list
        )

        # Insert saga, outbox, and mark event as processed atomically
        saga_data = context.to_db()
        
        result = await cur.fetchone()
        
        if result is None:
            logging.warning("Saga %s already exists or event processing failed", context.saga_id)
            return False  # conflict

        logging.info("INIT: Sent reserve stock command for saga_id %s", context.saga_id)
        return True  # success


    async def _on_stock_allocated(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
        cur,
    ) -> bool:
        """Returns False if there was a version conflict, True on success."""

        context.set_result({"amount": event.payload.amount})
        context.advance()  # STOCK_RESERVATION → PAYMENT
        user_id = context.get_user()

        if user_id is None:
            logging.error("STOCK_ALLOCATED: Missing user_id in saga context for saga_id %s", event.saga_id)
            await self.push_to_dead_letter_queue(
                db_pool=self.db_pool, event=event,
                reason="Missing user_id in saga context after stock allocated",
            )
            return False

        
        topic = utils.ROUTING_TABLE[event.event_type]
        outbox_msg = utils.build_start_payment_command(
            saga_id=context.saga_id,
            order_id=context.order_id,
            user_id=user_id,
            amount=event.payload.amount,
        )
        logging.info("STOCK_ALLOCATED: Sending start payment command for saga_id %s", event.saga_id)
        return await self._flush_writes(
            cur=cur, 
            context=context,
            event_id=event.id,
            result={"status": "success", "amount": event.payload.amount},
            outbox_topic=topic,
            outbox_msg=outbox_msg,
        )
       


    async def _on_stock_unavailable(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
        cur: psycopg.AsyncCursor,
    ):
        context.set_result({"status": "failed", "reason": "Stock unavailable"})
        context.fail()  # STOCK_RESERVATION → FAILED

        topic = utils.ROUTING_TABLE[event.event_type]
        outbox_msg = utils.build_saga_ended_event(
            order_id=context.order_id, saga_id=context.saga_id, reason="Stock unavailable", status="failed"
        )
        logging.info("STOCK_UNAVAILABLE: Sending saga ended event for saga_id %s", event.saga_id)
        return await self._flush_writes(
            cur=cur, 
            context=context, 
            event_id=event.id,
            result={"status": "failed", "reason": "Stock unavailable"},
            outbox_topic=topic,
            outbox_msg=outbox_msg,
        )

    async def _on_stock_failed(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
        cur: psycopg.AsyncCursor,
    ):
        reason = event.payload.get("reason", "Stock operation failed") if isinstance(event.payload, dict) else "Stock operation failed"
        context.set_result({"status": "failed", "reason": reason})
        context.fail()  # STOCK_RESERVATION → FAILED

        topic = utils.ROUTING_TABLE.get(event.event_type, "order.request")
        outbox_msg = utils.build_saga_ended_event(
            order_id=context.order_id, saga_id=context.saga_id, reason=reason, status="failed"
        )
        logging.info("STOCK_FAILED: Sending saga ended event for saga_id %s with reason: %s", event.saga_id, reason)
        return await self._flush_writes(
            cur=cur,
            context=context,
            event_id=event.id,
            result={"status": "failed", "reason": reason},
            outbox_topic=topic,
            outbox_msg=outbox_msg,
        )


    async def _on_payment_succeeded(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
        cur: psycopg.AsyncCursor,
    ):
        context.set_result({"status": "success"})
        context.advance()  # PAYMENT → COMPLETED
       
        topic = utils.ROUTING_TABLE[event.event_type]
        outbox_msg = utils.build_saga_ended_event(
            order_id=context.order_id, saga_id=context.saga_id, reason="Payment succeeded", status="success"
        )
        logging.info("PAYMENT_SUCCEEDED: Sending saga ended event for saga_id %s", event.saga_id)
        return await self._flush_writes(
            cur=cur, 
            context=context, 
            event_id=event.id,
            result={"status": "success"},
            outbox_topic=topic,
            outbox_msg=outbox_msg,
        )


    async def _on_payment_failed(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
        cur: psycopg.AsyncCursor,
    ) -> bool:
        context.set_result({"status": "failed", "reason": event.payload.reason})
        context.rollback()  # PAYMENT → STOCK_COMPENSATION
        
        topic = utils.ROUTING_TABLE[event.event_type]
        stock_reservation_result = context.get_result(SagaStep.STOCK_RESERVATION)
        items: list[tuple[str, int]] = []
        if isinstance(stock_reservation_result, dict):
            raw_items = stock_reservation_result.get("items")
            if isinstance(raw_items, list):
                items = cast(list[tuple[str, int]], raw_items)

        outbox_msg = utils.build_free_stock_command(
            saga_id=context.saga_id,
            order_id=context.order_id,
            items=items,
        )
        logging.info("PAYMENT_FAILED: Sending free stock command for saga_id %s", event.saga_id)
        return await self._flush_writes(
            cur=cur, 
            context=context, 
            event_id=event.id,
            result={"status": "failed", "reason": event.payload.reason},
            outbox_topic=topic,
            outbox_msg=outbox_msg,
        )


    async def _on_stock_freed(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
        cur: psycopg.AsyncCursor,
    ) -> bool:
        context.set_result({"status": "compensated", "reason": "Stock freed after payment failure"})
        context.advance()  # STOCK_COMPENSATION → COMPENSATED
        
        topic = utils.ROUTING_TABLE[event.event_type]
        outbox_msg = utils.build_saga_ended_event(
            order_id=context.order_id, saga_id=context.saga_id, reason="Payment failed and stock freed", status="compensated"
        )

        logging.info("STOCK_FREED: Sending saga ended event for saga_id %s", event.saga_id)
        return await self._flush_writes(
            cur=cur, 
            context=context, 
            event_id=event.id,
            result={"status": "compensated", "reason": "Stock freed after payment failure"},
            outbox_topic=topic,
            outbox_msg=outbox_msg,
        )