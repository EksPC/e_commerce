from asyncio.log import logger
from datetime import datetime
import logging
import services.utils as utils
from saga_core import OrchestratorState, SagaStatus, ServiceStatus 
import psycopg # type: ignore
import json as std_json
from psycopg.rows import dict_row
from msgspec import json
import uuid


class OrchestratorService:
    def __init__(self, db_pool):
        self.db_pool = db_pool


    async def handle_simple_event(self, event: utils.BaseEvent) -> None:

        if event.event_type == utils.OrderIntegrationEvent.CHECKOUT_INITIATED:
            await self._create_saga(event=event)
            return  # ← early return, must not fall through
        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    
                    
                    await cursor.execute(
                        """
                        INSERT INTO received_events (event_id)
                        VALUES (%s)
                        ON CONFLICT (event_id) DO NOTHING
                        RETURNING status                        
                        """,
                        (event.id,),
                    )

                    received = await cursor.fetchone()
                    if not received:
                        logger.warning("Event %s already received — skipping", event.id)
                        return
                    
                

                    await cursor.execute(
                        """
                        SELECT id, order_id, status, stock, payment, results, version
                        FROM sagas WHERE id = %s
                        """,
                        (event.saga_id,),
                    )

                    saga_row = await cursor.fetchone()
                    if not saga_row:
                        await self._handle_saga_not_existing(event, cursor)
                        return

                    saga = OrchestratorState.from_db(saga_row)

                    if saga.is_terminal:
                        logger.info(
                            "Saga %s already terminal (%s) — skipping %s",
                            saga.id, saga.status, event.event_type,
                        )
                        return

                    et = event.event_type

                    # logger.info("TEST: dispatch %s, order id %s, saga id %s, time: %s", et, saga.order_id, saga.id, datetime.now().strftime("%H:%M:%S.%f"))
                    
                    if et == utils.StockIntegrationEvent.STOCK_ALLOCATED:
                        saga.stock = ServiceStatus.COMPLETED
                        saga.payment = ServiceStatus.RUNNING  # In case we are retrying after a compensation, reset payment to RUNNING to allow completion
                        await self._request_payment(event, saga, cursor)  # async but don't await — we want to persist stock completion before sending payment request


                    elif et == utils.StockIntegrationEvent.STOCK_UNAVAILABLE:
                        saga.stock = ServiceStatus.COMPENSATED
                        saga.results['failure_reason'] = "stock unavailable"
                        await self._fail_saga(saga, event, cursor, reason=saga.results.get("failure_reason", "stock unavailable"))


                    elif et == utils.StockIntegrationEvent.STOCK_FREED:
                        saga.stock = ServiceStatus.COMPENSATED
                        await self._fail_saga(saga, event, cursor, reason=saga.results.get("failure_reason", "generic error"))

                    elif et == utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED:
                        saga.payment = ServiceStatus.COMPLETED
                        await self._complete_saga(saga, event, cursor)

                    elif et == utils.PaymentIntegrationEvent.PAYMENT_FAILED:
                        saga.results['failure_reason'] = event.payload.reason
                        saga.payment = ServiceStatus.COMPENSATED
                        saga.stock = ServiceStatus.COMPENSATING
                        await self._rollback_stock(saga, event, cursor)


        except Exception as e:
            logger.exception(
                "Error handling event %s for saga %s: %s",
                event.event_type, event.saga_id, str(e)
            )
            # Log the version conflict or other DB errors
            # if "version" in str(e).lower() or "rowcount" in str(e).lower():
            #     logger.warning(
            #         "Optimistic locking conflict for saga %s, event %s will be retried",
            #         event.saga_id, event.id
            #     )
            # else:
            #     logger.exception(
            #         "Error handling event %s for saga %s",
            #         event.event_type, event.saga_id
            #     )
            raise  # Re-raise to let Kafka consumer retry


    async def _flush_state_only(self, state, event, cursor):
        """Persist participant state change with no outbox message."""
        await self._flush(
            cursor,
            event_id=event.id,
            saga_id=event.saga_id,
            order_id=event.order_id,
            saga_update=state,
            outbox_messages=[],
    )

    
    async def _handle_saga_not_existing(
        self,
        event: utils.BaseEvent,
        cursor: psycopg.AsyncCursor,
    ) -> None:

        async with cursor.connection.transaction():
            # Outbox to order.request an error message
            error_msg = utils.build_end_checkout_command(
                saga_id=event.saga_id,
                order_id=event.order_id,
                reason=f"Saga with id {event.saga_id} not found for order {event.order_id}",
                status="error",
            )

            if hasattr(error_msg.payload, 'to_dict'):
                payload_dict = error_msg.payload.to_dict()
            elif hasattr(error_msg.payload, 'dict'):
                payload_dict = error_msg.payload.dict()
            elif isinstance(error_msg.payload, dict):
                payload_dict = error_msg.payload
            else:
                payload_dict = vars(error_msg.payload)

            await cursor.execute(
                """
                INSERT INTO outbox (id, topic, payload)
                VALUES (%s, %s, %s::jsonb)
                """,
                (str(uuid.uuid4()), "order.request", std_json.dumps(payload_dict)),
            )

            # Mark the received event as PROCESSED to avoid infinite retries
            await cursor.execute(
                """
                UPDATE received_events
                SET status = 'PROCESSED', result = %s::jsonb
                WHERE event_id = %s
                """,
                (std_json.dumps({"status": "ERROR", "reason": "saga not found"}), event.id),
            )
        


    async def _flush(
        self,
        cursor: psycopg.AsyncCursor,
        *,
        event_id: str,
        saga_id: str,
        order_id: str,
        saga_update: OrchestratorState ,           # keyword args for the sagas UPDATE
        outbox_messages: list[tuple[str, str, utils.BaseEvent]],  # (event_type, topic, payload)
        event_result: dict | None = None,
    ) -> None:
        """
        Atomically:
        - update saga row
        - insert N outbox messages
        - mark received_events as PROCESSED
        """
        try:
            async with cursor.connection.transaction():


                await cursor.execute(
                    """
                    UPDATE sagas
                    SET status = %s, stock = %s, payment = %s, results = %s::jsonb, version = version + 1
                    WHERE id = %s AND version = %s
                    """,
                    (
                        saga_update.status,
                        saga_update.stock,
                        saga_update.payment,
                        std_json.dumps(saga_update.results),
                        saga_update.id,
                        saga_update.version,
                    ),
                )
                if cursor.rowcount == 0:
                    raise ValueError(
                        f"Optimistic locking conflict: saga {saga_id} version {saga_update.version} "
                        f"was modified by another transaction"
                    )

                # Combine all outbox inserts into a single multi-row INSERT
                if outbox_messages:
                    outbox_values = []
                    outbox_params = []
                    for event_type, topic, payload in outbox_messages:
                        if hasattr(payload, 'to_dict'):
                            payload_dict = payload.to_dict()
                        elif hasattr(payload, 'dict'):
                            payload_dict = payload.dict()
                        elif isinstance(payload, dict):
                            payload_dict = payload
                        else:
                            payload_dict = vars(payload)
                        
                        outbox_values.append("(%s, %s, %s::jsonb)")
                        outbox_params.extend([str(uuid.uuid4()), topic, std_json.dumps(payload_dict)])

                    await cursor.execute(
                        f"""
                        INSERT INTO outbox (id, topic, payload)
                        VALUES {', '.join(outbox_values)}
                        """,
                        outbox_params,
                    )

                await cursor.execute(
                    """
                    UPDATE received_events
                    SET status = 'PROCESSED', result = %s::jsonb
                    WHERE event_id = %s
                    """,
                    (std_json.dumps(event_result or {"status": "PROCESSED"}), event_id),
                )
        except Exception as e:
            logger.exception(
                "Error in _flush for saga %s, states %s,%s: %s",
                saga_id, saga_update.stock, saga_update.payment, str(e)
            )
            raise

    async def _create_saga(
        self,
        event: utils.BaseEvent[utils.StockReservedPayload],
    ) -> None:


        stock_msg = utils.build_reserve_stock_command(
            order_id=event.order_id,
            saga_id=event.saga_id,
            items=event.payload.items,
        )
        

        async with self.db_pool.connection() as conn:
            async with conn.cursor(row_factory = dict_row) as cursor:
                async with conn.transaction():
                    # INSERT the new saga row (version 0, initial state)
                    await cursor.execute(
                        """
                        INSERT INTO sagas (id, order_id, status, stock, payment, results, version)
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                        """,
                        (
                            event.saga_id,
                            event.order_id,
                            SagaStatus.RUNNING.value,
                            ServiceStatus.RUNNING.value,
                            ServiceStatus.IDLE.value,
                            std_json.dumps({
                                "user_id": event.payload.user_id,
                                "items": event.payload.items,
                                "amount": event.payload.amount,
                            }),
                            0,  # Initial version
                        ),
                    )
                    
                    # INSERT the outbox message for stock reservation
                    if hasattr(stock_msg, 'to_dict'):
                        payload_dict = stock_msg.to_dict()
                    elif hasattr(stock_msg, 'dict'):
                        payload_dict = stock_msg.dict()
                    elif isinstance(stock_msg, dict):
                        payload_dict = stock_msg
                    else:
                        payload_dict = vars(stock_msg)
                    
                    await cursor.execute(
                        """
                        INSERT INTO outbox (id, topic, payload)
                        VALUES (%s, %s, %s::jsonb)
                        """,
                        (str(uuid.uuid4()), "stock.request", std_json.dumps(payload_dict)),
                    )
                    
                    # Mark the received event as PROCESSED
                    await cursor.execute(
                        """
                        UPDATE received_events
                        SET status = 'PROCESSED', result = %s::jsonb
                        WHERE event_id = %s
                        """,
                        (std_json.dumps({"status": "PROCESSED", "result": "saga created and stock reservation requested"}), event.id),
                    )

                logger.info("TEST: create saga - order %s - saga %s - TIME: %s",  event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))

    async def _fail_saga(
            self, 
            state:OrchestratorState, 
            event, 
            cursor,
            reason
        ):
        """Both compensations complete — notify order service of failure."""

        
        msg = utils.build_end_checkout_command(
            saga_id=event.saga_id,
            order_id=event.order_id,
            reason=reason,
            status="failed",
        )
        try:
            await self._flush(
                cursor,
                event_id=event.id,
                saga_id=event.saga_id,
                order_id=event.order_id,
                saga_update=state,
                outbox_messages=[(utils.Commands.END_CHECKOUT, "order.request", msg)],
                event_result={"status": "PROCESSED", "result": "saga compensated"},
            )
            logger.info("TEST: fail saga: %s - order %s - saga %s - TIME: %s", event.event_type, event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))
        except Exception as e:
            logger.exception(
                "Error in _fail_saga for saga %s, event %s: %s",
                event.saga_id, event.id, str(e)
            )
            raise

    async def _complete_saga(
        self,
        state: OrchestratorState,
        event: utils.BaseEvent,
        cursor: psycopg.AsyncCursor
    ) -> None:
        

        outbox_message = utils.build_end_checkout_command(
            saga_id=event.saga_id,
            order_id=event.order_id,
            reason="saga completed successfully",
            status="success",
        )

        await self._flush(
            cursor,
            event_id=event.id,
            saga_id=event.saga_id,
            order_id=event.order_id,
            saga_update=state,
            outbox_messages=[
                (utils.Commands.END_CHECKOUT, "order.request", outbox_message),
            ],
            event_result={"status": "PROCESSED", "result": "saga completed"},
        )

        logger.info("TEST: %s - order %s - saga %s - TIME: %s", event.event_type, event.order_id, event.saga_id, datetime.now().strftime("%H:%M:%S.%f"))

    async def _rollback_payment(
        self,
        state: OrchestratorState,
        event: utils.BaseEvent,
        cursor: psycopg.AsyncCursor,
    ) -> None:
        # Extract necessary info for rollback from saga state results
        user_id = state.results.get("user_id")
        amount = state.results.get("amount")
        items = state.results.get("items")

        if not user_id or not amount or not items:
            logger.error(
                "Missing fields for payment rollback in saga %s — dropping",
                event.saga_id,
            )
            return


        # if payment succeeded, we need to refund, otherwise
        rollback_message = utils.build_payment_rollback_command(
            saga_id=event.saga_id,
            order_id=event.order_id,
            items=items,
            user_id=user_id,
            amount=amount,
        )
        try:
            await self._flush(
                cursor,
                event_id=event.id,
                saga_id=event.saga_id,
                order_id=event.order_id,
                saga_update=state,
                outbox_messages=[
                    ("REFUND_PAYMENT", "payment.request", rollback_message),
                ],
                event_result={"status": "compensating", "reason": "payment rollback"},
            )
            logger.info("TEST rollback payment - order %s - saga %s - version %s - TIME: %s", event.order_id, event.saga_id, state.version, datetime.now().strftime("%H:%M:%S.%f"))
        except Exception as e:
            logger.exception(
                "Error in _rollback_payment for saga %s, event %s: %s",
                event.saga_id, event.id, str(e)
            )
            raise

    async def _rollback_stock(
        self,
        state: OrchestratorState,
        event: utils.BaseEvent,
        cursor: psycopg.AsyncCursor,
    ) -> None:
        
        # Extract necessary info for rollback from saga state results
        items = state.results.get("items")

        if not items:
            logger.error(
                "Missing fields for stock rollback in saga %s — dropping",
                event.saga_id,
            )
            return


        # if stock succeeded, we need to free, otherwise 
        rollback_message = utils.build_free_stock_command(
            saga_id=event.saga_id,
            order_id=event.order_id,
            items=items,
        )

        try:
            await self._flush(
                cursor,
                event_id=event.id,
                saga_id=event.saga_id,
                order_id=event.order_id,
                saga_update=state,
                outbox_messages=[
                    ("FREE_STOCK", "stock.request", rollback_message),
                ],
                event_result={"status": "compensating", "reason": "stock rollback"},
            )

            logger.info("TEST - rolling back stock: %s for order %s - version %s - TIME: %s", event.event_type, event.order_id, state.version, datetime.now().strftime("%H:%M:%S.%f"))
        except Exception as e:
            logger.exception(
                "Error in _rollback_stock for saga %s, event %s: %s",
                event.saga_id, event.id, str(e)
            )
            raise


    async def _request_payment(
        self,
        event: utils.BaseEvent, 
        state: OrchestratorState, 
        cursor: psycopg.AsyncCursor
        ) -> None:

        user_id = state.results.get("user_id") 
        if not user_id:
            logger.error(
                "Missing user_id for payment request in saga %s — dropping",
                event.saga_id,
            )
            return

        payment_msg = utils.build_start_payment_command(
            order_id=event.order_id,
            saga_id=event.saga_id,
            amount=event.payload.amount,
            user_id=user_id
        )
        try:
            await self._flush(
                cursor,
                event_id=event.id,
                saga_id=event.saga_id,
                order_id=event.order_id,
                saga_update=state,
                outbox_messages=[
                    ("START_PAYMENT", "payment.request", payment_msg),
                ],
                event_result={"status": "PROCESSED", "result": "payment requested"},
            )
            logger.info("TEST: request payment - order %s - saga %s - version %s - TIME: %s", event.order_id, event.saga_id, state.version, datetime.now().strftime("%H:%M:%S.%f"))
        except Exception as e:
            logger.exception(
                "Error in _request_payment for saga %s, event %s: %s",
                event.saga_id, event.id, str(e)
            )
            raise