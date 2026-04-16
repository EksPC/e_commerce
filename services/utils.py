import uuid
from enum import StrEnum
from typing import Generic, TypeVar
import uuid

from typing import TypeVar, Generic, Type, Any, Optional
from msgspec import json, convert, Struct, ValidationError
import uuid
import datetime
from dataclasses import dataclass
from typing import Generic, TypeVar, Union



# Generic type variables are used for the Result Pattern, that allows us to structure the return types
#  of our operations.

T = TypeVar("T")  # The type of the Success value (e.g., int)
E = TypeVar("E")  # The type of the Error value (e.g., str)

@dataclass(frozen=True)
class Success(Generic[T]):
    value: T
    is_ok: bool = True

@dataclass(frozen=True)
class Failure(Generic[E]):
    error: E
    is_ok: bool = False

# A type alias for convenience, especially in db calls or other simple applications.
CreditResult = Union[Success[int], Failure[str]]

J = TypeVar("J")


class BaseEvent(Struct, Generic[J]):
    """
    A generic event envelope that can wrap any payload type. 
    Useful for Kafka messages where we want a consistent structure but variable payloads.
    """
    id: str
    event_type: str
    order_id: str
    payload: J
    saga_id: str

    timestamp: float = datetime.datetime.now(datetime.timezone.utc).timestamp()

    @classmethod
    def create(cls, event_type: str, payload: J, order_id: str = "", saga_id: str = "" , id:str = ""):
        return cls(
            event_type=event_type,
            payload=payload,
            order_id=order_id,
            saga_id=saga_id,
            id=id or str(uuid.uuid4())
        )
    
    def to_dict(self):
        # Convert payload to dict: handle both dict and msgspec Struct types
        if isinstance(self.payload, dict):
            payload_dict = self.payload
        else:
            # For msgspec Structs, encode to JSON and decode back to get dict
            payload_dict = json.decode(json.encode(self.payload))
        
        return {
            "id": self.id,
            "event_type": self.event_type,
            "order_id": self.order_id,
            "payload": payload_dict,
            "saga_id": self.saga_id,
            "timestamp": self.timestamp
        }




class StartPaymentCommandPayload(Struct):
    """
    Payload for a start payment command event.
    """
    order_id: str
    user_id: str
    amount: int


class ReserveStockCommandPayload(Struct):
    """
    Payload for a reserve stock command event.
    """
    order_id: str
    items: list[tuple[str, int]]

class StockUnavailablePayload(Struct):
    """
    Payload for a stock unavailable event.
    """
    order_id: str
    # out_of_stock_items: list[tuple[str,int]]


class StockReservedPayload(Struct):
    """
    Payload for a stock reserved event.
    """
    order_id: str
    amount: int


class StockFreedPayload(Struct):
    """
    Payload for a stock freed event.
    """
    order_id: str


class FreeStockCommandPayload(Struct):
    """
    Payload for a free stock command event.
    """
    order_id: str
    items: list[tuple[str, int]]


class PaymentProcessedPayload(Struct):
    """
    Payload for a payment processed event.
    """
    order_id: str
    user_id: str
    amount: int
    remaining_credit: int

class PaymentFailedPayload(Struct):
    """
    Payload for a payment failed event.
    """
    user_id: str
    amount: int
    reason: str


class CheckoutPayload(Struct):
    amount: int
    user_id: str
    items: list[tuple[str, int]]  # List of (item_id, quantity)


class EventStatus(StrEnum):
    RECEIVED = "RECEIVED"
    PROCESSED = "PROCESSED"

class Commands(StrEnum):
    RESERVE_STOCK = "reserve_stock"
    FREE_STOCK = "free_stock"
    START_PAYMENT = "start_payment"
    ROLLBACK_PAYMENT = "rollback_payment"
    END_CHECKOUT = "end_checkout"

class IntegrationEvents(StrEnum):
    pass


def cast_integration_event(event_type: str) -> IntegrationEvents:
    # Determine which enum type based on event_type prefix or service origin
    event_type_str = event_type
    if event_type_str.startswith("integration.stock"):
        return StockIntegrationEvent(event_type_str)
    elif event_type_str.startswith("integration.payment"):
        return PaymentIntegrationEvent(event_type_str)
    else:
        return OrderIntegrationEvent(event_type_str)
    

    


class OrderInternalEvent(StrEnum):
    ORDER_CREATED = "order_created"
    ITEM_ADDED = "item_added"
    
    ORDER_CANCELLED = "order_cancelled"
    ORDER_COMPLETED = "order_completed"

class OrchestratorInternalEvents(StrEnum):
    # Lifecycle
    SAGA_CREATED   = "saga.created"
    SAGA_ENDED     = "saga.ended"
    SAGA_TIMEOUT   = "saga.timeout"


class PaymentInternalEvent(StrEnum):
    USER_CREATED = "user_created"
    FUNDS_ADDED = "funds_added"
    PAYMENT_RESERVED = "payment_reserved"
    PAYMENT_CONFIRMED = "payment_confirmed"

class StockInternalEvent(StrEnum):
    ITEM_CREATED = "item_created"
    STOCK_INCREMENTED = "stock_incremented"
    STOCK_DECREMENTED = "stock_decremented"
    STOCK_RESERVED = "stock_reserved"

class OrderIntegrationEvent(IntegrationEvents,StrEnum):
    # Triggered by /orders/checkout/{order_id}
    # Sent to Stock and Payment services
    CHECKOUT_INITIATED = "integration.checkout.initiated"
    CHECKOUT_CANCELLED = "integration.checkout.cancelled"
    # Sent when the entire saga completes
    CHECKOUT_COMPLETED = "integration.checkout.completed"


class PaymentIntegrationEvent(IntegrationEvents,StrEnum):
    # Sent to Order service to confirm billing success
    PAYMENT_SUCCEEDED = "integration.payment.succeeded"
    PAYMENT_FAILED = "integration.payment.failed"
    PAYMENT_REFUNDED = "integration.payment.refunded"

class StockIntegrationEvent(IntegrationEvents,StrEnum):
    # Sent to Order service after stock is successfully subtracted
    STOCK_ALLOCATED = "integration.stock.allocated"
    STOCK_UNAVAILABLE = "integration.stock.unavailable"
    STOCK_FAILED = "integration.stock.failed"
    STOCK_FREED = "integration.stock.freed"


class TestEventType(StrEnum):
    TEST_EVENT = "test_event"


PAYLOAD_REGISTRY: dict[str, type] = {

    TestEventType.TEST_EVENT: dict,  # For testing unknown event types

    Commands.RESERVE_STOCK: ReserveStockCommandPayload,
    Commands.FREE_STOCK: FreeStockCommandPayload,
    Commands.END_CHECKOUT: dict,
    Commands.START_PAYMENT: StartPaymentCommandPayload,

    OrderIntegrationEvent.CHECKOUT_INITIATED: CheckoutPayload,

    StockIntegrationEvent.STOCK_ALLOCATED: StockReservedPayload,
    StockIntegrationEvent.STOCK_UNAVAILABLE: StockUnavailablePayload,
    StockIntegrationEvent.STOCK_FREED: StockFreedPayload,
    StockIntegrationEvent.STOCK_FAILED: dict,

    PaymentIntegrationEvent.PAYMENT_SUCCEEDED: PaymentProcessedPayload,
    PaymentIntegrationEvent.PAYMENT_FAILED: PaymentFailedPayload
}

DecodeResult = Union[Success[BaseEvent[Any]], Failure[str]]



def decode_and_type_event(record: Any) -> DecodeResult:
    """
    Decodes a raw Kafka message into a specific BaseEvent[PayloadStruct].
    Returns None if the event type is unknown or validation fails.
    """
    try:

        if hasattr(record, "value"):
            raw_bytes = record.value
        else:
            raw_bytes = record

        # Decode the envelope with a dict payload
        envelope = json.decode(raw_bytes, type=BaseEvent[dict])
        # Registry Lookup
        payload_cls = PAYLOAD_REGISTRY.get(envelope.event_type)

        if not payload_cls:
            return Failure(error=f"UNKNOWN_EVENT_TYPE:{envelope.event_type}")

        # Convert dict to specific Struct
        typed_payload = convert(envelope.payload, payload_cls)
        # Return new instance with the typed payload
        return Success(
            value = BaseEvent (
                id=envelope.id,
                event_type=envelope.event_type,
                order_id=envelope.order_id,
                payload=typed_payload,
                timestamp=envelope.timestamp,
                saga_id=envelope.saga_id
            )
        )
    
    except ValidationError as e:
        print(f"Validation failed: {e}")
        return Failure(error=f"Validation failed: {e}")
    except Exception as e:
        print(f"Decoding error: {e}")
        return Failure(error=f"Decoding error: {e}")
    

class RedisMessageWrapper:
    """
    A wrapper for Redis messages to ensure consistent handling of byte strings.
    """
    def __init__(self, data):
        self.value = data.encode() if isinstance(data, str) else data


def build_generic_error_event(order_id: str, saga_id: str, error_message: str) -> BaseEvent[dict]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type="error",
        order_id=order_id,
        saga_id=saga_id,
        payload={"error": error_message},
    )

def build_unknown_saga_event(order_id: str, saga_id: str) -> BaseEvent[dict]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type="unknown_saga",
        order_id=order_id,
        saga_id=saga_id,
        payload={"message": "Saga not found for incoming event"},
    )

def build_dead_letter_event(order_id: str, saga_id: str, reason: str, original_event: BaseEvent) -> BaseEvent[dict]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type="dead_letter",
        order_id=order_id,
        saga_id=saga_id,
        payload={
            "reason": reason,
            "original_event": original_event
        },
    )

def build_reserve_stock_command(saga_id: str, order_id: str, items: list[tuple[str, int]]) -> BaseEvent[ReserveStockCommandPayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=Commands.RESERVE_STOCK,
        order_id=order_id,
        saga_id=saga_id,
        payload=ReserveStockCommandPayload(order_id=order_id, items=items),
    )

def build_start_payment_command(saga_id: str, order_id: str, user_id: str, amount: int) -> BaseEvent[StartPaymentCommandPayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=Commands.START_PAYMENT,
        order_id=order_id,
        saga_id=saga_id,
        payload=StartPaymentCommandPayload(order_id=order_id, user_id=user_id, amount=amount),
    )

def build_free_stock_command(saga_id: str, order_id: str, items: list[tuple[str, int]]) -> BaseEvent[FreeStockCommandPayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=Commands.FREE_STOCK,
        order_id=order_id,
        saga_id=saga_id,
        payload=FreeStockCommandPayload(order_id=order_id, items=items),
    )
    


def build_stock_allocated_event(order_id: str, saga_id: str, amount: int) -> BaseEvent[StockReservedPayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=StockIntegrationEvent.STOCK_ALLOCATED,
        order_id=order_id,
        saga_id=saga_id,
        payload=StockReservedPayload(order_id=order_id, amount=amount),
    )


def build_payment_rollback_command(saga_id: str, order_id: str, items: list[tuple[str,int]], user_id: str, amount: int) -> BaseEvent[dict]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=Commands.ROLLBACK_PAYMENT,
        order_id=order_id,
        saga_id=saga_id,
        payload={"items": items, "user_id": user_id, "amount": amount},
    )

def build_payment_failed_event(order_id: str, saga_id: str, user_id: str, amount: int, reason: str) -> BaseEvent[PaymentFailedPayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=PaymentIntegrationEvent.PAYMENT_FAILED,
        order_id=order_id,
        saga_id=saga_id,
        payload=PaymentFailedPayload(user_id=user_id, amount=amount, reason=reason),
    )

def build_payment_refund_event(order_id: str, saga_id: str, user_id: str, amount: int) -> BaseEvent[dict]:
    """Build a PAYMENT_REFUNDED event sent when payment compensation completes."""
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=PaymentIntegrationEvent.PAYMENT_REFUNDED,
        order_id=order_id,
        saga_id=saga_id,
        payload={"user_id": user_id, "amount": amount},
    )

def build_payment_succeeded_event(order_id: str, saga_id: str, user_id: str, amount: int, remaining_credit: int) -> BaseEvent[PaymentProcessedPayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=PaymentIntegrationEvent.PAYMENT_SUCCEEDED,
        order_id=order_id,
        saga_id=saga_id,
        payload=PaymentProcessedPayload(order_id=order_id, user_id=user_id, amount=amount, remaining_credit=remaining_credit),
    )

def build_stock_unavailable_event(saga_id: str, order_id: str) -> BaseEvent[StockUnavailablePayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=StockIntegrationEvent.STOCK_UNAVAILABLE,
        order_id=order_id,
        saga_id=saga_id,
        payload=StockUnavailablePayload(order_id=order_id),
    )

def build_stock_failure(saga_id: str, order_id: str, data: dict) -> BaseEvent[dict]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=StockIntegrationEvent.STOCK_FAILED,
        order_id=order_id,
        saga_id=saga_id,
        payload=data,
    )

def build_stock_freed_event(order_id: str, saga_id: str) -> BaseEvent[StockFreedPayload]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=StockIntegrationEvent.STOCK_FREED,
        order_id=order_id,
        saga_id=saga_id,
        payload=StockFreedPayload(order_id=order_id),
    )


def build_end_checkout_command(order_id: str, saga_id: str, reason: str, status: str) -> BaseEvent[dict]:
    return BaseEvent(
        id=str(uuid.uuid4()),
        event_type=Commands.END_CHECKOUT,
        order_id=order_id,
        saga_id=saga_id,
        payload={
            "reason": reason,
            "status": status
        },
    )
    
def print_test(
    location: str,
    step: str,
    saga_id: str
    ):
    time = datetime.datetime.now()    
    print(f"LGT {location}- {saga_id} - {step} - {time.strftime('%H:%M:%S.%f')}")

ROUTING_TABLE = {
    OrderIntegrationEvent.CHECKOUT_INITIATED: "stock.request",
    StockIntegrationEvent.STOCK_UNAVAILABLE: "order.request",
    StockIntegrationEvent.STOCK_FAILED: "order.request",
    StockIntegrationEvent.STOCK_FREED: "order.request",
    StockIntegrationEvent.STOCK_ALLOCATED: "payment.request",
    PaymentIntegrationEvent.PAYMENT_SUCCEEDED: "order.request",
    PaymentIntegrationEvent.PAYMENT_FAILED: "stock.request",
    PaymentIntegrationEvent.PAYMENT_REFUNDED: "stock.request",
}
