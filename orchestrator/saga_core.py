from __future__ import annotations
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Optional


# class SagaStatus(StrEnum):
#     RUNNING      = "RUNNING"
#     COMPLETED    = "COMPLETED"
#     COMPENSATING = "COMPENSATING"
#     COMPENSATED  = "COMPENSATED"
#     FAILED       = "FAILED"


# class SagaStep(StrEnum):
#     CREATED             = "ORDER_CHECKOUT_PHASE"
#     STOCK_RESERVATION   = "STOCK_RESERVATION_PHASE"
#     PAYMENT             = "PAYMENT_PHASE"
#     STOCK_COMPENSATION  = "STOCK_COMPENSATION_PHASE"
#     FINISHED            = "FINISHED"


# # Explicit transition tables — add a new step by adding one entry here.
# _FORWARD: dict[SagaStep, tuple[SagaStep, SagaStatus]] = {
#     SagaStep.CREATED:           (SagaStep.STOCK_RESERVATION, SagaStatus.RUNNING),
#     SagaStep.STOCK_RESERVATION: (SagaStep.PAYMENT,           SagaStatus.RUNNING),
#     SagaStep.PAYMENT:           (SagaStep.FINISHED,          SagaStatus.COMPLETED),
#     SagaStep.STOCK_COMPENSATION: (SagaStep.FINISHED,         SagaStatus.COMPENSATED), 
# }

# _ROLLBACK: dict[SagaStep, tuple[SagaStep, SagaStatus]] = {
#     SagaStep.PAYMENT:            (SagaStep.STOCK_COMPENSATION, SagaStatus.COMPENSATING),
#     SagaStep.STOCK_COMPENSATION: (SagaStep.FINISHED,           SagaStatus.COMPENSATED),
# }


# @dataclass
# class SagaContext:
#     saga_id: str
#     order_id: str
#     step: SagaStep   = SagaStep.CREATED
#     status: SagaStatus = SagaStatus.RUNNING
#     results: dict[str, Any] = field(default_factory=dict)
#     version: int = 0  # for optimistic concurrency control

#     def get_user(self)-> Optional[str]:
#         return self.results.get(SagaStep.CREATED.value, {}).get("user_id")

#     def advance(self) -> None:
#         if self.step not in _FORWARD:
#             raise ValueError(f"Cannot advance from terminal step {self.step!r}")
#         self.step, self.status = _FORWARD[self.step]

#     def rollback(self) -> None:
#         if self.step not in _ROLLBACK:
#             raise ValueError(f"Cannot roll back from step {self.step!r}")
#         self.step, self.status = _ROLLBACK[self.step]

#     def fail(self) -> None:
#         """Mark as failed without compensation (e.g. stock unavailable before any reservation)."""
#         self.step   = SagaStep.FINISHED
#         self.status = SagaStatus.FAILED


#     def set_result(self, result: Any,  step: Optional[SagaStep] = None) -> None:
#         self.results[(step or self.step).value] = result


#     def get_result(self, step: Optional[SagaStep] = None) -> Any | None:
#         return self.results.get((step or self.step).value)


#     def to_db(self) -> dict[str, Any]:
#         return {
#             "saga_id":  self.saga_id,
#             "order_id": self.order_id, 
#             "step":     self.step.value,
#             "status":   self.status.value,
#             "results":  self.results,
#             "version":  self.version,
#         }

#     @classmethod
#     def from_db(cls, row: dict[str, Any]) -> SagaContext:
#         return cls(
#             saga_id=row["id"],
#             order_id=row["order_id"],
#             step=SagaStep(row["step"]),
#             status=SagaStatus(row["status"]),
#             results=row.get("results") or {},
#             version=row.get("version", 0),
#         )


#     @property
#     def is_terminal(self) -> bool:
#         return self.step == SagaStep.FINISHED

#     @property
#     def is_compensating(self) -> bool:
#         return self.status == SagaStatus.COMPENSATING
    


from enum import StrEnum
from dataclasses import dataclass, field
from typing import Any, Optional


class ServiceStatus(StrEnum):
    """State of a single participant leg (stock or payment)."""
    IDLE         = "IDLE"
    RUNNING      = "RUNNING"
    COMPLETED    = "COMPLETED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED  = "COMPENSATED"


class SagaStatus(StrEnum):
    """Derived top-level state of the whole saga."""
    CREATED      = "CREATED"
    RUNNING      = "RUNNING"
    COMPLETED    = "COMPLETED"
    FAILED       = "FAILED"


@dataclass
class OrchestratorState:
    id:       Optional[str]        = None
    order_id: str                  = ""
    version:  int                  = 0
    results:  dict[str, Any]       = field(default_factory=dict)
    payment:  ServiceStatus            = ServiceStatus.RUNNING
    stock:    ServiceStatus            = ServiceStatus.RUNNING

    @property
    def status(self) -> SagaStatus:
        if self.payment == ServiceStatus.COMPLETED and self.stock == ServiceStatus.COMPLETED:
            return SagaStatus.COMPLETED
        if self.payment == ServiceStatus.COMPENSATING or self.stock == ServiceStatus.COMPENSATING:
            return SagaStatus.RUNNING
        if self.payment == ServiceStatus.COMPENSATED and self.stock == ServiceStatus.COMPENSATED:
            return SagaStatus.FAILED
        return SagaStatus.RUNNING

    @property
    def is_terminal(self) -> bool:
        return self.status in (SagaStatus.COMPLETED, SagaStatus.FAILED)

    def success(self, service: str) -> None:
        if not hasattr(self, service):
            raise ValueError(f"Unknown service {service!r}")
        current = getattr(self, service)
        if current == ServiceStatus.RUNNING:
            setattr(self, service, ServiceStatus.COMPLETED)
        elif current == ServiceStatus.COMPENSATING:
            setattr(self, service, ServiceStatus.COMPENSATED)
        else:
            raise ValueError(f"Cannot advance {service!r} from {current!r}")

    def fail(self, service: str) -> None:
        if not hasattr(self, service):
            raise ValueError(f"Unknown service {service!r}")
        current = getattr(self, service)
        if current == ServiceStatus.RUNNING:
            setattr(self, service, ServiceStatus.COMPENSATING)
        elif current == ServiceStatus.COMPENSATING:
            setattr(self, service, ServiceStatus.COMPENSATED)
        else:
            raise ValueError(f"Cannot fail {service!r} from {current!r}")

    @classmethod
    def from_db(cls, row: dict) -> "OrchestratorState":
        return cls(
            id=row["id"],
            order_id=row["order_id"],
            version=row["version"],
            results=row.get("results") or {},
            stock=ServiceStatus(row["stock"]),
            payment=ServiceStatus(row["payment"]),
        )

    def to_db(self) -> dict:
        return {
            "id":       self.id,
            "order_id": self.order_id,
            "version":  self.version,
            "status":   self.status.value,
            "stock":    self.stock.value,
            "payment":  self.payment.value,
            "results":  self.results,
        }