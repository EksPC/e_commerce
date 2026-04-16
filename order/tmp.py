from enum import StrEnum



class SagaStates(StrEnum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    COMPENSATED = "COMPENSATED"


class SagaSteps(StrEnum):
    RESERVE_STOCK = "RESERVE_STOCK"
    START_PAYMENT = "START_PAYMENT"

class SagaStep:
    step: SagaSte
    


class SagaContext:

    steps = [SagaStep.RESERVE_STOCK, SagaStep.START_PAYMENT]
    index = 0

    def __init__(self, saga_id: str, order_id: str):
        self.saga_id = saga_id
        self.order_id = order_id
        self.status = SagaState.PENDING
        self.results = {}
        self.steps = [SagaStep.RESERVE_STOCK, SagaStep.START_PAYMENT]

    def advance(self):
        """
            Advances to the next step in the saga. Returns the next step.
        """
        if self.index < len(self.steps):
            step = self.steps[self.index]
            self.index += 1
            return step
        return None
    
    def compensate(self):
        """

        """
        if self.index > 0:
            self.index -= 1
            return self.steps[self.index]
        return None