from __future__ import annotations

from execution.models import ChildOrderStatus, ExecutionStatus


class InvalidStateTransition(ValueError):
    pass


EXECUTION_TRANSITIONS: dict[ExecutionStatus, set[ExecutionStatus]] = {
    ExecutionStatus.CREATED: {ExecutionStatus.VALIDATING, ExecutionStatus.FAILED},
    ExecutionStatus.VALIDATING: {ExecutionStatus.RUNNING, ExecutionStatus.COMPLETED, ExecutionStatus.FAILED},
    ExecutionStatus.RUNNING: {
        ExecutionStatus.CANCELLING,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.PARTIALLY_COMPLETED,
        ExecutionStatus.EXPIRED,
        ExecutionStatus.FAILED,
    },
    ExecutionStatus.CANCELLING: {
        ExecutionStatus.CANCELLED,
        ExecutionStatus.PARTIALLY_COMPLETED,
        ExecutionStatus.FAILED,
    },
    ExecutionStatus.COMPLETED: set(),
    ExecutionStatus.PARTIALLY_COMPLETED: set(),
    ExecutionStatus.EXPIRED: set(),
    ExecutionStatus.CANCELLED: set(),
    ExecutionStatus.FAILED: set(),
}


CHILD_TRANSITIONS: dict[ChildOrderStatus, set[ChildOrderStatus]] = {
    ChildOrderStatus.PENDING_SUBMIT: {
        ChildOrderStatus.OPEN,
        ChildOrderStatus.REJECTED,
        ChildOrderStatus.UNKNOWN,
    },
    ChildOrderStatus.OPEN: {
        ChildOrderStatus.PARTIALLY_FILLED,
        ChildOrderStatus.FILLED,
        ChildOrderStatus.PENDING_CANCEL,
    },
    ChildOrderStatus.PARTIALLY_FILLED: {
        ChildOrderStatus.FILLED,
        ChildOrderStatus.PENDING_CANCEL,
    },
    ChildOrderStatus.PENDING_CANCEL: {
        ChildOrderStatus.OPEN,
        ChildOrderStatus.PARTIALLY_FILLED,
        ChildOrderStatus.CANCELLED,
        ChildOrderStatus.FILLED,
    },
    ChildOrderStatus.CANCELLED: set(),
    ChildOrderStatus.FILLED: set(),
    ChildOrderStatus.REJECTED: set(),
    ChildOrderStatus.UNKNOWN: {
        ChildOrderStatus.OPEN,
        ChildOrderStatus.PARTIALLY_FILLED,
        ChildOrderStatus.FILLED,
        ChildOrderStatus.CANCELLED,
        ChildOrderStatus.REJECTED,
    },
}


def transition_execution(current: ExecutionStatus, target: ExecutionStatus) -> ExecutionStatus:
    if target not in EXECUTION_TRANSITIONS.get(current, set()):
        raise InvalidStateTransition(f"execution transition {current} -> {target} is not allowed")
    return target


def transition_child(current: ChildOrderStatus, target: ChildOrderStatus) -> ChildOrderStatus:
    if target not in CHILD_TRANSITIONS.get(current, set()):
        raise InvalidStateTransition(f"child order transition {current} -> {target} is not allowed")
    return target
