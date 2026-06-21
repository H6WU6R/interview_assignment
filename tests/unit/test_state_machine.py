import pytest

from execution.models import ChildOrderStatus, ExecutionStatus
from execution.state_machine import InvalidStateTransition, transition_child, transition_execution


def test_execution_terminal_state_cannot_return_to_running() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_execution(ExecutionStatus.COMPLETED, ExecutionStatus.RUNNING)


def test_execution_running_can_move_to_cancelling() -> None:
    assert transition_execution(ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING) is ExecutionStatus.CANCELLING


def test_child_pending_cancel_can_fill_or_cancel() -> None:
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.FILLED) is ChildOrderStatus.FILLED
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.CANCELLED) is ChildOrderStatus.CANCELLED


def test_child_pending_cancel_can_reconcile_to_still_live_order() -> None:
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.OPEN) is ChildOrderStatus.OPEN
    assert transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.PARTIALLY_FILLED) is ChildOrderStatus.PARTIALLY_FILLED


def test_child_open_cannot_move_to_unknown() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_child(ChildOrderStatus.OPEN, ChildOrderStatus.UNKNOWN)


def test_child_unknown_can_be_reconciled_to_exchange_truth() -> None:
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.OPEN) is ChildOrderStatus.OPEN
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.PARTIALLY_FILLED) is ChildOrderStatus.PARTIALLY_FILLED
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.FILLED) is ChildOrderStatus.FILLED
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.CANCELLED) is ChildOrderStatus.CANCELLED
    assert transition_child(ChildOrderStatus.UNKNOWN, ChildOrderStatus.REJECTED) is ChildOrderStatus.REJECTED


def test_pending_cancel_does_not_turn_into_create_rejected_state() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_child(ChildOrderStatus.PENDING_CANCEL, ChildOrderStatus.REJECTED)


def test_unexpected_current_execution_state_raises_invalid_transition() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_execution("UNEXPECTED", ExecutionStatus.RUNNING)  # type: ignore[arg-type]


def test_unexpected_current_child_state_raises_invalid_transition() -> None:
    with pytest.raises(InvalidStateTransition):
        transition_child("UNEXPECTED", ChildOrderStatus.OPEN)  # type: ignore[arg-type]
