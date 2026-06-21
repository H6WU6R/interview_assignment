from decimal import Decimal

from execution.models import (
    Algorithm,
    ChildOrderStatus,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
    RepricingMode,
    Side,
    required_trade,
)


def test_execution_request_parses_decimal_strings() -> None:
    request = ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol="BTCUSDT",
        algorithm=Algorithm.CHASE,
        target_position=Decimal("0.010"),
        target_price_lower=Decimal("94000"),
        target_price_upper=Decimal("97000"),
        target_duration_seconds=300,
        deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        parameters=ExecutionParameters(
            reprice_threshold_bps=Decimal("2.0"),
            minimum_reprice_interval_ms=500,
            number_of_slices=10,
            child_order_timeout_seconds=20,
            repricing_mode=RepricingMode.ADVERSE_ONLY,
        ),
    )

    assert request.target_position == Decimal("0.010")
    assert request.parameters.repricing_mode is RepricingMode.ADVERSE_ONLY


def test_required_trade_uses_target_position_not_order_quantity() -> None:
    side, quantity = required_trade(
        target_position=Decimal("0.005"),
        current_position=Decimal("-0.003"),
    )

    assert side is Side.BUY
    assert quantity == Decimal("0.008")


def test_required_trade_no_action_when_target_reached() -> None:
    side, quantity = required_trade(
        target_position=Decimal("0.005"),
        current_position=Decimal("0.005"),
    )

    assert side is Side.NO_ACTION
    assert quantity == Decimal("0")


def test_terminal_statuses_are_terminal() -> None:
    assert ExecutionStatus.COMPLETED.is_terminal
    assert ExecutionStatus.PARTIALLY_COMPLETED.is_terminal
    assert ExecutionStatus.EXPIRED.is_terminal
    assert ExecutionStatus.CANCELLED.is_terminal
    assert ExecutionStatus.FAILED.is_terminal
    assert not ExecutionStatus.RUNNING.is_terminal
    assert not ChildOrderStatus.UNKNOWN.is_terminal
