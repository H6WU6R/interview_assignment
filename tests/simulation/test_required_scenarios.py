from __future__ import annotations

import csv
import json
import subprocess
from decimal import Decimal
from pathlib import Path

from exchanges.simulator import DeterministicSimulator
from execution.clock import ManualClock
from execution.engine import (
    CREATE_TIMEOUT_ORDER_NOT_FOUND,
    CREATE_TIMEOUT_PENDING_RECONCILIATION,
    CREATE_TIMEOUT_RECONCILED,
    DEADLINE_CANCEL_REMAINDER,
    PRICE_OUTSIDE_RANGE,
    STREAM_HEALTH_DEGRADED_RECONCILED,
)
from execution.ids import make_client_order_prefix
from execution.models import (
    Algorithm,
    ChildOrderStatus,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
    Fill,
    RepricingMode,
    Side,
)
from execution.service import ExecutionService


SYMBOL = "BTCUSDT"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def request(
    *,
    algorithm: Algorithm = Algorithm.CHASE,
    target_position: Decimal = Decimal("0.010"),
    lower: Decimal = Decimal("49000"),
    upper: Decimal = Decimal("51000"),
    duration: int = 100,
    deadline_policy: DeadlinePolicy = DeadlinePolicy.CANCEL_REMAINDER,
    parameters: ExecutionParameters | None = None,
) -> ExecutionRequest:
    return ExecutionRequest(
        environment=Environment.SIMULATION,
        symbol=SYMBOL,
        algorithm=algorithm,
        target_position=target_position,
        target_price_lower=lower,
        target_price_upper=upper,
        target_duration_seconds=duration,
        deadline_policy=deadline_policy,
        parameters=parameters or ExecutionParameters(),
    )


async def simulator_service(
    *,
    position: Decimal = Decimal("0"),
    bid: Decimal = Decimal("50000.00"),
    ask: Decimal = Decimal("50001.00"),
) -> tuple[ManualClock, DeterministicSimulator, ExecutionService]:
    clock = ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=position)
    await simulator.push_market_data(SYMBOL, bid, ask, exchange_event_time=1)
    return clock, simulator, ExecutionService(simulator, clock=clock)


async def push_fresh_market(
    clock: ManualClock,
    simulator: DeterministicSimulator,
    *,
    bid: Decimal,
    ask: Decimal,
) -> None:
    await simulator.push_market_data(
        SYMBOL,
        bid,
        ask,
        exchange_event_time=int(clock.monotonic() * 1000),
    )


def assert_exposure_safe(execution) -> None:
    assert execution.exposure.reserved_exposure <= execution.required_quantity
    assert (
        execution.exposure.confirmed_filled_quantity + execution.exposure.reserved_exposure
        <= execution.required_quantity
    )


async def test_t1_normal_chase_submits_passive_price_and_preserves_exposure_invariant() -> None:
    _, _, service = await simulator_service()
    execution = await service.create_execution(request())

    execution = await service.run_once(execution.execution_id)

    assert execution.side is Side.BUY
    assert execution.child_orders[0].price == Decimal("50000.00")
    assert execution.child_orders[0].status is ChildOrderStatus.OPEN
    assert execution.exposure.live_open_quantity == execution.required_quantity
    assert_exposure_safe(execution)
    assert execution.status is ExecutionStatus.RUNNING
    assert execution.final_reason != PRICE_OUTSIDE_RANGE


async def test_t2_chase_reprice_requires_threshold_and_minimum_interval() -> None:
    clock, simulator, service = await simulator_service()
    parameters = ExecutionParameters(
        reprice_threshold_bps=Decimal("2"),
        minimum_reprice_interval_ms=500,
        repricing_mode=RepricingMode.ADVERSE_ONLY,
    )
    execution = await service.create_execution(request(parameters=parameters))
    execution = await service.run_once(execution.execution_id)
    first_client_order_id = execution.child_orders[0].client_order_id

    await push_fresh_market(clock, simulator, bid=Decimal("50020.00"), ask=Decimal("50021.00"))
    execution = await service.run_once(execution.execution_id)

    assert [child.client_order_id for child in execution.child_orders] == [first_client_order_id]
    assert execution.child_orders[0].status is ChildOrderStatus.OPEN

    clock.advance(0.6)
    await push_fresh_market(clock, simulator, bid=Decimal("50020.00"), ask=Decimal("50021.00"))
    execution = await service.run_once(execution.execution_id)

    assert len(execution.child_orders) == 2
    assert execution.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert execution.child_orders[1].status is ChildOrderStatus.OPEN
    assert execution.child_orders[1].client_order_id != first_client_order_id
    assert execution.child_orders[1].price == Decimal("50020.00")
    assert execution.exposure.live_open_quantity == execution.required_quantity
    assert execution.exposure.pending_cancel_quantity == Decimal("0")
    assert_exposure_safe(execution)


async def test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_fill_during_cancel(prefix, Decimal("0.004"))
    execution = await service.run_once(execution.execution_id)

    clock.advance(0.6)
    await push_fresh_market(clock, simulator, bid=Decimal("50030.00"), ask=Decimal("50031.00"))
    execution = await service.run_once(execution.execution_id)

    assert execution.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert len(execution.child_orders) == 2
    assert execution.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert execution.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert execution.child_orders[1].submitted_quantity == Decimal("0.006")
    assert execution.exposure.live_open_quantity == Decimal("0.006")
    assert_exposure_safe(execution)


async def test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id() -> None:
    _, simulator, service = await simulator_service()
    execution = await service.create_execution(request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)

    execution = await service.run_once(execution.execution_id)
    first_client_order_id = execution.child_orders[0].client_order_id

    assert execution.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert execution.exposure.unknown_order_quantity == execution.required_quantity
    assert execution.final_reason == CREATE_TIMEOUT_PENDING_RECONCILIATION

    before_reconcile = await service.run_once(execution.execution_id)
    assert [child.client_order_id for child in before_reconcile.child_orders] == [first_client_order_id]

    reconciled = await service.reconcile_execution(execution.execution_id)

    assert [child.client_order_id for child in reconciled.child_orders] == [first_client_order_id]
    assert reconciled.child_orders[0].status is ChildOrderStatus.OPEN
    assert reconciled.child_orders[0].terminal_reason is None
    assert reconciled.final_reason == CREATE_TIMEOUT_RECONCILED
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")
    assert reconciled.exposure.live_open_quantity == reconciled.required_quantity
    assert_exposure_safe(reconciled)

    await simulator.push_fill(first_client_order_id, reconciled.required_quantity, Decimal("50000.00"))
    completed = await service.reconcile_execution(execution.execution_id)

    assert completed.status is ExecutionStatus.COMPLETED
    assert completed.summary is not None
    assert completed.summary.metrics["unknown_orders_reconciled"] == 1


async def test_t4b_create_timeout_not_found_releases_unknown_exposure_before_safe_retry() -> None:
    _, simulator, service = await simulator_service()
    execution = await service.create_execution(request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout_not_found(prefix)

    execution = await service.run_once(execution.execution_id)
    first_client_order_id = execution.child_orders[0].client_order_id

    assert execution.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert execution.exposure.unknown_order_quantity == execution.required_quantity

    before_reconcile = await service.run_once(execution.execution_id)
    assert [child.client_order_id for child in before_reconcile.child_orders] == [first_client_order_id]

    reconciled = await service.reconcile_execution(execution.execution_id)

    assert reconciled.child_orders[0].status is ChildOrderStatus.REJECTED
    assert reconciled.child_orders[0].terminal_reason == CREATE_TIMEOUT_ORDER_NOT_FOUND
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")

    retried = await service.run_once(execution.execution_id)

    assert len(retried.child_orders) == 2
    assert retried.child_orders[1].client_order_id != first_client_order_id
    assert retried.child_orders[1].status is ChildOrderStatus.OPEN
    assert_exposure_safe(retried)


async def test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(request(algorithm=Algorithm.TWAP, duration=100))

    clock.advance(10)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    execution = await service.run_once(execution.execution_id)
    first_child = execution.child_orders[0]

    assert first_child.submitted_quantity == Decimal("0.001")

    clock.advance(10)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    still_reserved = await service.run_once(execution.execution_id)

    assert len(still_reserved.child_orders) == 1
    assert still_reserved.exposure.live_open_quantity == Decimal("0.001")
    assert_exposure_safe(still_reserved)

    await simulator.cancel_order(SYMBOL, first_child.client_order_id)
    await service.reconcile_execution(execution.execution_id)
    execution = await service.run_once(execution.execution_id)

    assert len(execution.child_orders) == 2
    assert execution.child_orders[1].submitted_quantity == Decimal("0.002")
    assert execution.child_orders[1].submitted_quantity > first_child.submitted_quantity
    assert_exposure_safe(execution)


async def test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(
        request(
            algorithm=Algorithm.TWAP,
            target_position=Decimal("1.000"),
            duration=100,
            parameters=ExecutionParameters(number_of_slices=10),
        )
    )

    clock.advance(9)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    before_boundary = await service.run_once(execution.execution_id)

    assert before_boundary.child_orders == []
    assert before_boundary.status is ExecutionStatus.RUNNING

    clock.advance(1)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    at_boundary = await service.run_once(execution.execution_id)

    assert at_boundary.child_orders[0].submitted_quantity == Decimal("0.100")
    assert at_boundary.status is ExecutionStatus.RUNNING
    assert_exposure_safe(at_boundary)


async def test_t6_tail_quantity_records_dust_and_never_rounds_up() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(
        request(
            algorithm=Algorithm.TWAP,
            target_position=Decimal("0.0025"),
            duration=10,
            deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
        )
    )

    assert execution.raw_required_quantity == Decimal("0.0025")
    assert execution.required_quantity == Decimal("0.002")
    assert execution.target_dust_quantity == Decimal("0.0005")

    clock.advance(10)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    execution = await service.run_once(execution.execution_id)

    assert execution.child_orders[0].submitted_quantity == Decimal("0.002")
    assert execution.child_orders[0].submitted_quantity < execution.raw_required_quantity

    await simulator.push_fill(
        execution.child_orders[0].client_order_id,
        Decimal("0.002"),
        execution.child_orders[0].price,
    )
    execution = await service.reconcile_execution(execution.execution_id)
    execution = await service.run_once(execution.execution_id)

    assert execution.exposure.confirmed_filled_quantity == Decimal("0.002")
    assert execution.required_quantity == Decimal("0.002")
    assert len(execution.child_orders) == 1
    assert execution.status is ExecutionStatus.COMPLETED
    assert_exposure_safe(execution)


async def test_t7_price_outside_range_waits_then_expires_without_invalid_order() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(
        request(lower=Decimal("49000"), upper=Decimal("49999"), duration=10)
    )

    before_deadline = await service.run_once(execution.execution_id)

    assert before_deadline.child_orders == []
    assert before_deadline.status is ExecutionStatus.RUNNING
    assert before_deadline.final_reason == "WAITING_FOR_PRICE_RANGE"

    clock.advance(10)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    expired = await service.run_once(execution.execution_id)

    assert expired.child_orders == []
    assert expired.status is ExecutionStatus.EXPIRED
    assert expired.final_reason == PRICE_OUTSIDE_RANGE
    assert expired.summary is not None
    assert expired.summary.metrics["price_bound_violations"] == 1
    assert expired.summary.metrics["unfilled_quantity"] == "0.01"


async def test_t7b_cancel_remainder_deadline_terminalizes_unfilled_and_partial_results() -> None:
    clock, simulator, service = await simulator_service()
    unfilled = await service.create_execution(request(duration=1))
    unfilled = await service.run_once(unfilled.execution_id)

    clock.advance(1)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    terminal_unfilled = await service.run_once(unfilled.execution_id)

    assert unfilled.child_orders[0].status is ChildOrderStatus.OPEN
    assert terminal_unfilled.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert terminal_unfilled.status is ExecutionStatus.EXPIRED
    assert terminal_unfilled.final_reason == DEADLINE_CANCEL_REMAINDER
    assert terminal_unfilled.exposure.reserved_exposure == Decimal("0")
    assert terminal_unfilled.summary is not None
    assert_exposure_safe(terminal_unfilled)

    partial = await service.create_execution(request(duration=1))
    partial = await service.run_once(partial.execution_id)
    await simulator.push_fill(
        partial.child_orders[0].client_order_id,
        Decimal("0.004"),
        partial.child_orders[0].price,
    )

    clock.advance(1)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    terminal_partial = await service.run_once(partial.execution_id)

    assert terminal_partial.status is ExecutionStatus.PARTIALLY_COMPLETED
    assert terminal_partial.final_reason == DEADLINE_CANCEL_REMAINDER
    assert terminal_partial.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert terminal_partial.exposure.reserved_exposure == Decimal("0")
    assert terminal_partial.summary is not None
    assert_exposure_safe(terminal_partial)


async def test_t8_stream_disconnect_pauses_submit_reconciles_then_resumes() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(request())
    execution = await service.run_once(execution.execution_id)

    await simulator.push_fill(
        execution.child_orders[0].client_order_id,
        Decimal("0.004"),
        execution.child_orders[0].price,
    )
    simulator.set_stream_health(user_stream_healthy=False)
    paused = await service.run_once(execution.execution_id)

    assert paused.final_reason == STREAM_HEALTH_DEGRADED_RECONCILED
    assert paused.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert len(paused.child_orders) == 1
    assert_exposure_safe(paused)

    await simulator.cancel_order(SYMBOL, paused.child_orders[0].client_order_id)
    await service.reconcile_execution(execution.execution_id)
    simulator.set_stream_health(user_stream_healthy=True)
    clock.advance(0.1)
    await push_fresh_market(clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00"))
    resumed = await service.run_once(execution.execution_id)

    assert len(resumed.child_orders) == 2
    assert resumed.child_orders[1].submitted_quantity == Decimal("0.006")
    assert resumed.exposure.live_open_quantity == Decimal("0.006")
    assert_exposure_safe(resumed)


async def test_t9_duplicate_fill_event_does_not_double_count_cumulative_fill() -> None:
    _, simulator, service = await simulator_service()
    execution = await service.create_execution(request())
    execution = await service.run_once(execution.execution_id)
    client_order_id = execution.child_orders[0].client_order_id

    fill = await simulator.push_fill(client_order_id, Decimal("0.003"), execution.child_orders[0].price)
    simulator.inject_reconciliation_fill(
        Fill(
            client_order_id=client_order_id,
            trade_id=fill.trade_id,
            cumulative_filled_quantity=Decimal("0.003"),
            last_filled_quantity=Decimal("0.003"),
            last_fill_price=fill.last_fill_price,
            event_time_ms=fill.event_time_ms,
            transaction_time_ms=fill.transaction_time_ms,
        )
    )
    simulator.inject_reconciliation_fill(
        Fill(
            client_order_id=client_order_id,
            trade_id="sim_trade_stale",
            cumulative_filled_quantity=Decimal("0.002"),
            last_filled_quantity=Decimal("0.002"),
            last_fill_price=fill.last_fill_price,
            event_time_ms=fill.event_time_ms,
            transaction_time_ms=fill.transaction_time_ms,
        )
    )

    execution = await service.reconcile_execution(execution.execution_id)

    assert execution.exposure.confirmed_filled_quantity == Decimal("0.003")
    assert execution.child_orders[0].confirmed_filled_quantity == Decimal("0.003")
    assert_exposure_safe(execution)

    second_reconcile = await service.reconcile_execution(execution.execution_id)

    assert second_reconcile.exposure.confirmed_filled_quantity == Decimal("0.003")
    assert second_reconcile.metric_counts["duplicate_events_ignored"] == 2

    await simulator.push_fill(client_order_id, Decimal("0.007"), execution.child_orders[0].price)
    completed = await service.reconcile_execution(execution.execution_id)

    assert completed.status is ExecutionStatus.COMPLETED
    assert completed.summary is not None
    assert completed.summary.metrics["duplicate_events_ignored"] == 2
    assert completed.summary.metrics["filled_quantity"] == "0.01"
    assert completed.summary.metrics["overfill_quantity"] == "0"


async def test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity() -> None:
    _, _, buy_service = await simulator_service(position=Decimal("-0.003"))
    buy_execution = await buy_service.create_execution(request(target_position=Decimal("0.002")))

    _, _, sell_service = await simulator_service(position=Decimal("0.004"))
    sell_execution = await sell_service.create_execution(request(target_position=Decimal("-0.002")))

    assert buy_execution.side is Side.BUY
    assert buy_execution.required_quantity == Decimal("0.005")
    assert sell_execution.side is Side.SELL
    assert sell_execution.required_quantity == Decimal("0.006")


def test_cancel_race_script_writes_required_artifacts(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "scripts/run_sim_cancel_race.py",
            "--output-dir",
            str(tmp_path),
        ],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    assert "SIMULATOR DEMO: Cancel/Fill Race" in result.stdout
    execution_dirs = [path for path in tmp_path.iterdir() if path.is_dir()]
    assert len(execution_dirs) == 1
    artifact_dir = execution_dirs[0]
    expected_files = {
        "request_snapshot.json",
        "execution_log.jsonl",
        "execution_summary.json",
        "child_orders.csv",
        "fills.csv",
        "timeline.csv",
        "twap_slice_ledger.csv",
    }
    assert expected_files <= {path.name for path in artifact_dir.iterdir()}

    request_snapshot = json.loads((artifact_dir / "request_snapshot.json").read_text())
    assert request_snapshot["algorithm"] == "CHASE"
    assert request_snapshot["symbol"] == SYMBOL
    assert "parameters" in request_snapshot
    assert "target_position" in request_snapshot
    assert "target_price_range" in request_snapshot
    assert "duration_seconds" in request_snapshot
    assert "deadline_policy" in request_snapshot

    log_lines = (artifact_dir / "execution_log.jsonl").read_text().splitlines()
    assert log_lines
    log_events = [json.loads(line) for line in log_lines]
    assert all(event["execution_id"] == artifact_dir.name for event in log_events)
    assert any("client_order_id" in event for event in log_events)
    assert any("final_reason" in event for event in log_events)

    summary = json.loads((artifact_dir / "execution_summary.json").read_text())
    assert summary["execution_id"] == artifact_dir.name
    assert "final_status" in summary
    assert "final_reason" in summary
    assert "required_quantity" in summary
    assert "exposure" in summary

    child_rows = list(csv.DictReader((artifact_dir / "child_orders.csv").open()))
    fill_rows = list(csv.DictReader((artifact_dir / "fills.csv").open()))
    timeline_rows = list(csv.DictReader((artifact_dir / "timeline.csv").open()))
    assert child_rows
    assert fill_rows
    assert timeline_rows
    assert {
        "child_order_id",
        "client_order_id",
        "status",
        "submitted_quantity",
        "filled_quantity",
        "remaining_quantity",
        "price",
    } <= set(child_rows[0])
    assert "client_order_id" in fill_rows[0]


def test_create_timeout_script_writes_default_artifacts_with_resolved_reason() -> None:
    result = subprocess.run(
        ["uv", "run", "python", "scripts/run_sim_create_timeout.py"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    artifact_line = next(
        line for line in result.stdout.splitlines() if line.startswith("artifact_dir=")
    )
    artifact_dir = Path(artifact_line.split("=", 1)[1])

    assert artifact_dir.exists()
    assert {
        "request_snapshot.json",
        "execution_log.jsonl",
        "execution_summary.json",
        "child_orders.csv",
        "fills.csv",
        "timeline.csv",
        "twap_slice_ledger.csv",
    } <= {path.name for path in artifact_dir.iterdir()}

    summary = json.loads((artifact_dir / "execution_summary.json").read_text())
    assert summary["final_reason"] == CREATE_TIMEOUT_RECONCILED
    assert summary["exposure"]["unknown_order_quantity"] == "0"
    assert summary["exposure"]["live_open_quantity"] == "0.010"

    log_events = [
        json.loads(line)
        for line in (artifact_dir / "execution_log.jsonl").read_text().splitlines()
    ]
    assert any(event["event"] == "create_timeout_unknown" for event in log_events)
    assert any(event["event"] == "reconciled_original_open" for event in log_events)


def test_simulator_demo_scripts_run_successfully_for_chase_and_cancel_race(tmp_path: Path) -> None:
    chase = subprocess.run(
        ["uv", "run", "python", "scripts/run_sim_chase.py"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    cancel_race = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "scripts/run_sim_cancel_race.py",
            "--output-dir",
            str(tmp_path),
        ],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    assert "SIMULATOR DEMO: Chase" in chase.stdout
    assert "execution_id=" in chase.stdout
    assert "client_order_ids=" in chase.stdout
    assert "SIMULATOR DEMO: Cancel/Fill Race" in cancel_race.stdout
    assert "execution_id=" in cancel_race.stdout
    assert "client_order_ids=" in cancel_race.stdout
