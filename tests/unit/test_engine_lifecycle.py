from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from pathlib import Path

from exchanges.base import (
    OrderCancelTimeout,
    OrderCreateTimeout,
    OrderRejected,
    TerminalOrderRejected,
)
from exchanges.simulator import DeterministicSimulator
from execution.clock import ManualClock
from execution.engine import ExecutionEngine
from execution.ids import make_client_order_prefix
from execution.models import (
    Algorithm,
    ChildOrder,
    ChildOrderStatus,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    ExecutionStatus,
    Fill,
    OrderRequest,
    PositionSnapshot,
    ReconciliationResult,
    Side,
    SymbolRules,
)
from execution.service import ExecutionService


SYMBOL = "BTCUSDT"


def execution_request(
    *,
    algorithm: Algorithm = Algorithm.CHASE,
    target_position: Decimal = Decimal("0.010"),
    lower: Decimal = Decimal("94000"),
    upper: Decimal = Decimal("97000"),
    duration: int = 100,
    deadline_policy: DeadlinePolicy = DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
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


async def fresh_service(
    *,
    clock: ManualClock | None = None,
    bid: Decimal = Decimal("95000.00"),
    ask: Decimal = Decimal("95001.00"),
    position: Decimal = Decimal("0"),
) -> tuple[ExecutionService, DeterministicSimulator, ManualClock]:
    clock = clock or ManualClock()
    simulator = DeterministicSimulator(clock=clock, position=position)
    await simulator.push_market_data(SYMBOL, bid, ask, exchange_event_time=10)
    return ExecutionService(simulator, clock=clock), simulator, clock


def test_static_single_submit_gate() -> None:
    source = Path("src/execution/engine.py").read_text()

    assert "def _submit_child_locked(" in source
    assert source.count(".submit_limit_order(") == 1


def test_terminal_child_status_is_not_resurrected_by_stale_reconciliation_status() -> None:
    engine = ExecutionEngine(DeterministicSimulator())
    child = ChildOrder(
        child_order_id="child_0001",
        client_order_id="ce_0123456789ab_1",
        symbol=SYMBOL,
        side=Side.BUY,
        submitted_quantity=Decimal("0.010"),
        price=Decimal("95000.00"),
        status=ChildOrderStatus.FILLED,
        confirmed_filled_quantity=Decimal("0.010"),
    )

    engine._set_child_status(child, ChildOrderStatus.OPEN)

    assert child.status is ChildOrderStatus.FILLED


async def test_chase_run_once_submits_one_safe_child() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())

    snapshot = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.RUNNING
    assert len(snapshot.child_orders) == 1
    assert snapshot.child_orders[0].status is ChildOrderStatus.OPEN
    assert snapshot.child_orders[0].submitted_quantity == Decimal("0.010")
    assert snapshot.exposure.live_open_quantity == Decimal("0.010")
    assert snapshot.exposure.reserved_exposure <= snapshot.required_quantity
    assert [order.client_order_id for order in reconciliation.orders] == [
        snapshot.child_orders[0].client_order_id
    ]


async def test_no_action_summary_duration_uses_execution_start_not_clock_origin() -> None:
    clock = ManualClock()
    clock.advance(10)
    service, _, _ = await fresh_service(clock=clock, position=Decimal("0.010"))

    execution = await service.create_execution(execution_request())

    assert execution.status is ExecutionStatus.COMPLETED
    assert execution.summary is not None
    assert execution.summary.metrics["actual_duration_seconds"] == "0"


async def test_running_start_time_is_captured_after_validation_io() -> None:
    class SlowValidationSimulator(DeterministicSimulator):
        async def get_position(self, symbol: str) -> PositionSnapshot:
            self.clock.advance(2)
            return await super().get_position(symbol)

        async def get_symbol_rules(self, symbol: str) -> SymbolRules:
            self.clock.advance(3)
            return await super().get_symbol_rules(symbol)

    clock = ManualClock()
    simulator = SlowValidationSimulator(clock=clock)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(simulator, clock=clock)

    execution = await service.create_execution(execution_request())

    assert execution.status is ExecutionStatus.RUNNING
    assert execution.started_monotonic == Decimal("5.0")


async def test_create_execution_rejects_normalized_quantity_below_min_quantity_before_running() -> None:
    service, simulator, _ = await fresh_service()
    simulator.set_symbol_rules(
        SymbolRules(
            symbol=SYMBOL,
            tick_size=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.005"),
            min_notional=Decimal("5"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC", "GTX", "IOC"}),
        )
    )

    execution = await service.create_execution(execution_request(target_position=Decimal("0.0045")))

    assert execution.status is ExecutionStatus.COMPLETED
    assert execution.final_reason == "UNTRADEABLE_TARGET_DUST"
    assert execution.required_quantity == Decimal("0.004")
    assert execution.child_orders == []
    assert execution.summary is not None
    assert execution.summary.metrics["actual_duration_seconds"] == "0"


async def test_create_execution_rejects_quantity_below_min_notional_at_best_legal_bound_before_running() -> None:
    service, simulator, _ = await fresh_service()
    rules = SymbolRules(
        symbol=SYMBOL,
        tick_size=Decimal("0.10"),
        quantity_step=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        min_notional=Decimal("100"),
        status="TRADING",
        supported_time_in_force=frozenset({"GTC", "GTX", "IOC"}),
    )
    simulator.set_symbol_rules(rules)

    execution = await service.create_execution(
        execution_request(target_position=Decimal("0.001"), upper=Decimal("97000"))
    )

    assert execution.status is ExecutionStatus.COMPLETED
    assert execution.final_reason == "UNTRADEABLE_TARGET_DUST"
    assert execution.required_quantity == Decimal("0.001")
    assert execution.required_quantity >= rules.min_quantity
    assert execution.required_quantity * Decimal("97000") < rules.min_notional
    assert execution.child_orders == []
    assert execution.summary is not None
    assert execution.summary.metrics["actual_duration_seconds"] == "0"


async def test_create_execution_rejects_buy_below_min_notional_after_rounding_upper_to_tick() -> None:
    service, simulator, _ = await fresh_service()
    simulator.set_symbol_rules(
        SymbolRules(
            symbol=SYMBOL,
            tick_size=Decimal("1"),
            quantity_step=Decimal("1"),
            min_quantity=Decimal("1"),
            min_notional=Decimal("100.50"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC", "GTX", "IOC"}),
        )
    )

    execution = await service.create_execution(
        execution_request(target_position=Decimal("1"), upper=Decimal("100.99"))
    )

    assert execution.status is ExecutionStatus.COMPLETED
    assert execution.final_reason == "UNTRADEABLE_TARGET_DUST"
    assert execution.required_quantity == Decimal("1")
    assert execution.child_orders == []
    assert execution.summary is not None


async def test_create_execution_allows_sell_when_lower_bound_notional_is_low() -> None:
    service, simulator, _ = await fresh_service(
        position=Decimal("1"),
        bid=Decimal("100"),
        ask=Decimal("101"),
    )
    simulator.set_symbol_rules(
        SymbolRules(
            symbol=SYMBOL,
            tick_size=Decimal("1"),
            quantity_step=Decimal("1"),
            min_quantity=Decimal("1"),
            min_notional=Decimal("100"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC", "GTX", "IOC"}),
        )
    )

    execution = await service.create_execution(
        execution_request(target_position=Decimal("0"), lower=Decimal("1"), upper=Decimal("200"))
    )
    snapshot = await service.run_once(execution.execution_id)

    assert execution.status is ExecutionStatus.RUNNING
    assert execution.side is Side.SELL
    assert snapshot.child_orders[0].status is ChildOrderStatus.OPEN
    assert snapshot.child_orders[0].price == Decimal("101")


async def test_aggressive_deadline_submits_non_post_only_marketable_limit() -> None:
    class RecordingSimulator(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            return await super().submit_limit_order(order_request)

    clock = ManualClock()
    simulator = RecordingSimulator(clock=clock)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(simulator, clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"))
    )
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    snapshot = await service.run_once(execution.execution_id)

    assert len(snapshot.child_orders) == 1
    assert snapshot.child_orders[0].status is ChildOrderStatus.OPEN
    assert snapshot.child_orders[0].price == Decimal("95001.00")
    assert len(simulator.submissions) == 1
    assert simulator.submissions[0].client_order_id == snapshot.child_orders[0].client_order_id
    assert simulator.submissions[0].price == Decimal("95001.00")
    assert simulator.submissions[0].post_only is False
    assert getattr(simulator.submissions[0], "time_in_force", None) == "IOC"


async def test_non_post_only_order_reject_fails_parent_without_deadline_masking() -> None:
    class AggressiveRejectSimulator(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            raise OrderRejected("IOC order rejected by exchange")

    clock = ManualClock()
    simulator = AggressiveRejectSimulator(clock=clock)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(simulator, clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"))
    )
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    rejected = await service.run_once(execution.execution_id)
    second = await service.run_once(execution.execution_id)

    assert len(simulator.submissions) == 1
    assert simulator.submissions[0].post_only is False
    assert len(rejected.child_orders) == 1
    assert rejected.child_orders[0].status is ChildOrderStatus.REJECTED
    assert rejected.status is ExecutionStatus.FAILED
    assert rejected.final_reason == "TERMINAL_ORDER_REJECTED: IOC order rejected by exchange"
    assert len(second.child_orders) == 1
    assert second.status is ExecutionStatus.FAILED
    assert second.final_reason == "TERMINAL_ORDER_REJECTED: IOC order rejected by exchange"


async def test_aggressive_deadline_does_not_cancel_its_own_final_attempt() -> None:
    clock = ManualClock()
    params = ExecutionParameters(child_order_timeout_seconds=100)
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"), parameters=params)
    )
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    final_attempt = await service.run_once(execution.execution_id)
    second_run = await service.run_once(execution.execution_id)

    assert len(final_attempt.child_orders) == 1
    assert final_attempt.child_orders[0].price == Decimal("95001.00")
    assert len(second_run.child_orders) == 1
    assert second_run.child_orders[0].client_order_id == final_attempt.child_orders[0].client_order_id
    assert second_run.child_orders[0].status is ChildOrderStatus.OPEN


async def test_aggressive_deadline_terminalizes_after_final_attempt_timeout() -> None:
    clock = ManualClock()
    params = ExecutionParameters(child_order_timeout_seconds=1)
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"), parameters=params)
    )
    await service.run_once(execution.execution_id)
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)
    final_attempt = await service.run_once(execution.execution_id)

    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=30)
    terminal = await service.run_once(execution.execution_id)

    assert len(final_attempt.child_orders) == 2
    assert final_attempt.child_orders[1].price == Decimal("95001.00")
    assert final_attempt.child_orders[1].status is ChildOrderStatus.OPEN
    assert len(terminal.child_orders) == 2
    assert terminal.child_orders[1].status is ChildOrderStatus.CANCELLED
    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.final_reason == "DEADLINE_AGGRESSIVE_ATTEMPTED"
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.summary is not None


async def test_aggressive_deadline_terminalizes_partial_after_final_attempt_timeout() -> None:
    clock = ManualClock()
    params = ExecutionParameters(child_order_timeout_seconds=1)
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"), parameters=params)
    )
    await service.run_once(execution.execution_id)
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)
    final_attempt = await service.run_once(execution.execution_id)
    await simulator.push_fill(
        final_attempt.child_orders[1].client_order_id,
        Decimal("0.004"),
        final_attempt.child_orders[1].price,
    )

    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=30)
    terminal = await service.run_once(execution.execution_id)

    assert len(terminal.child_orders) == 2
    assert terminal.child_orders[1].status is ChildOrderStatus.CANCELLED
    assert terminal.status is ExecutionStatus.PARTIALLY_COMPLETED
    assert terminal.final_reason == "DEADLINE_AGGRESSIVE_ATTEMPTED"
    assert terminal.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.summary is not None


async def test_aggressive_deadline_cancels_passive_child_before_marketable_replacement() -> None:
    class RecordingSimulator(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            return await super().submit_limit_order(order_request)

    clock = ManualClock()
    params = ExecutionParameters(child_order_timeout_seconds=100)
    simulator = RecordingSimulator(clock=clock)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(simulator, clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"), parameters=params)
    )
    passive = await service.run_once(execution.execution_id)

    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)
    final_attempt = await service.run_once(execution.execution_id)

    assert len(passive.child_orders) == 1
    assert passive.child_orders[0].price == Decimal("95000.00")
    assert simulator.submissions[0].post_only is True
    assert len(final_attempt.child_orders) == 2
    assert final_attempt.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert final_attempt.child_orders[1].status is ChildOrderStatus.OPEN
    assert final_attempt.child_orders[1].price == Decimal("95001.00")
    assert simulator.submissions[1].client_order_id == final_attempt.child_orders[1].client_order_id
    assert simulator.submissions[1].post_only is False
    assert final_attempt.exposure.live_open_quantity == Decimal("0.010")
    assert final_attempt.exposure.reserved_exposure == Decimal("0.010")


async def test_aggressive_deadline_rejects_buy_when_marketable_price_exceeds_upper_bound() -> None:
    class RecordingSimulator(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            return await super().submit_limit_order(order_request)

    clock = ManualClock()
    simulator = RecordingSimulator(clock=clock)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(simulator, clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95000.90"))
    )
    prefix = make_client_order_prefix(execution.execution_id)
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    snapshot = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.EXPIRED
    assert snapshot.final_reason == "PRICE_OUTSIDE_RANGE"
    assert snapshot.child_orders == []
    assert simulator.submissions == []
    assert reconciliation.orders == []


async def test_create_timeout_discoverable_reserves_unknown_until_run_once_maps_live_open() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout(prefix)

    timed_out = await service.run_once(execution.execution_id)
    reconciled_by_run_once = await service.run_once(execution.execution_id)

    assert len(timed_out.child_orders) == 1
    assert timed_out.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert timed_out.exposure.unknown_order_quantity == Decimal("0.010")
    assert timed_out.exposure.reserved_exposure == timed_out.required_quantity
    assert len(reconciled_by_run_once.child_orders) == 1
    assert reconciled_by_run_once.child_orders[0].client_order_id == timed_out.child_orders[0].client_order_id
    assert reconciled_by_run_once.child_orders[0].status is ChildOrderStatus.OPEN
    assert reconciled_by_run_once.exposure.unknown_order_quantity == Decimal("0")
    assert reconciled_by_run_once.exposure.live_open_quantity == Decimal("0.010")


async def test_create_timeout_not_found_clears_unknown_and_retries_with_new_client_order_id() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_create_timeout_not_found(prefix)

    timed_out = await service.run_once(execution.execution_id)
    reconciled = await service.reconcile_execution(execution.execution_id)
    retried = await service.run_once(execution.execution_id)

    assert timed_out.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")
    assert len(retried.child_orders) == 2
    assert retried.child_orders[0].status is ChildOrderStatus.REJECTED
    assert retried.child_orders[0].terminal_reason == "CREATE_TIMEOUT_ORDER_NOT_FOUND"
    assert retried.child_orders[1].status is ChildOrderStatus.OPEN
    assert retried.child_orders[1].client_order_id != retried.child_orders[0].client_order_id


async def test_adapter_level_create_timeout_reserves_unknown_exposure() -> None:
    class TimeoutAdapter(DeterministicSimulator):
        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            raise OrderCreateTimeout(f"ambiguous create for {order_request.client_order_id}")

    clock = ManualClock()
    adapter = TimeoutAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    snapshot = await service.run_once(execution.execution_id)

    assert len(snapshot.child_orders) == 1
    assert snapshot.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert snapshot.exposure.unknown_order_quantity == Decimal("0.010")
    assert snapshot.exposure.reserved_exposure == snapshot.required_quantity


async def test_partially_filled_create_response_immediately_confirms_parent_fill() -> None:
    class PartialCreateAdapter(DeterministicSimulator):
        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            order = self._create_open_order(order_request)
            order.confirmed_filled_quantity = Decimal("0.004")
            order.status = ChildOrderStatus.PARTIALLY_FILLED
            return order

    clock = ManualClock()
    adapter = PartialCreateAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.child_orders[0].status is ChildOrderStatus.PARTIALLY_FILLED
    assert snapshot.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.live_open_quantity == Decimal("0.006")
    assert snapshot.exposure.confirmed_filled_quantity + snapshot.exposure.reserved_exposure <= snapshot.required_quantity


async def test_cancelled_create_response_from_pending_submit_clears_exposure() -> None:
    class CancelledCreateAdapter(DeterministicSimulator):
        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            return ChildOrder(
                child_order_id=order_request.child_order_id,
                client_order_id=order_request.client_order_id,
                symbol=order_request.symbol,
                side=order_request.side,
                submitted_quantity=order_request.quantity,
                price=order_request.price,
                status=ChildOrderStatus.CANCELLED,
                raw_status="EXPIRED",
            )

    clock = ManualClock()
    adapter = CancelledCreateAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"))
    )
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert snapshot.child_orders[0].confirmed_filled_quantity == Decimal("0")
    assert snapshot.exposure.confirmed_filled_quantity == Decimal("0")
    assert snapshot.exposure.reserved_exposure == Decimal("0")


async def test_cancelled_create_response_with_partial_fill_confirms_fill_and_clears_exposure() -> None:
    class PartiallyFilledCancelledCreateAdapter(DeterministicSimulator):
        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            return ChildOrder(
                child_order_id=order_request.child_order_id,
                client_order_id=order_request.client_order_id,
                symbol=order_request.symbol,
                side=order_request.side,
                submitted_quantity=order_request.quantity,
                price=order_request.price,
                status=ChildOrderStatus.CANCELLED,
                confirmed_filled_quantity=Decimal("0.004"),
                raw_status="EXPIRED",
            )

    clock = ManualClock()
    adapter = PartiallyFilledCancelledCreateAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(
        execution_request(duration=1, upper=Decimal("95001.00"))
    )
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert snapshot.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.reserved_exposure == Decimal("0")


async def test_exact_create_timeout_lookup_maps_found_order_to_live_open() -> None:
    class ExactLookupAdapter(DeterministicSimulator):
        stored_order: ChildOrder | None = None
        lookup_client_order_ids: list[str]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.lookup_client_order_ids = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.stored_order = self._create_open_order(order_request)
            raise OrderCreateTimeout(f"ambiguous create for {order_request.client_order_id}")

        async def get_order_by_client_order_id(
            self,
            symbol: str,
            client_order_id: str,
        ) -> ChildOrder | None:
            self.lookup_client_order_ids.append(client_order_id)
            return self.stored_order

        async def reconcile_orders_and_fills(
            self,
            symbol: str,
            client_order_prefix: str | None = None,
        ) -> ReconciliationResult:
            return ReconciliationResult(orders=[], fills=[])

    clock = ManualClock()
    adapter = ExactLookupAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    timed_out = await service.run_once(execution.execution_id)
    reconciled = await service.reconcile_execution(execution.execution_id)

    assert timed_out.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert adapter.lookup_client_order_ids == [timed_out.child_orders[0].client_order_id]
    assert reconciled.child_orders[0].status is ChildOrderStatus.OPEN
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")
    assert reconciled.exposure.live_open_quantity == Decimal("0.010")
    assert reconciled.final_reason == "CREATE_TIMEOUT_RECONCILED"


async def test_exact_create_timeout_lookup_not_found_clears_unknown_without_broad_warning() -> None:
    class ExactNotFoundAdapter(DeterministicSimulator):
        lookup_client_order_ids: list[str]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.lookup_client_order_ids = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            raise OrderCreateTimeout(f"ambiguous create for {order_request.client_order_id}")

        async def get_order_by_client_order_id(
            self,
            symbol: str,
            client_order_id: str,
        ) -> ChildOrder | None:
            self.lookup_client_order_ids.append(client_order_id)
            return None

        async def reconcile_orders_and_fills(
            self,
            symbol: str,
            client_order_prefix: str | None = None,
        ) -> ReconciliationResult:
            return ReconciliationResult(orders=[], fills=[])

    clock = ManualClock()
    adapter = ExactNotFoundAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    timed_out = await service.run_once(execution.execution_id)
    reconciled = await service.reconcile_execution(execution.execution_id)
    retried = await service.run_once(execution.execution_id)

    assert timed_out.child_orders[0].status is ChildOrderStatus.UNKNOWN
    assert adapter.lookup_client_order_ids == [timed_out.child_orders[0].client_order_id]
    assert reconciled.child_orders[0].status is ChildOrderStatus.REJECTED
    assert reconciled.child_orders[0].terminal_reason == "CREATE_TIMEOUT_ORDER_NOT_FOUND"
    assert reconciled.exposure.unknown_order_quantity == Decimal("0")
    assert len(retried.child_orders) == 2
    assert retried.child_orders[1].client_order_id != retried.child_orders[0].client_order_id


async def test_terminal_exchange_reject_fails_execution_without_repeated_retry() -> None:
    class TerminalRejectAdapter(DeterministicSimulator):
        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            raise TerminalOrderRejected("MARGIN_INSUFFICIENT")

    clock = ManualClock()
    adapter = TerminalRejectAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    failed = await service.run_once(execution.execution_id)
    second = await service.run_once(execution.execution_id)

    assert len(failed.child_orders) == 1
    assert failed.child_orders[0].status is ChildOrderStatus.REJECTED
    assert failed.child_orders[0].terminal_reason == "MARGIN_INSUFFICIENT"
    assert failed.status is ExecutionStatus.FAILED
    assert failed.final_reason == "TERMINAL_ORDER_REJECTED: MARGIN_INSUFFICIENT"
    assert failed.exposure.reserved_exposure == Decimal("0")
    assert failed.summary is not None
    assert len(second.child_orders) == 1
    assert second.status is ExecutionStatus.FAILED


async def test_retryable_order_reject_does_not_fail_parent_execution() -> None:
    class RetryableRejectAdapter(DeterministicSimulator):
        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            raise OrderRejected("post-only order would cross the current ask")

    clock = ManualClock()
    adapter = RetryableRejectAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    snapshot = await service.run_once(execution.execution_id)

    assert len(snapshot.child_orders) == 1
    assert snapshot.child_orders[0].status is ChildOrderStatus.REJECTED
    assert snapshot.status is ExecutionStatus.RUNNING
    assert snapshot.summary is None
    assert snapshot.exposure.reserved_exposure == Decimal("0")


async def test_retryable_order_reject_waits_for_backoff_and_fresh_passive_price() -> None:
    class RejectOnceAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            if len(self.submissions) == 1:
                raise OrderRejected("post-only order would cross the current ask")
            return self._create_open_order(order_request)

    clock = ManualClock()
    adapter = RejectOnceAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    rejected = await service.run_once(execution.execution_id)
    immediate = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)
    stale_price = await service.run_once(execution.execution_id)
    await adapter.push_market_data(SYMBOL, Decimal("95000.10"), Decimal("95001.10"), exchange_event_time=30)
    retried = await service.run_once(execution.execution_id)

    assert len(adapter.submissions) == 2
    assert rejected.child_orders[0].status is ChildOrderStatus.REJECTED
    assert len(immediate.child_orders) == 1
    assert immediate.final_reason == "RETRYABLE_ORDER_REJECT_BACKOFF"
    assert len(stale_price.child_orders) == 1
    assert stale_price.final_reason == "RETRYABLE_ORDER_REJECT_WAITING_FOR_FRESH_QUOTE"
    assert len(retried.child_orders) == 2
    assert retried.child_orders[1].status is ChildOrderStatus.OPEN
    assert retried.child_orders[1].price == Decimal("95000.10")
    assert retried.final_reason is None


async def test_retryable_order_reject_buy_unblocks_when_ask_changes_even_if_passive_bid_unchanged() -> None:
    class RejectOnceAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            if len(self.submissions) == 1:
                raise OrderRejected("post-only order would cross the current ask")
            return self._create_open_order(order_request)

    clock = ManualClock()
    adapter = RejectOnceAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    rejected = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95002.00"), exchange_event_time=20)
    retried = await service.run_once(execution.execution_id)

    assert rejected.child_orders[0].price == Decimal("95000.00")
    assert len(adapter.submissions) == 2
    assert retried.child_orders[1].status is ChildOrderStatus.OPEN
    assert retried.child_orders[1].price == Decimal("95000.00")


async def test_retryable_order_reject_sell_unblocks_when_bid_changes_even_if_passive_ask_unchanged() -> None:
    class RejectOnceAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            if len(self.submissions) == 1:
                raise OrderRejected("post-only order would cross the current bid")
            return self._create_open_order(order_request)

    clock = ManualClock()
    adapter = RejectOnceAdapter(clock=clock, position=Decimal("0.010"))
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request(target_position=Decimal("0")))

    rejected = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("94999.00"), Decimal("95001.00"), exchange_event_time=20)
    retried = await service.run_once(execution.execution_id)

    assert rejected.side is Side.SELL
    assert rejected.child_orders[0].price == Decimal("95001.00")
    assert len(adapter.submissions) == 2
    assert retried.child_orders[1].status is ChildOrderStatus.OPEN
    assert retried.child_orders[1].price == Decimal("95001.00")


async def test_retryable_order_reject_limit_terminalizes_after_three_consecutive_rejects() -> None:
    class AlwaysRejectAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            raise OrderRejected("post-only order would cross the current ask")

    clock = ManualClock()
    adapter = AlwaysRejectAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    first = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.10"), Decimal("95001.10"), exchange_event_time=20)
    second = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.20"), Decimal("95001.20"), exchange_event_time=30)
    terminal = await service.run_once(execution.execution_id)
    after_terminal = await service.run_once(execution.execution_id)

    assert first.status is ExecutionStatus.RUNNING
    assert second.status is ExecutionStatus.RUNNING
    assert len(adapter.submissions) == 3
    assert len(terminal.child_orders) == 3
    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.final_reason == "RETRYABLE_ORDER_REJECT_LIMIT_REACHED"
    assert terminal.summary is not None
    assert terminal.summary.metrics["retryable_order_reject_streak"] == 3
    assert terminal.summary.metrics["retryable_order_reject_limit"] == 3
    assert len(after_terminal.child_orders) == 3


async def test_retryable_order_reject_limit_uses_request_parameter() -> None:
    class AlwaysRejectAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            raise OrderRejected("post-only order would cross the current ask")

    clock = ManualClock()
    adapter = AlwaysRejectAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(
        execution_request(
            parameters=ExecutionParameters(max_post_only_reject_retries=2),
        )
    )

    first = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.10"), Decimal("95001.10"), exchange_event_time=20)
    terminal = await service.run_once(execution.execution_id)

    assert first.status is ExecutionStatus.RUNNING
    assert len(adapter.submissions) == 2
    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.final_reason == "RETRYABLE_ORDER_REJECT_LIMIT_REACHED"
    assert terminal.summary is not None
    assert terminal.summary.metrics["retryable_order_reject_streak"] == 2
    assert terminal.summary.metrics["retryable_order_reject_limit"] == 2


async def test_retryable_order_reject_limit_terminalizes_partially_completed_after_fill() -> None:
    class PartialThenRejectAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            if len(self.submissions) == 1:
                return ChildOrder(
                    child_order_id=order_request.child_order_id,
                    client_order_id=order_request.client_order_id,
                    symbol=order_request.symbol,
                    side=order_request.side,
                    submitted_quantity=order_request.quantity,
                    price=order_request.price,
                    status=ChildOrderStatus.CANCELLED,
                    confirmed_filled_quantity=Decimal("0.004"),
                    raw_status="EXPIRED",
                )
            raise OrderRejected("post-only order would cross the current ask")

    clock = ManualClock()
    adapter = PartialThenRejectAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    partial = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.10"), Decimal("95001.10"), exchange_event_time=20)
    await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.20"), Decimal("95001.20"), exchange_event_time=30)
    await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.30"), Decimal("95001.30"), exchange_event_time=40)
    terminal = await service.run_once(execution.execution_id)

    assert partial.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert len(adapter.submissions) == 4
    assert terminal.status is ExecutionStatus.PARTIALLY_COMPLETED
    assert terminal.final_reason == "RETRYABLE_ORDER_REJECT_LIMIT_REACHED"
    assert terminal.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert terminal.summary is not None


async def test_retryable_order_reject_streak_resets_after_immediate_successful_child() -> None:
    class RejectThenFillAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            if len(self.submissions) in {1, 2, 4}:
                raise OrderRejected("post-only order would cross the current ask")
            return ChildOrder(
                child_order_id=order_request.child_order_id,
                client_order_id=order_request.client_order_id,
                symbol=order_request.symbol,
                side=order_request.side,
                submitted_quantity=order_request.quantity,
                price=order_request.price,
                status=ChildOrderStatus.CANCELLED,
                confirmed_filled_quantity=Decimal("0.004"),
                raw_status="EXPIRED",
            )

    clock = ManualClock()
    adapter = RejectThenFillAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.10"), Decimal("95001.10"), exchange_event_time=20)
    await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.20"), Decimal("95001.20"), exchange_event_time=30)
    filled = await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.30"), Decimal("95001.30"), exchange_event_time=40)
    retried_after_fill = await service.run_once(execution.execution_id)

    assert filled.consecutive_retryable_order_rejects == 0
    assert len(adapter.submissions) == 4
    assert retried_after_fill.status is ExecutionStatus.RUNNING
    assert retried_after_fill.consecutive_retryable_order_rejects == 1
    assert retried_after_fill.exposure.confirmed_filled_quantity == Decimal("0.004")


async def test_retryable_order_reject_streak_resets_after_unknown_reconciles_to_filled() -> None:
    class RejectThenUnknownFillAdapter(DeterministicSimulator):
        submissions: list[OrderRequest]
        stored_order: ChildOrder | None

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            self.submissions = []
            self.stored_order = None

        async def submit_limit_order(self, order_request: OrderRequest) -> ChildOrder:
            self.submissions.append(order_request)
            if len(self.submissions) in {1, 2, 4}:
                raise OrderRejected("post-only order would cross the current ask")
            self.stored_order = ChildOrder(
                child_order_id=order_request.child_order_id,
                client_order_id=order_request.client_order_id,
                symbol=order_request.symbol,
                side=order_request.side,
                submitted_quantity=order_request.quantity,
                price=order_request.price,
                status=ChildOrderStatus.CANCELLED,
                confirmed_filled_quantity=Decimal("0.004"),
                raw_status="EXPIRED",
            )
            raise OrderCreateTimeout(f"ambiguous create for {order_request.client_order_id}")

        async def get_order_by_client_order_id(
            self,
            symbol: str,
            client_order_id: str,
        ) -> ChildOrder | None:
            return self.stored_order

        async def reconcile_orders_and_fills(
            self,
            symbol: str,
            client_order_prefix: str | None = None,
        ) -> ReconciliationResult:
            return ReconciliationResult(orders=[], fills=[])

    clock = ManualClock()
    adapter = RejectThenUnknownFillAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())

    await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.10"), Decimal("95001.10"), exchange_event_time=20)
    await service.run_once(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.20"), Decimal("95001.20"), exchange_event_time=30)
    unknown = await service.run_once(execution.execution_id)
    reconciled = await service.reconcile_execution(execution.execution_id)
    clock.advance(1)
    await adapter.push_market_data(SYMBOL, Decimal("95000.30"), Decimal("95001.30"), exchange_event_time=40)
    retried_after_reconcile = await service.run_once(execution.execution_id)

    assert unknown.child_orders[2].status is ChildOrderStatus.UNKNOWN
    assert reconciled.child_orders[2].status is ChildOrderStatus.CANCELLED
    assert reconciled.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert reconciled.consecutive_retryable_order_rejects == 0
    assert len(adapter.submissions) == 4
    assert retried_after_reconcile.status is ExecutionStatus.RUNNING
    assert retried_after_reconcile.consecutive_retryable_order_rejects == 1
    assert retried_after_reconcile.exposure.confirmed_filled_quantity == Decimal("0.004")


async def test_adapter_level_cancel_timeout_keeps_pending_cancel_exposure_until_reconcile() -> None:
    class CancelTimeoutAdapter(DeterministicSimulator):
        async def cancel_order(self, symbol: str, client_order_id: str) -> ChildOrder:
            raise OrderCancelTimeout(f"ambiguous cancel for {client_order_id}")

        async def reconcile_orders_and_fills(
            self,
            symbol: str,
            client_order_prefix: str | None = None,
        ) -> ReconciliationResult:
            return ReconciliationResult(orders=[], fills=[])

    clock = ManualClock()
    adapter = CancelTimeoutAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)

    cancelled = await service.cancel_execution(execution.execution_id)

    assert len(opened.child_orders) == 1
    assert cancelled.child_orders[0].status is ChildOrderStatus.PENDING_CANCEL
    assert cancelled.exposure.pending_cancel_quantity == Decimal("0.010")
    assert cancelled.exposure.live_open_quantity == Decimal("0")
    assert cancelled.exposure.reserved_exposure == Decimal("0.010")


async def test_child_order_timeout_cancels_stale_child_and_replaces_remaining_quantity() -> None:
    clock = ManualClock()
    params = ExecutionParameters(child_order_timeout_seconds=1)
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(execution_request(parameters=params))
    first = await service.run_once(execution.execution_id)
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    replaced = await service.run_once(execution.execution_id)

    assert len(first.child_orders) == 1
    assert len(replaced.child_orders) == 2
    assert replaced.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert replaced.child_orders[1].status is ChildOrderStatus.OPEN
    assert replaced.child_orders[1].submitted_quantity == Decimal("0.010")
    assert replaced.child_orders[1].client_order_id != first.child_orders[0].client_order_id
    assert replaced.exposure.live_open_quantity == Decimal("0.010")
    assert replaced.exposure.reserved_exposure == Decimal("0.010")


async def test_child_order_timeout_fill_during_cancel_reduces_replacement_size() -> None:
    clock = ManualClock()
    params = ExecutionParameters(child_order_timeout_seconds=1)
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(execution_request(parameters=params))
    first = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_fill_during_cancel(prefix, Decimal("0.004"))
    clock.advance(1)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)

    replaced = await service.run_once(execution.execution_id)

    assert len(first.child_orders) == 1
    assert len(replaced.child_orders) == 2
    assert replaced.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert replaced.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert replaced.child_orders[1].status is ChildOrderStatus.OPEN
    assert replaced.child_orders[1].submitted_quantity == Decimal("0.006")
    assert replaced.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert replaced.exposure.live_open_quantity == Decimal("0.006")
    assert replaced.exposure.confirmed_filled_quantity + replaced.exposure.reserved_exposure <= Decimal("0.010")


async def test_fill_during_cancel_reduces_replacement_size_without_overfill() -> None:
    clock = ManualClock()
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(execution_request())
    first = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_fill_during_cancel(prefix, Decimal("0.004"))
    clock.advance(0.5)
    await simulator.push_market_data(SYMBOL, Decimal("96000.00"), Decimal("96001.00"), exchange_event_time=20)

    repriced = await service.run_once(execution.execution_id)

    assert len(first.child_orders) == 1
    assert len(repriced.child_orders) == 2
    assert repriced.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert repriced.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert repriced.child_orders[1].status is ChildOrderStatus.OPEN
    assert repriced.child_orders[1].submitted_quantity == Decimal("0.006")
    assert repriced.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert repriced.exposure.live_open_quantity == Decimal("0.006")
    assert repriced.exposure.confirmed_filled_quantity + repriced.exposure.reserved_exposure <= Decimal("0.010")


async def test_ambiguous_cancel_reconciled_open_keeps_live_exposure_without_replacement() -> None:
    clock = ManualClock()
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_cancel_reconcile_open(prefix)
    clock.advance(0.5)
    await simulator.push_market_data(SYMBOL, Decimal("96000.00"), Decimal("96001.00"), exchange_event_time=20)

    cancelled = await service.cancel_execution(execution.execution_id)
    after_run = await service.run_once(execution.execution_id)

    assert len(opened.child_orders) == 1
    assert cancelled.status is ExecutionStatus.CANCELLING
    assert len(after_run.child_orders) == 1
    assert after_run.child_orders[0].status is ChildOrderStatus.OPEN
    assert after_run.exposure.live_open_quantity == Decimal("0.010")
    assert after_run.exposure.reserved_exposure == Decimal("0.010")
    assert after_run.summary is None


async def test_manual_cancel_terminalizes_cancelled_after_no_fill_and_no_exposure() -> None:
    service, _, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)

    terminal = await service.cancel_execution(execution.execution_id)

    assert opened.child_orders[0].status is ChildOrderStatus.OPEN
    assert terminal.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert terminal.status is ExecutionStatus.CANCELLED
    assert terminal.final_reason == "CANCEL_REQUESTED"
    assert terminal.exposure.confirmed_filled_quantity == Decimal("0")
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.completed_monotonic is not None
    assert terminal.summary is not None


async def test_manual_cancel_terminalizes_partially_completed_after_partial_fill_and_no_exposure() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(
        opened.child_orders[0].client_order_id,
        Decimal("0.004"),
        Decimal("95000.00"),
    )

    terminal = await service.cancel_execution(execution.execution_id)

    assert terminal.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert terminal.status is ExecutionStatus.PARTIALLY_COMPLETED
    assert terminal.final_reason == "CANCEL_REQUESTED"
    assert terminal.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.completed_monotonic is not None
    assert terminal.summary is not None


async def test_cancelling_run_once_reconciles_existing_fill_without_replacement() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_cancel_reconcile_open(prefix)
    await service.cancel_execution(execution.execution_id)
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95000.00"))

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.status is ExecutionStatus.CANCELLING
    assert len(snapshot.child_orders) == 1
    assert snapshot.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.live_open_quantity == Decimal("0.006")


async def test_cancelling_run_once_reports_completed_when_target_fills() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_cancel_reconcile_open(prefix)
    await service.cancel_execution(execution.execution_id)
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.010"), Decimal("95000.00"))

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.status is ExecutionStatus.COMPLETED
    assert snapshot.final_reason == "TARGET_QUANTITY_FILLED"
    assert snapshot.exposure.confirmed_filled_quantity == Decimal("0.010")
    assert snapshot.exposure.reserved_exposure == Decimal("0")
    assert snapshot.summary is not None


async def test_chase_reprice_waits_for_min_interval_and_threshold() -> None:
    clock = ManualClock()
    params = ExecutionParameters(reprice_threshold_bps=Decimal("2.0"), minimum_reprice_interval_ms=500)
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(execution_request(parameters=params))
    opened = await service.run_once(execution.execution_id)

    clock.advance(0.499)
    await simulator.push_market_data(SYMBOL, Decimal("96000.00"), Decimal("96001.00"), exchange_event_time=20)
    before_interval = await service.run_once(execution.execution_id)

    clock.advance(0.001)
    await simulator.push_market_data(SYMBOL, Decimal("95000.10"), Decimal("95001.10"), exchange_event_time=30)
    below_threshold = await service.run_once(execution.execution_id)

    assert len(opened.child_orders) == 1
    assert len(before_interval.child_orders) == 1
    assert before_interval.child_orders[0].status is ChildOrderStatus.OPEN
    assert len(below_threshold.child_orders) == 1
    assert below_threshold.child_orders[0].status is ChildOrderStatus.OPEN
    assert below_threshold.child_orders[0].client_order_id == opened.child_orders[0].client_order_id


async def test_chase_reprice_is_counted_in_terminal_summary_metrics() -> None:
    clock = ManualClock()
    params = ExecutionParameters(reprice_threshold_bps=Decimal("2.0"), minimum_reprice_interval_ms=500)
    service, simulator, _ = await fresh_service(clock=clock)
    execution = await service.create_execution(execution_request(parameters=params))
    await service.run_once(execution.execution_id)

    clock.advance(0.5)
    await simulator.push_market_data(SYMBOL, Decimal("96000.00"), Decimal("96001.00"), exchange_event_time=20)
    repriced = await service.run_once(execution.execution_id)
    terminal = await service.cancel_execution(execution.execution_id)

    assert len(repriced.child_orders) == 2
    assert terminal.summary is not None
    assert terminal.summary.metrics["reprices"] == 1


async def test_summary_metrics_count_known_maker_and_taker_fills() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    child = opened.child_orders[0]

    simulator.inject_reconciliation_fill(
        Fill(
            client_order_id=child.client_order_id,
            trade_id="maker-trade",
            cumulative_filled_quantity=Decimal("0.004"),
            last_filled_quantity=Decimal("0.004"),
            last_fill_price=Decimal("95000.00"),
            event_time_ms=1,
            transaction_time_ms=1,
            is_maker=True,
        )
    )
    simulator.inject_reconciliation_fill(
        Fill(
            client_order_id=child.client_order_id,
            trade_id="taker-trade",
            cumulative_filled_quantity=Decimal("0.010"),
            last_filled_quantity=Decimal("0.006"),
            last_fill_price=Decimal("95001.00"),
            event_time_ms=2,
            transaction_time_ms=2,
            is_maker=False,
        )
    )

    terminal = await service.reconcile_execution(execution.execution_id)

    assert terminal.status is ExecutionStatus.COMPLETED
    assert terminal.summary is not None
    assert terminal.summary.metrics["maker_fills"] == 1
    assert terminal.summary.metrics["taker_fills"] == 1
    assert terminal.summary.metrics["maker_filled_quantity"] == Decimal("0.004")
    assert terminal.summary.metrics["taker_filled_quantity"] == Decimal("0.006")


async def test_twap_uses_absolute_schedule_and_carries_forward_unfilled_deficit() -> None:
    clock = ManualClock()
    service, simulator, _ = await fresh_service(
        clock=clock,
        bid=Decimal("100.00"),
        ask=Decimal("101.00"),
    )
    execution = await service.create_execution(
        execution_request(
            algorithm=Algorithm.TWAP,
            target_position=Decimal("1.000"),
            lower=Decimal("90"),
            upper=Decimal("110"),
            duration=100,
            parameters=ExecutionParameters(child_order_timeout_seconds=1000),
        )
    )
    clock.advance(30)
    await simulator.push_market_data(SYMBOL, Decimal("100.00"), Decimal("101.00"), exchange_event_time=20)

    first = await service.run_once(execution.execution_id)
    clock.advance(30)
    await simulator.push_market_data(SYMBOL, Decimal("100.00"), Decimal("101.00"), exchange_event_time=30)
    still_reserved = await service.run_once(execution.execution_id)
    await simulator.cancel_order(SYMBOL, first.child_orders[0].client_order_id)
    carried_forward = await service.run_once(execution.execution_id)

    assert first.child_orders[0].submitted_quantity == Decimal("0.300")
    assert len(still_reserved.child_orders) == 1
    assert carried_forward.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert carried_forward.child_orders[1].submitted_quantity == Decimal("0.600")

    clock.advance(40)
    await simulator.push_market_data(SYMBOL, Decimal("100.00"), Decimal("101.00"), exchange_event_time=40)
    cancelled = await service.cancel_execution(execution.execution_id)
    terminal = await service.run_once(cancelled.execution_id)

    assert terminal.summary is not None
    ledger = terminal.summary.metrics["twap_slice_ledger"]
    assert len(ledger) == 10
    assert ledger[3 - 1] == {
        "execution_id": terminal.execution_id,
        "slice_index": 3,
        "slice_start_seconds": Decimal("20"),
        "slice_end_seconds": Decimal("30"),
        "planned_cumulative_quantity": Decimal("0.300"),
        "planned_slice_quantity": Decimal("0.100"),
        "submitted_quantity": Decimal("0.300"),
        "open_quantity": Decimal("0"),
        "filled_quantity": Decimal("0"),
        "cancelled_quantity": Decimal("0.300"),
        "unfilled_quantity": Decimal("0.100"),
        "schedule_deficit": Decimal("0.300"),
        "child_order_ids": [carried_forward.child_orders[0].child_order_id],
        "client_order_ids": [carried_forward.child_orders[0].client_order_id],
    }
    assert ledger[6 - 1]["submitted_quantity"] == Decimal("0.600")
    assert ledger[6 - 1]["cancelled_quantity"] == Decimal("0.600")
    assert ledger[6 - 1]["unfilled_quantity"] == Decimal("0.100")
    assert ledger[6 - 1]["schedule_deficit"] == Decimal("0.600")


async def test_chase_terminal_summary_has_empty_twap_slice_ledger() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(
        opened.child_orders[0].client_order_id,
        Decimal("0.010"),
        Decimal("95000.00"),
    )

    terminal = await service.run_once(execution.execution_id)

    assert terminal.status is ExecutionStatus.COMPLETED
    assert terminal.summary is not None
    assert terminal.summary.metrics["twap_slice_ledger"] == []


async def test_price_outside_range_does_not_submit_before_deadline() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(execution_request(upper=Decimal("94999"), duration=5))
    prefix = make_client_order_prefix(execution.execution_id)

    waiting = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert waiting.status is ExecutionStatus.RUNNING
    assert waiting.final_reason == "WAITING_FOR_PRICE_RANGE"
    assert waiting.child_orders == []
    assert reconciliation.orders == []

    clock.advance(5)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)
    expired = await service.run_once(execution.execution_id)

    assert expired.status is ExecutionStatus.EXPIRED
    assert expired.final_reason == "PRICE_OUTSIDE_RANGE"
    assert expired.child_orders == []
    assert expired.summary is not None


async def test_price_outside_range_after_partial_fill_terminalizes_partially_completed() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(execution_request(upper=Decimal("95000.00"), duration=5))
    opened = await service.run_once(execution.execution_id)

    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95000.00"))
    await simulator.cancel_order(SYMBOL, opened.child_orders[0].client_order_id)
    clock.advance(5)
    await simulator.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=20)
    partial = await service.run_once(execution.execution_id)

    assert partial.status is ExecutionStatus.PARTIALLY_COMPLETED
    assert partial.final_reason == "PRICE_OUTSIDE_RANGE"
    assert partial.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert partial.summary is not None


async def test_cancel_remainder_deadline_cancels_open_child_and_terminalizes() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(
        execution_request(duration=1, deadline_policy=DeadlinePolicy.CANCEL_REMAINDER)
    )
    opened = await service.run_once(execution.execution_id)

    clock.advance(1)
    await simulator.push_market_data(
        SYMBOL,
        Decimal("95000.00"),
        Decimal("95001.00"),
        exchange_event_time=30,
    )
    terminal = await service.run_once(execution.execution_id)

    assert opened.child_orders[0].status is ChildOrderStatus.OPEN
    assert terminal.child_orders[0].status is ChildOrderStatus.CANCELLED
    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.final_reason == "DEADLINE_CANCEL_REMAINDER"
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.summary is not None


async def test_cancel_remainder_deadline_does_not_submit_when_first_run_is_after_deadline() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(
        execution_request(duration=1, deadline_policy=DeadlinePolicy.CANCEL_REMAINDER)
    )
    prefix = make_client_order_prefix(execution.execution_id)

    clock.advance(2)
    terminal = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.final_reason == "DEADLINE_CANCEL_REMAINDER"
    assert terminal.child_orders == []
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.summary is not None
    assert reconciliation.orders == []


async def test_price_outside_range_deadline_terminalizes_without_requiring_fresh_quote() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(
        execution_request(
            upper=Decimal("94999"),
            duration=1,
            deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
        )
    )

    waiting = await service.run_once(execution.execution_id)
    clock.advance(2)
    terminal = await service.run_once(execution.execution_id)

    assert waiting.status is ExecutionStatus.RUNNING
    assert waiting.final_reason == "WAITING_FOR_PRICE_RANGE"
    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.final_reason == "PRICE_OUTSIDE_RANGE"
    assert terminal.child_orders == []
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.summary is not None


async def test_cancel_remainder_deadline_reports_partially_completed_after_fill() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(
        execution_request(duration=1, deadline_policy=DeadlinePolicy.CANCEL_REMAINDER)
    )
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(
        opened.child_orders[0].client_order_id,
        Decimal("0.004"),
        Decimal("95000.00"),
    )

    clock.advance(1)
    await simulator.push_market_data(
        SYMBOL,
        Decimal("95000.00"),
        Decimal("95001.00"),
        exchange_event_time=30,
    )
    terminal = await service.run_once(execution.execution_id)

    assert terminal.status is ExecutionStatus.PARTIALLY_COMPLETED
    assert terminal.final_reason == "DEADLINE_CANCEL_REMAINDER"
    assert terminal.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert terminal.summary is not None


async def test_cancel_remainder_deadline_reports_completed_after_full_fill_during_cancel() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(
        execution_request(duration=1, deadline_policy=DeadlinePolicy.CANCEL_REMAINDER)
    )
    opened = await service.run_once(execution.execution_id)
    prefix = make_client_order_prefix(execution.execution_id)
    simulator.script_fill_during_cancel(prefix, Decimal("0.010"))

    clock.advance(1)
    await simulator.push_market_data(
        SYMBOL,
        Decimal("95000.00"),
        Decimal("95001.00"),
        exchange_event_time=30,
    )
    terminal = await service.run_once(execution.execution_id)

    assert opened.child_orders[0].status is ChildOrderStatus.OPEN
    assert terminal.status is ExecutionStatus.COMPLETED
    assert terminal.final_reason == "TARGET_QUANTITY_FILLED"
    assert terminal.exposure.confirmed_filled_quantity == Decimal("0.010")
    assert terminal.exposure.reserved_exposure == Decimal("0")
    assert terminal.summary is not None


async def test_post_only_unsupported_rejects_before_submit() -> None:
    service, simulator, _ = await fresh_service()
    simulator.set_symbol_rules(
        SymbolRules(
            symbol=SYMBOL,
            tick_size=Decimal("0.10"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC"}),
        )
    )
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)

    snapshot = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.EXPIRED
    assert snapshot.final_reason == "POST_ONLY_UNSUPPORTED"
    assert snapshot.child_orders == []
    assert reconciliation.orders == []


async def test_post_only_crossing_rejects_before_submit(monkeypatch) -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)

    monkeypatch.setattr("execution.engine.chase_desired_price", lambda *_args, **_kwargs: Decimal("95001.00"))

    snapshot = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.EXPIRED
    assert snapshot.final_reason == "POST_ONLY_CROSSES"
    assert snapshot.child_orders == []
    assert reconciliation.orders == []


async def test_stream_health_failure_pauses_new_submit_and_reconciles() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    simulator.set_stream_health(user_stream_healthy=False)

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.status is ExecutionStatus.RUNNING
    assert snapshot.final_reason == "STREAM_HEALTH_DEGRADED_RECONCILED"
    assert snapshot.child_orders == []
    assert snapshot.exposure.reserved_exposure == Decimal("0")


async def test_stream_health_failure_reconciles_existing_fill_without_new_submit() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95000.00"))
    simulator.set_stream_health(user_stream_healthy=False)

    snapshot = await service.run_once(execution.execution_id)

    assert snapshot.status is ExecutionStatus.RUNNING
    assert snapshot.final_reason == "STREAM_HEALTH_DEGRADED_RECONCILED"
    assert len(snapshot.child_orders) == 1
    assert snapshot.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.live_open_quantity == Decimal("0.006")


async def test_stale_rest_snapshot_cannot_reduce_child_cumulative_fill() -> None:
    class LowerRestSnapshotAdapter(DeterministicSimulator):
        async def reconcile_orders_and_fills(
            self,
            symbol: str,
            client_order_prefix: str | None = None,
        ) -> ReconciliationResult:
            result = await super().reconcile_orders_and_fills(
                symbol,
                client_order_prefix=client_order_prefix,
            )
            lowered_orders = []
            for order in result.orders:
                stale_order = deepcopy(order)
                stale_order.confirmed_filled_quantity = Decimal("0.001")
                stale_order.status = ChildOrderStatus.PARTIALLY_FILLED
                lowered_orders.append(stale_order)
            return ReconciliationResult(
                orders=lowered_orders,
                fills=result.fills,
                warnings=result.warnings,
            )

    clock = ManualClock()
    adapter = LowerRestSnapshotAdapter(clock=clock)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), exchange_event_time=10)
    service = ExecutionService(adapter, clock=clock)
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    await adapter.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95000.00"))

    snapshot = await service.reconcile_execution(execution.execution_id)

    assert snapshot.child_orders[0].confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert snapshot.exposure.live_open_quantity == Decimal("0.006")
    assert snapshot.exposure.confirmed_filled_quantity + snapshot.exposure.reserved_exposure <= snapshot.required_quantity


async def test_stale_market_data_pauses_without_new_submit_or_raise() -> None:
    service, simulator, clock = await fresh_service()
    execution = await service.create_execution(execution_request())
    prefix = make_client_order_prefix(execution.execution_id)
    clock.advance(2)

    snapshot = await service.run_once(execution.execution_id)
    reconciliation = await simulator.reconcile_orders_and_fills(SYMBOL, client_order_prefix=prefix)

    assert snapshot.status is ExecutionStatus.RUNNING
    assert snapshot.final_reason == "MARKET_DATA_STALE_RECONCILED"
    assert snapshot.child_orders == []
    assert snapshot.exposure.reserved_exposure == Decimal("0")
    assert reconciliation.orders == []


async def test_duplicate_fill_reconciliation_does_not_double_count_confirmed_fills() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request())
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95000.00"))

    first_reconcile = await service.reconcile_execution(execution.execution_id)
    second_reconcile = await service.reconcile_execution(execution.execution_id)

    assert first_reconcile.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert second_reconcile.exposure.confirmed_filled_quantity == Decimal("0.004")


async def test_summary_vwap_uses_actual_fill_price_not_child_limit_price() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.004")))
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95010.00"))

    completed = await service.reconcile_execution(execution.execution_id)

    assert completed.status is ExecutionStatus.COMPLETED
    assert completed.summary is not None
    assert completed.summary.metrics["execution_vwap"] == "95010"
    expected_slippage = (Decimal("95010.00") - Decimal("95000.50")) / Decimal("95000.50") * Decimal("10000")
    assert Decimal(completed.summary.metrics["slippage_bps"]) == expected_slippage


async def test_summary_vwap_uses_each_actual_fill_price_for_multiple_fills() -> None:
    service, simulator, _ = await fresh_service()
    execution = await service.create_execution(execution_request(target_position=Decimal("0.006")))
    opened = await service.run_once(execution.execution_id)
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.002"), Decimal("95000.00"))
    await simulator.push_fill(opened.child_orders[0].client_order_id, Decimal("0.004"), Decimal("95010.00"))

    completed = await service.reconcile_execution(execution.execution_id)

    expected_vwap = (
        Decimal("95000.00") * Decimal("0.002")
        + Decimal("95010.00") * Decimal("0.004")
    ) / Decimal("0.006")
    assert completed.status is ExecutionStatus.COMPLETED
    assert completed.summary is not None
    assert Decimal(completed.summary.metrics["execution_vwap"]) == expected_vwap
    assert completed.summary.metrics["duplicate_events_ignored"] == 0
