"""Execution engine for lifecycle, exposure accounting, and reconciliation."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING
from typing import Any

from algorithms.chase import ChaseDecision, chase_desired_price, should_reprice
from algorithms.twap import (
    effective_slice_elapsed,
    safe_child_quantity,
    scheduled_cumulative_quantity,
    scheduled_deficit,
)
from exchanges.base import (
    ExchangeAdapter,
    NoFreshMarketData,
    OrderCancelTimeout,
    OrderCreateTimeout,
    OrderRejected,
    TerminalOrderRejected,
)
from execution.clock import Clock, ManualClock
from execution import ids
from execution.events import ExecutionEventActor
from execution.models import (
    Algorithm,
    ChildOrder,
    ChildOrderStatus,
    DeadlinePolicy,
    ExecutionRequest,
    ExecutionStatus,
    ExecutionSummary,
    Exposure,
    MarketSnapshot,
    OrderRequest,
    PositionSnapshot,
    ReconciliationResult,
    Side,
    SymbolRules,
    TimeInForce,
    required_trade,
)
from execution.state_machine import InvalidStateTransition, transition_child, transition_execution
from observability.summary import execution_vwap, summary_metrics
from risk.decimal_math import floor_to_step, round_price
from risk.validation import (
    ValidationError,
    check_exposure_invariant,
    validate_child_order_safety,
    validate_order_shape,
)


NO_ACTION_TARGET_ALREADY_REACHED = "NO_ACTION_TARGET_ALREADY_REACHED"
CANCEL_REQUESTED = "CANCEL_REQUESTED"
CREATE_TIMEOUT_PENDING_RECONCILIATION = "CREATE_TIMEOUT_PENDING_RECONCILIATION"
CREATE_TIMEOUT_RECONCILED = "CREATE_TIMEOUT_RECONCILED"
CREATE_TIMEOUT_ORDER_NOT_FOUND = "CREATE_TIMEOUT_ORDER_NOT_FOUND"
PRICE_OUTSIDE_RANGE = "PRICE_OUTSIDE_RANGE"
WAITING_FOR_PRICE_RANGE = "WAITING_FOR_PRICE_RANGE"
DEADLINE_CANCEL_REMAINDER = "DEADLINE_CANCEL_REMAINDER"
DEADLINE_AGGRESSIVE_ATTEMPTED = "DEADLINE_AGGRESSIVE_ATTEMPTED"
POST_ONLY_CROSSES = "POST_ONLY_CROSSES"
POST_ONLY_UNSUPPORTED = "POST_ONLY_UNSUPPORTED"
ORDER_SAFETY_REJECTED = "ORDER_SAFETY_REJECTED"
TERMINAL_ORDER_REJECTED = "TERMINAL_ORDER_REJECTED"
STREAM_HEALTH_DEGRADED_RECONCILED = "STREAM_HEALTH_DEGRADED_RECONCILED"
MARKET_DATA_STALE_RECONCILED = "MARKET_DATA_STALE_RECONCILED"
TARGET_QUANTITY_FILLED = "TARGET_QUANTITY_FILLED"
UNTRADEABLE_TARGET_DUST = "UNTRADEABLE_TARGET_DUST"
RETRYABLE_ORDER_REJECT_BACKOFF = "RETRYABLE_ORDER_REJECT_BACKOFF"
RETRYABLE_ORDER_REJECT_WAITING_FOR_FRESH_QUOTE = "RETRYABLE_ORDER_REJECT_WAITING_FOR_FRESH_QUOTE"
RETRYABLE_ORDER_REJECT_LIMIT_REACHED = "RETRYABLE_ORDER_REJECT_LIMIT_REACHED"

RETRYABLE_ORDER_REJECT_BACKOFF_SECONDS = Decimal("1")


@dataclass
class ExposureTracker:
    """Tracks filled and reserved exposure buckets for one parent execution."""

    target_quantity: Decimal
    exposure: Exposure = field(default_factory=Exposure)
    seen_trade_ids: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self._require_non_negative(self.target_quantity)

    def available_to_submit(self) -> Decimal:
        available = (
            self.target_quantity
            - self.exposure.confirmed_filled_quantity
            - self.exposure.reserved_exposure
        )
        return available if available > Decimal("0") else Decimal("0")

    def check_can_submit(self, new_child_quantity: Decimal) -> None:
        self._require_non_negative(new_child_quantity)
        check_exposure_invariant(
            self.exposure,
            new_child_quantity,
            self.target_quantity,
        )

    def reserve_live_open(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.live_open_quantity += quantity

    def reserve_pending_submit(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.pending_submit_quantity += quantity

    def release_pending_submit(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        self.exposure.pending_submit_quantity = self._subtract_floor_zero(
            self.exposure.pending_submit_quantity,
            quantity,
        )

    def reserve_unknown_create(self, quantity: Decimal) -> None:
        self.check_can_submit(quantity)
        self.exposure.unknown_order_quantity += quantity

    def clear_unknown_create(self, quantity: Decimal | None = None) -> None:
        if quantity is None:
            self.exposure.unknown_order_quantity = Decimal("0")
            return

        self._require_non_negative(quantity)
        self.exposure.unknown_order_quantity = self._subtract_floor_zero(
            self.exposure.unknown_order_quantity,
            quantity,
        )

    def mark_pending_cancel(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        moved_quantity = min(quantity, self.exposure.live_open_quantity)
        self.exposure.live_open_quantity -= moved_quantity
        self.exposure.pending_cancel_quantity += moved_quantity

    def release_pending_cancel(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        self.exposure.pending_cancel_quantity = self._subtract_floor_zero(
            self.exposure.pending_cancel_quantity,
            quantity,
        )

    def set_live_open(self, quantity: Decimal) -> None:
        self._require_non_negative(quantity)
        self.exposure.live_open_quantity = quantity

    def apply_fill(self, trade_id: str | None, cumulative: Decimal) -> Decimal:
        self._require_non_negative(cumulative)
        if trade_id is not None:
            if trade_id in self.seen_trade_ids:
                return Decimal("0")
            self.seen_trade_ids.add(trade_id)

        if cumulative <= self.exposure.confirmed_filled_quantity:
            return Decimal("0")

        delta = cumulative - self.exposure.confirmed_filled_quantity
        self.exposure.confirmed_filled_quantity = cumulative
        return delta

    @staticmethod
    def _require_non_negative(quantity: Decimal) -> None:
        if quantity < Decimal("0"):
            raise ValidationError(f"quantity {quantity} cannot be negative")

    @staticmethod
    def _subtract_floor_zero(current: Decimal, quantity: Decimal) -> Decimal:
        remaining = current - quantity
        return remaining if remaining > Decimal("0") else Decimal("0")


@dataclass
class ExecutionRecord:
    """Mutable engine-owned state for one execution."""

    execution_id: str
    request: ExecutionRequest
    status: ExecutionStatus
    side: Side
    required_quantity: Decimal
    initial_position: PositionSnapshot
    raw_required_quantity: Decimal = Decimal("0")
    target_dust_quantity: Decimal = Decimal("0")
    final_reason: str | None = None
    child_orders: list[ChildOrder] = field(default_factory=list)
    summary: ExecutionSummary | None = None
    exposure_tracker: ExposureTracker | None = None
    last_child_sequence: int = 0
    started_monotonic: Decimal = Decimal("0")
    completed_monotonic: Decimal | None = None
    last_reprice_monotonic: Decimal | None = None
    arrival_bid: Decimal | None = None
    arrival_ask: Decimal | None = None
    max_reserved_exposure: Decimal = Decimal("0")
    metric_counts: dict[str, int] = field(default_factory=dict)
    seen_fill_trade_ids: set[str] = field(default_factory=set)
    ignored_fill_trade_ids: set[str] = field(default_factory=set)
    fill_vwap_inputs: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    trade_fill_vwap_inputs: dict[str, tuple[Decimal, Decimal]] = field(default_factory=dict)
    trade_fill_sort_keys: dict[str, tuple[int, int, str]] = field(default_factory=dict)
    anonymous_fill_vwap_inputs: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    maker_filled_quantity: Decimal = Decimal("0")
    taker_filled_quantity: Decimal = Decimal("0")
    child_submitted_monotonic: dict[str, Decimal] = field(default_factory=dict)
    aggressive_child_client_order_ids: set[str] = field(default_factory=set)
    consecutive_retryable_order_rejects: int = 0
    last_retryable_order_reject_monotonic: Decimal | None = None
    last_retryable_order_reject_price: Decimal | None = None
    last_retryable_order_reject_bid: Decimal | None = None
    last_retryable_order_reject_ask: Decimal | None = None
    last_retryable_order_reject_reason: str | None = None

    @property
    def exposure(self) -> Exposure:
        if self.exposure_tracker is None:
            return Exposure()
        return self.exposure_tracker.exposure


class UnknownExecution(LookupError):
    """Raised when an execution identifier is not known to the engine."""

    def __init__(self, execution_id: str) -> None:
        super().__init__(f"unknown execution: {execution_id}")
        self.execution_id = execution_id


class ExecutionEngine:
    """Owns execution lifecycle, child orders, exposure accounting, and reconciliation."""

    def __init__(self, adapter: ExchangeAdapter, clock: Clock | None = None) -> None:
        self._adapter = adapter
        self._clock = clock or getattr(adapter, "clock", None) or ManualClock()
        self._records: dict[str, ExecutionRecord] = {}
        self._actors: dict[str, ExecutionEventActor] = {}

    async def create_execution(self, request: ExecutionRequest) -> ExecutionRecord:
        position = await self._adapter.get_position(request.symbol)
        side, raw_required_quantity = required_trade(
            target_position=request.target_position,
            current_position=position.position,
        )
        rules: SymbolRules | None = None
        if raw_required_quantity == Decimal("0"):
            required_quantity = Decimal("0")
            target_dust_quantity = Decimal("0")
        else:
            rules = await self._adapter.get_symbol_rules(request.symbol)
            required_quantity = floor_to_step(raw_required_quantity, rules.quantity_step)
            target_dust_quantity = raw_required_quantity - required_quantity
        execution_id = ids.execution_id()
        record = ExecutionRecord(
            execution_id=execution_id,
            request=request,
            status=ExecutionStatus.CREATED,
            side=side,
            required_quantity=required_quantity,
            initial_position=position,
            raw_required_quantity=raw_required_quantity,
            target_dust_quantity=target_dust_quantity,
        )
        actor = ExecutionEventActor(execution_id)
        self._records[execution_id] = record
        self._actors[execution_id] = actor

        async def start() -> ExecutionRecord:
            record.status = transition_execution(record.status, ExecutionStatus.VALIDATING)
            if (
                side is Side.NO_ACTION
                or required_quantity == Decimal("0")
                or self._target_cannot_meet_min_quantity_or_buy_min_notional(record, rules)
            ):
                record.status = transition_execution(record.status, ExecutionStatus.COMPLETED)
                record.final_reason = (
                    NO_ACTION_TARGET_ALREADY_REACHED
                    if raw_required_quantity == Decimal("0")
                    else UNTRADEABLE_TARGET_DUST
                )
                completed_monotonic = self._now_decimal()
                record.started_monotonic = completed_monotonic
                record.completed_monotonic = completed_monotonic
                record.summary = self._summary(record)
                return self._snapshot(record)

            record.exposure_tracker = ExposureTracker(required_quantity)
            record.started_monotonic = self._now_decimal()
            record.status = transition_execution(record.status, ExecutionStatus.RUNNING)
            return self._snapshot(record)

        return await actor.apply(start)

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        record, actor = self._lookup_execution(execution_id)

        async def read() -> ExecutionRecord:
            return self._snapshot(record)

        return await actor.apply(read)

    async def cancel_execution(self, execution_id: str) -> ExecutionRecord:
        record, actor = self._lookup_execution(execution_id)

        async def cancel() -> ExecutionRecord:
            if record.status.is_terminal:
                return self._snapshot(record)

            if record.status is not ExecutionStatus.CANCELLING:
                record.status = transition_execution(record.status, ExecutionStatus.CANCELLING)
                record.final_reason = CANCEL_REQUESTED

            await self._reconcile_locked(record, exact_unknown_lookup=True)
            await self._cancel_active_children_locked(record)
            await self._reconcile_locked(record, exact_unknown_lookup=True)
            if self._target_filled(record):
                self._complete_locked(record, TARGET_QUANTITY_FILLED)
            else:
                self._terminalize_manual_cancel_if_clear_locked(record)
            return self._snapshot(record)

        return await actor.apply(cancel)

    async def run_once(self, execution_id: str) -> ExecutionRecord:
        record, actor = self._lookup_execution(execution_id)

        async def run() -> ExecutionRecord:
            if record.status.is_terminal:
                return self._snapshot(record)

            if record.status is ExecutionStatus.CANCELLING:
                await self._reconcile_locked(record)
                self._terminalize_manual_cancel_if_clear_locked(record)
                return self._snapshot(record)

            if record.exposure.unknown_order_quantity > Decimal("0"):
                await self._reconcile_locked(record, exact_unknown_lookup=True)
                if record.exposure.unknown_order_quantity > Decimal("0"):
                    if self._deadline_reached(record):
                        record.final_reason = CREATE_TIMEOUT_PENDING_RECONCILIATION
                    return self._snapshot(record)
                return self._snapshot(record)

            await self._reconcile_locked(record)

            if self._target_filled(record):
                self._complete_locked(record, TARGET_QUANTITY_FILLED)
                return self._snapshot(record)

            stream_healthy = await self._adapter.health_check_streams()
            if self._deadline_reached(record) and not stream_healthy:
                record.final_reason = STREAM_HEALTH_DEGRADED_RECONCILED
                await self._cancel_active_children_locked(record)
                await self._reconcile_locked(record, exact_unknown_lookup=True)
                if self._target_filled(record):
                    self._complete_locked(record, TARGET_QUANTITY_FILLED)
                    return self._snapshot(record)
                self._terminalize_deadline_locked(record, STREAM_HEALTH_DEGRADED_RECONCILED)
                return self._snapshot(record)

            if not stream_healthy:
                record.final_reason = STREAM_HEALTH_DEGRADED_RECONCILED
                return self._snapshot(record)

            if self._should_terminalize_aggressive_deadline(record):
                self._terminalize_deadline_locked(record, DEADLINE_AGGRESSIVE_ATTEMPTED)
                return self._snapshot(record)

            if self._should_terminalize_cancel_remainder_deadline(record):
                await self._cancel_active_children_locked(record)
                await self._reconcile_locked(record)
                if self._target_filled(record):
                    self._complete_locked(record, TARGET_QUANTITY_FILLED)
                    return self._snapshot(record)
                self._terminalize_deadline_locked(record, DEADLINE_CANCEL_REMAINDER)
                return self._snapshot(record)

            if self._should_terminalize_cancel_remainder_without_demand(record):
                reason = (
                    PRICE_OUTSIDE_RANGE
                    if record.final_reason == WAITING_FOR_PRICE_RANGE
                    else DEADLINE_CANCEL_REMAINDER
                )
                self._terminalize_deadline_locked(record, reason)
                return self._snapshot(record)

            if self._should_cancel_for_aggressive_deadline(record):
                await self._cancel_passive_children_for_aggressive_deadline_locked(record)
                await self._reconcile_locked(record)
                if self._target_filled(record):
                    self._complete_locked(record, TARGET_QUANTITY_FILLED)
                    return self._snapshot(record)

            if await self._cancel_timed_out_children_locked(record):
                await self._reconcile_locked(record)
                if self._target_filled(record):
                    self._complete_locked(record, TARGET_QUANTITY_FILLED)
                    return self._snapshot(record)
                if self._should_terminalize_aggressive_deadline(record):
                    self._terminalize_deadline_locked(record, DEADLINE_AGGRESSIVE_ATTEMPTED)
                    return self._snapshot(record)

            try:
                if record.exposure.reserved_exposure > Decimal("0"):
                    if record.request.algorithm is Algorithm.CHASE:
                        await self._maybe_reprice_chase_locked(record)
                        await self._reconcile_locked(record)
                    if record.exposure.reserved_exposure > Decimal("0"):
                        return self._snapshot(record)

                demand = await self._build_child_demand_locked(record)
            except NoFreshMarketData:
                record.final_reason = MARKET_DATA_STALE_RECONCILED
                await self._reconcile_locked(record, exact_unknown_lookup=True)
                if self._deadline_reached(record):
                    await self._cancel_active_children_locked(record)
                    await self._reconcile_locked(record, exact_unknown_lookup=True)
                    if self._target_filled(record):
                        self._complete_locked(record, TARGET_QUANTITY_FILLED)
                        return self._snapshot(record)
                    self._terminalize_deadline_locked(record, MARKET_DATA_STALE_RECONCILED)
                return self._snapshot(record)

            if demand is None:
                return self._snapshot(record)

            quantity, price, post_only, market_bid, market_ask = demand
            if quantity > Decimal("0"):
                try:
                    await self._submit_child_locked(
                        record,
                        quantity,
                        price,
                        post_only=post_only,
                        market_bid=market_bid,
                        market_ask=market_ask,
                    )
                except NoFreshMarketData:
                    record.final_reason = MARKET_DATA_STALE_RECONCILED
                    await self._reconcile_locked(record, exact_unknown_lookup=True)
                    if self._deadline_reached(record):
                        await self._cancel_active_children_locked(record)
                        await self._reconcile_locked(record, exact_unknown_lookup=True)
                        if self._target_filled(record):
                            self._complete_locked(record, TARGET_QUANTITY_FILLED)
                            return self._snapshot(record)
                        self._terminalize_deadline_locked(record, MARKET_DATA_STALE_RECONCILED)
                    return self._snapshot(record)

            if self._target_filled(record):
                self._complete_locked(record, TARGET_QUANTITY_FILLED)
            return self._snapshot(record)

        return await actor.apply(run)

    async def reconcile_execution(
        self,
        execution_id: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> ExecutionRecord:
        record, actor = self._lookup_execution(execution_id)

        async def reconcile() -> ExecutionRecord:
            await self._reconcile_locked(
                record,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            if record.status is ExecutionStatus.CANCELLING:
                self._terminalize_manual_cancel_if_clear_locked(record)
            elif not record.status.is_terminal:
                if self._target_filled(record):
                    self._complete_locked(record, TARGET_QUANTITY_FILLED)
            return self._snapshot(record)

        return await actor.apply(reconcile)

    async def apply_reconciliation_result(
        self,
        execution_id: str,
        result: ReconciliationResult,
    ) -> ExecutionRecord:
        record, actor = self._lookup_execution(execution_id)

        async def apply() -> ExecutionRecord:
            await self._apply_reconciliation_result_locked(
                record,
                result,
                exact_unknown_lookup=False,
            )
            if record.status is ExecutionStatus.CANCELLING:
                self._terminalize_manual_cancel_if_clear_locked(record)
            elif not record.status.is_terminal and self._target_filled(record):
                self._complete_locked(record, TARGET_QUANTITY_FILLED)
            return self._snapshot(record)

        return await actor.apply(apply)

    def _lookup_execution(self, execution_id: str) -> tuple[ExecutionRecord, ExecutionEventActor]:
        record = self._records.get(execution_id)
        actor = self._actors.get(execution_id)
        if record is None or actor is None:
            raise UnknownExecution(execution_id)
        return record, actor

    def _target_cannot_meet_min_quantity_or_buy_min_notional(
        self,
        record: ExecutionRecord,
        rules: SymbolRules | None,
    ) -> bool:
        if rules is None:
            return False
        if record.required_quantity < rules.min_quantity:
            return True

        if record.side is not Side.BUY:
            return False

        if rules.tick_size <= Decimal("0"):
            return False

        highest_legal_buy_price = round_price(
            record.request.target_price_upper,
            rules.tick_size,
            Side.BUY,
            passive=True,
        )
        return record.required_quantity * highest_legal_buy_price < rules.min_notional

    async def _submit_child_locked(
        self,
        record: ExecutionRecord,
        quantity: Decimal,
        price: Decimal,
        *,
        post_only: bool,
        market_bid: Decimal,
        market_ask: Decimal,
    ) -> ChildOrder:
        tracker = self._require_exposure_tracker(record)
        tracker.check_can_submit(quantity)
        record.last_child_sequence += 1
        sequence = record.last_child_sequence
        submitted_at = self._now_decimal()
        child = ChildOrder(
            child_order_id=ids.child_order_id(sequence),
            client_order_id=ids.make_client_order_id(record.execution_id, sequence),
            symbol=record.request.symbol,
            side=record.side,
            submitted_quantity=quantity,
            price=price,
        )
        record.child_orders.append(child)
        record.child_submitted_monotonic[child.client_order_id] = submitted_at
        if not post_only:
            record.aggressive_child_client_order_ids.add(child.client_order_id)
        tracker.reserve_pending_submit(quantity)
        self._increment_metric(record, "orders_submitted")
        self._record_max_reserved_exposure(record)
        order_request = OrderRequest(
            execution_id=record.execution_id,
            child_order_id=child.child_order_id,
            client_order_id=child.client_order_id,
            symbol=record.request.symbol,
            side=record.side,
            quantity=quantity,
            price=price,
            post_only=post_only,
            time_in_force=TimeInForce.GTX if post_only else TimeInForce.IOC,
        )

        try:
            submitted = await self._adapter.submit_limit_order(order_request)
        except OrderCreateTimeout as exc:
            tracker.release_pending_submit(quantity)
            self._set_child_status(child, ChildOrderStatus.UNKNOWN)
            child.terminal_reason = CREATE_TIMEOUT_PENDING_RECONCILIATION
            tracker.reserve_unknown_create(quantity)
            self._record_max_reserved_exposure(record)
            record.final_reason = CREATE_TIMEOUT_PENDING_RECONCILIATION
            self._assert_exposure_invariant_locked(record)
            return child
        except OrderRejected as exc:
            tracker.release_pending_submit(quantity)
            self._set_child_status(child, ChildOrderStatus.REJECTED)
            child.terminal_reason = str(exc)
            self._increment_metric(record, "rejections")
            if isinstance(exc, TerminalOrderRejected):
                self._fail_locked(record, f"{TERMINAL_ORDER_REJECTED}: {exc}")
            elif post_only:
                self._record_retryable_order_reject_locked(
                    record,
                    price,
                    bid=market_bid,
                    ask=market_ask,
                    reason=str(exc),
                )
                self._terminalize_retryable_order_reject_limit_locked(record)
            else:
                self._fail_locked(record, f"{TERMINAL_ORDER_REJECTED}: {exc}")
            self._assert_exposure_invariant_locked(record)
            return child
        except Exception:
            tracker.release_pending_submit(quantity)
            record.child_orders.remove(child)
            record.child_submitted_monotonic.pop(child.client_order_id, None)
            record.aggressive_child_client_order_ids.discard(child.client_order_id)
            raise

        tracker.release_pending_submit(quantity)
        submitted_filled_quantity = submitted.confirmed_filled_quantity
        self._copy_exchange_child(child, submitted, include_filled=False)
        self._set_child_status(child, submitted.status)
        if submitted_filled_quantity > Decimal("0"):
            self._update_child_cumulative_fill_locked(
                record,
                child,
                submitted_filled_quantity,
                trade_id=None,
                fill_price=child.price,
                authoritative_trade=False,
            )

        if child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
            tracker.reserve_live_open(child.remaining_quantity)
        elif child.status is ChildOrderStatus.UNKNOWN:
            tracker.reserve_unknown_create(child.remaining_quantity)
        if child.status not in {ChildOrderStatus.UNKNOWN, ChildOrderStatus.REJECTED}:
            self._reset_retryable_order_reject_gate_locked(record)
        self._record_max_reserved_exposure(record)
        self._assert_exposure_invariant_locked(record)
        return child

    async def _cancel_active_children_locked(self, record: ExecutionRecord) -> bool:
        cancelled_any = False
        for child in list(record.child_orders):
            cancelled_any = await self._cancel_child_locked(record, child) or cancelled_any
        return cancelled_any

    async def _cancel_passive_children_for_aggressive_deadline_locked(
        self,
        record: ExecutionRecord,
    ) -> bool:
        cancelled_any = False
        for child in list(record.child_orders):
            if not self._needs_aggressive_deadline_cancel(record, child):
                continue
            cancelled_any = await self._cancel_child_locked(record, child) or cancelled_any
        return cancelled_any

    async def _cancel_timed_out_children_locked(self, record: ExecutionRecord) -> bool:
        cancelled_any = False
        for child in list(record.child_orders):
            if not self._child_order_timed_out(record, child):
                continue
            cancelled_any = await self._cancel_child_locked(record, child) or cancelled_any
        return cancelled_any

    async def _cancel_child_locked(self, record: ExecutionRecord, child: ChildOrder) -> bool:
        tracker = self._require_exposure_tracker(record)
        if child.status not in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
            return False

        remaining_before_cancel = child.remaining_quantity
        if remaining_before_cancel <= Decimal("0"):
            return False

        tracker.mark_pending_cancel(remaining_before_cancel)
        self._increment_metric(record, "cancels_requested")
        self._record_max_reserved_exposure(record)
        self._set_child_status(child, ChildOrderStatus.PENDING_CANCEL)
        try:
            cancelled = await self._adapter.cancel_order(record.request.symbol, child.client_order_id)
        except OrderCancelTimeout as exc:
            child.terminal_reason = str(exc)
            self._assert_exposure_invariant_locked(record)
            return True
        except Exception as exc:
            tracker.release_pending_cancel(remaining_before_cancel)
            tracker.reserve_live_open(remaining_before_cancel)
            self._record_max_reserved_exposure(record)
            child.terminal_reason = str(exc)
            self._set_child_status(child, ChildOrderStatus.OPEN)
            self._assert_exposure_invariant_locked(record)
            return False

        cancelled_filled_quantity = cancelled.confirmed_filled_quantity
        self._copy_exchange_child(child, cancelled, include_filled=False)
        self._set_child_status(child, cancelled.status)

        if cancelled_filled_quantity > Decimal("0"):
            self._update_child_cumulative_fill_locked(
                record,
                child,
                cancelled_filled_quantity,
                trade_id=None,
                fill_price=child.price,
                authoritative_trade=False,
            )

        if child.status in {ChildOrderStatus.CANCELLED, ChildOrderStatus.FILLED}:
            tracker.release_pending_cancel(remaining_before_cancel)
        elif child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
            tracker.release_pending_cancel(remaining_before_cancel)
            tracker.reserve_live_open(child.remaining_quantity)
        self._record_max_reserved_exposure(record)
        self._assert_exposure_invariant_locked(record)
        return True

    async def _reconcile_locked(
        self,
        record: ExecutionRecord,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        exact_unknown_lookup: bool = True,
    ) -> None:
        tracker = record.exposure_tracker
        if tracker is None:
            return

        prefix = ids.make_client_order_prefix(record.execution_id)
        reconciliation_kwargs = {}
        if start_time_ms is not None:
            reconciliation_kwargs["start_time_ms"] = start_time_ms
        if end_time_ms is not None:
            reconciliation_kwargs["end_time_ms"] = end_time_ms
        result = await self._adapter.reconcile_orders_and_fills(
            record.request.symbol,
            client_order_prefix=prefix,
            **reconciliation_kwargs,
        )
        await self._apply_reconciliation_result_locked(
            record,
            result,
            exact_unknown_lookup=exact_unknown_lookup,
        )

    async def _apply_reconciliation_result_locked(
        self,
        record: ExecutionRecord,
        result: ReconciliationResult,
        *,
        exact_unknown_lookup: bool,
    ) -> None:
        tracker = record.exposure_tracker
        if tracker is None:
            return
        children_by_client_id = {child.client_order_id: child for child in record.child_orders}
        exchange_client_ids = {order.client_order_id for order in result.orders}
        fill_client_ids = {fill.client_order_id for fill in result.fills}

        for exchange_order in result.orders:
            child = children_by_client_id.get(exchange_order.client_order_id)
            if child is None:
                child = deepcopy(exchange_order)
                if child.client_order_id in fill_client_ids:
                    child.confirmed_filled_quantity = Decimal("0")
                record.child_orders.append(child)
                children_by_client_id[child.client_order_id] = child
                continue
            self._copy_exchange_child(child, exchange_order, include_filled=False)

        sorted_fills = sorted(
            result.fills,
            key=lambda fill: (
                fill.event_time_ms if fill.event_time_ms is not None else 0,
                fill.transaction_time_ms if fill.transaction_time_ms is not None else 0,
                fill.trade_id or "",
            ),
        )
        for fill in sorted_fills:
            child = children_by_client_id.get(fill.client_order_id)
            if child is None:
                continue
            self._update_child_cumulative_fill_locked(
                record,
                child,
                fill.cumulative_filled_quantity,
                trade_id=fill.trade_id,
                fill_price=fill.last_fill_price,
                is_maker=fill.is_maker,
                authoritative_trade=True,
                fill_quantity_delta=fill.last_filled_quantity,
                event_time_ms=fill.event_time_ms,
                transaction_time_ms=fill.transaction_time_ms,
            )

        for exchange_order in result.orders:
            child = children_by_client_id.get(exchange_order.client_order_id)
            if child is None:
                continue
            was_unknown = child.status is ChildOrderStatus.UNKNOWN
            self._update_child_cumulative_fill_locked(
                record,
                child,
                exchange_order.confirmed_filled_quantity,
                trade_id=None,
                fill_price=child.price,
                authoritative_trade=False,
            )
            self._copy_exchange_child(child, exchange_order)
            self._set_child_status(child, exchange_order.status)
            if was_unknown and child.status is not ChildOrderStatus.UNKNOWN:
                self._increment_metric(record, "unknown_orders_reconciled")
                if self._reconciled_unknown_child_resets_retryable_reject_gate(child):
                    self._reset_retryable_order_reject_gate_locked(record)

        if exact_unknown_lookup:
            await self._reconcile_unknown_children_exact_locked(record)

        if CREATE_TIMEOUT_ORDER_NOT_FOUND in result.warnings:
            for child in record.child_orders:
                if child.status is ChildOrderStatus.UNKNOWN and child.client_order_id not in exchange_client_ids:
                    self._set_child_status(child, ChildOrderStatus.REJECTED)
                    child.terminal_reason = CREATE_TIMEOUT_ORDER_NOT_FOUND
                    record.final_reason = CREATE_TIMEOUT_ORDER_NOT_FOUND
                    self._increment_metric(record, "unknown_orders_reconciled")
            tracker.clear_unknown_create()

        self._refresh_reserved_exposure_locked(record)
        self._record_max_reserved_exposure(record)
        self._assert_exposure_invariant_locked(record)
        if record.exposure.unknown_order_quantity == Decimal("0"):
            for child in record.child_orders:
                if (
                    child.terminal_reason == CREATE_TIMEOUT_PENDING_RECONCILIATION
                    and child.status is not ChildOrderStatus.UNKNOWN
                ):
                    child.terminal_reason = None
            if record.final_reason == CREATE_TIMEOUT_PENDING_RECONCILIATION:
                record.final_reason = CREATE_TIMEOUT_RECONCILED
        if record.summary is not None:
            record.summary = self._summary(record)

    async def _reconcile_unknown_children_exact_locked(self, record: ExecutionRecord) -> None:
        for child in list(record.child_orders):
            if child.status is not ChildOrderStatus.UNKNOWN:
                continue

            exchange_child = await self._adapter.get_order_by_client_order_id(
                record.request.symbol,
                child.client_order_id,
            )
            if exchange_child is None:
                self._set_child_status(child, ChildOrderStatus.REJECTED)
                child.terminal_reason = CREATE_TIMEOUT_ORDER_NOT_FOUND
                record.final_reason = CREATE_TIMEOUT_ORDER_NOT_FOUND
                self._increment_metric(record, "unknown_orders_reconciled")
                continue

            exchange_filled_quantity = exchange_child.confirmed_filled_quantity
            self._copy_exchange_child(child, exchange_child, include_filled=False)
            self._set_child_status(child, getattr(exchange_child, "status", child.status))
            if exchange_filled_quantity > Decimal("0"):
                self._update_child_cumulative_fill_locked(
                    record,
                    child,
                    exchange_filled_quantity,
                    trade_id=None,
                    fill_price=child.price,
                    authoritative_trade=False,
                )
            self._increment_metric(record, "unknown_orders_reconciled")
            if self._reconciled_unknown_child_resets_retryable_reject_gate(child):
                self._reset_retryable_order_reject_gate_locked(record)

    async def _maybe_reprice_chase_locked(self, record: ExecutionRecord) -> None:
        active_child = self._first_active_child(record)
        if active_child is None:
            return

        market = await self._adapter.get_best_bid_ask(record.request.symbol)
        rules = await self._adapter.get_symbol_rules(record.request.symbol)
        desired_price = self._rounded_passive_price(record.side, market, rules)
        elapsed_ms = int((self._now_decimal() - (record.last_reprice_monotonic or record.started_monotonic)) * 1000)
        decision = should_reprice(
            record.side,
            active_order_price=active_child.price,
            desired_price=desired_price,
            threshold_bps=record.request.parameters.reprice_threshold_bps,
            min_interval_ms=record.request.parameters.minimum_reprice_interval_ms,
            elapsed_since_last_reprice_ms=elapsed_ms,
            repricing_mode=record.request.parameters.repricing_mode,
        )
        if decision is ChaseDecision.REPRICE:
            record.last_reprice_monotonic = self._now_decimal()
            self._increment_metric(record, "reprices")
            await self._cancel_active_children_locked(record)

    async def _build_child_demand_locked(
        self,
        record: ExecutionRecord,
    ) -> tuple[Decimal, Decimal, bool, Decimal, Decimal] | None:
        tracker = self._require_exposure_tracker(record)
        market = await self._adapter.get_best_bid_ask(record.request.symbol)
        if record.arrival_bid is None:
            record.arrival_bid = market.bid
            record.arrival_ask = market.ask
        rules = await self._adapter.get_symbol_rules(record.request.symbol)
        use_aggressive_deadline = self._use_aggressive_deadline_price(record)
        post_only = not use_aggressive_deadline

        if record.request.algorithm is Algorithm.TWAP:
            elapsed = self._now_decimal() - record.started_monotonic
            total_duration = Decimal(str(record.request.target_duration_seconds))
            effective_elapsed = effective_slice_elapsed(
                elapsed_time=elapsed,
                total_duration=total_duration,
                number_of_slices=record.request.parameters.number_of_slices,
            )
            scheduled = scheduled_cumulative_quantity(
                total_trade_quantity=record.required_quantity,
                elapsed_time=effective_elapsed,
                total_duration=total_duration,
            )
            deficit = scheduled_deficit(scheduled, tracker.exposure.confirmed_filled_quantity)
            quantity = safe_child_quantity(deficit, tracker.exposure)
        else:
            quantity = tracker.available_to_submit()

        quantity = floor_to_step(quantity, rules.quantity_step)
        if quantity <= Decimal("0"):
            return None

        if use_aggressive_deadline:
            price = self._rounded_aggressive_price(record.side, market, rules)
        else:
            price = self._rounded_passive_price(record.side, market, rules)
        try:
            validate_child_order_safety(
                quantity=quantity,
                price=price,
                side=record.side,
                rules=rules,
                best_bid=market.bid,
                best_ask=market.ask,
                post_only=post_only,
                lower=record.request.target_price_lower,
                upper=record.request.target_price_upper,
            )
        except ValidationError as exc:
            if self._is_price_outside_range_error(exc):
                self._increment_metric(record, "price_bound_violations")
                try:
                    validate_order_shape(
                        quantity=quantity,
                        price=price,
                        side=record.side,
                        rules=rules,
                        best_bid=market.bid,
                        best_ask=market.ask,
                        post_only=post_only,
                    )
                except ValidationError as shape_exc:
                    self._expire_for_validation_locked(record, shape_exc)
                    return None

                if self._deadline_reached(record):
                    self._expire_for_validation_locked(record, exc)
                else:
                    record.final_reason = WAITING_FOR_PRICE_RANGE
                return None

            self._expire_for_validation_locked(record, exc)
            return None

        if post_only and self._retryable_order_reject_gate_blocks(record, price, market.bid, market.ask):
            return None

        if record.final_reason == WAITING_FOR_PRICE_RANGE:
            record.final_reason = None
        if record.final_reason in {
            RETRYABLE_ORDER_REJECT_BACKOFF,
            RETRYABLE_ORDER_REJECT_WAITING_FOR_FRESH_QUOTE,
        }:
            record.final_reason = None
        return quantity, price, post_only, market.bid, market.ask

    def _retryable_order_reject_gate_blocks(
        self,
        record: ExecutionRecord,
        passive_price: Decimal,
        bid: Decimal,
        ask: Decimal,
    ) -> bool:
        if record.consecutive_retryable_order_rejects <= 0:
            return False

        now = self._now_decimal()
        last_reject_monotonic = record.last_retryable_order_reject_monotonic
        if (
            last_reject_monotonic is not None
            and now - last_reject_monotonic < RETRYABLE_ORDER_REJECT_BACKOFF_SECONDS
        ):
            record.final_reason = RETRYABLE_ORDER_REJECT_BACKOFF
            self._increment_metric(record, "retryable_order_reject_backoff_blocks")
            return True

        if (
            passive_price == record.last_retryable_order_reject_price
            and self._retryable_reject_crossing_quote(record, bid=bid, ask=ask)
            == self._last_retryable_reject_crossing_quote(record)
        ):
            record.final_reason = RETRYABLE_ORDER_REJECT_WAITING_FOR_FRESH_QUOTE
            self._increment_metric(record, "retryable_order_reject_same_price_blocks")
            return True

        if record.final_reason in {
            RETRYABLE_ORDER_REJECT_BACKOFF,
            RETRYABLE_ORDER_REJECT_WAITING_FOR_FRESH_QUOTE,
        }:
            record.final_reason = None
        return False

    def _retryable_reject_crossing_quote(
        self,
        record: ExecutionRecord,
        *,
        bid: Decimal,
        ask: Decimal,
    ) -> Decimal:
        return ask if record.side is Side.BUY else bid

    def _last_retryable_reject_crossing_quote(self, record: ExecutionRecord) -> Decimal | None:
        if record.side is Side.BUY:
            return record.last_retryable_order_reject_ask
        return record.last_retryable_order_reject_bid

    def _is_price_outside_range_error(self, exc: ValidationError) -> bool:
        reason = str(exc)
        return "exceeds upper bound" in reason or "below lower bound" in reason

    def _use_aggressive_deadline_price(self, record: ExecutionRecord) -> bool:
        return (
            record.request.deadline_policy is DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE
            and self._deadline_reached(record)
        )

    def _should_cancel_for_aggressive_deadline(self, record: ExecutionRecord) -> bool:
        if not self._use_aggressive_deadline_price(record):
            return False
        return any(
            self._needs_aggressive_deadline_cancel(record, child)
            for child in record.child_orders
        )

    def _rounded_passive_price(
        self,
        side: Side,
        market: MarketSnapshot,
        rules: SymbolRules,
    ) -> Decimal:
        desired = chase_desired_price(side, market.bid, market.ask, passive=True)
        return round_price(desired, rules.tick_size, side, passive=True)

    def _rounded_aggressive_price(
        self,
        side: Side,
        market: MarketSnapshot,
        rules: SymbolRules,
    ) -> Decimal:
        desired = chase_desired_price(side, market.bid, market.ask, passive=False)
        return round_price(desired, rules.tick_size, side, passive=False)

    def _child_order_timed_out(self, record: ExecutionRecord, child: ChildOrder) -> bool:
        if child.status not in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
            return False
        timeout = Decimal(str(record.request.parameters.child_order_timeout_seconds))
        submitted_at = record.child_submitted_monotonic.get(child.client_order_id)
        if submitted_at is None:
            submitted_at = record.started_monotonic
        return self._now_decimal() - submitted_at >= timeout

    def _needs_aggressive_deadline_cancel(self, record: ExecutionRecord, child: ChildOrder) -> bool:
        return (
            child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}
            and child.client_order_id not in record.aggressive_child_client_order_ids
        )

    def _deadline_reached(self, record: ExecutionRecord) -> bool:
        elapsed = self._now_decimal() - record.started_monotonic
        return elapsed >= Decimal(str(record.request.target_duration_seconds))

    def _should_terminalize_cancel_remainder_deadline(self, record: ExecutionRecord) -> bool:
        return (
            self._cancel_remainder_deadline_reached(record)
            and (record.exposure.reserved_exposure > Decimal("0") or bool(record.child_orders))
        )

    def _should_terminalize_cancel_remainder_without_demand(self, record: ExecutionRecord) -> bool:
        return (
            self._cancel_remainder_deadline_reached(record)
            and record.exposure.reserved_exposure == Decimal("0")
            and not record.child_orders
        )

    def _cancel_remainder_deadline_reached(self, record: ExecutionRecord) -> bool:
        return (
            self._deadline_reached(record)
            and record.request.deadline_policy is DeadlinePolicy.CANCEL_REMAINDER
        )

    def _should_terminalize_aggressive_deadline(self, record: ExecutionRecord) -> bool:
        return (
            self._deadline_reached(record)
            and record.request.deadline_policy is DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE
            and bool(record.aggressive_child_client_order_ids)
            and record.exposure.reserved_exposure == Decimal("0")
        )

    def _expire_for_validation_locked(self, record: ExecutionRecord, exc: ValidationError) -> None:
        reason = str(exc)
        if "bound" in reason:
            record.final_reason = PRICE_OUTSIDE_RANGE
        elif "unsupported" in reason:
            record.final_reason = POST_ONLY_UNSUPPORTED
        elif "post-only" in reason:
            record.final_reason = POST_ONLY_CROSSES
        else:
            record.final_reason = ORDER_SAFETY_REJECTED
        if record.status is ExecutionStatus.RUNNING:
            target_status = (
                ExecutionStatus.PARTIALLY_COMPLETED
                if record.exposure.confirmed_filled_quantity > Decimal("0")
                else ExecutionStatus.EXPIRED
            )
            record.status = transition_execution(record.status, target_status)
            record.completed_monotonic = self._now_decimal()
            record.summary = self._summary(record)

    def _complete_locked(self, record: ExecutionRecord, reason: str) -> None:
        if record.status in {ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING}:
            record.status = transition_execution(record.status, ExecutionStatus.COMPLETED)
            record.final_reason = reason
            record.completed_monotonic = self._now_decimal()
            record.summary = self._summary(record)

    def _fail_locked(self, record: ExecutionRecord, reason: str) -> None:
        if record.status in {ExecutionStatus.RUNNING, ExecutionStatus.CANCELLING}:
            record.status = transition_execution(record.status, ExecutionStatus.FAILED)
            record.final_reason = reason
            record.completed_monotonic = self._now_decimal()
            record.summary = self._summary(record)

    def _terminalize_deadline_locked(self, record: ExecutionRecord, reason: str) -> None:
        if record.status is not ExecutionStatus.RUNNING:
            return
        if record.exposure.reserved_exposure > Decimal("0"):
            return

        target_status = (
            ExecutionStatus.PARTIALLY_COMPLETED
            if record.exposure.confirmed_filled_quantity > Decimal("0")
            else ExecutionStatus.EXPIRED
        )
        record.status = transition_execution(record.status, target_status)
        record.final_reason = reason
        record.completed_monotonic = self._now_decimal()
        record.summary = self._summary(record)

    def _terminalize_retryable_order_reject_limit_locked(self, record: ExecutionRecord) -> None:
        if record.status is not ExecutionStatus.RUNNING:
            return
        retry_limit = record.request.parameters.max_post_only_reject_retries
        if record.consecutive_retryable_order_rejects < retry_limit:
            return
        if record.exposure.reserved_exposure > Decimal("0"):
            return

        target_status = (
            ExecutionStatus.PARTIALLY_COMPLETED
            if record.exposure.confirmed_filled_quantity > Decimal("0")
            else ExecutionStatus.EXPIRED
        )
        record.status = transition_execution(record.status, target_status)
        record.final_reason = RETRYABLE_ORDER_REJECT_LIMIT_REACHED
        record.completed_monotonic = self._now_decimal()
        record.summary = self._summary(record)

    def _terminalize_manual_cancel_if_clear_locked(self, record: ExecutionRecord) -> None:
        if record.status is not ExecutionStatus.CANCELLING:
            return

        if self._target_filled(record):
            self._complete_locked(record, TARGET_QUANTITY_FILLED)
            return

        if record.exposure.reserved_exposure > Decimal("0"):
            return

        target_status = (
            ExecutionStatus.PARTIALLY_COMPLETED
            if record.exposure.confirmed_filled_quantity > Decimal("0")
            else ExecutionStatus.CANCELLED
        )
        record.status = transition_execution(record.status, target_status)
        record.final_reason = record.final_reason or CANCEL_REQUESTED
        record.completed_monotonic = self._now_decimal()
        record.summary = self._summary(record)

    def _target_filled(self, record: ExecutionRecord) -> bool:
        return record.exposure.confirmed_filled_quantity >= record.required_quantity

    def _require_exposure_tracker(self, record: ExecutionRecord) -> ExposureTracker:
        if record.exposure_tracker is None:
            record.exposure_tracker = ExposureTracker(record.required_quantity)
        return record.exposure_tracker

    def _increment_metric(self, record: ExecutionRecord, name: str) -> None:
        record.metric_counts[name] = record.metric_counts.get(name, 0) + 1

    def _record_retryable_order_reject_locked(
        self,
        record: ExecutionRecord,
        price: Decimal,
        *,
        bid: Decimal,
        ask: Decimal,
        reason: str,
    ) -> None:
        record.consecutive_retryable_order_rejects += 1
        record.last_retryable_order_reject_monotonic = self._now_decimal()
        record.last_retryable_order_reject_price = price
        record.last_retryable_order_reject_bid = bid
        record.last_retryable_order_reject_ask = ask
        record.last_retryable_order_reject_reason = reason
        self._increment_metric(record, "retryable_order_rejections")

    def _reset_retryable_order_reject_gate_locked(self, record: ExecutionRecord) -> None:
        record.consecutive_retryable_order_rejects = 0
        record.last_retryable_order_reject_monotonic = None
        record.last_retryable_order_reject_price = None
        record.last_retryable_order_reject_bid = None
        record.last_retryable_order_reject_ask = None
        record.last_retryable_order_reject_reason = None
        if record.final_reason in {
            RETRYABLE_ORDER_REJECT_BACKOFF,
            RETRYABLE_ORDER_REJECT_WAITING_FOR_FRESH_QUOTE,
        }:
            record.final_reason = None

    def _reconciled_unknown_child_resets_retryable_reject_gate(self, child: ChildOrder) -> bool:
        if child.status in {
            ChildOrderStatus.OPEN,
            ChildOrderStatus.PARTIALLY_FILLED,
            ChildOrderStatus.FILLED,
        }:
            return True
        return (
            child.status is ChildOrderStatus.CANCELLED
            and child.confirmed_filled_quantity > Decimal("0")
        )

    def _record_max_reserved_exposure(self, record: ExecutionRecord) -> None:
        if record.exposure.reserved_exposure > record.max_reserved_exposure:
            record.max_reserved_exposure = record.exposure.reserved_exposure

    def _assert_exposure_invariant_locked(self, record: ExecutionRecord) -> None:
        check_exposure_invariant(
            record.exposure,
            Decimal("0"),
            record.required_quantity,
        )

    def _refresh_reserved_exposure_locked(self, record: ExecutionRecord) -> None:
        tracker = self._require_exposure_tracker(record)
        confirmed = tracker.exposure.confirmed_filled_quantity
        tracker.exposure = Exposure(confirmed_filled_quantity=confirmed)
        for child in record.child_orders:
            remaining = child.remaining_quantity
            if child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
                tracker.exposure.live_open_quantity += remaining
            elif child.status is ChildOrderStatus.PENDING_SUBMIT:
                tracker.exposure.pending_submit_quantity += remaining
            elif child.status is ChildOrderStatus.PENDING_CANCEL:
                tracker.exposure.pending_cancel_quantity += remaining
            elif child.status is ChildOrderStatus.UNKNOWN:
                tracker.exposure.unknown_order_quantity += remaining

    def _update_child_cumulative_fill_locked(
        self,
        record: ExecutionRecord,
        child: ChildOrder,
        incoming_cumulative: Decimal,
        *,
        trade_id: str | None,
        fill_price: Decimal,
        is_maker: bool | None = None,
        authoritative_trade: bool = True,
        fill_quantity_delta: Decimal | None = None,
        event_time_ms: int | None = None,
        transaction_time_ms: int | None = None,
    ) -> None:
        tracker = self._require_exposure_tracker(record)

        if trade_id is not None and trade_id in record.seen_fill_trade_ids:
            self._record_ignored_fill_trade_id_locked(record, trade_id)
            return

        previous_child_cumulative = child.confirmed_filled_quantity
        child_delta = Decimal("0")
        if incoming_cumulative > previous_child_cumulative:
            child_delta = incoming_cumulative - previous_child_cumulative
            child.confirmed_filled_quantity = incoming_cumulative
            aggregate_cumulative = tracker.exposure.confirmed_filled_quantity + child_delta
            tracker.apply_fill(trade_id, aggregate_cumulative)

        if not authoritative_trade:
            return

        if incoming_cumulative < previous_child_cumulative:
            if trade_id is not None:
                record.seen_fill_trade_ids.add(trade_id)
                self._record_ignored_fill_trade_id_locked(record, trade_id)
            return

        trade_delta = fill_quantity_delta if fill_quantity_delta is not None else child_delta
        if trade_delta <= Decimal("0"):
            if trade_id is not None:
                record.seen_fill_trade_ids.add(trade_id)
                self._record_ignored_fill_trade_id_locked(record, trade_id)
            return

        if trade_id is not None:
            record.seen_fill_trade_ids.add(trade_id)
            tracker.seen_trade_ids.add(trade_id)
            record.trade_fill_vwap_inputs[trade_id] = (fill_price, trade_delta)
            record.trade_fill_sort_keys[trade_id] = (
                event_time_ms if event_time_ms is not None else 0,
                transaction_time_ms if transaction_time_ms is not None else 0,
                trade_id,
            )
            self._rebuild_trade_fill_vwap_inputs_locked(record)
        else:
            record.anonymous_fill_vwap_inputs.append((fill_price, trade_delta))
            record.fill_vwap_inputs.append((fill_price, trade_delta))

        if is_maker is True:
            record.maker_filled_quantity += trade_delta
            self._increment_metric(record, "maker_fills")
        elif is_maker is False:
            record.taker_filled_quantity += trade_delta
            self._increment_metric(record, "taker_fills")

    def _record_ignored_fill_trade_id_locked(self, record: ExecutionRecord, trade_id: str) -> None:
        if trade_id in record.ignored_fill_trade_ids:
            return
        record.ignored_fill_trade_ids.add(trade_id)
        self._increment_metric(record, "duplicate_events_ignored")

    def _rebuild_trade_fill_vwap_inputs_locked(self, record: ExecutionRecord) -> None:
        ordered_trade_ids = sorted(
            record.trade_fill_vwap_inputs,
            key=lambda trade_id: record.trade_fill_sort_keys.get(trade_id, (0, 0, trade_id)),
        )
        record.fill_vwap_inputs = [
            record.trade_fill_vwap_inputs[trade_id] for trade_id in ordered_trade_ids
        ] + record.anonymous_fill_vwap_inputs

    def _copy_exchange_child(
        self,
        child: ChildOrder,
        exchange_child: Any,
        *,
        include_filled: bool = True,
    ) -> None:
        child.exchange_order_id = getattr(exchange_child, "exchange_order_id", child.exchange_order_id)
        child.raw_status = getattr(exchange_child, "raw_status", child.raw_status)
        if include_filled:
            incoming_filled_quantity = getattr(
                exchange_child,
                "confirmed_filled_quantity",
                child.confirmed_filled_quantity,
            )
            if incoming_filled_quantity > child.confirmed_filled_quantity:
                child.confirmed_filled_quantity = incoming_filled_quantity

    def _set_child_status(self, child: ChildOrder, target: ChildOrderStatus) -> bool:
        if child.status is target:
            return True
        try:
            child.status = transition_child(child.status, target)
            return True
        except InvalidStateTransition:
            pass

        if target is ChildOrderStatus.CANCELLED and child.status in {
            ChildOrderStatus.OPEN,
            ChildOrderStatus.PARTIALLY_FILLED,
        }:
            child.status = transition_child(child.status, ChildOrderStatus.PENDING_CANCEL)
            child.status = transition_child(child.status, target)
            return True

        if target is ChildOrderStatus.CANCELLED and child.status is ChildOrderStatus.PENDING_SUBMIT:
            child.status = transition_child(child.status, ChildOrderStatus.OPEN)
            child.status = transition_child(child.status, ChildOrderStatus.PENDING_CANCEL)
            child.status = transition_child(child.status, target)
            return True

        if child.status is ChildOrderStatus.PENDING_SUBMIT and target in {
            ChildOrderStatus.PARTIALLY_FILLED,
            ChildOrderStatus.FILLED,
        }:
            child.status = transition_child(child.status, ChildOrderStatus.OPEN)
            child.status = transition_child(child.status, target)
            return True

        return False

    def _first_active_child(self, record: ExecutionRecord) -> ChildOrder | None:
        for child in record.child_orders:
            if child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
                return child
        return None

    def _now_decimal(self) -> Decimal:
        return Decimal(str(self._clock.monotonic()))

    def _summary(self, record: ExecutionRecord) -> ExecutionSummary:
        return ExecutionSummary(
            execution_id=record.execution_id,
            final_status=record.status,
            final_reason=record.final_reason or "",
            metrics=self._summary_metrics(record),
        )

    def _summary_metrics(self, record: ExecutionRecord) -> dict[str, Any]:
        arrival_bid = record.arrival_bid if record.arrival_bid is not None else Decimal("0")
        arrival_ask = record.arrival_ask if record.arrival_ask is not None else Decimal("0")
        vwap_inputs = record.fill_vwap_inputs or self._provisional_fill_vwap_inputs(record)
        vwap = execution_vwap(vwap_inputs) if vwap_inputs else Decimal("0")
        completed_at = record.completed_monotonic if record.completed_monotonic is not None else self._now_decimal()
        actual_duration = completed_at - record.started_monotonic
        if actual_duration < Decimal("0"):
            actual_duration = Decimal("0")

        metrics = summary_metrics(
            final_status=record.status,
            side=record.side,
            raw_required_quantity=record.raw_required_quantity,
            required_quantity=record.required_quantity,
            target_dust_quantity=record.target_dust_quantity,
            filled_quantity=record.exposure.confirmed_filled_quantity,
            arrival_bid=arrival_bid,
            arrival_ask=arrival_ask,
            vwap=vwap,
            requested_duration_seconds=record.request.target_duration_seconds,
            actual_duration_seconds=actual_duration,
            price_bound_violations=record.metric_counts.get("price_bound_violations", 0),
            duplicate_events_ignored=record.metric_counts.get("duplicate_events_ignored", 0),
            unknown_orders_reconciled=record.metric_counts.get("unknown_orders_reconciled", 0),
            max_reserved_exposure=record.max_reserved_exposure,
        )
        metrics.update(
            {
                "initial_position": record.initial_position.position,
                "target_position": record.request.target_position,
                "side": record.side,
                "child_order_count": len(record.child_orders),
                "orders_submitted": record.metric_counts.get("orders_submitted", 0),
                "cancels_requested": record.metric_counts.get("cancels_requested", 0),
                "reprices": record.metric_counts.get("reprices", 0),
                "rejections": record.metric_counts.get("rejections", 0),
                "maker_fills": record.metric_counts.get("maker_fills", 0),
                "taker_fills": record.metric_counts.get("taker_fills", 0),
                "maker_filled_quantity": record.maker_filled_quantity,
                "taker_filled_quantity": record.taker_filled_quantity,
                "retryable_order_rejections": record.metric_counts.get("retryable_order_rejections", 0),
                "retryable_order_reject_streak": record.consecutive_retryable_order_rejects,
                "retryable_order_reject_limit": record.request.parameters.max_post_only_reject_retries,
                "retryable_order_reject_backoff_seconds": RETRYABLE_ORDER_REJECT_BACKOFF_SECONDS,
                "retryable_order_reject_backoff_blocks": record.metric_counts.get(
                    "retryable_order_reject_backoff_blocks",
                    0,
                ),
                "retryable_order_reject_same_price_blocks": record.metric_counts.get(
                    "retryable_order_reject_same_price_blocks",
                    0,
                ),
                "twap_slice_ledger": self._twap_slice_ledger(record),
            }
        )
        return metrics

    @staticmethod
    def _provisional_fill_vwap_inputs(record: ExecutionRecord) -> list[tuple[Decimal, Decimal]]:
        return [
            (child.price, child.confirmed_filled_quantity)
            for child in record.child_orders
            if child.confirmed_filled_quantity > Decimal("0")
        ]

    def _twap_slice_ledger(self, record: ExecutionRecord) -> list[dict[str, Any]]:
        if (
            record.request.algorithm is not Algorithm.TWAP
            or record.side is Side.NO_ACTION
            or record.required_quantity <= Decimal("0")
        ):
            return []

        number_of_slices = record.request.parameters.number_of_slices
        if number_of_slices <= 0:
            return []

        total_duration = Decimal(str(record.request.target_duration_seconds))
        slice_duration = total_duration / Decimal(number_of_slices)
        rows = [
            self._empty_twap_slice_ledger_row(
                record,
                slice_index=slice_index,
                slice_duration=slice_duration,
                number_of_slices=number_of_slices,
            )
            for slice_index in range(1, number_of_slices + 1)
        ]

        for child in record.child_orders:
            slice_index = self._twap_child_slice_index(
                record,
                child,
                number_of_slices=number_of_slices,
                slice_duration=slice_duration,
            )
            row = rows[slice_index - 1]
            row["submitted_quantity"] += child.submitted_quantity
            row["filled_quantity"] += child.confirmed_filled_quantity
            if child.status in {ChildOrderStatus.OPEN, ChildOrderStatus.PARTIALLY_FILLED}:
                row["open_quantity"] += child.remaining_quantity
            if child.status is ChildOrderStatus.CANCELLED:
                row["cancelled_quantity"] += child.remaining_quantity
            row["child_order_ids"].append(child.child_order_id)
            row["client_order_ids"].append(child.client_order_id)

        cumulative_filled = Decimal("0")
        for row in rows:
            cumulative_filled += row["filled_quantity"]
            row["unfilled_quantity"] = max(
                row["planned_slice_quantity"] - row["filled_quantity"],
                Decimal("0"),
            )
            row["schedule_deficit"] = max(
                row["planned_cumulative_quantity"] - cumulative_filled,
                Decimal("0"),
            )
        return rows

    def _empty_twap_slice_ledger_row(
        self,
        record: ExecutionRecord,
        *,
        slice_index: int,
        slice_duration: Decimal,
        number_of_slices: int,
    ) -> dict[str, Any]:
        previous_planned_cumulative = (
            record.required_quantity * Decimal(slice_index - 1) / Decimal(number_of_slices)
        )
        planned_cumulative = record.required_quantity * Decimal(slice_index) / Decimal(number_of_slices)
        return {
            "execution_id": record.execution_id,
            "slice_index": slice_index,
            "slice_start_seconds": slice_duration * Decimal(slice_index - 1),
            "slice_end_seconds": slice_duration * Decimal(slice_index),
            "planned_cumulative_quantity": planned_cumulative,
            "planned_slice_quantity": planned_cumulative - previous_planned_cumulative,
            "submitted_quantity": Decimal("0"),
            "open_quantity": Decimal("0"),
            "filled_quantity": Decimal("0"),
            "cancelled_quantity": Decimal("0"),
            "unfilled_quantity": Decimal("0"),
            "schedule_deficit": Decimal("0"),
            "child_order_ids": [],
            "client_order_ids": [],
        }

    def _twap_child_slice_index(
        self,
        record: ExecutionRecord,
        child: ChildOrder,
        *,
        number_of_slices: int,
        slice_duration: Decimal,
    ) -> int:
        submitted_at = record.child_submitted_monotonic.get(child.client_order_id)
        if submitted_at is None or slice_duration <= Decimal("0"):
            return number_of_slices

        elapsed = submitted_at - record.started_monotonic
        if elapsed <= Decimal("0"):
            return 1

        slice_index = int((elapsed / slice_duration).to_integral_value(rounding=ROUND_CEILING))
        if slice_index < 1:
            return 1
        if slice_index > number_of_slices:
            return number_of_slices
        return slice_index

    def _snapshot(self, record: ExecutionRecord) -> ExecutionRecord:
        return deepcopy(record)
