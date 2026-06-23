from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import httpx
import pytest

from api.app import create_app
from api.schemas import ExecutionCreateRequest
from exchanges.simulator import DeterministicSimulator
from execution import ids
from execution.models import (
    ChildOrder,
    ChildOrderStatus,
    Environment,
    Fill,
    MarketSnapshot,
    ReconciliationResult,
    Side,
)


SYMBOL = "BTCUSDT"


def execution_payload(
    *,
    environment: str = "simulation",
    symbol: str = SYMBOL,
    algorithm: str = "CHASE",
    target_position: str = "0.010",
    target_price_lower: str = "94000",
    target_price_upper: str = "97000",
    target_duration_seconds: int = 300,
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "environment": environment,
        "symbol": symbol,
        "algorithm": algorithm,
        "target_position": target_position,
        "target_price_lower": target_price_lower,
        "target_price_upper": target_price_upper,
        "target_duration_seconds": target_duration_seconds,
        "deadline_policy": "AGGRESSIVE_WITHIN_RANGE",
    }
    if parameters is not None:
        payload["parameters"] = parameters
    return payload


async def post_json(app: Any, url: str, payload: dict[str, Any] | None = None) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post(url, json=payload)


async def get_json(app: Any, url: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(url)


async def wait_for_execution(
    app: Any,
    execution_id: str,
    predicate,
    *,
    timeout_seconds: float = 1.5,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last_body: dict[str, Any] | None = None
    while asyncio.get_running_loop().time() < deadline:
        response = await get_json(app, f"/executions/{execution_id}")
        assert response.status_code == 200
        last_body = response.json()
        if predicate(last_body):
            return last_body
        await asyncio.sleep(0.02)
    assert last_body is not None
    return last_body


def assert_decimal_field(body: dict[str, Any], field: str, expected: str) -> None:
    assert isinstance(body[field], str)
    assert Decimal(body[field]) == Decimal(expected)


@pytest.mark.asyncio
async def test_create_execution_no_action_completes() -> None:
    app = create_app(simulator_position="0.010")

    response = await post_json(app, "/executions", execution_payload())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"
    assert body["child_orders"] == []
    assert_decimal_field(body, "raw_required_quantity", "0")
    assert_decimal_field(body, "required_quantity", "0")
    assert_decimal_field(body, "target_dust_quantity", "0")
    assert_decimal_field(body, "unfilled_quantity", "0")
    assert_decimal_field(body, "initial_position", "0.010")
    assert body["side"] == "NO_ACTION"
    assert body["request"] == {
        "environment": "simulation",
        "symbol": SYMBOL,
        "algorithm": "CHASE",
        "target_position": "0.010",
        "target_price_lower": "94000",
        "target_price_upper": "97000",
        "target_duration_seconds": 300,
        "deadline_policy": "AGGRESSIVE_WITHIN_RANGE",
        "parameters": {
            "reprice_threshold_bps": "2.0",
            "minimum_reprice_interval_ms": 500,
            "number_of_slices": 10,
            "child_order_timeout_seconds": 20,
            "max_post_only_reject_retries": 3,
            "repricing_mode": "ADVERSE_ONLY",
        },
    }
    assert body["summary_final_status"] == "COMPLETED"
    assert body["summary_final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"
    summary_metrics = body["summary_metrics"]
    assert summary_metrics["target_position"] == "0.010"
    assert summary_metrics["side"] == "NO_ACTION"
    assert summary_metrics["child_order_count"] == 0
    assert_decimal_field(summary_metrics, "initial_position", "0.010")
    assert_decimal_field(summary_metrics, "required_quantity", "0")
    assert body["started_monotonic"] is None
    assert body["last_reprice_monotonic"] is None


@pytest.mark.asyncio
async def test_testnet_request_constructs_binance_adapter_with_system_clock_and_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from config import BinanceUsdmCredentials
    from execution.clock import SystemClock
    from execution.models import Environment

    runtime_module = importlib.import_module("api.runtime")
    constructed: list[DeterministicSimulator] = []

    class RecordingBinanceAdapter(DeterministicSimulator):
        def __init__(self, *, settings: Any, clock: Any) -> None:
            super().__init__(clock=clock, position=Decimal("0.010"))
            self.settings = settings
            constructed.append(self)

    monkeypatch.setattr(
        runtime_module,
        "load_binance_usdm_credentials",
        lambda: BinanceUsdmCredentials(api_key="test-key", api_secret="test-secret"),
    )
    monkeypatch.setattr(runtime_module, "BinanceUsdmAdapter", RecordingBinanceAdapter)
    app = create_app()

    response = await post_json(
        app,
        "/executions",
        execution_payload(environment="testnet", target_position="0.010"),
    )

    assert response.status_code == 200
    assert len(constructed) == 1
    adapter = constructed[0]
    assert isinstance(adapter.clock, SystemClock)
    assert adapter.settings.environment is Environment.TESTNET
    assert adapter.settings.binance_api_key == "test-key"
    assert adapter.settings.binance_api_secret == "test-secret"
    assert response.json()["request"]["environment"] == "testnet"


@pytest.mark.asyncio
async def test_mainnet_request_requires_explicit_allow_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from config import BinanceUsdmCredentials

    runtime_module = importlib.import_module("api.runtime")
    monkeypatch.setattr(
        runtime_module,
        "load_binance_usdm_credentials",
        lambda: BinanceUsdmCredentials(api_key="test-key", api_secret="test-secret"),
    )
    monkeypatch.setattr(runtime_module, "load_allow_mainnet_trading", lambda: False)
    app = create_app()

    response = await post_json(
        app,
        "/executions",
        execution_payload(environment="mainnet", target_position="0.010"),
    )

    assert response.status_code == 503
    assert "ALLOW_MAINNET_TRADING=true" in response.json()["detail"]


@pytest.mark.asyncio
async def test_mainnet_request_constructs_adapter_only_when_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from config import BinanceUsdmCredentials

    runtime_module = importlib.import_module("api.runtime")
    constructed: list[DeterministicSimulator] = []

    class RecordingMainnetAdapter(DeterministicSimulator):
        def __init__(self, *, settings: Any, clock: Any) -> None:
            super().__init__(clock=clock, position=Decimal("0.010"))
            self.settings = settings
            constructed.append(self)

    monkeypatch.setattr(
        runtime_module,
        "load_binance_usdm_credentials",
        lambda: BinanceUsdmCredentials(api_key="mainnet-key", api_secret="mainnet-secret"),
    )
    monkeypatch.setattr(runtime_module, "load_allow_mainnet_trading", lambda: True)
    monkeypatch.setattr(runtime_module, "BinanceUsdmAdapter", RecordingMainnetAdapter)
    app = create_app()

    response = await post_json(
        app,
        "/executions",
        execution_payload(environment="mainnet", target_position="0.010"),
    )

    assert response.status_code == 200
    assert len(constructed) == 1
    adapter = constructed[0]
    assert adapter.settings.environment is Environment.MAINNET
    assert adapter.settings.allow_mainnet_trading is True
    assert adapter.settings.binance_api_key == "mainnet-key"
    assert adapter.settings.binance_api_secret == "mainnet-secret"


@pytest.mark.asyncio
async def test_runtime_applies_matching_user_event_without_rest_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(simulator_position="0")
    runtime = app.state.runtime
    adapter = app.state.adapter
    request = ExecutionCreateRequest.model_validate(execution_payload()).to_domain()
    created = await runtime.create_execution(request)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)
    opened = await runtime.run_once(created.execution_id)
    child = opened.child_orders[0]

    async def unexpected_rest_reconcile(*_args: Any, **_kwargs: Any) -> ReconciliationResult:
        raise AssertionError("REST reconciliation should not be used for a matching user event")

    def parse_event(_event: object) -> ReconciliationResult:
        return ReconciliationResult(
            orders=[
                ChildOrder(
                    child_order_id=child.child_order_id,
                    client_order_id=child.client_order_id,
                    symbol=SYMBOL,
                    side=Side.BUY,
                    submitted_quantity=child.submitted_quantity,
                    price=child.price,
                    status=ChildOrderStatus.PARTIALLY_FILLED,
                    confirmed_filled_quantity=Decimal("0.004"),
                )
            ],
            fills=[
                Fill(
                    client_order_id=child.client_order_id,
                    trade_id="stream-trade",
                    cumulative_filled_quantity=Decimal("0.004"),
                    last_filled_quantity=Decimal("0.004"),
                    last_fill_price=child.price,
                    event_time_ms=123,
                    transaction_time_ms=124,
                    is_maker=True,
                )
            ],
        )

    monkeypatch.setattr(adapter, "reconcile_orders_and_fills", unexpected_rest_reconcile)
    monkeypatch.setattr(adapter, "reconciliation_from_user_event", parse_event, raising=False)

    applied = await runtime._apply_user_event_reconciliation(
        Environment.SIMULATION,
        {"event_type": "ORDER_TRADE_UPDATE", "event_time_ms": 123},
    )
    updated = await runtime.get_execution(created.execution_id)

    assert applied is True
    assert updated.exposure.confirmed_filled_quantity == Decimal("0.004")
    assert updated.maker_filled_quantity == Decimal("0.004")
    assert updated.child_orders[0].status is ChildOrderStatus.PARTIALLY_FILLED


@pytest.mark.asyncio
async def test_runtime_applies_matching_user_event_to_terminal_execution_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(simulator_position="0")
    runtime = app.state.runtime
    adapter = app.state.adapter
    request = ExecutionCreateRequest.model_validate(
        execution_payload(target_position="0.004")
    ).to_domain()
    created = await runtime.create_execution(request)
    await adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)
    opened = await runtime.run_once(created.execution_id)
    child = opened.child_orders[0]
    await adapter.push_fill(child.client_order_id, Decimal("0.004"), Decimal("95010.00"))

    terminal = await runtime.run_once(created.execution_id)
    assert terminal.status.value == "COMPLETED"
    assert terminal.final_reason == "TARGET_QUANTITY_FILLED"
    assert terminal.summary is not None
    assert terminal.summary.metrics["execution_vwap"] == "95000"
    assert terminal.summary.metrics["maker_fills"] == 0
    assert terminal.summary.metrics["taker_fills"] == 0

    async def unexpected_rest_reconcile(*_args: Any, **_kwargs: Any) -> ReconciliationResult:
        raise AssertionError("REST reconciliation should not be used for a matching user event")

    def parse_event(_event: object) -> ReconciliationResult:
        return ReconciliationResult(
            orders=[],
            fills=[
                Fill(
                    client_order_id=child.client_order_id,
                    trade_id="late-stream-trade",
                    cumulative_filled_quantity=Decimal("0.004"),
                    last_filled_quantity=Decimal("0.004"),
                    last_fill_price=Decimal("95010.00"),
                    event_time_ms=123,
                    transaction_time_ms=124,
                    is_maker=False,
                )
            ],
        )

    monkeypatch.setattr(adapter, "reconcile_orders_and_fills", unexpected_rest_reconcile)
    monkeypatch.setattr(adapter, "reconciliation_from_user_event", parse_event, raising=False)

    applied = await runtime._apply_user_event_reconciliation(
        Environment.SIMULATION,
        {"event_type": "ORDER_TRADE_UPDATE", "event_time_ms": 123},
    )
    updated = await runtime.get_execution(created.execution_id)

    assert applied is True
    assert updated.status is terminal.status
    assert updated.final_reason == terminal.final_reason
    assert updated.completed_monotonic == terminal.completed_monotonic
    assert updated.summary is not None
    assert updated.summary.metrics["execution_vwap"] == "95010"
    assert updated.summary.metrics["maker_fills"] == 0
    assert updated.summary.metrics["taker_fills"] == 1
    assert updated.summary.metrics["taker_filled_quantity"] == Decimal("0.004")


@pytest.mark.asyncio
async def test_runtime_starts_binance_stream_supervisors_and_renews_listen_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from config import BinanceUsdmCredentials

    runtime_module = importlib.import_module("api.runtime")
    constructed: list[DeterministicSimulator] = []

    class StreamingBinanceAdapter(DeterministicSimulator):
        def __init__(self, *, settings: Any, clock: Any) -> None:
            super().__init__(clock=clock, position=Decimal("0.010"))
            self.settings = settings
            self.latest_listen_key = "listen-1"
            self.market_stream_healthy = False
            self.user_stream_healthy = False
            self.market_started = asyncio.Event()
            self.market_closed = asyncio.Event()
            self.market_continue = asyncio.Event()
            self.user_started = asyncio.Event()
            self.user_closed = asyncio.Event()
            self.user_continue = asyncio.Event()
            self.renewed = asyncio.Event()
            self.renewed_listen_keys: list[str] = []
            constructed.append(self)

        def stream_market_data(self):
            async def events():
                self.market_stream_healthy = True
                self.market_started.set()
                try:
                    yield MarketSnapshot(
                        symbol=SYMBOL,
                        bid=Decimal("95000.00"),
                        ask=Decimal("95001.00"),
                        last_market_event_time_exchange=1,
                        last_market_event_time_local_monotonic=self.clock.monotonic(),
                    )
                    await self.market_continue.wait()
                    yield MarketSnapshot(
                        symbol=SYMBOL,
                        bid=Decimal("95000.10"),
                        ask=Decimal("95001.10"),
                        last_market_event_time_exchange=2,
                        last_market_event_time_local_monotonic=self.clock.monotonic(),
                    )
                finally:
                    self.market_stream_healthy = False
                    self.market_closed.set()

            return events()

        def stream_user_events(self):
            async def events():
                self.user_stream_healthy = True
                self.user_started.set()
                try:
                    await self.user_continue.wait()
                    yield {"event_type": "noop"}
                finally:
                    self.user_stream_healthy = False
                    self.user_closed.set()

            return events()

        async def renew_listen_key(self, listen_key: str) -> None:
            self.renewed_listen_keys.append(listen_key)
            self.renewed.set()

        async def health_check_streams(self) -> bool:
            return self.market_stream_healthy and self.user_stream_healthy

    monkeypatch.setattr(
        runtime_module,
        "load_binance_usdm_credentials",
        lambda: BinanceUsdmCredentials(api_key="test-key", api_secret="test-secret"),
    )
    monkeypatch.setattr(runtime_module, "BinanceUsdmAdapter", StreamingBinanceAdapter)

    runtime = runtime_module.ExecutionRuntime(
        background_tick_interval_seconds=0.01,
        stream_keepalive_interval_seconds=0.01,
    )
    await runtime.start()
    try:
        request = ExecutionCreateRequest.model_validate(
            execution_payload(environment="testnet", target_position="0.010")
        ).to_domain()
        created = await runtime.create_execution(request)
        adapter = constructed[0]

        await asyncio.wait_for(adapter.market_started.wait(), timeout=0.5)
        await asyncio.wait_for(adapter.user_started.wait(), timeout=0.5)
        await asyncio.wait_for(adapter.renewed.wait(), timeout=0.5)

        assert adapter.renewed_listen_keys
        assert set(adapter.renewed_listen_keys) == {"listen-1"}
        assert adapter.market_stream_healthy is True
        assert not adapter.market_closed.is_set()
    finally:
        await runtime.stop()

    assert constructed[0].market_closed.is_set()
    assert constructed[0].user_closed.is_set()


@pytest.mark.asyncio
async def test_runtime_restarts_binance_user_stream_after_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from config import BinanceUsdmCredentials

    runtime_module = importlib.import_module("api.runtime")
    constructed: list[DeterministicSimulator] = []

    class DisconnectingUserStreamAdapter(DeterministicSimulator):
        def __init__(self, *, settings: Any, clock: Any) -> None:
            super().__init__(clock=clock, position=Decimal("0"))
            self.settings = settings
            self.latest_listen_key = "listen-1"
            self.market_stream_healthy = False
            self.user_stream_healthy = False
            self.market_continue = asyncio.Event()
            self.first_user_started = asyncio.Event()
            self.second_user_started = asyncio.Event()
            self.disconnect_user = asyncio.Event()
            self.user_continue = asyncio.Event()
            self.user_runs = 0
            self.reconciliation_windows: list[tuple[str | None, int | None, int | None]] = []
            constructed.append(self)

        def stream_market_data(self):
            async def events():
                self.market_stream_healthy = True
                try:
                    await self.market_continue.wait()
                    yield MarketSnapshot(
                        symbol=SYMBOL,
                        bid=Decimal("95000.00"),
                        ask=Decimal("95001.00"),
                        last_market_event_time_exchange=1,
                        last_market_event_time_local_monotonic=self.clock.monotonic(),
                    )
                finally:
                    self.market_stream_healthy = False

            return events()

        def stream_user_events(self):
            async def events():
                self.user_runs += 1
                run_number = self.user_runs
                self.user_stream_healthy = True
                if run_number == 1:
                    self.first_user_started.set()
                    try:
                        await self.disconnect_user.wait()
                        return
                    finally:
                        self.user_stream_healthy = False

                self.second_user_started.set()
                try:
                    await self.user_continue.wait()
                    yield {"event_type": "noop"}
                finally:
                    self.user_stream_healthy = False

            return events()

        async def health_check_streams(self) -> bool:
            return self.market_stream_healthy and self.user_stream_healthy

        async def reconcile_orders_and_fills(
            self,
            symbol: str,
            client_order_prefix: str | None = None,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ):
            self.reconciliation_windows.append((client_order_prefix, start_time_ms, end_time_ms))
            return await super().reconcile_orders_and_fills(
                symbol,
                client_order_prefix=client_order_prefix,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )

    monkeypatch.setattr(
        runtime_module,
        "load_binance_usdm_credentials",
        lambda: BinanceUsdmCredentials(api_key="test-key", api_secret="test-secret"),
    )
    monkeypatch.setattr(runtime_module, "BinanceUsdmAdapter", DisconnectingUserStreamAdapter)

    runtime = runtime_module.ExecutionRuntime(
        background_tick_interval_seconds=0.01,
        stream_restart_delay_seconds=0.01,
    )
    await runtime.start()
    try:
        request = ExecutionCreateRequest.model_validate(
            execution_payload(environment="testnet", target_position="0.010")
        ).to_domain()
        created = await runtime.create_execution(request)
        adapter = constructed[0]

        await asyncio.wait_for(adapter.first_user_started.wait(), timeout=0.5)
        adapter.disconnect_user.set()
        await asyncio.wait_for(adapter.second_user_started.wait(), timeout=0.5)

        assert adapter.user_runs == 2
        assert await adapter.health_check_streams() is True
        bounded_windows = [
            window
            for window in adapter.reconciliation_windows
            if window[1] is not None and window[2] is not None
        ]
        assert bounded_windows
        prefix, start_time_ms, end_time_ms = bounded_windows[-1]
        assert prefix == ids.make_client_order_prefix(created.execution_id)
        assert start_time_ms is not None
        assert end_time_ms is not None
        assert end_time_ms >= start_time_ms
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_user_stream_event_reconciles_active_execution_with_event_time_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from config import BinanceUsdmCredentials

    runtime_module = importlib.import_module("api.runtime")
    constructed: list[DeterministicSimulator] = []

    class EventReconcilingUserStreamAdapter(DeterministicSimulator):
        def __init__(self, *, settings: Any, clock: Any) -> None:
            super().__init__(clock=clock, position=Decimal("0"))
            self.settings = settings
            self.latest_listen_key = "listen-1"
            self.user_started = asyncio.Event()
            self.event_to_emit: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
            self.reconciliation_windows: list[tuple[str | None, int | None, int | None]] = []
            constructed.append(self)

        def stream_user_events(self):
            async def events():
                self._user_stream_healthy = True
                self.user_started.set()
                try:
                    yield await self.event_to_emit.get()
                    await asyncio.Event().wait()
                finally:
                    self._user_stream_healthy = False

            return events()

        async def reconcile_orders_and_fills(
            self,
            symbol: str,
            client_order_prefix: str | None = None,
            *,
            start_time_ms: int | None = None,
            end_time_ms: int | None = None,
        ):
            self.reconciliation_windows.append((client_order_prefix, start_time_ms, end_time_ms))
            return await super().reconcile_orders_and_fills(
                symbol,
                client_order_prefix=client_order_prefix,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )

    monkeypatch.setattr(
        runtime_module,
        "load_binance_usdm_credentials",
        lambda: BinanceUsdmCredentials(api_key="test-key", api_secret="test-secret"),
    )
    monkeypatch.setattr(runtime_module, "BinanceUsdmAdapter", EventReconcilingUserStreamAdapter)

    runtime = runtime_module.ExecutionRuntime(background_tick_interval_seconds=0.01)
    await runtime.start()
    try:
        request = ExecutionCreateRequest.model_validate(
            execution_payload(environment="testnet", target_position="0.010")
        ).to_domain()
        created = await runtime.create_execution(request)
        adapter = constructed[0]
        await asyncio.wait_for(adapter.user_started.wait(), timeout=0.5)

        await adapter.event_to_emit.put({"event_type": "ORDER_TRADE_UPDATE", "event_time_ms": 123_456})

        deadline = asyncio.get_running_loop().time() + 0.5
        while (
            not any(window[2] == 123_456 for window in adapter.reconciliation_windows)
            and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.01)

        bounded_window = next(
            window for window in adapter.reconciliation_windows if window[2] == 123_456
        )
        prefix, start_time_ms, end_time_ms = bounded_window
        assert prefix == ids.make_client_order_prefix(created.execution_id)
        assert start_time_ms == 63_456
        assert end_time_ms == 123_456
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_background_loop_advances_twap_without_external_run_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from config import BinanceUsdmCredentials

    runtime_module = importlib.import_module("api.runtime")

    class AutoMarketBinanceAdapter(DeterministicSimulator):
        def __init__(self, *, settings: Any, clock: Any) -> None:
            super().__init__(clock=clock, position=Decimal("0"))
            self.settings = settings

        async def get_best_bid_ask(self, symbol: str) -> MarketSnapshot:
            return MarketSnapshot(
                symbol=symbol,
                bid=Decimal("95000.00"),
                ask=Decimal("95001.00"),
                last_market_event_time_exchange=1,
                last_market_event_time_local_monotonic=self.clock.monotonic(),
            )

    monkeypatch.setattr(
        runtime_module,
        "load_binance_usdm_credentials",
        lambda: BinanceUsdmCredentials(api_key="test-key", api_secret="test-secret"),
    )
    monkeypatch.setattr(runtime_module, "BinanceUsdmAdapter", AutoMarketBinanceAdapter)
    app = create_app(background_tick_interval_seconds=0.01)
    await app.state.runtime.start()
    try:
        created_response = await post_json(
            app,
            "/executions",
            execution_payload(
                environment="testnet",
                algorithm="TWAP",
                target_duration_seconds=1,
                parameters={
                    "number_of_slices": 2,
                    "child_order_timeout_seconds": 10,
                },
            ),
        )
        created = created_response.json()

        progressed = await wait_for_execution(
            app,
            created["execution_id"],
            lambda body: len(body["child_orders"]) == 1,
        )

        assert created_response.status_code == 200
        assert progressed["status"] == "RUNNING"
        assert progressed["child_orders"][0]["submitted_quantity"] == "0.005"
    finally:
        await app.state.runtime.stop()


@pytest.mark.asyncio
async def test_background_loop_reconciles_unknown_child_without_manual_reconcile() -> None:
    app = create_app(simulator_position="0", background_tick_interval_seconds=0.01)
    await app.state.runtime.start()
    try:
        created_response = await post_json(app, "/executions", execution_payload())
        created = created_response.json()
        prefix = ids.make_client_order_prefix(created["execution_id"])
        app.state.adapter.script_create_timeout(prefix)
        await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)

        reconciled = await wait_for_execution(
            app,
            created["execution_id"],
            lambda body: bool(body["child_orders"])
            and body["child_orders"][0]["status"] == "OPEN",
            timeout_seconds=1.0,
        )

        assert created_response.status_code == 200
        assert reconciled["unknown_order_quantity"] == "0"
        assert reconciled["child_orders"][0]["terminal_reason"] is None
    finally:
        await app.state.runtime.stop()


@pytest.mark.asyncio
async def test_background_loop_records_unexpected_failure_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(simulator_position="0", background_tick_interval_seconds=0.01)
    created_response = await post_json(app, "/executions", execution_payload())
    created = created_response.json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)
    original_run_once = app.state.service.run_once
    calls = 0

    async def flaky_run_once(execution_id: str):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("background loop boom")
        return await original_run_once(execution_id)

    monkeypatch.setattr(app.state.service, "run_once", flaky_run_once)
    await app.state.runtime.start()
    try:
        progressed = await wait_for_execution(
            app,
            created["execution_id"],
            lambda body: len(body["child_orders"]) == 1,
            timeout_seconds=1.0,
        )

        assert progressed["child_orders"][0]["status"] == "OPEN"
        assert calls >= 2
        assert "background loop boom" in app.state.runtime.runtime_errors[created["execution_id"]][-1]
        assert app.state.runtime.background_task_count == 1
    finally:
        await app.state.runtime.stop()


@pytest.mark.asyncio
async def test_background_loop_resolves_unknown_by_exact_lookup_without_broad_reconcile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(simulator_position="0", background_tick_interval_seconds=0.01)
    created_response = await post_json(app, "/executions", execution_payload())
    created = created_response.json()
    prefix = ids.make_client_order_prefix(created["execution_id"])
    app.state.adapter.script_create_timeout(prefix)
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)

    calls = 0

    async def unexpected_reconcile(execution_id: str):
        nonlocal calls
        calls += 1
        raise AssertionError(f"unexpected broad reconcile for {execution_id}")

    monkeypatch.setattr(app.state.runtime, "reconcile_execution", unexpected_reconcile)
    await app.state.runtime.start()
    try:
        reconciled = await wait_for_execution(
            app,
            created["execution_id"],
            lambda body: bool(body["child_orders"]) and body["unknown_order_quantity"] == "0",
            timeout_seconds=1.0,
        )

        assert reconciled["child_orders"][0]["status"] == "OPEN"
        assert calls == 0
        assert app.state.runtime.background_task_count == 1
    finally:
        await app.state.runtime.stop()


@pytest.mark.asyncio
async def test_runtime_stop_cancels_and_reconciles_active_execution() -> None:
    app = create_app(simulator_position="0", background_tick_interval_seconds=0.05)
    await app.state.runtime.start()
    created_response = await post_json(app, "/executions", execution_payload())
    created = created_response.json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)
    opened_response = await post_json(app, f"/executions/{created['execution_id']}/run-once")

    assert created_response.status_code == 200
    assert opened_response.status_code == 200
    assert opened_response.json()["child_orders"][0]["status"] == "OPEN"

    await app.state.runtime.stop()
    stopped = await app.state.runtime.get_execution(created["execution_id"])

    assert stopped.status.value == "CANCELLED"
    assert stopped.exposure.reserved_exposure == Decimal("0")
    assert stopped.child_orders[0].status.value == "CANCELLED"


@pytest.mark.asyncio
async def test_runtime_stop_cancels_background_execution_tasks() -> None:
    app = create_app(background_tick_interval_seconds=0.01)
    await app.state.runtime.start()
    created_response = await post_json(app, "/executions", execution_payload())

    assert created_response.status_code == 200
    assert app.state.runtime.background_task_count == 1

    await app.state.runtime.stop()

    assert app.state.runtime.background_task_count == 0
    assert app.state.runtime.is_started is False


@pytest.mark.asyncio
async def test_runtime_start_during_stop_does_not_restart_until_shutdown_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    runtime_module = importlib.import_module("api.runtime")
    runtime = runtime_module.ExecutionRuntime()
    stop_entered = asyncio.Event()
    release_stop = asyncio.Event()

    async def slow_cancel_and_reconcile() -> None:
        stop_entered.set()
        await release_stop.wait()

    monkeypatch.setattr(runtime, "_cancel_and_reconcile_active_executions", slow_cancel_and_reconcile)

    await runtime.start()
    stop_task = asyncio.create_task(runtime.stop())
    await asyncio.wait_for(stop_entered.wait(), timeout=0.5)

    await runtime.start()

    assert runtime.is_started is False

    release_stop.set()
    await asyncio.wait_for(stop_task, timeout=0.5)

    assert runtime.is_started is False


@pytest.mark.asyncio
async def test_cancel_terminal_execution_is_idempotent_and_preserves_reason() -> None:
    app = create_app(simulator_position="0.010")
    created = (await post_json(app, "/executions", execution_payload())).json()

    first = await post_json(app, f"/executions/{created['execution_id']}/cancel")
    second = await post_json(app, f"/executions/{created['execution_id']}/cancel")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["status"] == "COMPLETED"
    assert second.json()["status"] == "COMPLETED"
    assert first.json()["final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"
    assert second.json()["final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"


@pytest.mark.asyncio
async def test_create_nonzero_execution_then_get_returns_running_buy() -> None:
    app = create_app(simulator_position="0")
    created_response = await post_json(app, "/executions", execution_payload())
    created = created_response.json()

    fetched_response = await get_json(app, f"/executions/{created['execution_id']}")

    assert created_response.status_code == 200
    assert fetched_response.status_code == 200
    fetched = fetched_response.json()
    assert fetched["execution_id"] == created["execution_id"]
    assert fetched["status"] == "RUNNING"
    assert fetched["side"] == "BUY"
    assert_decimal_field(fetched, "raw_required_quantity", "0.010")
    assert_decimal_field(fetched, "required_quantity", "0.010")
    assert_decimal_field(fetched, "target_dust_quantity", "0")
    assert_decimal_field(fetched, "unfilled_quantity", "0.010")
    assert fetched["child_orders"] == []


@pytest.mark.asyncio
async def test_second_active_execution_for_same_environment_and_symbol_returns_409() -> None:
    app = create_app(simulator_position="0")
    first = await post_json(app, "/executions", execution_payload())

    second = await post_json(app, "/executions", execution_payload(target_position="0.020"))

    assert first.status_code == 200
    assert second.status_code == 409
    assert "active execution already exists" in second.json()["detail"]


@pytest.mark.asyncio
async def test_create_execution_below_step_returns_untradeable_dust_fields() -> None:
    app = create_app(simulator_position="0")

    response = await post_json(app, "/executions", execution_payload(target_position="0.0005"))

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["final_reason"] == "UNTRADEABLE_TARGET_DUST"
    assert_decimal_field(body, "raw_required_quantity", "0.0005")
    assert_decimal_field(body, "required_quantity", "0")
    assert_decimal_field(body, "target_dust_quantity", "0.0005")
    assert_decimal_field(body, "unfilled_quantity", "0")
    assert body["child_orders"] == []


@pytest.mark.asyncio
async def test_run_once_creates_child_order_when_market_data_is_present() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload())).json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)

    response = await post_json(app, f"/executions/{created['execution_id']}/run-once")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RUNNING"
    assert len(body["child_orders"]) == 1
    child = body["child_orders"][0]
    assert child["child_order_id"] == "child_0001"
    assert child["status"] == "OPEN"
    assert child["side"] == "BUY"
    assert child["submitted_quantity"] == "0.010"
    assert child["filled_quantity"] == "0"
    assert child["remaining_quantity"] == "0.010"
    assert child["price"] == "95000.00"
    assert child["terminal_reason"] is None


@pytest.mark.asyncio
async def test_terminal_response_serializes_rich_summary_metrics() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload(target_position="0.004"))).json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)
    opened = (await post_json(app, f"/executions/{created['execution_id']}/run-once")).json()
    child = opened["child_orders"][0]
    await app.state.adapter.push_fill(child["client_order_id"], Decimal("0.004"), Decimal("95010.00"))

    response = await post_json(app, f"/executions/{created['execution_id']}/reconcile")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    metrics = body["summary_metrics"]
    assert metrics["final_status"] == "COMPLETED"
    assert metrics["raw_required_quantity"] == "0.004"
    assert metrics["required_quantity"] == "0.004"
    assert Decimal(metrics["target_dust_quantity"]) == Decimal("0")
    assert metrics["filled_quantity"] == "0.004"
    assert metrics["unfilled_quantity"] == "0"
    assert metrics["completion_rate"] == "1"
    assert metrics["arrival_bid"] == "95000"
    assert metrics["arrival_ask"] == "95001"
    assert metrics["arrival_mid"] == "95000.5"
    assert metrics["execution_vwap"] == "95010"
    assert Decimal(metrics["slippage_bps"]) > Decimal("0")
    assert metrics["requested_duration_seconds"] == 300
    assert metrics["actual_duration_seconds"] == "0"
    assert metrics["max_reserved_exposure"] == "0.004"
    assert metrics["overfill_quantity"] == "0"


@pytest.mark.asyncio
async def test_reconcile_endpoint_returns_current_state() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload())).json()

    response = await post_json(app, f"/executions/{created['execution_id']}/reconcile")

    assert response.status_code == 200
    body = response.json()
    assert body["execution_id"] == created["execution_id"]
    assert body["status"] == "RUNNING"
    assert body["child_orders"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/executions/missing"),
        ("POST", "/executions/missing/cancel"),
        ("POST", "/executions/missing/run-once"),
        ("POST", "/executions/missing/reconcile"),
    ],
)
async def test_unknown_execution_id_returns_404(method: str, path: str) -> None:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.request(method, path)

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_internal_key_error_is_not_converted_to_404(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app()

    async def raise_key_error(execution_id: str) -> None:
        raise KeyError("internal state bug")

    monkeypatch.setattr(app.state.service, "get_execution", raise_key_error)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(KeyError, match="internal state bug"):
            await client.get("/executions/anything")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        execution_payload(target_position=0.010),  # type: ignore[arg-type]
        execution_payload(target_price_lower=94000),  # type: ignore[arg-type]
        execution_payload(target_price_upper=97000),  # type: ignore[arg-type]
        execution_payload(parameters={"reprice_threshold_bps": 2.0}),
    ],
)
async def test_json_float_decimal_fields_are_rejected(payload: dict[str, Any]) -> None:
    app = create_app()

    response = await post_json(app, "/executions", payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_lower_greater_than_upper_is_rejected() -> None:
    app = create_app()

    response = await post_json(
        app,
        "/executions",
        execution_payload(target_price_lower="97000", target_price_upper="94000"),
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_empty_parameters_default_and_repricing_mode_accepts_enum() -> None:
    app = create_app(simulator_position="0")
    created = await post_json(
        app,
        "/executions",
        execution_payload(
            parameters={
                "repricing_mode": "TWO_SIDED",
                "max_post_only_reject_retries": 5,
            }
        ),
    )
    empty = await post_json(
        create_app(simulator_position="0"),
        "/executions",
        execution_payload(parameters={}),
    )

    assert created.status_code == 200
    assert empty.status_code == 200
    assert created.json()["status"] == "RUNNING"
    assert created.json()["request"]["parameters"]["repricing_mode"] == "TWO_SIDED"
    assert created.json()["request"]["parameters"]["max_post_only_reject_retries"] == 5
    assert empty.json()["status"] == "RUNNING"
    assert empty.json()["request"]["parameters"]["max_post_only_reject_retries"] == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        execution_payload(symbol="ETHUSDT"),
        execution_payload(target_price_lower="0"),
        execution_payload(target_price_upper="0"),
        execution_payload(target_price_lower="-1"),
        execution_payload(target_price_upper="-1"),
        execution_payload(target_position="NaN"),
        execution_payload(parameters={"reprice_threshold_bps": "-0.1"}),
        execution_payload(parameters={"minimum_reprice_interval_ms": -1}),
        execution_payload(parameters={"number_of_slices": 0}),
        execution_payload(parameters={"child_order_timeout_seconds": 0}),
        execution_payload(parameters={"max_post_only_reject_retries": 0}),
    ],
)
async def test_invalid_execution_request_fields_are_rejected(payload: dict[str, Any]) -> None:
    app = create_app()

    response = await post_json(app, "/executions", payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_response_child_and_exposure_decimal_fields_are_strings_after_run_once() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload())).json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)

    response = await post_json(app, f"/executions/{created['execution_id']}/run-once")

    assert response.status_code == 200
    body = response.json()
    for field in [
        "raw_required_quantity",
        "required_quantity",
        "target_dust_quantity",
        "unfilled_quantity",
        "confirmed_filled_quantity",
        "live_open_quantity",
        "pending_submit_quantity",
        "pending_cancel_quantity",
        "unknown_order_quantity",
        "reserved_exposure",
    ]:
        assert isinstance(body[field], str)

    child = body["child_orders"][0]
    for field in ["submitted_quantity", "filled_quantity", "remaining_quantity", "price"]:
        assert isinstance(child[field], str)
    assert body["request"]["target_position"] == "0.010"
    assert body["request"]["target_price_lower"] == "94000"
    assert body["request"]["target_price_upper"] == "97000"
    assert body["request"]["parameters"]["reprice_threshold_bps"] == "2.0"
    assert body["summary_final_status"] is None
    assert body["summary_final_reason"] is None
    assert body["summary_metrics"] is None
    assert body["started_monotonic"] == "0.0"
    assert body["last_reprice_monotonic"] is None
