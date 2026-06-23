"""Runtime supervisor for execution services and exchange streams."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from typing import Any

from config import Settings, load_allow_mainnet_trading, load_binance_usdm_credentials
from exchanges.binance_usdm import BinanceUsdmAdapter
from exchanges.simulator import DeterministicSimulator
from execution import ids
from execution.clock import ManualClock, SystemClock
from execution.engine import ExecutionRecord, UnknownExecution
from execution.models import Environment, ExecutionRequest, ReconciliationResult
from execution.service import ExecutionService


class ActiveExecutionConflict(RuntimeError):
    """Raised when a symbol already has an active execution in an environment."""

    pass


class RuntimeConfigurationError(RuntimeError):
    """Raised when a requested runtime environment is not configured."""

    pass


USER_EVENT_RECONCILIATION_LOOKBACK_MS = 60_000


class ExecutionRuntime:
    """Supervises execution services, background loops, streams, and shutdown."""

    def __init__(
        self,
        *,
        simulator_position: str = "0",
        background_tick_interval_seconds: float = 0.25,
        stream_keepalive_interval_seconds: float = 30 * 60,
        stream_restart_delay_seconds: float = 1.0,
    ) -> None:
        if background_tick_interval_seconds <= 0:
            raise ValueError("background_tick_interval_seconds must be greater than 0")
        if stream_keepalive_interval_seconds <= 0:
            raise ValueError("stream_keepalive_interval_seconds must be greater than 0")
        if stream_restart_delay_seconds <= 0:
            raise ValueError("stream_restart_delay_seconds must be greater than 0")

        self._background_tick_interval_seconds = background_tick_interval_seconds
        self._stream_keepalive_interval_seconds = stream_keepalive_interval_seconds
        self._stream_restart_delay_seconds = stream_restart_delay_seconds
        self._services: dict[Environment, ExecutionService] = {}
        self._adapters: dict[Environment, object] = {}
        self._clocks: dict[Environment, object] = {}
        self._execution_environments: dict[str, Environment] = {}
        self._execution_tasks: dict[str, asyncio.Task[None]] = {}
        self._stream_tasks: dict[tuple[Environment, str], asyncio.Task[None]] = {}
        self._listen_key_tasks: dict[Environment, asyncio.Task[None]] = {}
        self._runtime_errors: dict[str, list[str]] = {}
        self._lock = asyncio.Lock()
        self._started = False
        self._stopping = False

        simulation_clock = ManualClock()
        simulation_adapter = DeterministicSimulator(
            clock=simulation_clock,
            position=Decimal(simulator_position),
        )
        self._register(
            Environment.SIMULATION,
            adapter=simulation_adapter,
            clock=simulation_clock,
            service=ExecutionService(simulation_adapter, clock=simulation_clock),
        )

    @property
    def simulation_clock(self) -> ManualClock:
        return self._clocks[Environment.SIMULATION]  # type: ignore[return-value]

    @property
    def simulation_adapter(self) -> DeterministicSimulator:
        return self._adapters[Environment.SIMULATION]  # type: ignore[return-value]

    @property
    def simulation_service(self) -> ExecutionService:
        return self._services[Environment.SIMULATION]

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def background_task_count(self) -> int:
        return sum(1 for task in self._execution_tasks.values() if not task.done())

    @property
    def runtime_errors(self) -> dict[str, list[str]]:
        return {key: list(errors) for key, errors in self._runtime_errors.items()}

    async def start(self) -> None:
        async with self._lock:
            if self._started or self._stopping:
                return
            self._started = True
            for environment, adapter in list(self._adapters.items()):
                self._start_stream_supervisors(environment, adapter)
            for service in list(self._services.values()):
                for record in await service.active_executions():
                    self._remember_execution(record)
                    self._schedule_background_loop(record)

    async def stop(self) -> None:
        async with self._lock:
            if self._stopping:
                return
            self._stopping = True
            self._started = False
            execution_tasks = list(self._execution_tasks.values())
            runtime_tasks = [
                *self._stream_tasks.values(),
                *self._listen_key_tasks.values(),
            ]

        for task in execution_tasks:
            task.cancel()
        if execution_tasks:
            await asyncio.gather(*execution_tasks, return_exceptions=True)

        await self._cancel_and_reconcile_active_executions()

        for task in runtime_tasks:
            task.cancel()
        if runtime_tasks:
            await asyncio.gather(*runtime_tasks, return_exceptions=True)

        async with self._lock:
            self._execution_tasks.clear()
            self._stream_tasks.clear()
            self._listen_key_tasks.clear()
            self._started = False
            self._stopping = False

    async def create_execution(self, request: ExecutionRequest) -> ExecutionRecord:
        async with self._lock:
            if self._stopping:
                raise RuntimeConfigurationError("runtime is stopping")
            service = self._service_for_environment(request.environment)
            active = await service.active_execution_for(request.environment, request.symbol)
            if active is not None:
                raise ActiveExecutionConflict(
                    "active execution already exists for "
                    f"{request.environment.value} {request.symbol}: {active.execution_id}"
                )

            record = await service.create_execution(request)
            self._remember_execution(record)
            self._schedule_background_loop(record)
            return record

    async def get_execution(self, execution_id: str) -> ExecutionRecord:
        service = await self._service_for_execution(execution_id)
        record = await service.get_execution(execution_id)
        self._remember_execution(record)
        return record

    async def cancel_execution(self, execution_id: str) -> ExecutionRecord:
        service = await self._service_for_execution(execution_id)
        record = await service.cancel_execution(execution_id)
        self._remember_execution(record)
        self._cancel_background_loop_if_terminal(record)
        return record

    async def run_once(self, execution_id: str) -> ExecutionRecord:
        service = await self._service_for_execution(execution_id)
        record = await service.run_once(execution_id)
        self._remember_execution(record)
        self._cancel_background_loop_if_terminal(record)
        return record

    async def reconcile_execution(
        self,
        execution_id: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> ExecutionRecord:
        service = await self._service_for_execution(execution_id)
        record = await service.reconcile_execution(
            execution_id,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        self._remember_execution(record)
        self._cancel_background_loop_if_terminal(record)
        return record

    def _register(
        self,
        environment: Environment,
        *,
        adapter: object,
        clock: object,
        service: ExecutionService,
    ) -> None:
        self._adapters[environment] = adapter
        self._clocks[environment] = clock
        self._services[environment] = service
        if self._started:
            self._start_stream_supervisors(environment, adapter)

    def _service_for_environment(self, environment: Environment) -> ExecutionService:
        if environment in self._services:
            return self._services[environment]
        if environment in {Environment.TESTNET, Environment.MAINNET}:
            return self._build_binance_service(environment)
        raise RuntimeConfigurationError(f"{environment.value} execution is not enabled")

    def _build_binance_service(self, environment: Environment) -> ExecutionService:
        credentials = load_binance_usdm_credentials()
        if not credentials.is_configured:
            raise RuntimeConfigurationError(
                f"Binance USDM credentials are required for {environment.value} execution"
            )
        allow_mainnet_trading = False
        if environment is Environment.MAINNET:
            allow_mainnet_trading = load_allow_mainnet_trading()
        if environment is Environment.MAINNET and not allow_mainnet_trading:
            raise RuntimeConfigurationError(
                "mainnet execution requires ALLOW_MAINNET_TRADING=true"
            )

        clock = SystemClock()
        settings = Settings(
            environment=environment,
            allow_mainnet_trading=allow_mainnet_trading,
            binance_api_key=credentials.api_key,
            binance_api_secret=credentials.api_secret,
        )
        adapter = BinanceUsdmAdapter(settings=settings, clock=clock)
        service = ExecutionService(adapter, clock=clock)
        self._register(environment, adapter=adapter, clock=clock, service=service)
        return service

    async def _service_for_execution(self, execution_id: str) -> ExecutionService:
        if environment := self._execution_environments.get(execution_id):
            return self._service_for_environment(environment)

        for environment, service in list(self._services.items()):
            try:
                record = await service.get_execution(execution_id)
            except UnknownExecution:
                continue
            self._execution_environments[execution_id] = environment
            return service
        raise UnknownExecution(execution_id)

    def _remember_execution(self, record: ExecutionRecord) -> None:
        self._execution_environments[record.execution_id] = record.request.environment

    def _schedule_background_loop(self, record: ExecutionRecord) -> None:
        if not self._started or record.status.is_terminal:
            return
        if record.execution_id in self._execution_tasks:
            return

        task = asyncio.create_task(self._run_background_loop(record.execution_id))
        self._execution_tasks[record.execution_id] = task
        task.add_done_callback(
            lambda completed, execution_id=record.execution_id: self._discard_background_task(
                execution_id,
                completed,
            )
        )

    async def _run_background_loop(self, execution_id: str) -> None:
        while self._started:
            try:
                record = await self.run_once(execution_id)
            except asyncio.CancelledError:
                raise
            except UnknownExecution as exc:
                self._record_runtime_error(execution_id, exc)
                return
            except Exception as exc:
                self._record_runtime_error(execution_id, exc)
                try:
                    await self.reconcile_execution(execution_id)
                except asyncio.CancelledError:
                    raise
                except Exception as reconcile_exc:
                    self._record_runtime_error(execution_id, reconcile_exc)
                await asyncio.sleep(self._background_tick_interval_seconds)
                continue
            if record.status.is_terminal:
                return
            await asyncio.sleep(self._background_tick_interval_seconds)

    def _cancel_background_loop_if_terminal(self, record: ExecutionRecord) -> None:
        if not record.status.is_terminal:
            return
        task = self._execution_tasks.get(record.execution_id)
        if task is None or task is asyncio.current_task():
            return
        task.cancel()

    def _discard_background_task(self, execution_id: str, task: asyncio.Task[None]) -> None:
        if self._execution_tasks.get(execution_id) is task:
            self._execution_tasks.pop(execution_id, None)
        if task.cancelled():
            return
        try:
            task.exception()
        except asyncio.CancelledError:
            return

    def _start_stream_supervisors(self, environment: Environment, adapter: object) -> None:
        if environment is Environment.SIMULATION:
            return

        market_stream = getattr(adapter, "stream_market_data", None)
        if callable(market_stream):
            self._schedule_stream_supervisor(
                environment,
                "market",
                adapter,
                market_stream,
            )

        user_stream = getattr(adapter, "stream_user_events", None)
        if callable(user_stream):
            self._schedule_stream_supervisor(
                environment,
                "user",
                adapter,
                user_stream,
            )

        renew_listen_key = getattr(adapter, "renew_listen_key", None)
        if callable(renew_listen_key):
            current = self._listen_key_tasks.get(environment)
            if current is None or current.done():
                self._listen_key_tasks[environment] = asyncio.create_task(
                    self._run_listen_key_keepalive(environment, adapter)
                )

    def _schedule_stream_supervisor(
        self,
        environment: Environment,
        name: str,
        adapter: object,
        stream_factory: Callable[[], AsyncIterator[object]],
    ) -> None:
        key = (environment, name)
        current = self._stream_tasks.get(key)
        if current is not None and not current.done():
            return
        self._stream_tasks[key] = asyncio.create_task(
            self._supervise_adapter_stream(environment, name, adapter, stream_factory)
        )

    async def _supervise_adapter_stream(
        self,
        environment: Environment,
        name: str,
        adapter: object,
        stream_factory: Callable[[], AsyncIterator[object]],
    ) -> None:
        while self._started and self._adapters.get(environment) is adapter:
            stream_started_ms = self._clock_wall_ms(environment)
            try:
                async for event in stream_factory():
                    if not self._started:
                        break
                    if name == "user":
                        await self._reconcile_active_executions_for_user_event(
                            environment,
                            event,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._record_runtime_error(f"{environment.value}.{name}_stream", exc)

            if self._started and self._adapters.get(environment) is adapter:
                if name == "user":
                    await self._reconcile_active_executions_for_environment(
                        environment,
                        start_time_ms=stream_started_ms,
                        end_time_ms=self._clock_wall_ms(environment),
                    )
                await asyncio.sleep(self._stream_restart_delay_seconds)

    async def _run_listen_key_keepalive(
        self,
        environment: Environment,
        adapter: object,
    ) -> None:
        renew_listen_key = getattr(adapter, "renew_listen_key")
        while self._started and self._adapters.get(environment) is adapter:
            await asyncio.sleep(self._stream_keepalive_interval_seconds)
            listen_key = getattr(adapter, "latest_listen_key", None)
            if not listen_key:
                continue
            try:
                await renew_listen_key(listen_key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._record_runtime_error(f"{environment.value}.listen_key_keepalive", exc)

    async def _cancel_and_reconcile_active_executions(self) -> None:
        for service in list(self._services.values()):
            try:
                active_records = await service.active_executions()
            except Exception as exc:
                self._record_runtime_error("runtime.stop.active_executions", exc)
                continue

            for record in active_records:
                try:
                    cancelled = await service.cancel_execution(record.execution_id)
                    self._remember_execution(cancelled)
                except Exception as exc:
                    self._record_runtime_error(record.execution_id, exc)
                    continue

                try:
                    reconciled = await service.reconcile_execution(record.execution_id)
                    self._remember_execution(reconciled)
                except Exception as exc:
                    self._record_runtime_error(record.execution_id, exc)

    async def _reconcile_active_executions_for_user_event(
        self,
        environment: Environment,
        event: object,
    ) -> None:
        if await self._apply_user_event_reconciliation(environment, event):
            return

        event_time_ms = self._extract_event_time_ms(event)
        if event_time_ms is None:
            event_time_ms = self._clock_wall_ms(environment)
        await self._reconcile_active_executions_for_environment(
            environment,
            start_time_ms=max(0, event_time_ms - USER_EVENT_RECONCILIATION_LOOKBACK_MS),
            end_time_ms=event_time_ms,
        )

    async def _apply_user_event_reconciliation(
        self,
        environment: Environment,
        event: object,
    ) -> bool:
        adapter = self._adapters.get(environment)
        service = self._services.get(environment)
        if adapter is None or service is None:
            return False

        parser = getattr(adapter, "reconciliation_from_user_event", None)
        if not callable(parser):
            return False

        result = parser(event)
        if not isinstance(result, ReconciliationResult):
            return False

        candidate_records, active_lookup_failed = await self._direct_user_event_candidates(
            environment,
            service,
            result,
        )

        applied = False
        for record in candidate_records:
            prefix = ids.make_client_order_prefix(record.execution_id)
            if not self._reconciliation_result_matches_prefix(result, prefix):
                continue
            try:
                updated = await service.apply_reconciliation_result(record.execution_id, result)
                self._remember_execution(updated)
                self._cancel_background_loop_if_terminal(updated)
                applied = True
            except Exception as exc:
                self._record_runtime_error(record.execution_id, exc)
                applied = True
        return applied or active_lookup_failed

    async def _direct_user_event_candidates(
        self,
        environment: Environment,
        service: ExecutionService,
        result: ReconciliationResult,
    ) -> tuple[list[ExecutionRecord], bool]:
        records: list[ExecutionRecord] = []
        seen_execution_ids: set[str] = set()
        client_order_ids = [
            *(order.client_order_id for order in result.orders),
            *(fill.client_order_id for fill in result.fills),
        ]
        for execution_id, known_environment in list(self._execution_environments.items()):
            if known_environment is not environment:
                continue
            prefix = ids.make_client_order_prefix(execution_id)
            if not any(client_order_id.startswith(prefix) for client_order_id in client_order_ids):
                continue
            try:
                record = await service.get_execution(execution_id)
            except UnknownExecution:
                continue
            except Exception as exc:
                self._record_runtime_error(execution_id, exc)
                continue
            records.append(record)
            seen_execution_ids.add(record.execution_id)

        active_lookup_failed = False
        try:
            active_records = await service.active_executions()
        except Exception as exc:
            self._record_runtime_error(f"{environment.value}.active_executions", exc)
            active_lookup_failed = True
        else:
            for record in active_records:
                if record.execution_id in seen_execution_ids:
                    continue
                records.append(record)
                seen_execution_ids.add(record.execution_id)

        return records, active_lookup_failed

    @staticmethod
    def _reconciliation_result_matches_prefix(
        result: ReconciliationResult,
        prefix: str,
    ) -> bool:
        return any(order.client_order_id.startswith(prefix) for order in result.orders) or any(
            fill.client_order_id.startswith(prefix) for fill in result.fills
        )

    async def _reconcile_active_executions_for_environment(
        self,
        environment: Environment,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> None:
        service = self._services.get(environment)
        if service is None:
            return

        try:
            active_records = await service.active_executions()
        except Exception as exc:
            self._record_runtime_error(f"{environment.value}.active_executions", exc)
            return

        for record in active_records:
            try:
                reconciled = await service.reconcile_execution(
                    record.execution_id,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                )
                self._remember_execution(reconciled)
                self._cancel_background_loop_if_terminal(reconciled)
            except Exception as exc:
                self._record_runtime_error(record.execution_id, exc)

    def _clock_wall_ms(self, environment: Environment) -> int:
        clock = self._clocks.get(environment)
        if clock is None:
            return 0
        return int(clock.utc_now().timestamp() * 1000)

    @staticmethod
    def _extract_event_time_ms(event: object) -> int | None:
        if not isinstance(event, dict):
            return None
        for key in ("event_time_ms", "E"):
            value = event.get(key)
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
        raw = event.get("raw")
        if isinstance(raw, dict) and raw.get("E") is not None:
            try:
                return int(raw["E"])
            except (TypeError, ValueError):
                return None
        return None

    def _record_runtime_error(self, key: str, exc: BaseException) -> None:
        self._runtime_errors.setdefault(key, []).append(f"{type(exc).__name__}: {exc}")
