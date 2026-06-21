# Calais Execution Algorithm Report Draft

## Problem Statement

Build a compact execution service for Binance USD-M that can receive a target position, compute the required trade, place passive child orders, reconcile fills and ambiguous exchange outcomes, and preserve exposure safety throughout the lifecycle. The implementation is small but correct by design. It prioritizes deterministic order-lifecycle safety over feature breadth. The API runtime includes compact background execution, Binance Testnet adapter construction, stream supervision, listen-key keepalive, controlled retry, and graceful shutdown; production persistence, multi-process coordination, and operational alerting remain out of scope.

## Architecture

The system has four main layers:

- FastAPI `ExecutionRuntime` and `ExecutionService` facade for create, query, run-once, reconcile, cancel, background progression, and graceful shutdown.
- `ExecutionEngine` for lifecycle state, exposure accounting, child order management, reconciliation, and per-execution serialization.
- Narrow algorithm modules for Chase and TWAP decisions.
- `ExchangeAdapter` implementations for deterministic simulation and Binance USD-M REST/WebSocket hooks.

The runtime is the process boundary, while the engine is the trading-correctness boundary. Algorithms do not mutate execution state directly. Exchange adapters return normalized domain objects so simulator and Binance paths exercise the same engine logic.

## Lifecycle

An execution starts in `CREATED`, validates the current position and requested target, then moves to `RUNNING` if a trade is required. Runtime background ticks and manual `run_once` calls both use the same engine path: check stream health, evaluate active reserved exposure, and compute new child demand only after safety checks. UNKNOWN create outcomes block progress until reconciliation resolves the original child. Cancels move the execution to `CANCELLING`, attempt to cancel active children, and use reconciliation to keep fills and reserved exposure accurate.

State changes are guarded by `execution/state_machine.py`. Engine helpers perform transitions instead of assigning arbitrary statuses.

## Invariants

Before every submit, the engine enforces:

```text
confirmed_filled + live_open + pending_submit + pending_cancel + unknown_order + new_child_quantity <= normalized_target_trade_quantity
```

This includes ambiguous create-timeout children as `unknown_order_quantity`, so the engine will not submit a replacement child until reconciliation resolves whether the original exists. Reconciliation is execution-scoped by client-order prefix `ce_<short_exec>_`, never broad `ce_`.

All quantities and prices use `Decimal`; API decimal values are accepted as strings. Market timing and schedule calculations use monotonic time. Per-child cumulative fills are monotonic, so a duplicate, out-of-order, or lower cumulative snapshot cannot reduce or double-count parent filled quantity.

Price range is a safety gate, not a fake completion mechanism. Before deadline, out-of-range quotes produce no submit and the execution remains running. At deadline, the engine returns the actual partial or unfilled result. Under `CANCEL_REMAINDER`, a manual `run_once` deadline observation cancels active children, reconciles exchange state, and terminalizes only after reserved exposure clears. Under `AGGRESSIVE_WITHIN_RANGE`, the engine allows one bounded final marketable limit attempt; after that aggressive attempt is cancelled or reconciled and no exposure remains reserved, the execution terminalizes with the actual filled or unfilled result instead of repeatedly submitting new aggressive children.

## Algorithms

Chase places passive orders at the current best bid for buys or best ask for sells. It reprices only after the configured minimum interval and bps threshold. The default repricing mode is adverse-only, with two-sided repricing available as a parameter.

TWAP uses absolute slice boundaries from `number_of_slices`; it never sleeps and does not accumulate scheduler drift. At each observed boundary, it computes scheduled cumulative quantity from elapsed monotonic time, subtracts confirmed fills and reserved exposure, floors to quantity step, and submits only the safe deficit.

## Simulator Evidence

Deterministic scripts cover the main demo scenarios:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

The cancel-race and create-timeout scripts write artifacts under `/tmp/calais-sim-cancel-race` and `/tmp/calais-sim-create-timeout` by default. Artifacts include request snapshots, JSONL logs, execution summaries, child orders, fills, and timelines.

The automated suite covers engine lifecycle, exposure invariants, state machine transitions, adapter contracts, simulator scenarios, API runtime behavior, Binance mutation classification, and observability serialization. Current local result: `311 passed`.

## Binance Testnet Plan and Evidence

The Binance adapter targets USD-M Testnet REST base `https://demo-fapi.binance.com` and WebSocket root `wss://fstream.binancefuture.com` with `/public/ws/...` market streams and `/private/ws/...` user streams. Signed requests include `timestamp`, `recvWindow`, and API-key headers; tests assert secrets are not leaked into recorded request structures. Runtime supervisors keep market/user streams alive, renew listen keys, mark stream health degraded on disconnect, and retry/reconcile before safe continuation.

Order mutations use `POST /fapi/v1/order` and `DELETE /fapi/v1/order`; exact lookup uses `GET /fapi/v1/order` with `origClientOrderId`. Reconciliation combines `openOrders`, `allOrders`, `userTrades`, and exact client-order lookup for UNKNOWN children. `openOrders` alone is not used to conclude terminal status.

Testnet scripts require:

```bash
BINANCE_USDM_API_KEY
BINANCE_USDM_API_SECRET
--confirm-send-orders
--target-position
--target-price-lower
--target-price-upper
```

If credentials are absent, Testnet contract tests are skipped and Testnet scripts exit before any network order send. The scripts never fall back to simulation. Their default output root is `/tmp/calais-binance-testnet`. If the account cannot pass Binance margin/risk checks, the resulting raw artifact should be kept as connectivity/error evidence and the accepted-order run should be repeated once the Testnet account can accept a small BTCUSDT order.

These scripts are validation hooks for the take-home. Mainnet mutation is hard-disabled by default and is not part of the demo.

## Real Failure Case

During development, ambiguous Binance create outcomes were initially too easy to mishandle. HTTP 408 on order creation can mean the exchange accepted the order but the client did not receive the response. Treating that as a terminal reject, or clearing UNKNOWN exposure from broad reconciliation alone, could allow a duplicate replacement child and violate the exposure invariant.

The issue was exposed and locked down by:

- `test_signed_request_http_408_maps_mutations_to_ambiguous_outcome`
- `test_exact_create_timeout_lookup_maps_found_order_to_live_open`
- `test_exact_create_timeout_lookup_not_found_clears_unknown_without_broad_warning`

The fix maps create/cancel HTTP 408 and transport timeouts to ambiguous mutation exceptions, reserves create-timeout children as UNKNOWN exposure, and resolves each UNKNOWN child with exact `GET /fapi/v1/order` lookup by `origClientOrderId`. Found orders become live open exposure; not-found orders clear UNKNOWN exposure without relying on broad `ce_` warnings.

Additional review-driven regressions now covered include:

- `test_partial_fill_from_create_response_updates_parent_fills_immediately`
- `test_lower_rest_cumulative_snapshot_cannot_reduce_child_or_parent_fill`
- `test_pending_submit_unknown_and_new_child_invariant_is_checked_after_every_stage`
- `test_runtime_stop_cancels_and_reconciles_active_execution`
- `test_background_loop_records_unknown_reconcile_failure_and_retries`
- `test_user_stream_event_reconciles_active_execution_with_event_time_bounds`
- `test_runtime_restarts_binance_user_stream_after_disconnect`
- `test_deadline_aggressive_child_uses_ioc_time_in_force`
- `test_simulator_fill_updates_account_position_for_buy_and_sell`
- `test_second_active_execution_for_same_environment_and_symbol_returns_409`

## Limitations

- In-memory execution state only.
- No production persistence, restart recovery, durable replay, alerting, or multi-process coordination.
- Runtime stream supervision is compact and Testnet-focused rather than a full production operations framework.
- Accepted Testnet order evidence requires a Testnet account that can pass Binance margin/risk checks.
- Mainnet support is configuration-compatible only; mutation is disabled by default.

## Demo Checklist

- Run `uv run pytest -q`.
- Run the four simulator scripts and inspect printed execution IDs, statuses, exposure, and artifact directories.
- Demonstrate API create, query, background progression, run-once, reconcile, and cancel against the simulator app.
- If Testnet credentials are available, run one Chase or TWAP Testnet script with explicit small target and price bounds.
- Show artifact files for a simulator race or create-timeout scenario.
