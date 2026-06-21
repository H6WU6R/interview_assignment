# Calais Execution Algorithm Report Draft

## Problem Statement

Build a compact execution service for Binance USD-M that can receive a target position, compute the required trade, place passive child orders, reconcile fills and ambiguous exchange outcomes, and preserve exposure safety throughout the lifecycle. The implementation favors correctness and inspectability over production breadth.

## Architecture

The system has four main layers:

- FastAPI `ExecutionService` facade for create, query, run-once, reconcile, and cancel operations.
- `ExecutionEngine` for lifecycle state, exposure accounting, child order management, reconciliation, and per-execution serialization.
- Narrow algorithm modules for Chase and TWAP decisions.
- `ExchangeAdapter` implementations for deterministic simulation and Binance USD-M REST/WebSocket hooks.

The engine is the correctness boundary. Algorithms do not mutate execution state directly. Exchange adapters return normalized domain objects so simulator and Binance paths exercise the same engine logic.

## Lifecycle

An execution starts in `CREATED`, validates the current position and requested target, then moves to `RUNNING` if a trade is required. Each `run_once` call reconciles known exchange state, checks stream health, evaluates active reserved exposure, and only then computes new child demand. Cancels move the execution to `CANCELLING`, attempt to cancel active children, and rely on reconciliation before terminal state.

State changes are guarded by `execution/state_machine.py`. Engine helpers perform transitions instead of assigning arbitrary statuses.

## Invariants

Before every submit, the engine enforces:

```text
confirmed_filled + live_open + pending_submit + pending_cancel + unknown_order + new_child_quantity <= normalized_target_trade_quantity
```

This includes ambiguous create-timeout children as `unknown_order_quantity`, so the engine will not submit a replacement child until reconciliation resolves whether the original exists. Reconciliation is execution-scoped by client-order prefix `ce_<short_exec>_`, never broad `ce_`.

All quantities and prices use `Decimal`; API decimal values are accepted as strings. Market timing and schedule calculations use monotonic time.

## Algorithms

Chase places passive orders at the current best bid for buys or best ask for sells. It reprices only after the configured minimum interval and bps threshold. The default repricing mode is adverse-only, with two-sided repricing available as a parameter.

TWAP uses elapsed time to compute scheduled cumulative quantity, subtracts confirmed fills, subtracts reserved exposure, floors to quantity step, and submits only the remaining safe deficit. `number_of_slices` is available in `ExecutionParameters` and Testnet CLI arguments, but the implemented scheduling model is schedule-deficit based rather than fixed equal-slice sleep.

## Simulator Evidence

Deterministic scripts cover the main demo scenarios:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

The cancel-race and create-timeout scripts write artifacts under `/tmp/calais-sim-cancel-race` and `/tmp/calais-sim-create-timeout` by default. Artifacts include request snapshots, JSONL logs, execution summaries, child orders, fills, and timelines.

The automated suite covers engine lifecycle, exposure invariants, state machine transitions, adapter contracts, simulator scenarios, API schema behavior, and observability serialization.

## Binance Testnet Plan and Evidence

The Binance adapter targets USD-M Testnet REST base `https://demo-fapi.binance.com` and WebSocket root `wss://fstream.binancefuture.com` with `/public/ws/...` market streams and `/private/ws/...` user streams. Signed requests include `timestamp`, `recvWindow`, and API-key headers; tests assert secrets are not leaked into recorded request structures.

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

If credentials are absent, Testnet contract tests are skipped and Testnet scripts exit before any network order send. The scripts never fall back to simulation. Their default output root is `/tmp/calais-binance-testnet`.

These scripts are validation hooks for the take-home, not production-grade WebSocket recovery or operations tooling. Mainnet mutation is hard-disabled by default and is not part of the demo.

## Real Failure Case

During development, ambiguous Binance create outcomes were initially too easy to mishandle. HTTP 408 on order creation can mean the exchange accepted the order but the client did not receive the response. Treating that as a terminal reject, or clearing UNKNOWN exposure from broad reconciliation alone, could allow a duplicate replacement child and violate the exposure invariant.

The issue was exposed and locked down by:

- `test_signed_request_http_408_maps_mutations_to_ambiguous_outcome`
- `test_exact_create_timeout_lookup_maps_found_order_to_live_open`
- `test_exact_create_timeout_lookup_not_found_clears_unknown_without_broad_warning`

The fix maps create/cancel HTTP 408 and transport timeouts to ambiguous mutation exceptions, reserves create-timeout children as UNKNOWN exposure, and resolves each UNKNOWN child with exact `GET /fapi/v1/order` lookup by `origClientOrderId`. Found orders become live open exposure; not-found orders clear UNKNOWN exposure without relying on broad `ce_` warnings.

## Limitations

- In-memory execution state only.
- No production persistence, restart recovery, or multi-process coordination.
- No autonomous background scheduler; callers drive progress through `run_once`.
- Testnet scripts are intentionally compact and do not implement robust reconnect, listen-key renewal loops, or alerting.
- Mainnet support is configuration-compatible only; mutation is disabled by default.

## Demo Checklist

- Run `uv run pytest -q`.
- Run the four simulator scripts and inspect printed execution IDs, statuses, exposure, and artifact directories.
- Demonstrate API create, run-once, query, reconcile, and cancel against the simulator app.
- If Testnet credentials are available, run one Chase or TWAP Testnet script with explicit small target and price bounds.
- Show artifact files for a simulator race or create-timeout scenario.
