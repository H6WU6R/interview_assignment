# Current Pipeline Code Review Summary

Date: 2026-06-22

Purpose: provide an external reviewer with a concise map of the current implementation, how it answers the Calais execution algorithm assignment, what has been verified, and what risks remain.

Local verification:

```bash
uv run pytest -q
```

Result:

```text
311 passed
```

## Assignment Interpretation

The project targets the Calais execution algorithm brief for Binance USD-M Futures BTCUSDT. The implementation is intentionally "small but correct": it prioritizes order lifecycle safety, deterministic simulator coverage, Decimal precision, and auditability over feature breadth.

The core assignment requirements mapped into code are:

- Input is `target_position`, not target order size.
- Required trade is derived from current position as `target_position - current_position`.
- Chase and TWAP share one execution engine and one exchange adapter contract.
- The engine owns parent state, child state, fills, cancellations, timeouts, reconciliation, exposure accounting, and final summaries.
- Algorithms remain narrow and do not call Binance or simulator methods directly.
- Deterministic simulator tests prove race and boundary behavior that Testnet cannot reliably reproduce.
- Binance adapter implements the compact Testnet-facing integration path, with mainnet disabled by default.
- Quantities and prices use `Decimal`, not Python float.
- Structured artifacts can reconstruct execution ID, child order ID, client order ID, fills, timeline, and final summary.

## High-Level Pipeline

```text
FastAPI app / scripts
  -> ExecutionRuntime
  -> ExecutionService
  -> ExecutionEngine
  -> Algorithm decision helper
  -> Risk and precision validation
  -> ExchangeAdapter
       -> DeterministicSimulator
       -> BinanceUsdmAdapter
  -> Reconciliation and summary
  -> Logs and artifacts
```

## API Runtime

Files:

- `src/api/app.py`
- `src/api/runtime.py`
- `src/api/schemas.py`
- `src/execution/service.py`

Responsibilities:

- `create_app()` builds a FastAPI app using the deterministic simulator by default.
- Lifespan startup starts `ExecutionRuntime`; shutdown stops new work, cancels/reconciles active executions, and tears down runtime tasks.
- API supports create, query, cancel, run-once, and reconcile operations.
- `environment=simulation` uses the deterministic simulator and manual/test controls.
- `environment=testnet` constructs `BinanceUsdmAdapter` with `SystemClock` and environment credentials.
- A second active execution for the same environment and symbol returns a conflict instead of sharing exposure blindly.
- Nonterminal executions are advanced by a background loop; TWAP can progress over real time without external `/run-once`.
- Runtime supervisors start Binance market/user streams, renew listen keys, record stream failures, reconcile active executions on user events or user-stream disconnect, and restart streams after disconnect.
- Runtime errors are recorded and retried where safe. Unknown child exposure triggers automatic reconciliation during normal ticks.

API schema behavior:

- Decimal request fields must be JSON strings.
- Unsupported symbols, nonpositive price bounds, invalid durations, invalid slices/timeouts, negative reprice settings, nonfinite decimals, and JSON floats are rejected.
- Decimal response fields are serialized as strings.

## Execution Engine

File:

- `src/execution/engine.py`

The engine is the central correctness boundary. It owns:

- Parent execution lifecycle.
- Child order lifecycle.
- Derived side and normalized required quantity.
- Confirmed cumulative fills.
- Live open exposure.
- Pending submit exposure.
- Pending cancel exposure.
- Unknown order exposure.
- Cancel flow.
- Timeout handling.
- Reconciliation.
- Final summary metrics.

The main submit invariant is enforced before child submission and after lifecycle changes:

```text
confirmed_filled
+ live_open
+ pending_submit
+ pending_cancel
+ unknown_order
+ new_child_quantity
<= normalized_target_trade_quantity
```

There is one exchange submit call site, `_submit_child_locked()`. Initial submit, Chase replacement, TWAP slice demand, retry after reconciliation, timeout recovery, and aggressive deadline attempts all pass through the same exposure gate.

Important implemented behaviors:

- `PARTIALLY_FILLED` returned by create immediately updates parent confirmed fills.
- Lower REST/user-stream cumulative snapshots cannot reduce a child or parent fill.
- Duplicate/stale fill events do not double-count cumulative filled quantity.
- Unknown create outcomes reserve `unknown_order_quantity` and block new client order IDs until exact reconciliation resolves the child.
- Cancel ambiguity is treated as pending-cancel exposure until reconciliation determines open, filled, or cancelled state.
- Stale market data pauses or terminalizes with a controlled reason instead of escaping as an uncaught exception.
- State changes go through `execution/state_machine.py` via engine helpers.

## Algorithms

Files:

- `src/algorithms/chase.py`
- `src/algorithms/twap.py`

Chase:

- Passive buy price is best bid; passive sell price is best ask.
- Repricing requires the configured bps threshold and minimum interval.
- Default repricing mode is `ADVERSE_ONLY`; `TWO_SIDED` is available as a parameter.
- Deadline aggressive demand uses an IOC-capable child order inside the configured price range.

TWAP:

- Uses absolute schedule boundaries rather than sleep-based equal slices.
- Computes scheduled cumulative quantity from elapsed monotonic time:

```text
scheduled_cumulative_quantity(t)
  = total_trade_quantity * elapsed_time / total_duration
```

- Computes safe child quantity separately by subtracting confirmed fills and reserved exposure.
- Carries previous unfilled deficit forward.
- Uses positive absolute trade quantities; side is tracked separately.
- Floors to quantity step and never rounds up beyond target.

## Exchange Layer

Files:

- `src/exchanges/base.py`
- `src/exchanges/simulator.py`
- `src/exchanges/binance_usdm.py`

Adapter contract includes:

- Symbol rules.
- Position query.
- Best bid/ask.
- Market data stream.
- Submit and cancel.
- Lookup by client order ID.
- User event stream.
- Execution-scoped orders/fills reconciliation.
- Stream health.

Simulator:

- Supports fresh/stale/crossed market data, order submit/cancel, fills, fill during cancel, post-only rejection, create timeout found/not-found, delayed/duplicate/out-of-order event scenarios, stream health scripting, and execution-scoped reconciliation.
- Rejects broad prefixes and requires `ce_<short_exec>_`.
- Fills update simulated account position, so later target-position calculations use realistic account state.

Binance adapter:

- Uses signed REST requests with `timestamp`, `recvWindow`, HMAC signature, and Decimal string serialization.
- Sanitizes logs/artifacts so secrets, signatures, listen keys, and raw authenticated payloads are not written.
- Maps passive post-only orders to `GTX`; aggressive deadline orders use `IOC`.
- Validates client order ID pattern and 36-character length.
- Parses symbol rules, trading status, rate limits, and position mode.
- Uses `/fapi/v1/order` by `origClientOrderId` for exact status repair, rather than relying only on open-order lookup.
- Reconciles from open orders, all orders, user trades, and exact UNKNOWN-child lookup.
- Treats connection resets, protocol/read errors, timeouts, invalid JSON, HTTP 408, and message-aware 503 create/cancel cases conservatively.

## Required Scenario Coverage

The deterministic suite covers the PDF T1-T10 scenarios with behavioral assertions:

| Scenario | Coverage |
| --- | --- |
| T1 Normal Chase | passive price and exposure invariant |
| T2 Chase Reprice | threshold and minimum interval before cancel/replace |
| T3 Partial Fill + Cancel Race | fill during cancel reduces replacement quantity |
| T4 Create Timeout | UNKNOWN blocks new client order until exact reconciliation |
| T5 TWAP Carry-forward | later slice includes previous unfilled deficit |
| T6 Tail Quantity | dust recorded; no rounding up |
| T7 Price Outside Range | no invalid child order; final result is partial/unfilled |
| T8 Stream Disconnect | pause, reconcile, safely resume |
| T9 Duplicate Event | cumulative fill not counted twice |
| T10 Cross-zero Position | target minus current position with absolute trade quantity |

Additional review-driven regressions cover:

- Lower REST cumulative snapshot cannot reduce a child fill.
- `PARTIALLY_FILLED` create response updates parent fills.
- `confirmed + reserved` never exceeds target after reconciliation stages.
- `ConnectError` after mutation creates `UNKNOWN` or `PENDING_CANCEL`.
- UNKNOWN child is automatically reconciled by runtime.
- Stale market data pauses instead of raising.
- Testnet API request constructs `BinanceUsdmAdapter`.
- API TWAP progresses in the background.
- Deadline aggressive order has IOC.
- Simulator fills update account position.
- Second active BTCUSDT execution is rejected.
- User-stream disconnect restarts and records health degradation.
- User-stream events and disconnects trigger execution-scoped reconciliation with bounded time windows.

## Observability And Artifacts

Files:

- `src/observability/logging.py`
- `src/observability/artifacts.py`
- `src/observability/summary.py`

Artifacts:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`

Audit features:

- Decimal and enum values serialize safely.
- UTC wall-clock timestamps and monotonic timing are preserved.
- CSV writer tolerates heterogeneous timeline rows.
- Sanitizer removes API keys, secret keys, signatures, listen keys, signed payload aliases, and raw authenticated request aliases.

## Testnet Status

Testnet scripts:

```bash
uv run python scripts/run_testnet_chase.py --confirm-send-orders ...
uv run python scripts/run_testnet_twap.py --confirm-send-orders ...
```

They require:

- `BINANCE_USDM_API_KEY`
- `BINANCE_USDM_API_SECRET`
- `--confirm-send-orders`
- explicit target position
- explicit price bounds

They never fall back to simulation when credentials are missing. Output goes under `/tmp/calais-binance-testnet` by default.

If Binance rejects before accepting an order because the account lacks margin or fails risk checks, keep the raw artifact as connectivity/error evidence and rerun once the Testnet account can accept a small BTCUSDT order. Deterministic simulator artifacts remain the proof path for race conditions.

## Remaining Limitations

- Execution state is in-memory; process restart loses active records.
- Runtime stream supervision is compact and Testnet-focused, not a production operations framework with durable replay, alerting, or multi-process coordination.
- Mainnet mutation is configuration-compatible but hard-disabled by default and should not be used for demos.
- BTCUSDT and One-way Mode are the intended submission scope.
- Accepted Testnet order evidence requires a funded/configured Testnet account.

## Assessment

The current codebase directly addresses the external review blockers that were decisive for final submission: fill-ledger monotonicity, the exposure invariant, conservative mutation uncertainty, API runtime execution, stale market handling, IOC deadline orders, simulator account position, active-execution conflicts, stream supervision, and automatic reconciliation. The strongest evidence is the deterministic T1-T10 simulator suite plus focused unit tests for Binance mutation classification and API runtime behavior.
