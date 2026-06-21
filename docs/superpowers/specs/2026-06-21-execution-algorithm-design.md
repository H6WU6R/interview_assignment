# Calais Execution Algorithm Design

Date: 2026-06-21

## Purpose

This project implements a compact execution algorithm service for Binance USD-M Futures BTCUSDT Perpetual. The design goal is small but correct: a focused system that demonstrates execution correctness, state management, deterministic testing, and clear failure-mode reasoning rather than broad infrastructure.

The service receives a final target position, an acceptable price range, a target execution duration, and an algorithm choice. It adjusts the account position using either Chase or TWAP execution while respecting price bounds, precision rules, deadline policy, and overfill-prevention invariants.

The submission target is a production-quality but compact take-home project:

- FastAPI service for create, query, and cancel execution.
- Deterministic simulator as the primary automated correctness proof.
- Real Binance USD-M Futures adapter for Testnet integration and optional mainnet configuration.
- Unit, simulation, and credential-gated integration tests.
- README, AI usage disclosure, reproducible logs, execution summaries, and a Markdown report draft for later LaTeX/PDF conversion.

## Architecture

The project uses direct folders under `src/`, matching the assignment's suggested structure while keeping module boundaries clear:

```text
src/
  api/
  algorithms/
    chase.py
    twap.py
  exchanges/
    base.py
    binance_usdm.py
    simulator.py
  execution/
    engine.py
    service.py
    state_machine.py
    models.py
  risk/
  observability/
  config.py
```

The `ExecutionService` handles API-facing operations: create execution, query execution, and cancel execution. Its validation is request-format validation: enum values, required fields, decimal string parsing, `lower <= upper`, positive duration, and allowed environment. It creates the execution ID, stores request metadata, and delegates lifecycle ownership to the `ExecutionEngine`.

The `ExecutionEngine` owns trading correctness: parent execution state, child order state transitions, confirmed cumulative fills, open-order exposure including live open quantity, pending submit, pending cancel, and unknown order exposure, cancellation flow, timeout handling, reconciliation, and final summary. Trading-safety validation lives here and in `risk/`: price bounds, tick size, quantity step, minimum quantity, minimum notional, post-only safety, one-way mode, stale market data, and mainnet protection.

`Chase` decides desired child-order price, repricing timing, passive/aggressive behavior, and deadline policy behavior. `TWAP` owns absolute schedule timing, scheduled cumulative quantity, schedule deficit, and tail quantity handling, but still submits through the same safe engine path.

Both algorithms talk only to an `ExchangeAdapter` interface. The interface explicitly includes position query, symbol rules, market data, order submission, cancellation, order lookup by `clientOrderId`, user execution events, and reconciliation. The simulator and Binance adapter implement the same contract, so deterministic tests exercise the same execution logic used for Binance Testnet.

The exchange layer has two implementations: `DeterministicSimulator` for scripted repeatable tests, and `BinanceUsdmAdapter` for Binance USD-M Futures Testnet plus optional mainnet configuration. Mainnet support exists through config, but real mainnet order submission is hard-disabled by default and requires an explicit override.

Structured logging records execution IDs, child order IDs, client order IDs, UTC timestamps, monotonic elapsed time, state transitions, submitted quantity, filled quantity, remaining quantity, cancellation reason, timeout status, and reconciliation outcome, so the report can reconstruct the full order timeline.

## Algorithm And Lifecycle

Execution input is a final `target_position`, not an order quantity. At start, the engine queries current BTCUSDT position and computes:

```text
required_trade_quantity = target_position - current_position
```

The sign determines side. The absolute quantity is normalized down to Binance step size. If the normalized target trade quantity is below minimum quantity or minimum notional, the engine rejects the execution. If only the rounding remainder is too small to execute, the engine records it as dust in the final summary. It never silently rounds up into over-trading.

Parent execution states are monotonic:

```text
CREATED -> VALIDATING -> RUNNING -> CANCELLING -> COMPLETED
                                             |-> PARTIALLY_COMPLETED
                                             |-> EXPIRED
                                             |-> CANCELLED
                                             |-> FAILED
```

Child order states are monotonic and tolerate REST/WebSocket ordering differences:

```text
PENDING_SUBMIT
  -> OPEN
  -> REJECTED
  -> UNKNOWN

OPEN
  -> PARTIALLY_FILLED
  -> FILLED
  -> PENDING_CANCEL

PARTIALLY_FILLED
  -> FILLED
  -> PENDING_CANCEL

PENDING_CANCEL
  -> CANCELLED
  -> FILLED
```

The engine uses these quantity buckets for overfill prevention:

```text
confirmed_filled_quantity
live_open_quantity
pending_submit_quantity
pending_cancel_quantity
unknown_order_quantity
```

`unknown_order_quantity` is treated as real exposure until reconciled. Replacement order size is always computed from parent-level confirmed fills and reserved exposure, not from one child order's remaining quantity. A timed-out create request becomes `UNKNOWN`; the engine must reconcile by `clientOrderId` before retrying.

For Chase, `repricing_mode` is a config enum. Default behavior is `ADVERSE_ONLY`: buy orders reprice only when best bid moves up enough; sell orders reprice only when best ask moves down enough. This is a deliberate design choice allowed by the brief. `TWO_SIDED` is supported as an optional enum value so it can be changed quickly during interview discussion or future experiments. Repricing requires both `reprice_threshold_bps` and `minimum_reprice_interval_ms`. Passive orders use post-only checks; if near deadline and policy is `AGGRESSIVE_WITHIN_RANGE`, the engine may submit marketable limits bounded by `upper` for buys and `lower` for sells.

For TWAP, all quantities are represented as positive absolute trade quantities. Side is tracked separately, so formulas such as `scheduled_deficit` and `safe_child_quantity` do not become confusing for sell executions. The schedule uses absolute monotonic timestamps, not chained `sleep(interval)`:

```text
scheduled_cumulative_quantity(t)
  = total_trade_quantity * elapsed_time / total_duration

scheduled_deficit(t)
  = scheduled_cumulative_quantity(t) - confirmed_cumulative_filled_quantity(t)

reserved_exposure
  = live_open_quantity + pending_submit_quantity + pending_cancel_quantity + unknown_order_quantity

safe_child_quantity
  = scheduled_deficit - reserved_exposure
```

Each slice submits only positive, normalized `safe_child_quantity`. Unfilled earlier slices carry forward automatically. The final slice absorbs legal rounding remainder but cannot exceed the normalized total quantity.

Before submitting any new child order, the engine checks that confirmed fills plus all reserved or unknown exposure plus the new child quantity cannot exceed the normalized target trade quantity.

The initial implementation uses a single active child order per execution. If TWAP is later extended to allow multiple active child orders, each active order must reserve open exposure through the same invariant before any additional child order can be submitted.

Fill deduplication uses the exchange trade ID when available. If trade IDs are unavailable in a simulator or reconciliation result, the engine falls back to monotonic cumulative executed quantity per order. It never counts fills by raw message arrival count.

Deadline behavior is explicit:

- `CANCEL_REMAINDER`: cancel active exposure, reconcile, report filled and unfilled quantity.
- `AGGRESSIVE_WITHIN_RANGE`: attempt bounded marketable limit execution near deadline, then cancel/reconcile any remainder. If market price is outside the configured range, it returns partial or unfilled status instead of pretending completion.

## Exchange Adapters, Simulator, And Binance Integration

The exchange layer is defined by a narrow async `ExchangeAdapter` interface:

```text
get_symbol_rules(symbol)
get_position(symbol)
get_best_bid_ask(symbol)
stream_market_data()
submit_limit_order(order_request)
cancel_order(symbol, client_order_id)
get_order_by_client_order_id(symbol, client_order_id)
stream_user_events()
reconcile_orders_and_fills(symbol)
health_check_streams()
```

`get_best_bid_ask()` reads the latest cached market-data snapshot maintained by `stream_market_data()`. `SymbolRules` is loaded dynamically rather than hardcoded. It includes tick size, quantity step, minimum quantity, minimum notional, trading status, supported time-in-force modes, and post-only support assumptions. The engine uses these rules for all order normalization and safety checks.

Before the first fresh market-data snapshot arrives, `get_best_bid_ask()` returns no actionable quote. The engine must not submit new orders or reprice existing orders until a fresh, non-crossed snapshot exists.

Every child order uses a traceable `clientOrderId` derived from the execution ID and child order ID, so timed-out create requests can be reconciled without generating duplicate exposure. The format must stay inside Binance's documented `newClientOrderId` constraints: allowed characters and 36-character maximum length. The planned compact format is `ce_<short_exec>_<child_seq>`.

`reconcile_orders_and_fills(symbol)` is execution-scoped by default. It reconciles orders and fills whose `clientOrderId` matches the execution prefix, which keeps recovery auditable and avoids taking ownership of unrelated manual or external orders. A broader symbol-level diagnostic mode may be added separately, but it is not part of the default execution lifecycle.

`DeterministicSimulator` is the primary automated correctness proof. It accepts scripted timelines of market data, order events, fills, cancellations, rejections, timeouts, stream disconnects, and stale data. Tests use predefined scripts rather than random behavior unless a fixed seed is explicitly configured. The simulator must support at least:

```text
partial fill
fill after cancel request
cancel delay
post-only rejection
unsupported post-only time-in-force rejection
create timeout where order may still exist
duplicate execution event
delayed/out-of-order execution event
market price outside target range
stale market data
private stream disconnect
cross-zero starting position
```

The simulator exposes an internal monotonic clock controlled by tests. This lets tests advance time deterministically and verify TWAP schedule behavior without real sleeps or flaky timing.

`BinanceUsdmAdapter` targets Binance USD-M Futures. It supports Testnet by default and mainnet only through explicit configuration. Real mainnet orders are blocked unless a separate hard opt-in flag is set. API keys come only from environment variables or a local secret file excluded from git.

Binance responsibilities:

```text
load exchangeInfo for BTCUSDT rules
load exchangeInfo rateLimits for REQUEST_WEIGHT and ORDERS controls
query current one-way position
reject unsupported hedge mode by default
maintain server-time offset for signed requests
subscribe to bookTicker or equivalent best bid/ask stream
subscribe to user-data stream for order and fill events
renew listenKey before expiry
handle expected 24h WebSocket disconnect with reconnect and reconciliation
submit post-only and marketable limit orders with clientOrderId
map passive post-only LIMIT orders to timeInForce=GTX when supported by SymbolRules
reject passive post-only orders clearly if GTX support cannot be confirmed from SymbolRules
cancel orders by clientOrderId
handle cancel responses where the order is already filled as terminal reconciliation, not fatal engine failure
look up orders by clientOrderId after timeout
reconcile open orders, final order states, and recent fills after timeout or disconnect
detect stale market data and private stream health
record Binance transaction time T and event time E when available
```

Signed Binance requests use an adjusted timestamp based on the latest server-time offset and include a configured `recvWindow`, defaulting to 5000 ms or less. If server-time drift or timestamp rejection is detected, the adapter refreshes the offset before retrying eligible non-mutating reads.

REST requests use bounded timeouts. Non-order-mutating reads may use bounded retry and backoff. Order-mutating create requests are never blindly retried without idempotency reconciliation by `clientOrderId`.

The Binance adapter tracks venue rate-limit headers and configured `exchangeInfo` limits. HTTP 429 triggers backoff and suppresses new optional REST work. HTTP 418 is treated as a hard venue-ban condition: the adapter stops new exchange actions, marks stream/REST health as failed, and returns a clear failure reason.

Exchange/API errors are classified before they reach the engine:

```text
terminal reject:
  invalid parameters, unsupported mode, price/quantity filter failure, or clear exchange rejection

retryable read failure:
  transient GET/query failure, service unavailable, or retryable non-mutating timeout

unknown order-mutating failure:
  create/cancel/order mutation with unknown exchange outcome; exposure remains reserved until reconciliation

stream health failure:
  market-data or user stream disconnect/staleness requiring pause and reconciliation
```

When Binance event timestamps are available, the adapter records both transaction time `T` and event time `E`. The engine does not rely on cross-stream arrival order for correctness; `E` is used for ordering diagnostics and audit timelines.

Integration tests will be present but skipped when testnet keys are missing. This lets the repository be runnable immediately through simulator tests, while remaining ready for real Testnet validation once credentials are added.

## Risk, Precision, And Validation

All numeric trading values use `Decimal` from request parsing through order submission. Decimal strings are accepted at the API boundary; Python `float` is not used to construct prices, quantities, fills, notional, or slippage metrics.

Validation is split into two layers:

```text
API validation:
  request format, required fields, enums, decimal parsing, lower <= upper,
  positive duration, supported symbol, supported environment

Trading validation:
  target position to required trade quantity, symbol rules, side-aware rounding,
  price bounds, minimum quantity, minimum notional, post-only safety,
  stale market data, one-way mode, mainnet protection, exposure invariant
```

Trading validation is repeated before every child order submission because market data, exposure, and execution state can change during the job. Before submit, the engine rechecks that confirmed fills plus reserved exposure plus the new child quantity cannot exceed the normalized target trade quantity.

Quantity handling is conservative. Required trade quantity is rounded down toward zero to the exchange quantity step. If the normalized target trade quantity is below the minimum quantity or minimum notional, the execution is rejected. If only the rounding remainder is too small to execute, it is reported as dust. The engine never rounds quantity upward in a way that could exceed the target trade quantity.

All candidate prices are first converted to valid tick-size prices using side-aware rounding. Price handling is side-aware:

```text
Passive buy:
  rounded price must not cross best ask and must not exceed upper bound

Passive sell:
  rounded price must not cross best bid and must not fall below lower bound

Aggressive buy:
  marketable limit price must not exceed upper bound

Aggressive sell:
  marketable limit price must not fall below lower bound
```

Post-only orders are checked before submit using the latest bid/ask snapshot. If an order is rejected as post-only, the engine refreshes market data, applies reprice throttling, and either retries safely or waits. It does not loop indefinitely.

Market-data freshness is a hard safety gate. Each market-data snapshot records `last_market_event_time_exchange` when exchange event time is available and `last_market_event_time_local_monotonic` when the adapter receives the event locally. Stale decisions use the local monotonic clock so tests are deterministic and not affected by wall-clock changes; exchange timestamps are retained for audit and diagnostics. If best bid/ask is stale, missing, crossed, or outside the adapter's health tolerance, the engine pauses new submits and reprices. It may cancel existing exposure depending on deadline policy and final safety state, but it does not blindly chase with stale prices.

Position mode is explicit. The default implementation supports Binance One-way Mode only. If Hedge Mode is detected, the Binance adapter returns a clear unsupported-mode error instead of guessing `positionSide`.

Mainnet is protected in two stages: environment config must select mainnet, and a separate explicit `ALLOW_MAINNET_TRADING=true` guard must be present. Without both, any real mainnet order submission is rejected before reaching the adapter. Mainnet is configuration-compatible only; assignment demo and validation are performed on the simulator and Binance Testnet.

## API, CLI Scripts, Outputs, And Observability

The primary interface is FastAPI. It exposes the minimum required service operations:

```text
POST /executions
GET  /executions/{execution_id}
POST /executions/{execution_id}/cancel
```

`POST /executions` accepts the assignment request shape, including environment, symbol, algorithm, target position, price range, duration, deadline policy, and algorithm parameters. It returns immediately with an `execution_id`, initial status, normalized request summary, and warnings such as dust quantity or skipped optional fields.

`GET /executions/{execution_id}` returns current execution state, `status_reason` or `final_reason`, child order summaries, cumulative fills, open and unknown exposure, elapsed time, completion rate, and final metrics if terminal.

`POST /executions/{execution_id}/cancel` requests cancellation by moving a running execution into `CANCELLING`. The engine cancels active exposure, continues accepting fills that arrive during cancellation, reconciles orders and fills, and returns `CANCELLED` or `PARTIALLY_COMPLETED` depending on confirmed fills.

On graceful shutdown, the service rejects new execution requests, stops creating new child orders, requests cancellation for active execution-scoped orders, reconciles final fills when possible, and writes a terminal or interrupted summary. This is best-effort because the compact version does not include durable database persistence.

Small CLI/demo scripts will call the API or engine for repeatable demos:

```text
scripts/run_api.py
scripts/run_sim_chase.py
scripts/run_sim_twap.py
scripts/run_sim_cancel_race.py
scripts/run_sim_create_timeout.py
scripts/run_testnet_chase.py
scripts/run_testnet_twap.py
```

Simulator scripts are runnable without credentials. Testnet scripts require environment variables and fail with a clear missing-credentials message rather than silently falling back to simulation.

Outputs are written under `reports/` or `outputs/` with execution IDs in filenames:

```text
request_snapshot.json
execution_log.jsonl
execution_summary.json
child_orders.csv
fills.csv
timeline.csv
testnet_run_notes.md
AI_USAGE.md
```

The execution summary includes:

```text
target position
initial position
required quantity
normalized target trade quantity
submitted quantity
confirmed filled quantity
open quantity
cancelled quantity
unfilled quantity
completion rate
arrival bid/ask/mid
execution VWAP
slippage bps
price-bound violations
orders submitted
cancels
reprices
rejections
maker/taker fills
requested duration
actual duration
TWAP schedule deficit
overfill quantity
duplicate events ignored
unknown orders reconciled
final status and reason
```

Completion rate is computed on absolute required trade quantity. Slippage bps is side-aware: buy slippage compares execution VWAP above arrival mid; sell slippage compares arrival mid above execution VWAP.

Structured logs are JSONL and append-only per execution. They include UTC wall-clock timestamp and monotonic elapsed time, making them suitable for the PDF report and live debugging. The goal is that every reported summary metric can be traced to raw child order, fill, cancel, timeout, or reconciliation events.

## Testing Strategy And Acceptance Criteria

Testing is simulator-first and deterministic. The repository should pass automated tests without Binance credentials. Binance Testnet tests exist separately and are skipped unless credentials are configured.

Test layers:

```text
unit tests:
  models, Decimal parsing, rounding, state transitions,
  exposure invariant, price-bound validation, summary metrics

simulation tests:
  full engine + algorithm + deterministic simulator scenarios

integration tests:
  Binance adapter contract checks, exchangeInfo parsing,
  credential-gated Testnet Chase and TWAP smoke tests,
  raw execution log, order IDs, parameter snapshot, and result summary
```

Required simulator scenarios map directly to the assignment:

```text
T1 Normal Chase:
  stable market completes or reports partial completion with accurate unfilled quantity and final reason

T2 Chase Reprice:
  price crosses threshold, cancel-and-replace happens, minimum interval is enforced

T3 Partial Fill + Cancel Race:
  old order receives fills during cancellation; replacement uses safe remaining quantity

T4 Create Timeout:
  create request times out; engine reconciles by clientOrderId before retrying

T5 TWAP Carry-forward:
  earlier unfilled quantity becomes later schedule deficit

T6 Tail Quantity:
  step size, minimum quantity, minimum notional, and dust are handled explicitly

T7 Price Outside Range:
  algorithm does not violate bounds and returns unfilled or partial status

T8 Stream Disconnect:
  engine pauses new action, reconciles after reconnect, then resumes or exits safely

T9 Duplicate Event:
  duplicate fill event is ignored and cumulative fill is not double counted

T10 Cross-zero Position:
  negative starting position to positive target computes correct required quantity
```

Additional tests will cover:

```text
post-only rejection retry limit
unsupported GTX/post-only rejection path in simulator
stale market data safety gate
no submit or reprice before first fresh market-data snapshot
stale decisions use local monotonic receive time rather than wall-clock time
unknown order exposure counted as reserved exposure
execution-scoped reconciliation by clientOrderId prefix
duplicate fill handling by exchange trade ID or monotonic cumulative executed quantity
filled-during-cancel treated as valid terminal reconciliation
signed request timestamp/recvWindow construction
rate-limit 429 backoff and 418 hard-stop classification
exchange error classification for terminal, retryable, unknown, and stream-health failures
graceful shutdown cancels/reconciles active execution-scoped orders when possible
terminal state cannot return to RUNNING
AGGRESSIVE_WITHIN_RANGE bounded by price range
CANCEL_REMAINDER cancels and reports remaining quantity
side-aware completion rate and slippage metrics
```

Acceptance criteria:

```text
all simulator and unit tests pass locally
no Python float in order construction path
all prices/quantities normalized using symbol rules
no test allows confirmed fills + reserved exposure to exceed target quantity
duplicate events are counted once
create timeout never generates a second clientOrderId before reconciliation
clientOrderId format satisfies Binance allowed-character and 36-character limits
signed Binance requests use server-time offset and configured recvWindow
TWAP uses absolute monotonic schedule times
logs and summaries are generated for demo executions
all required scenarios produce reproducible logs and execution summaries linked to execution_id and clientOrderId
testnet scripts are ready and credential-gated
README explains assumptions, known limits, and how to run demos/tests
```

This gives a strong live interview story: run Chase, run TWAP, show cancel/fill race timeline, show create-timeout recovery, show price-outside-range partial result, and explain exactly how overfill is prevented.

## Final Deliverables And Scope Boundaries

The final submission should include:

```text
source code under src/
tests under tests/
pyproject.toml
configs/example.yaml
.env.example
README.md
AI_USAGE.md
reports or outputs with simulator run artifacts
optional Testnet artifacts after API keys are added
Dockerfile if time permits
```

README should be treated as part of the engineering deliverable, not an afterthought. It will explain:

```text
project goal and scope
architecture diagram
how target_position becomes required_trade_quantity
state machines
overfill prevention invariant
Chase design and repricing policy
TWAP schedule and carry-forward logic
deadline policies
precision and price-bound rules
simulator design
Binance Testnet setup
how to run API, demos, and tests
known limitations
```

`AI_USAGE.md` should clearly disclose that AI tools were used for planning, implementation assistance, and review, while correctness was validated through deterministic tests, manual examples, and code review. This aligns with both assignments' AI policy and avoids pretending the project was not AI-assisted.

The report will be a compact Markdown draft under `reports/`, intended for later LaTeX/PDF conversion. If time allows, it can be converted into a compact PDF report summarizing design decisions, invariants, simulator results, Testnet evidence, failure cases, and known limitations.

Explicitly out of scope for this compact production-quality version:

```text
multi-exchange support
multi-symbol support beyond BTCUSDT
Binance Hedge Mode support
database persistence
user authentication
web dashboard
portfolio-level risk system
real mainnet trading by default
advanced smart order routing
using third-party TWAP/Chase libraries
```

Known limitations will be stated honestly:

```text
Testnet liquidity may not reliably produce partial fills, so race scenarios are proven in simulator.
Only One-way Mode is supported initially.
Market data uses best bid/ask, not full order book queue modeling.
Simulator proves execution invariants but does not model every Binance matching-engine behavior.
Mainnet is configuration-supported but disabled by default.
```

This scope keeps the project aligned with "small and correct" while still showing professional execution thinking.

## Implementation Checklist

These are implementation hygiene requirements rather than separate design goals:

```text
Serialize Decimal values to strings before Binance REST calls.
Keep clientOrderId short enough after execution ID shortening.
Make the stale quote threshold configurable and covered by tests.
Route every state transition through state_machine.py rather than ad hoc assignments.
Ensure reconciliation cannot absorb unrelated manual or external orders.
Label Testnet scripts clearly so they cannot be confused with simulator demos.
```
