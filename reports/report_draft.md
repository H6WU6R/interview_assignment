# Calais Execution Algorithm Report Draft

This file is the source material for the final 8-15 page PDF report. It is intentionally more complete than the final PDF should be. The PDF can compress the matrices, keep the strongest evidence, and move detailed command output or artifact listings to an appendix.

## Executive Summary

The project implements a compact execution algorithm service for Binance USD-M Futures BTCUSDT. The service accepts a final target position, a valid execution price range, a target duration, and an execution algorithm (`CHASE` or `TWAP`). It computes the required net trade from the current account position, places bounded child orders, reconciles partial fills and ambiguous exchange outcomes, and preserves exposure safety across creates, cancels, reprices, stream events, and deadline handling.

The design is intentionally "small and correct": one symbol, one exchange family, one-way mode, deterministic simulator coverage for races, and a Binance Testnet path for live connectivity and order lifecycle evidence. The highest priority is not feature breadth. The highest priority is proving that the system does not silently overfill, fake completion, double-count fills, or retry ambiguous create requests with a fresh client order ID before reconciliation.

Current report status:

- Source code, simulator, tests, scripts, README, AI usage disclosure, and markdown report source exist.
- Deterministic simulator evidence exists through scripts and test coverage.
- The final PDF has not been generated in this documentation pass.
- Accepted Binance Testnet Chase and TWAP evidence must be attached before final submission if the account can pass Binance margin/risk checks.
- If Testnet order acceptance is blocked by funding, permissions, or risk configuration, keep the raw rejected/error artifact as connectivity evidence and label accepted-order evidence as pending. Do not replace required Testnet evidence with simulator output.

## Project Brief Requirements

The brief asks for a Python 3.11+ execution service for Binance USD-M Futures BTCUSDT Perpetual. The input is a final target account position, not a target order quantity. The service must support Chase and TWAP, enforce price bounds, use precise decimal arithmetic, use monotonic time for execution duration, provide at least CLI or HTTP API controls, implement a deterministic simulator, run Binance Futures Testnet evidence, output structured logs/results, and explain safety invariants, failure cases, limitations, and AI usage.

The direct pass/fail risks from the brief are:

- Partial fill after cancel or reprice must not cause resubmission of the original total quantity.
- A create timeout must remain `UNKNOWN` until exact reconciliation by client order ID.
- `float` must not construct prices or quantities.
- TWAP must not be a naive `slice_qty / sleep` loop.
- Symbol rules such as tick size, quantity step, minimum quantity, and minimum notional must be read and enforced.
- If price never enters range, the system must report the real unfilled result, not pretend completion.
- Tests must be traceable to execution IDs and client order IDs.

## Requirement Matrix

| Brief area | Requirement | Repository coverage | Remaining evidence or risk |
| --- | --- | --- | --- |
| Target position | Compute `target_position - current_position`; support buy, sell, no action, and cross-zero. | `ExecutionEngine.create_execution`, `required_trade`, API schemas, cross-zero tests. | Include one cross-zero result in PDF or appendix. |
| Price range | Buy cannot aggressively execute above upper bound; sell cannot aggressively execute below lower bound; passive orders still obey bounds and post-only constraints. | Validation/risk layer, engine price checks, simulator outside-range tests. | Show outside-range terminal result in demo. |
| Duration | `target_duration` starts when job enters `RUNNING`; use monotonic time for schedule/deadline; log UTC wall-clock. | Engine clock abstraction and artifact timestamp fields. | PDF should show both monotonic and UTC artifact fields. |
| API/CLI | Provide create, query, cancel; HTTP recommended. | FastAPI app with create/query/cancel/run-once/reconcile. | Include curl demo or screenshots only if useful. |
| Chase | Passive best bid/ask, post-only, threshold repricing, minimum interval, no predictable overlap overfill. | `src/algorithms/chase.py`, engine cancel-and-replace path, tests. | Explain chosen default: `ADVERSE_ONLY`, with optional `TWO_SIDED`. |
| Partial fill safety | Replacement quantity must be based on parent confirmed cumulative fill, not current-order remaining alone. | Exposure invariant and per-child cumulative fill application. | Use cancel-race simulator timeline in PDF. |
| Deadline policy | `CANCEL_REMAINDER` cancels active orders; `AGGRESSIVE_WITHIN_RANGE` may use bounded marketable limit only within range. | Engine deadline handling and tests. | Include one policy example in final report. |
| TWAP | Absolute schedule, carry-forward deficits, rounding remainder, no drift, no naive sleep loop. | `src/algorithms/twap.py`, engine TWAP ledger, tests. | Include slice ledger with planned/submitted/open/filled/unfilled. |
| State machines | Execution and child state transitions must be monotonic and auditable. | `src/execution/state_machine.py`, domain models, tests. | Add diagram to PDF if space allows. |
| Binance symbol rules | Read tick size, quantity step, minimum quantity, minimum notional, status. | Binance adapter and simulator symbol rules. | Testnet evidence should include exchangeInfo/rules snapshot. |
| Market/user streams | Market data via book ticker; orders/fills preferably via private stream; REST for initialization, order mutation, reconciliation. | Runtime stream supervisors, Binance adapter hooks, user-stream event application, REST reconciliation fallback. | Final evidence should show user stream order/trade events or a labeled REST fallback. |
| Staleness/disconnect | Pause new actions if streams are stale/disconnected; reconnect and reconcile. | Runtime health checks and simulator stream-disconnect tests. | Include T8 result summary. |
| Decimal precision | No direct Python float for order price/quantity. | Decimal models, Pydantic decimal strings, decimal math helpers. | Mention in architecture and AI defense. |
| Environments | Support simulation/testnet/mainnet config; mainnet default hard disabled. | Simulation/Testnet runtime paths; mainnet requires credentials plus `ALLOW_MAINNET_TRADING=true`, and no mainnet demo is required. | State clearly that mainnet is gated and should not be used for the take-home demo. |
| Idempotency | Execution, child, and client order IDs must be traceable. | ID helpers, client order prefixing, artifacts, tests. | Include sample `execution_id`, `child_order_id`, `client_order_id`. |
| Logging | Structured logs sufficient to reconstruct order timeline. | Artifact writers and demo scripts. | Attach raw JSONL/CSV artifact bundle. |
| Tests | Deterministic simulator plus Testnet E2E Chase/TWAP. | Unit/simulation tests and Testnet scripts. | Accepted Testnet artifacts remain the biggest final submission gap. |
| Results | Quantity, price, order, time, and safety metrics. | Summary metrics and artifact tables, including reprices and known maker/taker fill counts. | Populate the final PDF tables from simulator and Testnet artifacts. |
| Deliverables | Repo, README, tests, raw results, PDF report, AI disclosure. | Repo docs exist; PDF not generated in this pass. | Generate PDF and attach final evidence bundle before submission. |

## Scope and Assumptions

- Exchange: Binance USD-M Futures.
- Symbol: BTCUSDT perpetual.
- Account mode: one-way mode. Hedge mode should be rejected or clearly unsupported.
- Primary algorithms: `CHASE` and `TWAP`.
- Runtime language: Python 3.11+ with asyncio-friendly service boundaries.
- Mainnet: configuration-compatible only and hard disabled by default for mutation. It should not be used in the demo.
- Persistence: in-memory. Restart recovery, durable replay, multi-process leadership, and production alerting are out of scope.
- Simulator: deterministic and intentionally stronger than Testnet for forcing races and abnormal event orderings.

## Architecture

The system has four correctness layers:

1. API/runtime layer: FastAPI app, request validation, environment adapter construction, background progression, stream supervision, listen-key renewal, controlled retry, and graceful shutdown.
2. Service/engine layer: `ExecutionService` delegates to `ExecutionEngine`, which owns execution state, child order state, exposure accounting, reconciliation, deadline handling, and terminal summaries.
3. Algorithm layer: Chase and TWAP helpers compute desired prices or scheduled quantities. They do not mutate execution state directly.
4. Exchange layer: `ExchangeAdapter` contract normalizes simulator and Binance behavior into domain objects such as `MarketSnapshot`, `PositionSnapshot`, `ChildOrder`, `Fill`, and `ReconciliationResult`.

Key source locations:

| Area | Files |
| --- | --- |
| API and runtime | `src/api/app.py`, `src/api/runtime.py`, `src/api/schemas.py` |
| Engine and models | `src/execution/engine.py`, `src/execution/models.py`, `src/execution/state_machine.py`, `src/execution/service.py` |
| Algorithms | `src/algorithms/chase.py`, `src/algorithms/twap.py` |
| Exchange adapters | `src/exchanges/base.py`, `src/exchanges/simulator.py`, `src/exchanges/binance_usdm.py` |
| Risk and Decimal helpers | `src/risk/validation.py`, `src/risk/decimal_math.py` |
| Artifacts and summaries | `src/observability/artifacts.py`, `src/observability/summary.py` |
| Simulator demos | `scripts/run_sim_chase.py`, `scripts/run_sim_twap.py`, `scripts/run_sim_cancel_race.py`, `scripts/run_sim_create_timeout.py` |
| Testnet demos | `scripts/run_testnet_chase.py`, `scripts/run_testnet_twap.py`, `scripts/testnet_runner.py` |

Design choice: the engine is the trading-correctness boundary. Runtime code may supervise streams and retries, but it does not own exposure math. Algorithms compute decisions, but the engine decides whether a child order is safe to submit.

## Execution Lifecycle

An execution starts from an API request or script request.

1. `CREATED`: request is accepted and domain objects are built from decimal-string inputs.
2. `VALIDATING`: account position, symbol rules, requested price range, duration, and algorithm parameters are validated.
3. `RUNNING`: the engine computes required side and quantity. If no trade is required, the execution immediately completes with `NO_ACTION`.
4. Child lifecycle: the engine may create a child order, reserve exposure, submit it, observe fills, cancel it, reprice it, or reconcile it.
5. `CANCELLING`: manual cancel or deadline cancel requests move active children to cancel flow.
6. Terminal states: `COMPLETED`, `PARTIALLY_COMPLETED`, `EXPIRED`, `CANCELLED`, or `FAILED`.

The engine serializes operations per execution so `create`, `run_once`, `cancel`, and `reconcile` do not race inside one execution. Terminal execution states cannot return to `RUNNING`.

## Child Order Lifecycle

The required child states from the brief are represented as:

```text
PENDING_SUBMIT -> OPEN -> PARTIALLY_FILLED -> PENDING_CANCEL -> CANCELLED | FILLED | REJECTED | UNKNOWN
```

Important behavior:

- A child is `UNKNOWN` when create outcome is ambiguous, such as HTTP 408 or transport timeout.
- `UNKNOWN` quantity remains reserved until exact reconciliation by `origClientOrderId` proves whether the exchange accepted the order.
- A child in `PENDING_CANCEL` still reserves exposure because fills can occur after the cancel request is sent but before final cancel status is known.
- REST responses and user stream events may arrive out of order. Cumulative fill application must be monotonic.

## Core Safety Invariant

The main invariant is enforced before every child submit:

```text
confirmed_filled
+ live_open
+ pending_submit
+ pending_cancel
+ unknown_order
+ new_child_quantity
<= normalized_target_trade_quantity
```

Why this matters:

- `confirmed_filled` is the parent cumulative filled quantity.
- `live_open` is still executable on exchange.
- `pending_submit` protects the interval between local intent and exchange response.
- `pending_cancel` protects the cancel/fill race window.
- `unknown_order` protects ambiguous create outcomes.
- `new_child_quantity` is the proposed new order.

This invariant is stronger than "recalculate remaining quantity" because it accounts for all reserved exposure buckets. It prevents a replacement order from overlapping with old live, cancelling, pending, or unknown quantity.

Other invariants to state in the PDF:

1. Confirmed parent filled quantity is monotonic.
2. Per-child cumulative filled quantity is monotonic.
3. Duplicate execution events cannot increase cumulative fills twice.
4. A timed-out create request remains `UNKNOWN` until reconciled.
5. No aggressive order may violate the configured price bound.
6. Price-out-of-range execution may expire partially filled or unfilled, but may not fake completion.
7. Reconciliation is scoped to the exact execution client-order prefix, not a broad prefix that could mix executions.
8. Terminal execution state cannot return to `RUNNING`.

## Target Position Handling

The input is final account position, not order quantity:

```text
required_trade_quantity = target_position - current_position
required_trade_quantity > 0 -> BUY
required_trade_quantity < 0 -> SELL
required_trade_quantity = 0 -> NO_ACTION
```

Example from the brief: if the account is `-0.003 BTC` and the target is `+0.005 BTC`, the required trade is buy `0.008 BTC`, not buy `0.005 BTC`. This is covered by cross-zero tests and should be shown in the final report because it is a common implementation error.

Dust and minimum quantity handling must never silently round upward into an overfill. If normalized quantity is below exchange minimums, the system should complete with dust, reject clearly, or record an explicit final reason according to the implemented rule.

## Price Bounds and Rounding

The configured range `[target_price_lower, target_price_upper]` is the user's allowed active execution boundary.

- Buy: no aggressive execution above `upper_bound`.
- Sell: no aggressive execution below `lower_bound`.
- Passive buy: should not round upward through best ask or upper bound.
- Passive sell: should not round downward through best bid or lower bound.
- Aggressive buy at deadline: bounded marketable limit at or below upper bound.
- Aggressive sell at deadline: bounded marketable limit at or above lower bound.

All prices and quantities use `Decimal`. API inputs are decimal strings. Binance order parameters should be serialized as decimal strings, not floats.

If market price never becomes executable within the configured range, the execution can end unfilled or partially filled. The correct result is an explicit unfilled quantity and final reason such as price outside range or expired, not a claim that the target position was guaranteed.

## Chase Algorithm

Chase places a passive limit order at the current best queue price:

- Buy desired price: current best bid.
- Sell desired price: current best ask.
- Default time-in-force: post-only where supported.
- Reprice trigger: desired price moves by at least `reprice_threshold_bps`.
- Cancel storm protection: `minimum_reprice_interval_ms`.

The brief allows either `TWO_SIDED` or `ADVERSE_ONLY` repricing if explained. This implementation defaults to `ADVERSE_ONLY`, meaning it reprices when the market moves away from the order in the direction that makes the current order less competitive:

- Buy reprices upward when best bid moves up enough.
- Sell reprices downward when best ask moves down enough.

`TWO_SIDED` can be configured for cases where favorable movement should also move the passive order. For the report, explain `ADVERSE_ONLY` as the safer default because it reduces churn and avoids unnecessary cancel activity when the existing order is already more passive or favorable.

Partial fill safety:

- Canceling an old order does not mean the old order stopped filling immediately.
- The replacement quantity is based on parent confirmed cumulative fills and reserved exposure.
- The system only submits replacement quantity after accounting for live, pending cancel, and unknown exposure.

## TWAP Algorithm

TWAP uses an absolute schedule, not a sleep loop.

For elapsed monotonic time `t`:

```text
scheduled_cumulative_quantity(t)
= total_trade_quantity * elapsed_time / total_duration

quantity_deficit(t)
= scheduled_cumulative_quantity(t) - confirmed_cumulative_filled_quantity(t)
```

The engine then subtracts reserved exposure before submitting more. This avoids submitting the same deficit twice when an earlier slice is still open, pending cancel, or unknown.

The final slice absorbs legal rounding remainder without exceeding total target quantity. The PDF should show a TWAP slice ledger with at least:

- planned quantity
- submitted quantity
- open/reserved quantity
- filled quantity
- cancelled quantity
- unfilled quantity
- schedule deficit

This directly addresses the brief's warning that a loop of `total_qty / n` plus `sleep(interval)` is not a valid TWAP.

## Deadline Policies

`CANCEL_REMAINDER`:

- At deadline, cancel active children.
- Reconcile exchange state.
- Report actual filled and unfilled quantities.
- Never cross price boundary to force completion.

`AGGRESSIVE_WITHIN_RANGE`:

- Near or at deadline, allow a bounded marketable limit attempt.
- Buy price must not exceed upper bound.
- Sell price must not go below lower bound.
- If market is outside range, keep the remainder unfilled and report the reason.
- Do not repeatedly submit aggressive children after the final bounded attempt has been cancelled or reconciled.

## Binance Integration

Expected Binance USD-M path:

- Dynamic symbol rules via exchangeInfo or equivalent official endpoint.
- Market data via best bid/ask or book ticker stream.
- Private order and trade events via user-data stream.
- REST for initialization, create/cancel requests, exact order lookup, and reconciliation after disconnect.
- Signed REST requests include timestamp, recvWindow, and API-key header.
- Secrets are injected through environment variables or local secret files, not committed.

Order mutation endpoints and reconciliation sources:

- Create order: `POST /fapi/v1/order`.
- Cancel order: `DELETE /fapi/v1/order`.
- Exact order lookup: `GET /fapi/v1/order` with `origClientOrderId`.
- Broad reconciliation: `openOrders`, `allOrders`, `userTrades`.

Ambiguous outcomes:

- HTTP 408 and transport timeout on create/cancel are ambiguous.
- The system must not treat ambiguous create as a terminal reject.
- The system must not retry ambiguous create using a fresh client order ID before exact reconciliation.

Mainnet:

- Mainnet mutation must be hard-disabled by default.
- Do not demonstrate mainnet order sending.
- Before claiming full mainnet runtime support in the PDF, verify the runtime path, configuration gate, and tests.

## Deterministic Simulator

The simulator is required because Testnet cannot reliably force all edge cases. It should be deterministic, seed-free or fixed-seed, and scriptable.

Simulator capabilities to highlight:

- Configurable bid/ask sequence and time advancement.
- Partial fills.
- Cancel delay and cancel/fill races.
- Post-only rejection.
- Create timeout where the order actually exists.
- Duplicate, delayed, or out-of-order execution events.
- WebSocket disconnect or stale market data.
- Reconciliation snapshots.

Simulator demo commands:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Default artifact directories:

- `/tmp/calais-sim-cancel-race`
- `/tmp/calais-sim-create-timeout`

Expected artifact files:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`

## Required Edge Case Matrix

| Edge case | Brief scenario | Expected behavior | Repository evidence to cite |
| --- | --- | --- | --- |
| E1 | Target position already reached. | No order, `NO_ACTION` or completed result. | API/engine no-action tests. |
| E2 | Target quantity below minimum order size. | Reject, record dust, or apply explicit rule. Never silently round up into overfill. | Validation and tail quantity tests. |
| E3 | Partial fill triggers cancel/reprice. | Replacement quantity is safe remaining parent quantity. | Chase lifecycle and cancel-race tests. |
| E4 | Fill occurs while cancel is pending. | Update parent cumulative fills before replacement decision. | `run_sim_cancel_race.py` artifacts and tests. |
| E5 | Create order request timeout. | Keep child `UNKNOWN`; exact client-order lookup; no fresh-ID retry. | `run_sim_create_timeout.py` and mutation tests. |
| E6 | Duplicate or out-of-order fill events. | Use trade IDs/cumulative quantities; no double count; no state regression. | Duplicate event tests and cumulative fill tests. |
| E7 | Price jumps outside range. | Stop active chasing; apply deadline policy; report unfilled quantity. | Price outside range tests. |
| E8 | Frequent price movement. | Minimum reprice interval/debounce prevents cancel storm. | Chase threshold and interval tests. |
| E9 | WebSocket disconnect or stale data. | Pause new actions, reconnect, reconcile, then resume or exit safely. | Stream disconnect/runtime tests. |
| E10 | Deadline reached with remainder. | Cancel active orders or bounded aggressive attempt; report actual filled/unfilled. | Deadline policy tests. |
| E11 | Post-only rejection. | Refresh state and retry within limits/backoff; no infinite loop. | Binance mutation and retryable rejection tests. |
| E12 | Target crosses zero. | Use net position delta, not close/open split confusion. | Cross-zero tests. |

## Required Test Scenario Matrix

| Scenario | Requirement | Evidence to include |
| --- | --- | --- |
| T1 Normal Chase | Stable market completes or reasonably partially completes. | `run_sim_chase.py` output and summary. |
| T2 Chase Reprice | Price crosses threshold; cancel-and-replace; interval respected. | Chase unit tests plus optional timeline. |
| T3 Partial Fill + Cancel Race | New fill during cancel does not cause overfill. | `run_sim_cancel_race.py` timeline and summary. |
| T4 Create Timeout | Order exists after timeout; no duplicate replacement. | `run_sim_create_timeout.py` timeline and exact lookup evidence. |
| T5 TWAP Carry-forward | Earlier unfilled amount flows into later schedule deficit. | TWAP tests and slice ledger. |
| T6 Tail Quantity | Step size, min quantity, dust, and remainder handled explicitly. | Validation/tests and final slice ledger. |
| T7 Price Outside Range | No price-bound violation; unfilled result reported. | Simulation test result. |
| T8 Stream Disconnect | Pause, reconnect, reconcile, resume or exit safely. | Runtime stream health test and artifact/log line. |
| T9 Duplicate Event | Duplicate fill event ignored. | Duplicate event tests and metric count. |
| T10 Cross-zero Position | Negative to positive target computes full net trade. | Cross-zero test or API example. |

## Result Metrics

The brief requires each execution to output quantity, price, order, time, and safety metrics. The final PDF should include one populated table from a simulator run and one from each accepted Testnet run.

| Category | Required metrics | Current source or action |
| --- | --- | --- |
| Quantity | target position, initial position, required quantity, filled quantity, unfilled quantity, completion rate. | Summary metrics and execution response. |
| Price | arrival bid/ask/mid, execution VWAP, slippage bps, price-bound violations. | Summary metrics and fill artifacts. |
| Orders | orders submitted, cancels, reprices, rejections, maker/taker fills. | Summary metrics include `orders_submitted`, `cancels_requested`, `reprices`, `rejections`, `maker_fills`, `taker_fills`, `maker_filled_quantity`, and `taker_filled_quantity`. |
| Time | requested duration, actual duration, TWAP schedule deficit. | Summary metrics and TWAP slice ledger. |
| Safety | overfill quantity, duplicate events ignored, unknown orders reconciled, final status/reason. | Summary metrics and simulator tests. |

Formulae to include:

```text
execution_vwap = sum(fill_price * fill_quantity) / sum(fill_quantity)
completion_rate = filled_quantity / required_trade_quantity
slippage_bps = side-aware difference between arrival_mid and execution_vwap
```

The PDF should explicitly state that a completion rate below 100 percent can be correct if price bounds, minimum quantity, deadline, or exchange rejects prevent legal completion.

## Binance Testnet Evidence Plan

Testnet evidence is mandatory in the brief. The repository provides Testnet scripts, but the final submission still needs raw accepted-order artifacts from at least one Chase execution and one TWAP execution when account funding and permissions allow.

Required environment variables:

```bash
export BINANCE_USDM_API_KEY=...
export BINANCE_USDM_API_SECRET=...
```

Chase command template:

```bash
uv run python scripts/run_testnet_chase.py \
  --confirm-send-orders \
  --symbol BTCUSDT \
  --target-position 0.001 \
  --target-price-lower 90000 \
  --target-price-upper 120000 \
  --output-dir /tmp/calais-binance-testnet/chase
```

TWAP command template:

```bash
uv run python scripts/run_testnet_twap.py \
  --confirm-send-orders \
  --symbol BTCUSDT \
  --target-position 0.001 \
  --target-price-lower 90000 \
  --target-price-upper 120000 \
  --number-of-slices 5 \
  --output-dir /tmp/calais-binance-testnet/twap
```

Evidence bundle checklist:

- Request parameter snapshot.
- Symbol rules snapshot from Binance.
- Market snapshot at arrival.
- Raw sanitized REST create response with exchange order ID.
- Client order ID and child order ID.
- Raw sanitized REST cancel response if cancel occurs.
- Raw private user stream order/trade events if received.
- REST reconciliation snapshots used after stream disconnect or ambiguous outcomes.
- Fill records with trade IDs, fill price, fill quantity, cumulative quantity, and maker/taker flag if available.
- Execution summary with final status, reason, filled/unfilled quantity, VWAP, slippage, and safety metrics.
- Timeline showing UTC timestamps and monotonic timing.

Do not include:

- API secrets.
- Listen keys.
- Request signatures.
- Local secret files.

If Binance rejects before acceptance:

- Keep the raw sanitized error artifact.
- State whether rejection was due to margin, account permissions, min notional, symbol state, post-only behavior, or price bounds.
- Label accepted-order Testnet evidence as pending account configuration.
- Do not claim simulator artifacts satisfy the Testnet E2E requirement.

## Real Failure Case

The most important development failure case involved ambiguous Binance create outcomes.

Problem:

- `POST /fapi/v1/order` can return HTTP 408 or hit a transport timeout.
- That response does not prove the order failed.
- The exchange may have accepted the order and created live exposure.
- If the client immediately retries with a fresh client order ID, both the original and replacement can fill.

Correct fix:

- Classify create timeout as ambiguous.
- Reserve the original child quantity in `UNKNOWN`.
- Reconcile by exact `origClientOrderId`.
- If found, promote the order to live/open/filled state according to exchange status.
- If not found, clear unknown exposure.
- Do not use broad prefix scans as proof that a specific timed-out order does not exist.

Regression evidence to cite:

- Timeout mutation classification tests.
- Exact create-timeout lookup tests.
- Simulator create-timeout artifact.
- Unknown orders reconciled metric.

This failure case is strong for the PDF because it directly maps to one of the brief's direct deduction items.

## Current Verification Snapshot

Commands to run before final PDF:

```bash
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Latest local verification result after this documentation update:

```text
343 passed
```

The README and final PDF should be updated again if later code changes alter the test count.

## Demo Checklist

The 30-45 minute demonstration should be structured around the scoring criteria.

1. Show architecture and module ownership.
2. Show execution and child order state machines.
3. Run normal Chase in simulator.
4. Run TWAP in simulator and show schedule carry-forward.
5. Show partial fill plus cancel race timeline.
6. Show create-timeout idempotency and exact reconciliation.
7. Show price outside range result and explain why not completing can be correct.
8. Explain the exposure invariant bucket by bucket.
9. Show one real failure case and tests that prevent regression.
10. If credentials/account state allow, run or display accepted Binance Testnet Chase and TWAP artifact bundles.
11. Be prepared for a small live code change around parameters, metrics, or validation.

## Known Limitations

- In-memory state only. Process restart loses executions.
- No durable event log replay.
- No production multi-process leader election.
- No alerting, dashboards, or operations runbook beyond structured artifacts.
- Runtime stream supervision is compact and Testnet-focused.
- Binance Testnet order acceptance depends on external account funding, permission, and risk configuration.
- Hedge Mode is not a primary supported path.
- Mainnet mutation is disabled by default and should not be demonstrated.
- Simulator proves deterministic races that Testnet may not naturally reproduce.
- Final PDF and accepted Testnet evidence bundle remain to be assembled.

## Improvements Before Final Submission

Highest priority:

- Attach accepted-order Binance Testnet evidence for one Chase and one TWAP run.
- Populate order metrics tables with `reprices` and known maker/taker fill counts from artifacts.
- State that mainnet requires `ALLOW_MAINNET_TRADING=true` and is not part of the take-home demo evidence.
- Include a small table mapping T1-T10 to test files and artifact names.

Medium priority:

- Add a state machine diagram to the PDF.
- Add one concrete execution summary table with values.
- Add a short "how to defend this design live" note around UNKNOWN exposure and cancel/fill races.
- Include exact artifact directory names and execution IDs from final runs.

## Final PDF Outline

Recommended 8-15 page structure:

1. Title, scope, and executive summary.
2. Requirements coverage matrix.
3. Architecture and lifecycle.
4. Safety invariant and state machines.
5. Chase design and repricing policy.
6. TWAP design and schedule carry-forward.
7. Binance integration and precision handling.
8. Simulator and Testnet evidence.
9. Edge cases E1-E12 and test scenarios T1-T10.
10. Result metrics and selected run summaries.
11. Real failure case and fix.
12. Limitations, improvement roadmap, and AI usage.

## Final PDF Checklist

- Include final commit hash or branch name.
- Include exact test command and output count.
- Include exact simulator artifact paths.
- Include accepted Testnet artifact paths or clearly labeled pending status.
- Include execution IDs and client order IDs for all showcased runs.
- Include raw parameter snapshots.
- Include result summary tables.
- Include edge-case and test-scenario matrices.
- Include AI usage disclosure.
- Do not include secrets, signatures, listen keys, or local private account data.
