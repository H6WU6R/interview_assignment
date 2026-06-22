# External Code Review Summary

Date: 2026-06-22

Purpose: give an external reviewer a complete map of the repository, execution pipeline, project-brief coverage, verification status, and remaining submission gaps. This file is intentionally detailed so the reviewer can audit the code without first reconstructing the architecture from scattered notes.

## Current Verification Baseline

Commands run on the current working tree:

```bash
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
git diff --check
```

Results:

```text
351 passed
simulator Chase demo passed
simulator TWAP demo passed
simulator cancel/fill race demo passed
simulator create-timeout demo passed
git diff --check clean
```

The cancel/fill race and create-timeout demos write artifacts under `/tmp/calais-sim-cancel-race/...` and `/tmp/calais-sim-create-timeout/...` by default.

## Repository Map

| Area | Files | Review focus |
| --- | --- | --- |
| API | `src/api/app.py`, `src/api/schemas.py`, `src/api/runtime.py` | Request validation, environment construction, background execution, stream supervision, shutdown behavior. |
| Execution core | `src/execution/engine.py`, `src/execution/models.py`, `src/execution/service.py`, `src/execution/state_machine.py`, `src/execution/events.py`, `src/execution/ids.py`, `src/execution/clock.py` | Parent/child state transitions, exposure accounting, fills, cancels, timeouts, reconciliation, summaries. |
| Algorithms | `src/algorithms/chase.py`, `src/algorithms/twap.py` | Chase desired price/reprice decision; TWAP absolute schedule and safe deficit math. |
| Exchanges | `src/exchanges/base.py`, `src/exchanges/simulator.py`, `src/exchanges/binance_usdm.py` | Adapter contract, deterministic simulator, Binance REST/WebSocket integration and mutation classification. |
| Risk | `src/risk/validation.py`, `src/risk/decimal_math.py` | Decimal rounding, price bounds, tick/step/min-notional checks, post-only safety. |
| Observability | `src/observability/logging.py`, `src/observability/artifacts.py`, `src/observability/summary.py` | Sanitization, JSONL/CSV artifacts, summary metrics. |
| Scripts | `scripts/run_sim_*.py`, `scripts/run_testnet_*.py`, `scripts/testnet_runner.py`, `scripts/_sim_demo_common.py` | Reproducible simulator demos and credential-gated Binance Testnet demos. |
| Tests | `tests/unit/`, `tests/simulation/`, `tests/integration/` | Unit and deterministic scenario coverage; Testnet contract tests skipped without credentials. |
| Reports/docs | `README.md`, `AI_USAGE.md`, `reports/report_draft.md`, `reports/external_code_review_summary.md`, `reports/failure_case_log.md`, `reports/submission_manifest.md` | Human explanation, PDF source material, external review map, disclosure, submission packaging. |
| Packaging | `Dockerfile`, `.dockerignore` | Optional simulation API container packaging; credentials must be passed at runtime, not baked into the image. |

## Project Brief Summary

The PDF asks for a compact Python 3.11+ execution service for Binance USD-M Futures BTCUSDT Perpetual. The service receives:

- `target_position`
- `target_price_range`
- `target_duration`
- `algorithm`: `CHASE` or `TWAP`
- `deadline_policy`: `CANCEL_REMAINDER` or `AGGRESSIVE_WITHIN_RANGE`

The key interpretation is that `target_position` is the final desired account position, not order size:

```text
required_trade_quantity = target_position - current_position
> 0 -> BUY
< 0 -> SELL
= 0 -> NO_ACTION
```

The brief emphasizes correctness over breadth. The repository should prove no predictable overfill, no fake completion outside the price range, safe handling of partial fills and cancel races, idempotent create-timeout repair, Decimal precision, deterministic simulator coverage, Binance Testnet connectivity/evidence, and clear reporting.

## Pipeline Overview

```text
HTTP API or script request
  -> Pydantic request validation
  -> ExecutionRuntime
       -> choose simulation/testnet/mainnet environment
       -> supervise background ticks
       -> supervise market/user streams
       -> renew Binance listen key
       -> reconcile on stream events or disconnects
  -> ExecutionService
       -> thin facade over ExecutionEngine
  -> ExecutionEngine
       -> compute target-position delta
       -> own execution and child state
       -> apply Chase/TWAP demand
       -> enforce exposure invariant
       -> submit/cancel/reconcile through ExchangeAdapter
       -> terminalize and summarize
  -> ExchangeAdapter
       -> DeterministicSimulator or BinanceUsdmAdapter
  -> Observability
       -> sanitized logs, CSV/JSON artifacts, summary metrics
```

The engine is the trading-correctness boundary. Runtime code can schedule work and supervise streams, but it does not own exposure math. Algorithm modules compute narrow decisions; they do not submit orders directly.

## API and Runtime Pipeline

Entry point:

- `src/api/app.py:create_app`

Supported API operations:

- `POST /executions`
- `GET /executions/{execution_id}`
- `POST /executions/{execution_id}/run-once`
- `POST /executions/{execution_id}/reconcile`
- `POST /executions/{execution_id}/cancel`

Request validation:

- Decimal trading fields must be JSON strings.
- Only `BTCUSDT` is accepted.
- Price bounds must be positive and `lower <= upper`.
- Duration and timeout/slice parameters must be positive.
- Pydantic rejects extra fields.

Runtime responsibilities in `src/api/runtime.py`:

- Build a deterministic simulator service for `simulation`.
- Build `BinanceUsdmAdapter` for `testnet`.
- Build mainnet adapter only if credentials exist and `ALLOW_MAINNET_TRADING=true`.
- Reject a second active execution for the same environment and symbol.
- Start background execution loops when app lifespan starts.
- Start market and private user stream supervisors for Binance-like adapters.
- Renew listen keys.
- Reconcile active executions on user stream events and user stream disconnect.
- Cancel and reconcile active executions during shutdown.
- Record runtime errors instead of silently losing them.

Mainnet behavior:

- Mainnet is configuration-compatible, but hard disabled by default.
- Mainnet is not needed for the take-home demo.
- A mainnet execution request returns a clear `503` unless `ALLOW_MAINNET_TRADING=true`.

## Execution Engine Pipeline

Core file:

- `src/execution/engine.py`

Creation flow:

1. Query current position.
2. Compute `target_position - current_position`.
3. Determine side: buy, sell, or no action.
4. Query symbol rules.
5. Normalize quantity to quantity step.
6. Record dust rather than rounding up beyond the target.
7. Validate min quantity and buy-side notional feasibility.
8. Move into `RUNNING` or terminal `COMPLETED` for no-action/dust cases.

Tick flow through `run_once`:

1. If terminal, return snapshot.
2. If cancelling, reconcile and terminalize if exposure is clear.
3. If unknown create exposure exists, block new work.
4. Reconcile exchange state.
5. Check stream health.
6. Check filled target and deadline conditions.
7. Cancel timed-out children if needed.
8. If Chase has active exposure, possibly reprice.
9. Build new child demand for Chase or TWAP.
10. Validate price/quantity/post-only safety.
11. Submit through the single child-submit path.
12. Update summary if terminal.

Single submit path:

- `_submit_child_locked` is the only place that calls the adapter's `submit_limit_order`.
- It reserves `pending_submit_quantity` before mutation.
- It maps create timeouts to `UNKNOWN` and reserves `unknown_order_quantity`.
- It maps retryable post-only rejects to bounded retry behavior.
- It maps terminal rejects to failed/expired execution according to context.

Reconciliation flow:

- REST reconciliation calls `reconcile_orders_and_fills`.
- Runtime/user-stream reconciliation can apply a `ReconciliationResult` directly.
- Both paths share `_apply_reconciliation_result_locked`.
- Unknown children can be repaired with exact lookup by `origClientOrderId`.
- Fills update parent cumulative fill only when incoming cumulative quantity advances.

## State Machines

Execution states from the brief:

```text
CREATED -> VALIDATING -> RUNNING -> CANCELLING
  -> COMPLETED | PARTIALLY_COMPLETED | EXPIRED | CANCELLED | FAILED
```

Child order states from the brief:

```text
PENDING_SUBMIT -> OPEN -> PARTIALLY_FILLED -> PENDING_CANCEL
  -> CANCELLED | FILLED | REJECTED | UNKNOWN
```

Implementation:

- `src/execution/state_machine.py` defines legal transitions.
- Engine helpers call the transition functions.
- Invalid state transitions do not resurrect terminal children.
- Terminal execution states do not return to `RUNNING`.

The reviewer should check that any future edits keep status mutation behind these helpers.

## Exposure Model and Main Invariant

The core invariant:

```text
confirmed_filled
+ live_open
+ pending_submit
+ pending_cancel
+ unknown_order
+ new_child_quantity
<= normalized_target_trade_quantity
```

Bucket meanings:

- `confirmed_filled_quantity`: parent cumulative filled quantity.
- `live_open_quantity`: remaining quantity on active exchange orders.
- `pending_submit_quantity`: quantity reserved while create mutation outcome is not known.
- `pending_cancel_quantity`: quantity reserved while cancel outcome is not known.
- `unknown_order_quantity`: quantity reserved after ambiguous create outcome.
- `new_child_quantity`: proposed new child order.

This invariant directly addresses the PDF warning that recalculating `remaining_quantity` alone is insufficient. It prevents overlap between replacement orders and old live, cancelling, pending, or unknown exposure.

Other invariants:

- Parent confirmed fill is monotonic.
- Per-child confirmed fill is monotonic.
- Duplicate trade IDs do not increase fills twice.
- Lower cumulative REST/user snapshots cannot reduce filled quantity.
- Create timeout does not permit a fresh client order ID before exact reconciliation.
- No aggressive order can violate price bounds.
- Price-out-of-range executions report actual unfilled quantity.

## Chase Algorithm

Files:

- `src/algorithms/chase.py`
- Engine integration in `src/execution/engine.py`

Behavior:

- Passive buy uses current best bid.
- Passive sell uses current best ask.
- Passive orders are post-only when supported.
- Reprice requires both:
  - `reprice_threshold_bps`
  - `minimum_reprice_interval_ms`
- Default `repricing_mode` is `ADVERSE_ONLY`.
- `TWO_SIDED` is available as a parameter.
- Cancel-and-replace happens only after reserved exposure is reconciled enough to safely submit replacement quantity.
- `reprices` are counted in summary metrics.

Deadline behavior:

- `CANCEL_REMAINDER`: cancel active children and report actual result.
- `AGGRESSIVE_WITHIN_RANGE`: allow one bounded marketable limit attempt near deadline, using IOC where supported, without crossing the configured price range.

## TWAP Algorithm

Files:

- `src/algorithms/twap.py`
- Engine integration in `src/execution/engine.py`

The TWAP implementation is not a naive `slice_qty` plus `sleep` loop. It uses absolute monotonic time:

```text
scheduled_cumulative_quantity(t)
= total_trade_quantity * elapsed_time / total_duration

quantity_deficit(t)
= scheduled_cumulative_quantity(t) - confirmed_cumulative_filled_quantity(t)

safe_child_quantity
= quantity_deficit(t) - reserved_exposure
```

Important behavior:

- Previous unfilled slices carry forward automatically.
- Open/pending/unknown exposure is subtracted before new child demand.
- Quantity is floored to step size.
- The final slice can absorb legal rounding remainder but cannot exceed target.
- Summary metrics include a `twap_slice_ledger`.

## Risk and Validation

Files:

- `src/risk/decimal_math.py`
- `src/risk/validation.py`

The system uses `Decimal` for trading numbers. API decimal fields are strings, and Binance order parameters are serialized as decimal strings.

Risk checks include:

- tick-size price rounding
- quantity-step flooring
- min quantity
- min notional
- price bounds
- post-only crossing
- supported time-in-force
- market staleness/crossed-market rejection

Sell-side min-notional note:

- The engine does not pre-reject sells based only on `target_price_lower`, because sell passive order price is derived from the current ask and is validated against the actual submitted price.
- Tests explicitly cover a low sell lower-bound case where the actual ask/order price satisfies min-notional.
- This is intentional. A lower-bound-only pre-reject would falsely reject valid sell executions.

## Exchange Adapter Contract

File:

- `src/exchanges/base.py`

The adapter contract includes:

- `get_symbol_rules`
- `get_position`
- `get_best_bid_ask`
- `stream_market_data`
- `submit_limit_order`
- `cancel_order`
- `get_order_by_client_order_id`
- `stream_user_events`
- `reconcile_orders_and_fills`
- `health_check_streams`

Both simulator and Binance adapters implement this contract so the same engine path is used for tests and live Testnet scripts.

## Deterministic Simulator

File:

- `src/exchanges/simulator.py`

Simulator capabilities:

- controllable bid/ask sequence
- manual monotonic clock
- fresh/stale/crossed market data
- submit/cancel child orders
- partial fills
- fill during cancel
- post-only rejection
- create timeout where order exists
- create timeout where order is not found
- duplicate/lower cumulative reconciliation events
- stream health scripting
- execution-scoped reconciliation
- simulated account position updates after fills

This simulator is mandatory because Binance Testnet cannot reliably force races such as cancel/fill overlap or duplicate/out-of-order events.

## Binance USD-M Adapter

File:

- `src/exchanges/binance_usdm.py`

Binance behavior:

- Testnet REST base: `https://demo-fapi.binance.com`.
- Testnet WebSocket root: `wss://fstream.binancefuture.com`.
- Mainnet REST/WebSocket roots are configured but guarded by `ALLOW_MAINNET_TRADING=true`.
- Signed requests include `timestamp`, `recvWindow`, API key header, and HMAC signature.
- Passive post-only orders map to `timeInForce=GTX` when supported.
- Aggressive deadline orders can use `IOC`.
- Exact order lookup uses `GET /fapi/v1/order` with `origClientOrderId`.
- Broad reconciliation uses open orders, all orders, and user trades, scoped by execution client-order prefix.
- `ORDER_TRADE_UPDATE` private stream events can be converted to `ReconciliationResult`.
- `userTrades` maker flags populate maker/taker fill metrics when available.
- Hedge mode is rejected by position parsing if `positionSide` is not `BOTH`.

Mutation classification:

- Create HTTP 408 and transport timeout -> `UNKNOWN_CREATE_OUTCOME`.
- Cancel HTTP 408 and transport timeout -> `PENDING_CANCEL_OUTCOME`.
- Retryable reads -> `RetryableReadFailure`.
- Terminal venue rejects -> `ExchangeTerminalReject`.
- Specific ambiguous server errors remain conservative.

Security:

- API keys and secrets are loaded from environment or `.env`.
- The sanitizer removes API keys, secrets, signatures, listen keys, signed payloads, and raw authenticated request containers from logs/artifacts.

## Testnet Runner

Files:

- `scripts/run_testnet_chase.py`
- `scripts/run_testnet_twap.py`
- `scripts/testnet_runner.py`

Runner behavior:

- Requires `BINANCE_USDM_API_KEY`.
- Requires `BINANCE_USDM_API_SECRET`.
- Requires `--confirm-send-orders`.
- Requires explicit target position and price bounds.
- Never falls back to simulator.
- Starts market stream and user stream before creating the execution.
- Records timestamped market snapshots, run ticks, cancel/final reconcile events, private user-stream events, and user-stream-applied updates.
- Applies matching private user-stream reconciliation to the active execution.
- Writes artifacts under `/tmp/calais-binance-testnet` by default.
- Writes Testnet-specific evidence files:
  - `symbol_rules.json`: exchange rule snapshot used for rounding/validation.
  - `reconciliation_orders.csv`: final reconciliation order rows scoped to the execution.
  - `evidence_manifest.json`: execution ID, order IDs, exchange-order evidence status, stream-event evidence flags, warnings, reconciliation counts, and rate-limit metadata.

Important evidence limitation:

- The code path and artifact generation are unit-tested, but the repository currently does not contain accepted-order Binance Testnet Chase/TWAP artifacts.
- Those artifacts are mandatory for final submission if the account can pass Binance margin/risk checks.

## Observability and Artifacts

Files:

- `src/observability/logging.py`
- `src/observability/artifacts.py`
- `src/observability/summary.py`
- `scripts/_sim_demo_common.py`

Artifact files:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`
- `twap_slice_ledger.csv`
- `symbol_rules.json` for Testnet runs.
- `reconciliation_orders.csv` for Testnet runs.
- `evidence_manifest.json` for Testnet runs.

Execution summary metrics include:

- target position
- initial position
- raw required quantity
- normalized required quantity
- target dust quantity
- filled quantity
- unfilled quantity
- completion rate
- arrival bid/ask/mid
- execution VWAP
- slippage bps
- price-bound violations
- orders submitted
- cancels requested
- reprices
- rejections
- maker/taker fill counts
- maker/taker filled quantities
- requested duration
- actual duration
- duplicate events ignored
- unknown orders reconciled
- max reserved exposure
- overfill quantity
- final status/reason
- TWAP slice ledger

The artifact writer supports heterogeneous timeline rows and sanitizes payloads before writing.

## Test Coverage Map

The test suite is split into:

- `tests/unit/`: engine, API, Binance adapter, risk math, state machine, IDs, observability.
- `tests/simulation/`: deterministic exchange behavior and required scenario coverage.
- `tests/integration/`: credential-gated Binance Testnet contract tests.

PDF scenario coverage:

| Scenario | Status | Evidence |
| --- | --- | --- |
| T1 Normal Chase | COVERED | Simulator tests and `scripts/run_sim_chase.py`. |
| T2 Chase Reprice | COVERED | Reprice threshold/min-interval tests and summary metric test. |
| T3 Partial Fill + Cancel Race | COVERED | Simulator race tests and `scripts/run_sim_cancel_race.py` artifacts. |
| T4 Create Timeout | COVERED | UNKNOWN exposure tests and `scripts/run_sim_create_timeout.py` artifacts. |
| T5 TWAP Carry-forward | COVERED | TWAP absolute schedule and deficit tests. |
| T6 Tail Quantity | COVERED | Dust/min-quantity/rounding tests. |
| T7 Price Outside Range | COVERED | Validation and deadline/outside-range tests. |
| T8 Stream Disconnect | COVERED | Runtime stream restart/reconcile tests. |
| T9 Duplicate Event | COVERED | Duplicate trade/cumulative fill tests and metric coverage. |
| T10 Cross-zero Position | COVERED | Required-trade and account-position tests. |

Additional regression coverage:

- create response with `PARTIALLY_FILLED`
- lower cumulative snapshot cannot reduce fills
- post-only reject retry limit
- unknown child exact lookup found/not-found
- mainnet opt-in gate
- private user event direct reconciliation
- Testnet runner stream failure stops before unsafe next tick
- Testnet runner logs and applies matching user-stream events

## Requirement Gap Assessment Against PDF

Status legend:

- COVERED: implemented and locally tested.
- PARTIAL: code exists but final submission needs external/live evidence.
- GAP: missing from current repo/submission package.
- OUT_OF_SCOPE: not required by the brief or deliberately excluded with explanation.

| PDF requirement | Current status | Notes |
| --- | --- | --- |
| Python 3.11+ / asyncio core path | COVERED | `pyproject.toml` requires Python >=3.11; async service/runtime/adapter paths. |
| BTCUSDT USD-M Futures scope | COVERED | API accepts only BTCUSDT. |
| Target position, not order quantity | COVERED | Engine computes delta from current position; no-action/cross-zero tested. |
| Chase execution | COVERED | Passive best bid/ask, post-only, reprice threshold, min interval, adverse/two-sided mode. |
| TWAP execution | COVERED | Absolute schedule, carry-forward deficit, reserved exposure subtraction. |
| Deadline policies | COVERED | `CANCEL_REMAINDER` and `AGGRESSIVE_WITHIN_RANGE` implemented/tested. |
| Execution and child state machines | COVERED | Explicit states and transition helpers. |
| Dynamic symbol rules | COVERED | Binance `exchangeInfo`; simulator configurable rules. |
| Decimal precision | COVERED | Decimal models, string API, tests rejecting floats. |
| Price range enforcement | COVERED | Risk validation and outside-range behavior. |
| Cancel/fill race safety | COVERED | Pending cancel exposure and simulator race artifacts. |
| Create timeout idempotency | COVERED | UNKNOWN exposure and exact `clientOrderId` lookup. |
| Duplicate/out-of-order fills | COVERED | Trade ID and cumulative fill monotonic handling. |
| Stream disconnect/staleness | COVERED | Runtime health checks, restart, bounded reconciliation tests. |
| One-way mode support, hedge rejection | COVERED | Position parser rejects non-`BOTH` `positionSide`. |
| Simulation environment | COVERED | Deterministic simulator and scripts. |
| Testnet integration code | COVERED | Adapter, contract tests, runner scripts. |
| Testnet accepted Chase E2E evidence | GAP | No accepted-order artifact currently committed/generated in this environment. |
| Testnet accepted TWAP E2E evidence | GAP | No accepted-order artifact currently committed/generated in this environment. |
| Raw Testnet logs/order IDs/params/results | COVERED for tooling; evidence pending | Runner writes request snapshots, sanitized timeline/logs, child/fill CSVs, symbol rules, reconciliation orders, and an evidence manifest. Final accepted-order bundles are still pending. |
| Mainnet config path default-disabled | COVERED | Mainnet requires credentials and explicit `ALLOW_MAINNET_TRADING=true`; no demo needed. |
| Structured logs reconstruct timeline | COVERED | JSONL/CSV artifacts and summaries. |
| Result metrics | COVERED | Summary metrics include quantity/price/order/time/safety fields. |
| Final PDF report | GAP | `reports/report_draft.md` is prepared; final PDF not generated by current request. |
| AI usage disclosure | COVERED | `AI_USAGE.md`. |
| Clear commit history | PARTIAL | Current workspace has uncommitted changes; commit intentionally before final external submission. |
| Dockerfile | COVERED | Optional container packaging is present for the simulation API. |

## Most Important Remaining Submission Risks

1. Accepted Binance Testnet evidence is still missing.

The PDF explicitly requires at least one Chase and one TWAP end-to-end Binance Futures Testnet run, with raw execution logs, order IDs, parameters, and summaries. The code has scripts and tests for the path, but final accepted-order artifacts are not present.

2. Final PDF is not generated.

`reports/report_draft.md` is now comprehensive source material, but the requested final deliverable is a PDF report. The user explicitly deferred PDF generation for now, so this remains a packaging gap.

3. Current changes are uncommitted.

For external review or final submission, commit the working tree so the reviewer has a stable diff and the "clear commit history" requirement is easier to defend.

## Suggested External Review Order

1. Read `reports/external_code_review_summary.md`.
2. Read `README.md`.
3. Read `reports/report_draft.md` requirement matrix and limitations.
4. Inspect `src/execution/engine.py` for exposure accounting and state transitions.
5. Inspect `src/risk/validation.py` and `src/risk/decimal_math.py` for price/quantity safety.
6. Inspect `src/algorithms/chase.py` and `src/algorithms/twap.py`.
7. Inspect `src/exchanges/binance_usdm.py` for mutation classification and reconciliation.
8. Inspect `src/exchanges/simulator.py` and `tests/simulation/test_required_scenarios.py`.
9. Run `uv run pytest -q`.
10. Run the four simulator scripts.
11. If credentials are available, run the two Testnet scripts with very small bounded BTCUSDT parameters.

## Reviewer Questions to Answer

Ask the external reviewer to specifically challenge:

- Can any path submit a replacement child while live, pending-cancel, pending-submit, or unknown exposure still makes overfill possible?
- Can any create timeout generate a new client order ID before exact reconciliation?
- Can any stale/lower cumulative fill snapshot reduce parent filled quantity?
- Can duplicate fill events increase parent fill twice?
- Can aggressive deadline behavior violate upper/lower price bounds?
- Does TWAP subtract reserved exposure before submitting new deficit quantity?
- Does shutdown leave active exchange orders unattended?
- Are Testnet artifacts sufficient to prove real Binance order lifecycle behavior?
- Are report claims stronger than what the code and artifacts prove?

## Current Final Assessment

The implementation is strong on deterministic correctness, engine invariants, simulator coverage, Decimal discipline, API/runtime layering, and documented failure-mode reasoning. The main remaining gaps are not core algorithm code gaps; they are final submission evidence gaps:

- accepted Binance Testnet Chase artifact
- accepted Binance Testnet TWAP artifact
- final PDF generation
- committing the working tree for review/submission

If those evidence and release-management items are completed, the repo is well aligned with the PDF requirements.
