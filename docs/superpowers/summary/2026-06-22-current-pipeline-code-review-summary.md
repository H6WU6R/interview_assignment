# Current Pipeline Code Review Summary

Date: 2026-06-22

Purpose: provide an external reviewer with a concise but detailed map of the current implementation, how it answers the Calais execution algorithm assignment, what has been verified, and what risks remain.

Local verification at the time of this summary:

```bash
uv run pytest -q
```

Result:

```text
252 passed, 2 skipped
```

The two skipped tests are Binance Testnet contract tests gated by missing API credentials.

## Assignment Interpretation

The project targets the Calais execution algorithm brief for Binance USD-M Futures BTCUSDT. The implementation is intentionally "small but correct": it prioritizes order lifecycle safety, deterministic simulator coverage, Decimal precision, and auditability over feature breadth.

The core assignment requirements mapped into the code are:

- Input is `target_position`, not target order size.
- Required trade is derived from current position as `target_position - current_position`.
- Chase and TWAP use a common execution engine and exchange adapter path.
- The engine owns parent execution state, child order state, fills, cancellations, timeouts, reconciliation, and final summary.
- Algorithms remain narrow and do not call Binance or simulator methods directly.
- Deterministic simulator proves race and boundary scenarios that Testnet cannot reliably create.
- Binance adapter implements the compact Testnet-facing integration path, with mainnet disabled by default.
- All order quantities and prices use `Decimal`, not Python float.
- Structured artifacts can reconstruct execution ID, child order ID, client order ID, fills, timeline, and final summary.

## High-Level Pipeline

```text
HTTP API / scripts
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

### API and Service Layer

Files:

- `src/api/app.py`
- `src/api/schemas.py`
- `src/execution/service.py`

Responsibilities:

- `create_app()` builds a FastAPI app using the deterministic simulator by default.
- API supports create, query, cancel, run-once, and reconcile operations.
- Pydantic schemas reject JSON floats for Decimal fields and serialize Decimal output as strings.
- `ExecutionService` is intentionally thin. It forwards requests to `ExecutionEngine` and keeps API-facing concerns separate from trading correctness.

Important details:

- Request format validation is in `src/api/schemas.py`.
- Trading safety validation is in `src/risk/validation.py` and is called from the engine before child order demand is submitted.
- Cancel endpoint is idempotent for already terminal or already cancelling executions.

Reviewer note:

- API parameters such as `number_of_slices`, `minimum_reprice_interval_ms`, `child_order_timeout_seconds`, and `reprice_threshold_bps` are only lightly validated today. Invalid values are caught later in algorithm or engine paths, but stronger Pydantic validation would produce cleaner 422 responses.
- The local API app does not expose a market-data seeding endpoint. Simulator scripts and tests seed market data directly. A manual API-only demo should either seed the simulator in code or add a small demo-only market-data route.

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

The main submit invariant is enforced before child submission:

```text
confirmed_filled
+ live_open
+ pending_submit
+ pending_cancel
+ unknown_order
+ new_child_quantity
<= normalized_target_trade_quantity
```

Where implemented:

- `ExposureTracker.check_can_submit()`
- `ExposureTracker.reserve_live_open()`
- `ExposureTracker.reserve_pending_submit()`
- `ExposureTracker.reserve_unknown_create()`
- `risk.validation.check_exposure_invariant()`

The engine has one exchange submit call site: `_submit_child_locked()`. That is important because initial submit, Chase replacement, TWAP slice demand, create-timeout retry after reconciliation, child timeout replacement, and final aggressive attempt all pass through the same safety gate.

### Target Position Handling

Implementation:

- `execution.models.required_trade()`
- `ExecutionEngine.create_execution()`

Behavior:

- BUY if target position is above current position.
- SELL if target position is below current position.
- NO_ACTION if target equals current position.
- Cross-zero trades are handled as absolute net quantity.
- Quantity is floored to symbol `quantity_step`, and dust is recorded instead of silently rounding up.

Covered by:

- `tests/simulation/test_required_scenarios.py::test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity`
- `tests/simulation/test_required_scenarios.py::test_t6_tail_quantity_records_dust_and_never_rounds_up`
- API dust/no-action tests in `tests/unit/test_api.py`

### Execution and Child State Machines

Files:

- `src/execution/models.py`
- `src/execution/state_machine.py`

Execution states:

```text
CREATED -> VALIDATING -> RUNNING -> CANCELLING -> COMPLETED | PARTIALLY_COMPLETED | EXPIRED | CANCELLED | FAILED
```

Child order states:

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
  -> OPEN
  -> PARTIALLY_FILLED
  -> CANCELLED
  -> FILLED

UNKNOWN
  -> OPEN
  -> PARTIALLY_FILLED
  -> FILLED
  -> CANCELLED
  -> REJECTED
```

The engine calls `transition_execution()` and `transition_child()` through helper methods. Tests cover invalid transitions and stale reconciliation not resurrecting terminal child status.

Reviewer note:

- A review pass found one genuine lifecycle gap: manual user cancellation can remain in `CANCELLING` after active exposure is cleared unless the target fully fills. Deadline cancellation terminalizes correctly, but manual cancel should likely move to `CANCELLED` if no fills occurred, or `PARTIALLY_COMPLETED` if fills occurred, after reconciliation confirms no reserved exposure remains. This should be fixed before final submission.

## Per-Execution Serialization

File:

- `src/execution/events.py`

Each execution uses `ExecutionEventActor`, an `asyncio.Lock` wrapper that serializes local state mutation for that execution. This avoids concurrent `run_once`, `cancel`, and `reconcile` calls mutating the same parent state at the same time.

Important limitation:

- The actor serializes engine mutation only. It does not replace exchange event-time ordering. Binance event time and transaction time are retained for audit and reconciliation diagnostics.

## Algorithms

Files:

- `src/algorithms/chase.py`
- `src/algorithms/twap.py`

### Chase

Chase decides:

- Passive desired price: buy at best bid, sell at best ask.
- Aggressive deadline price: buy at best ask, sell at best bid.
- Whether repricing threshold and minimum interval allow cancel-and-replace.

Default repricing mode:

- `ADVERSE_ONLY`

Optional repricing mode:

- `TWO_SIDED`

Covered by:

- `tests/simulation/test_required_scenarios.py::test_t2_chase_reprice_requires_threshold_and_minimum_interval`
- `tests/unit/test_chase.py`
- lifecycle tests for child timeout and fill-during-cancel replacement sizing.

### TWAP

TWAP uses absolute schedule boundaries rather than sleep-based equal slices.

Formula:

```text
scheduled_cumulative_quantity(t)
  = total_trade_quantity * elapsed_time / total_duration
```

Then safe child quantity is computed separately:

```text
safe_child_quantity
  = scheduled_deficit - reserved_exposure
```

Properties:

- Quantities are positive absolute trade quantities.
- Side is tracked separately.
- Previous unfilled deficit carries forward.
- Open or reserved exposure is subtracted before submitting another child.
- Rounding is floored to quantity step and cannot exceed normalized target quantity.

Covered by:

- `tests/simulation/test_required_scenarios.py::test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`
- `tests/simulation/test_required_scenarios.py::test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary`
- `tests/unit/test_twap.py`
- `tests/unit/test_engine_lifecycle.py::test_twap_uses_absolute_schedule_and_carries_forward_unfilled_deficit`

## Risk and Precision Layer

Files:

- `src/risk/decimal_math.py`
- `src/risk/validation.py`

Responsibilities:

- Side-aware price rounding.
- Quantity step flooring.
- Minimum quantity and minimum notional validation.
- Price bound validation.
- Symbol trading status validation.
- Post-only crossing checks.
- GTX support validation for post-only orders.
- Exposure invariant check.

Important behaviors:

- Passive buy rounds down and cannot cross best ask.
- Passive sell rounds up and cannot cross best bid.
- Aggressive buy rounds up but cannot exceed upper bound.
- Aggressive sell rounds down but cannot go below lower bound.
- Non-trading symbols are rejected.
- Unsupported post-only time-in-force is rejected clearly.

Covered by:

- `tests/unit/test_decimal_math.py`
- `tests/unit/test_validation.py`
- engine-level tests for price outside range and post-only behavior.

## Deterministic Simulator

File:

- `src/exchanges/simulator.py`

Capabilities:

- Dynamic symbol rules.
- Position query.
- Fresh/stale/crossed market snapshots.
- Market data stream iterator.
- Limit order submission.
- Post-only rejection.
- Cancel order.
- Fill injection.
- Fill during cancel.
- Create timeout where the order exists.
- Create timeout where the order does not exist.
- Stream health degradation.
- Execution-scoped reconciliation.

The simulator rejects broad reconciliation prefixes. It requires an execution-scoped prefix of the form:

```text
ce_<12 lowercase hex chars>_
```

This avoids accidentally absorbing unrelated manual orders or other executions.

Covered by:

- `tests/simulation/test_simulator_market_data.py`
- `tests/simulation/test_simulator_orders.py`
- `tests/simulation/test_required_scenarios.py`

## Binance USD-M Adapter

File:

- `src/exchanges/binance_usdm.py`

Implemented responsibilities:

- Testnet and mainnet URL selection.
- Mainnet mutation guard through `allow_mainnet_trading`.
- Signed REST requests with `timestamp`, `recvWindow`, and HMAC signature.
- Decimal serialization to API strings.
- HTTP error classification.
- Ambiguous create timeout mapped to `UnknownCreateOutcome`.
- Ambiguous cancel timeout mapped to `PendingCancelOutcome`.
- Post-only mapping to `timeInForce=GTX`.
- Client order ID pattern and 36-character length validation.
- Symbol rules parsing from exchange info.
- Rate-limit metadata parsing from exchange info.
- Position lookup and Hedge Mode rejection.
- Book ticker stream parser.
- Listen key create and renew methods.
- Exact order lookup by `/fapi/v1/order` with `origClientOrderId`.
- Reconciliation from open orders, all orders, and user trades, filtered by execution prefix.

Important interpretation:

- The assignment requires same-code-path mainnet configurability but default prohibition of real orders. The current implementation supports an explicit `allow_mainnet_trading=True` opt-in. If the desired interpretation is stricter, meaning no mainnet mutation should ever be possible in this take-home, this should be changed.

Current limitations:

- `health_check_streams()` returns `True` in the Binance adapter, so live stream health is not actively tracked.
- `stream_user_events()` currently yields parsed raw event dictionaries rather than normalized fill/order events consumed directly by the engine.
- Reconciliation uses fixed `limit=100` calls and does not paginate or apply start-time/order-id windows.
- Testnet live mutation evidence cannot be produced until credentials are available.

This is acceptable only if described as a compact Testnet integration path, not production-grade WebSocket recovery.

Covered by:

- `tests/unit/test_binance_adapter.py`
- `tests/unit/test_binance_order_mutations.py`
- `tests/unit/test_exchange_contract.py`
- credential-gated `tests/integration/test_binance_testnet_contract.py`

## Observability and Artifacts

Files:

- `src/observability/logging.py`
- `src/observability/artifacts.py`
- `src/observability/summary.py`
- `scripts/_sim_demo_common.py`

Artifacts:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`

Safety and audit features:

- Decimal values are serialized as strings.
- Enum values are serialized as strings.
- UTC wall-clock timestamps and monotonic time are included in simulator demo logs.
- CSV writer uses union fieldnames so heterogeneous timeline rows do not fail or lose columns.
- Log sanitizer removes API keys, secret keys, signatures, listen keys, signed payload aliases, and raw authenticated request aliases.

Covered by:

- `tests/unit/test_observability.py`
- simulator script artifact tests in `tests/simulation/test_required_scenarios.py`

## Required Scenario Coverage

The PDF requires deterministic proof for T1-T10. The current simulation suite covers them as follows:

| Scenario | Current coverage |
| --- | --- |
| T1 Normal Chase | `test_t1_normal_chase_submits_passive_price_and_preserves_exposure_invariant` |
| T2 Chase Reprice | `test_t2_chase_reprice_requires_threshold_and_minimum_interval` |
| T3 Partial Fill + Cancel Race | `test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing` |
| T4 Create Timeout | `test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id`, `test_t4b_create_timeout_not_found_releases_unknown_exposure_before_safe_retry` |
| T5 TWAP Carry-forward | `test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`, `test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary` |
| T6 Tail Quantity | `test_t6_tail_quantity_records_dust_and_never_rounds_up` |
| T7 Price Outside Range | `test_t7_price_outside_range_waits_then_expires_without_invalid_order` |
| T8 Stream Disconnect | `test_t8_stream_disconnect_pauses_submit_reconciles_then_resumes` |
| T9 Duplicate Event | `test_t9_duplicate_fill_event_does_not_double_count_cumulative_fill` |
| T10 Cross-zero Position | `test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity` |

The tests assert actual safety behavior, not only "does not crash" behavior:

- Replacement quantity is reduced after fill during cancel.
- Unknown create exposure blocks new client order IDs until reconciliation.
- TWAP carries unfilled deficit forward.
- Price outside range creates no invalid child order.
- Duplicate/stale fills do not double-count cumulative fills.
- Cross-zero position uses target minus current position.

Reviewer note:

- T9 currently mutates simulator private `_fills` directly to inject duplicate/stale events. This proves engine deduplication, but a more polished simulator API for duplicate/out-of-order event injection would be cleaner.

## Scripts and Demo Path

Simulator scripts:

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Testnet scripts:

```bash
uv run python scripts/run_testnet_chase.py --confirm-send-orders ...
uv run python scripts/run_testnet_twap.py --confirm-send-orders ...
```

Testnet scripts require:

- `BINANCE_USDM_API_KEY`
- `BINANCE_USDM_API_SECRET`
- `--confirm-send-orders`
- explicit target position
- explicit price bounds

They never fall back to simulation when credentials are missing.

## External Review Findings From This Pass

### Strengths

- Architecture separation is clean and matches the assignment structure.
- The engine is the correctness owner and algorithms are intentionally narrow.
- Exposure model includes live open, pending submit, pending cancel, and unknown order exposure.
- Create timeout and cancel/fill race are modeled conservatively.
- Decimal discipline is strong at API, domain, risk, summary, and Binance serialization boundaries.
- Simulator tests are mostly behavioral and map well to the PDF scenarios.
- Binance adapter has the right compact primitives for Testnet validation.
- Logs and artifacts are sanitized and traceable.

### Important Open Issue

Manual cancellation lifecycle should be fixed before final submission.

Current behavior:

- `cancel_execution()` moves a running execution to `CANCELLING`, cancels children, reconciles, and returns.
- If no further fills arrive and no exposure remains, parent state can remain `CANCELLING`.
- `run_once()` while `CANCELLING` only completes the execution if the target fully fills.

Why it matters:

- The PDF includes `CANCELLING -> CANCELLED | PARTIALLY_COMPLETED | COMPLETED | FAILED`.
- A user cancel with no remaining exchange exposure should produce a terminal summary.
- External reviewers are likely to flag a parent execution that can remain `CANCELLING` indefinitely.

Expected fix:

- After cancellation and reconciliation, if reserved exposure is zero:
  - `CANCELLED` if confirmed filled quantity is zero.
  - `PARTIALLY_COMPLETED` if confirmed filled quantity is greater than zero but below target.
  - `COMPLETED` if target is filled.
- Add tests for manual cancel with no fill, partial fill, and fill-to-target during cancel.

### Mainnet Policy Clarification

The current implementation default is safe:

- `allow_mainnet_trading=False`
- mainnet mutations reject unless explicitly enabled
- README says mainnet is not used for demos

There is an interpretation question:

- If "mainnet hard-disabled by default" means "must require explicit opt-in", the current implementation is aligned.
- If reviewers expect "mainnet mutation impossible in this take-home no matter what config says", change the adapter to reject all mainnet mutations and keep mainnet only as a configuration-compatible placeholder.

### Binance Follow-Up Items After API Keys

These are not blockers for simulator correctness, but should be validated with credentials:

- Live Testnet submit/cancel/query/reconcile artifact bundle.
- Actual exchangeInfo contract details for BTCUSDT.
- Market stream snapshot path.
- Position query and One-way Mode behavior.
- Listen key lifecycle.
- Whether stream health should be tracked beyond the current stub.
- Whether reconciliation should paginate or use tighter time/order windows.

### Documentation Polish

README and report should keep wording precise:

- Do not imply production-grade stream recovery.
- Say Binance Testnet path is compact and credential-gated.
- Say deterministic simulator proves race conditions; Testnet proves integration plumbing.
- Clarify that manual API `run-once` needs a fresh market snapshot in the simulator.

## Assessment

The current codebase is strong on the assignment's core theme: execution correctness under partial fills, cancel/fill races, create timeout ambiguity, Decimal precision, and deterministic simulator proof. The implementation is much more professional than a happy-path TWAP or Chase demo.

However, before using this as a final submission, the manual cancellation terminal-state gap should be fixed. The Binance adapter should also be described honestly as compact Testnet integration rather than full production stream supervision. With those caveats, the repository is in good shape for another external review round focused on correctness and assignment coverage.
