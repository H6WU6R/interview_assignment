# Non-API Correctness Hardening Plan

This plan covers work that can be completed without Binance API keys. It focuses on simulator-proven correctness, state safety, and final-report evidence.

Note: this file was reconstructed after the untracked working copy was removed. It is aligned with the current hardened implementation.

## Scope

Do now:

- Strengthen execution invariants.
- Make simulator scenarios behavioral rather than smoke tests.
- Improve state-machine discipline.
- Tighten reconciliation scoping.
- Improve artifact and summary correctness.

Do later with API keys:

- Live Testnet order submission.
- Signed endpoint timing validation against Binance.
- User-data stream listenKey lifecycle evidence.
- Testnet artifact bundle.

## Task 1: Single Submit Gate

Ensure every child submit path goes through one shared safety check:

- Initial child submit.
- Chase replacement.
- TWAP slice.
- Retry after rejection when allowed.
- Retry after timeout reconciliation.
- `AGGRESSIVE_WITHIN_RANGE` final attempt.

The gate enforces:

```text
confirmed_filled
+ live_open
+ pending_submit
+ pending_cancel
+ unknown_order
+ new_child_quantity
<= normalized_target_trade_quantity
```

No algorithm module may bypass this gate or call an exchange adapter directly.

## Task 2: Exposure Buckets

Keep exposure buckets explicit and auditable:

- `confirmed_filled_quantity`
- `live_open_quantity`
- `pending_submit_quantity`
- `pending_cancel_quantity`
- `unknown_order_quantity`

Unknown create-timeout exposure blocks new client order IDs until reconciliation resolves the exact child. Cancel ambiguity remains pending-cancel exposure until the engine knows whether the venue order filled, cancelled, partially filled, or stayed open.

## Task 3: State Machine Discipline

All execution and child-order status changes go through `src/execution/state_machine.py`.

Implementation guardrail:

- Engine helper methods may wrap state-machine calls.
- Tests should catch important invalid transitions.
- Direct status assignment should be avoided except object initialization.

## Task 4: Execution-Scoped Reconciliation

Reconciliation must use an execution-specific client order prefix such as:

```text
ce_<short_exec>_
```

It must not use a broad prefix such as `ce_`, because that can absorb unrelated executions or manual orders.

For create-timeout scenarios, simulator tests should cover:

- Timeout then reconciliation finds the order open.
- Timeout then reconciliation finds no venue order and safe retry becomes allowed.

## Task 5: Market Data Freshness

The engine must not submit or reprice before the first fresh market snapshot exists.

Track enough market-data metadata for audit and decisions:

- Exchange event time when available.
- Local monotonic receive time for stale checks.
- UTC wall-clock receive timestamp for logs.

Stale quote threshold should be configurable and tested.

## Task 6: TWAP Correctness

TWAP should use absolute scheduled cumulative quantity, not equal slices plus sleep.

Required behavior:

- Quantities are positive absolute trade quantities.
- Side is tracked separately.
- Previous unfilled deficit carries forward.
- Safe child quantity subtracts reserved exposure.
- Final rounding remainder is handled without exceeding normalized target quantity.

## Task 7: Chase Correctness

Chase should:

- Reprice only after the bps threshold and minimum reprice interval.
- Treat `ADVERSE_ONLY` as the default deliberate design.
- Keep `TWO_SIDED` as configurable optional behavior.
- Avoid replacement sizing that ignores fills arriving during cancel.

## Task 8: Behavioral Simulator Tests

Final simulator tests must prove safety behavior, not only status progression.

Required assertions:

- T2: cancel-and-replace only after threshold and minimum interval.
- T3: fill during cancel updates parent fills before replacement sizing.
- T4: unknown create-timeout exposure blocks new `clientOrderId` before reconciliation.
- T5: TWAP later slice includes previous unfilled deficit.
- T7: no order violates target price range; final reason is partial or unfilled.
- T9: duplicate fill event does not double-count cumulative filled quantity.

## Task 9: Fill Deduplication

Deduplicate fills by exchange trade ID when available.

If trade ID is unavailable, use monotonic cumulative executed quantity per order:

```text
existing = confirmed_filled_quantity
incoming = cumulative_filled_quantity

if incoming <= existing:
    ignore duplicate or stale event
else:
    delta = incoming - existing
    confirmed_filled_quantity = incoming
```

Do not deduplicate by raw message arrival count.

## Task 10: Artifact Robustness

Artifacts should let the final report reconstruct the full execution timeline:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`

CSV writing should tolerate heterogeneous event rows by using a stable schema or the union of keys.

## Verification

Run:

```bash
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Expected result:

- All local tests pass without Binance credentials.
- Simulator artifacts include execution ID and client order ID linkage.
- Report draft can explain one real development failure case and the fix.
