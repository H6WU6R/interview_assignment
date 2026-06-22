# Chase

Chase places a passive limit order at the current near touch and reprices when the market has moved enough to justify cancel-and-replace.

## Passive Price

- Buy desired price: best bid.
- Sell desired price: best ask.
- Default order shape: post-only limit when supported.

## Repricing

`reprice_threshold_bps` controls how far the desired price must move from the active order price. `minimum_reprice_interval_ms` prevents a cancel storm.

The default repricing mode is `ADVERSE_ONLY`:

- Buy reprices upward when best bid moves up enough.
- Sell reprices downward when best ask moves down enough.

`TWO_SIDED` is available when favorable movement should also trigger repricing.

## Partial Fill Safety

Canceling a child does not prove it stopped filling. The replacement order is sized after accounting for confirmed parent fills plus live, pending, cancelling, and unknown exposure. The deterministic cancel/fill race example proves this behavior.

## Implementation And Proof

- Decision helpers: `src/algorithms/chase.py`
- Engine path: `src/execution/engine.py`
- Tests: `tests/unit/test_chase.py`
- Scenario proof: `tests/simulation/test_required_scenarios.py::test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing`
