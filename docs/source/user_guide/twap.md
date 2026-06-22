# TWAP

TWAP uses absolute elapsed time and cumulative schedule deficit. It is not implemented as `slice_qty / sleep`.

## Schedule

```text
effective_elapsed(t)
= completed_slice_boundary(t, total_duration, number_of_slices)

scheduled_cumulative_quantity(t)
= total_trade_quantity * effective_elapsed(t) / total_duration

quantity_deficit(t)
= scheduled_cumulative_quantity(t) - confirmed_cumulative_filled_quantity(t)
```

Before the first completed absolute slice boundary, `effective_elapsed` is zero and no child order is due.
The engine subtracts reserved exposure before submitting another child order.

## Carry-Forward Deficit

If an earlier slice remains unfilled or partially filled, later slices inherit the cumulative deficit. This prevents the schedule from pretending a previous child filled just because time advanced.

## Rounding

Quantities are normalized to exchange step size using Decimal arithmetic. The system records dust rather than rounding upward into an overfill.

## Implementation And Proof

- Schedule helpers: `src/algorithms/twap.py`
- Engine ledger: `src/execution/engine.py`
- Tests: `tests/unit/test_twap.py`
- Scenario proof: `tests/simulation/test_required_scenarios.py::test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`
