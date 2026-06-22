# Normal TWAP

This example proves that TWAP uses absolute elapsed time. With a 100 second duration and 10 slices, the first child appears at the 10 second boundary.

## Run

```bash
uv run python scripts/run_sim_twap.py
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: TWAP
schedule=absolute-time TWAP target over 100 seconds
status=RUNNING
twap_order id=<generated-child-order-id> clientOrderId=<generated-client-order-id> status=OPEN qty=0.001 price=50000.00
schedule_summary=elapsed_seconds=10.0 required_quantity=0.010 confirmed=0 reserved=0.001
```

The stable behavior is that the first submitted quantity is the scheduled deficit at the first absolute slice boundary.

## Related Tests

- `tests/simulation/test_required_scenarios.py::test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`
- `tests/simulation/test_required_scenarios.py::test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary`
