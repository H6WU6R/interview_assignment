# Price Outside Range

This scenario proves that the engine waits when the quote is outside the allowed execution range and expires without submitting an invalid order if the market never becomes executable.

## Run The Scenario Test

```bash
uv run pytest tests/simulation/test_required_scenarios.py::test_t7_price_outside_range_waits_then_expires_without_invalid_order -q
```

## Expected Evidence

The test asserts:

```text
child_orders == []
status == ExecutionStatus.EXPIRED
final_reason == PRICE_OUTSIDE_RANGE
summary.metrics["price_bound_violations"] == 1
summary.metrics["unfilled_quantity"] == "0.01"
```

This is the correct result for an unexecutable price path. The system reports unfilled quantity rather than claiming completion.
