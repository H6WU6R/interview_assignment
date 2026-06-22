# Cross-Zero Target Position

This scenario proves that the input is a final account position, not an order quantity.

## Run The Scenario Test

```bash
uv run pytest tests/simulation/test_required_scenarios.py::test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity -q
```

## Expected Evidence

The test covers both directions:

```text
current_position = -0.003, target_position = 0.002 -> BUY 0.005
current_position = 0.004, target_position = -0.002 -> SELL 0.006
```

This prevents the common implementation error of treating `target_position` as the child order quantity.
