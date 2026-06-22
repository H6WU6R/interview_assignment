# Duplicate Fill Events

This scenario proves that duplicate and stale cumulative fill events do not double-count parent filled quantity.

## Run The Scenario Test

```bash
uv run pytest tests/simulation/test_required_scenarios.py::test_t9_duplicate_fill_event_does_not_double_count_cumulative_fill -q
```

## Expected Evidence

The test injects a valid cumulative fill of `0.003`, a duplicate event with the same trade ID, and a stale cumulative fill of `0.002`.

The stable assertions are:

```text
confirmed_filled_quantity == 0.003
duplicate_events_ignored == 2
filled_quantity == "0.01"
overfill_quantity == "0"
```

The engine applies cumulative fills monotonically and does not let stale snapshots reduce or duplicate parent fill accounting.
