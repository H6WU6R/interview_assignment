# Cancel/Fill Race

This example proves that a fill arriving during cancel reduces the replacement quantity. It targets the assignment risk where an implementation resubmits the original total quantity after cancel and creates a predictable overfill.

## Run

```bash
uv run python scripts/run_sim_cancel_race.py
```

To write artifacts to a chosen directory:

```bash
uv run python scripts/run_sim_cancel_race.py --output-dir /tmp/calais-sim-cancel-race-docs
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: Cancel/Fill Race
confirmed_filled=0.004
reserved_exposure=0.006
artifact_dir=<generated-artifact-directory>
```

Inspect `child_orders.csv`. The first child is cancelled with `filled_quantity` of `0.004`; the replacement child has `submitted_quantity` of `0.006`.

## Related Test

`tests/simulation/test_required_scenarios.py::test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing`
