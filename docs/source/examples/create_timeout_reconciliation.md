# Create Timeout Reconciliation

This example proves that an ambiguous create timeout becomes `UNKNOWN`, reserves exposure, and blocks fresh client order IDs until exact reconciliation resolves the original order.

## Run

```bash
uv run python scripts/run_sim_create_timeout.py
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: Create Timeout
unknown_before_reconcile=0.010
unknown_after_reconcile=0
live_open_after_reconcile=0.010
artifact_dir=<generated-artifact-directory>
```

Inspect `execution_log.jsonl`. It should include `create_timeout_unknown`, `run_before_reconcile_no_new_client_order_id`, and `reconciled_original_open`.

Inspect `execution_summary.json`. The final reason should be `CREATE_TIMEOUT_RECONCILED`.

## Related Tests

- `tests/simulation/test_required_scenarios.py::test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id`
- `tests/simulation/test_required_scenarios.py::test_t4b_create_timeout_not_found_releases_unknown_exposure_before_safe_retry`
