# Submission Manifest

## Include

- `src/`
- `tests/`
- `scripts/`
- `configs/example.yaml`
- `.env.example`
- `pyproject.toml`
- `uv.lock`
- `README.md`
- `AI_USAGE.md`
- `reports/report_draft.md`
- `reports/failure_case_log.md`

## Exclude

- `.venv/`
- `.pytest_cache/`
- `.DS_Store`
- `docs/superpowers/`
- local Testnet secret files
- generated temporary artifacts outside the selected evidence bundle

## Evidence Bundle

Before final submission, include deterministic simulator artifact directories from:

- `uv run python scripts/run_sim_cancel_race.py`
- `uv run python scripts/run_sim_create_timeout.py`

When Binance Testnet credentials and account margin are available, include Testnet artifacts from:

- `uv run python scripts/run_testnet_chase.py --confirm-send-orders ...`
- `uv run python scripts/run_testnet_twap.py --confirm-send-orders ...`

If Binance rejects before order acceptance, include the raw error artifact as connectivity evidence and clearly label accepted-order evidence as pending account funding/risk configuration. Do not substitute simulator output for Testnet evidence.
