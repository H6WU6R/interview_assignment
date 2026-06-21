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

After Binance Testnet credentials are available, include Testnet artifacts from the API-key follow-up plan.
