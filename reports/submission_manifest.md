# Submission Manifest

## Include

- `src/`
- `tests/`
- `scripts/`
- `configs/example.yaml`
- `.env.example`
- `pyproject.toml`
- `uv.lock`
- `Dockerfile`
- `.dockerignore`
- `README.md`
- `AI_USAGE.md`
- `reports/report_draft.md`
- `reports/external_code_review_summary.md`
- `reports/failure_case_log.md`
- `reports/latex/report.pdf`
- `reports/evidence/simulation/chase/exec_d10652a300e544dc`
- `reports/evidence/simulation/twap/exec_1da27294d07f47af`
- `reports/evidence/simulation/cancel-race/exec_2d3534bffa694b40`
- `reports/evidence/simulation/create-timeout/exec_a03dec73abde450b`
- `reports/evidence/testnet/chase/exec_85051310eb714ebe`
- `reports/evidence/testnet/twap/exec_30ed2b4cac4346a1`

## Exclude

- `.venv/`
- `.pytest_cache/`
- `.DS_Store`
- `docs/superpowers/`
- local Testnet secret files
- generated temporary artifacts outside the selected evidence bundle

## Evidence Bundle

Committed deterministic simulator artifact directories:

- `reports/evidence/simulation/chase/exec_d10652a300e544dc`
- `reports/evidence/simulation/twap/exec_1da27294d07f47af`
- `reports/evidence/simulation/cancel-race/exec_2d3534bffa694b40`
- `reports/evidence/simulation/create-timeout/exec_a03dec73abde450b`

Committed credentialed Binance Testnet rejection/connectivity artifacts:

- `reports/evidence/testnet/chase/exec_85051310eb714ebe`: `FAILED`, `TERMINAL_ORDER_REJECTED: BINANCE_-2019:Margin is insufficient.`, `accepted_exchange_order_evidence: false`, `exchange_order_ids: []`.
- `reports/evidence/testnet/twap/exec_30ed2b4cac4346a1`: `FAILED`, `TERMINAL_ORDER_REJECTED: BINANCE_-2019:Margin is insufficient.`, `accepted_exchange_order_evidence: false`, `exchange_order_ids: []`.

When Binance Testnet account margin is available, add accepted-order Testnet artifacts from:

- `uv run python scripts/run_testnet_chase.py --confirm-send-orders ...`
- `uv run python scripts/run_testnet_twap.py --confirm-send-orders ...`

Each Testnet evidence bundle should include raw sanitized execution logs, request parameter snapshots, `symbol_rules.json`, order IDs, client order IDs, private order/trade events when available, `reconciliation_orders.csv`, fill records, `execution_summary.json`, and `evidence_manifest.json` with exchange-order evidence status and `accepted_exchange_order_evidence`.

Accepted Binance Testnet evidence remains pending. It requires at least one Chase artifact bundle and at least one TWAP artifact bundle whose `evidence_manifest.json` has `accepted_exchange_order_evidence: true` and at least one non-empty `exchange_order_id`.

If Binance rejects before order acceptance, include the raw error artifact as connectivity evidence and clearly label accepted-order evidence as pending account funding/risk configuration. Do not substitute simulator output or optional read-only Testnet contract tests for accepted-order Testnet E2E artifacts.
