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
- `reports/failure_case_log.md`
- `reports/latex/report.pdf`
- `reports/evidence/simulation/chase/exec_c8dc942476764355`
- `reports/evidence/simulation/twap/exec_61fadac604f4440a`
- `reports/evidence/simulation/cancel-race/exec_6106c8ca78fa4659`
- `reports/evidence/simulation/create-timeout/exec_048f48e8d58c4d2b`
- `reports/evidence/testnet/chase/exec_3168600ee25b4193`
- `reports/evidence/testnet/twap/exec_85bef3985ea3431a`
- `reports/evidence/testnet/chase/exec_85051310eb714ebe`
- `reports/evidence/testnet/twap/exec_30ed2b4cac4346a1`

## Exclude

- `.venv/`
- `.pytest_cache/`
- `.DS_Store`
- local Testnet secret files
- generated temporary artifacts outside the selected evidence bundle

## Evidence Bundle

Committed deterministic simulator artifact directories:

- `reports/evidence/simulation/chase/exec_c8dc942476764355`
- `reports/evidence/simulation/twap/exec_61fadac604f4440a`
- `reports/evidence/simulation/cancel-race/exec_6106c8ca78fa4659`
- `reports/evidence/simulation/create-timeout/exec_048f48e8d58c4d2b`

Accepted Binance Testnet artifacts:

- `reports/evidence/testnet/chase/exec_3168600ee25b4193`: `COMPLETED`, `accepted_exchange_order_evidence: true`, `exchange_order_ids: ["16277695886"]`.
- `reports/evidence/testnet/twap/exec_85bef3985ea3431a`: `COMPLETED`, `accepted_exchange_order_evidence: true`, `exchange_order_ids: ["16277882286", "16277899689"]`.

Rejected Binance Testnet connectivity/error artifacts:

- `reports/evidence/testnet/chase/exec_85051310eb714ebe`: `FAILED`, `TERMINAL_ORDER_REJECTED: BINANCE_-2019:Margin is insufficient.`, `accepted_exchange_order_evidence: false`, `exchange_order_ids: []`.
- `reports/evidence/testnet/twap/exec_30ed2b4cac4346a1`: `FAILED`, `TERMINAL_ORDER_REJECTED: BINANCE_-2019:Margin is insufficient.`, `accepted_exchange_order_evidence: false`, `exchange_order_ids: []`.

To rerun or refresh accepted-order Testnet artifacts, use:

- `uv run python scripts/run_testnet_chase.py --confirm-send-orders ...`
- `uv run python scripts/run_testnet_twap.py --confirm-send-orders ...`

Each Testnet evidence bundle should include raw sanitized execution logs, request parameter snapshots, `symbol_rules.json`, order IDs, client order IDs, private order/trade events when available, `reconciliation_orders.csv`, fill records, `execution_summary.json`, and `evidence_manifest.json` with exchange-order evidence status and `accepted_exchange_order_evidence`.

Artifact privacy statement: accepted raw logs redact account balance/position updates from `ACCOUNT_UPDATE` events, but preserve order/trade updates needed to prove exchange acceptance. If Binance rejects before order acceptance, include the raw error artifact only as rejected connectivity evidence and do not substitute simulator output or optional read-only Testnet contract tests for accepted-order Testnet E2E artifacts.
