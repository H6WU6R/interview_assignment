# Observability

The project writes structured artifacts so a reviewer can reconstruct request parameters, child orders, fills, timeline events, summaries, and TWAP slice behavior.

## Standard Artifact Bundle

Simulator and Testnet runs write:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`
- `twap_slice_ledger.csv`

Testnet runs also write exchange evidence such as `symbol_rules.json`, `reconciliation_orders.csv`, and `evidence_manifest.json`.

## Sanitization

Logging helpers remove secrets, signatures, authorization headers, listen keys, and raw signed payload aliases before artifacts are written.

## Summary Metrics

Terminal summaries report filled quantity, unfilled quantity, overfill quantity, VWAP, completion rate, slippage, reprices, duplicate events ignored, and reconciliation counters where relevant.

## Implementation And Proof

- Artifact writer: `src/observability/artifacts.py`
- Sanitization: `src/observability/logging.py`
- Metrics: `src/observability/summary.py`
- Tests: `tests/unit/test_observability.py`
