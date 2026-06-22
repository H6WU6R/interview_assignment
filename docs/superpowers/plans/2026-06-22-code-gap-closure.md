# Code Gap Closure Plan

## Goal

Close the remaining code-feasible gaps from `reports/external_code_review_summary.md` without generating the final PDF and without claiming live Binance Testnet accepted-order evidence.

## Non-Code Or Externally Blocked Gaps

- Accepted Binance Testnet Chase and TWAP evidence cannot be produced without credentials, account margin, and explicit order-send approval.
- Final PDF generation is intentionally deferred by the user.
- Clear commit history requires a final commit after this pass.

## Tasks

1. Strengthen evidence artifact generation
   - Extend artifact writing to support named extra JSON and CSV files.
   - Make Testnet runner write `symbol_rules.json`, `reconciliation_orders.csv`, and `evidence_manifest.json`.
   - Include enough metadata for external review: symbol rules, rate limits if available, order IDs/client order IDs, final reconciliation counts, private user-stream evidence presence, and exchange-order evidence status.
   - Add unit tests for generic extra artifacts and Testnet runner evidence extras.

2. Add submission packaging support
   - Add a Dockerfile and `.dockerignore` suitable for running the FastAPI simulation API with `uv`.
   - Do not bake secrets into the image.
   - Update README/submission docs to mention optional Docker usage.

3. Update review summary
   - Update `reports/external_code_review_summary.md` and `reports/report_draft.md` so Dockerfile is no longer listed as a gap.
   - Keep live accepted Testnet evidence and final PDF listed as remaining gaps.

4. Verify
   - Run focused tests for artifact writing and Testnet runner.
   - Run full `uv run pytest -q`.
   - Run simulator scripts.
   - Run `git diff --check`.
