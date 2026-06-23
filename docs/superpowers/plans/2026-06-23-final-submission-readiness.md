# Final Submission Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Calais execution algorithm repository submission-ready from an interviewer perspective by committing sanitized accepted Binance Testnet evidence, removing stale "accepted evidence pending" claims, preventing private account data from being written to artifacts, keeping generated build noise out of the repo, and rerunning the full verification path.

**Architecture:** Keep the current execution engine, API, simulator, and Binance adapter architecture intact. This plan touches only artifact hygiene, Testnet runner evidence output, documentation/reporting, one bounded reconciliation hardening point, simulator demo presentation, and submission packaging.

**Tech Stack:** Python 3.11+, uv, pytest, Ruff, pydantic, httpx/websockets, Decimal arithmetic, LaTeX report source under `reports/latex`, Sphinx documentation under `docs/source`.

---

## Current Interviewer-Risk Ranking

1. Accepted Testnet evidence is present in the working tree but untracked: `reports/evidence/testnet/chase/exec_3168600ee25b4193` and `reports/evidence/testnet/twap/exec_85bef3985ea3431a`.
2. Those accepted artifacts contain raw Binance `ACCOUNT_UPDATE` balances and position fields in `execution_log.jsonl` and `timeline.csv`.
3. README/report/manifest text still says accepted Binance Testnet evidence is pending.
4. Generated LaTeX files are untracked and not ignored: `reports/latex/report.aux`, `report.blg`, `report.fdb_latexmk`, `report.fls`, and `report.out`.
5. The corrected Binance Testnet WebSocket root is modified but not committed: `wss://demo-fstream.binance.com`.
6. Normal simulator CHASE/TWAP demos produce useful placement artifacts but currently read as nonterminal `RUNNING` demos.
7. Bounded Binance reconciliation can miss fill attribution if a user-stream reconnect window returns `userTrades` with only `orderId` and the bounded `allOrders` response omits the matching order needed to recover `clientOrderId`.

## Task 0: Create A Clean Working Branch And Baseline

- [ ] Start from the repository root.

```bash
pwd
```

Expected output:

```text
/Users/wuhaoran/Downloads/calais_execution_algorithm
```

- [ ] Create or switch to a final-submission branch.

```bash
git switch codex/final-submission-readiness || git switch -c codex/final-submission-readiness
```

Expected output: either switches to `codex/final-submission-readiness` or creates it.

- [ ] Capture current status before editing.

```bash
git status --short
```

Expected current high-level status before this plan is implemented:

```text
 M README.md
 M src/exchanges/binance_usdm.py
 M tests/unit/test_binance_order_mutations.py
?? reports/evidence/testnet/chase/exec_3168600ee25b4193/
?? reports/evidence/testnet/twap/exec_85bef3985ea3431a/
?? reports/latex/report.aux
?? reports/latex/report.blg
?? reports/latex/report.fdb_latexmk
?? reports/latex/report.fls
?? reports/latex/report.out
```

- [ ] Run the focused evidence tests before changing them.

```bash
uv run pytest -q tests/unit/test_testnet_runner_evidence.py
```

Expected output contains:

```text
passed
```

## Task 1: Redact Private User-Stream Account Data At Artifact Source

- [ ] Add a small artifact-specific user-event sanitizer in `scripts/testnet_runner.py`.

Place this near the existing `_jsonable` and user-stream helper functions:

```python
def _artifact_user_event(event: Any) -> Any:
    user_event = _jsonable(event)
    if not isinstance(user_event, dict):
        return user_event
    if user_event.get("event_type") != "ACCOUNT_UPDATE":
        return user_event

    redacted = {
        "event_type": "ACCOUNT_UPDATE",
        "raw": {
            "e": "ACCOUNT_UPDATE",
            "redacted": True,
        },
    }
    if "event_time_ms" in user_event:
        redacted["event_time_ms"] = user_event["event_time_ms"]
    if "transaction_time_ms" in user_event:
        redacted["transaction_time_ms"] = user_event["transaction_time_ms"]
    return redacted
```

- [ ] Replace the raw artifact write inside `_start_user_stream_once`.

Change:

```python
events.append(_runtime_event(adapter, "user_stream_event", user_event=_jsonable(event)))
```

to:

```python
events.append(_runtime_event(adapter, "user_stream_event", user_event=_artifact_user_event(event)))
```

- [ ] Keep the in-memory reconciliation logic using the original event object. The sanitizer must only affect persisted artifact payloads, not `reconciliation_from_user_event(event)`.

- [ ] Add regression coverage in `tests/unit/test_testnet_runner_evidence.py`.

Add this test:

```python
def test_artifact_user_event_redacts_account_update_balance_and_position_fields() -> None:
    module = load_runner_module()
    event = {
        "event_type": "ACCOUNT_UPDATE",
        "event_time_ms": 1782220792498,
        "transaction_time_ms": 1782220792498,
        "raw": {
            "e": "ACCOUNT_UPDATE",
            "a": {
                "B": [{"a": "USDT", "wb": "4999.9875575", "cw": "4999.9875575"}],
                "P": [
                    {
                        "s": "BTCUSDT",
                        "pa": "0.001",
                        "ep": "62212.5",
                        "bep": "62224.9425",
                        "up": "-0.10743784",
                    }
                ],
            },
        },
    }

    sanitized = module._artifact_user_event(event)
    rendered = str(sanitized)

    assert sanitized == {
        "event_type": "ACCOUNT_UPDATE",
        "raw": {"e": "ACCOUNT_UPDATE", "redacted": True},
        "event_time_ms": 1782220792498,
        "transaction_time_ms": 1782220792498,
    }
    assert "4999.9875575" not in rendered
    assert "62212.5" not in rendered
    assert "62224.9425" not in rendered
    assert "-0.10743784" not in rendered
```

Add this test immediately after it:

```python
def test_artifact_user_event_preserves_order_trade_update_client_order_id() -> None:
    module = load_runner_module()
    event = {
        "event_type": "ORDER_TRADE_UPDATE",
        "event_time_ms": 1782220792498,
        "transaction_time_ms": 1782220792498,
        "raw": {
            "e": "ORDER_TRADE_UPDATE",
            "o": {
                "c": "ce_abcdef123456_1",
                "i": 16277695886,
                "X": "FILLED",
            },
        },
    }

    assert module._artifact_user_event(event) == event
```

- [ ] Run the focused test.

```bash
uv run pytest -q tests/unit/test_testnet_runner_evidence.py
```

Expected output contains:

```text
passed
```

- [ ] Commit this source-level redaction fix.

```bash
git add scripts/testnet_runner.py tests/unit/test_testnet_runner_evidence.py
git commit -m "Redact Testnet account updates in artifacts"
```

Expected output contains:

```text
Redact Testnet account updates in artifacts
```

## Task 2: Sanitize The Existing Accepted Testnet Evidence

- [ ] Add `scripts/sanitize_testnet_evidence.py`.

The script must:

- Accept one or more evidence directory paths as positional arguments.
- Rewrite `execution_log.jsonl` in place.
- Rewrite `timeline.csv` in place.
- Apply the same `ACCOUNT_UPDATE` redaction shape from Task 1 to any JSON object or CSV cell containing a `user_event` object.
- Preserve `ORDER_TRADE_UPDATE` raw payloads so client order IDs, exchange order IDs, and private order/trade lifecycle evidence remain available.
- Fail with exit code 1 if any rewritten file still contains private account/position keys.

Use these private-key checks inside the script:

```python
PRIVATE_ACCOUNT_KEYS = {"cw", "wb", "bep", "ep", "iw", "ma", "mt", "pa", "ps", "up"}
```

The post-write scan must check exact JSON keys, not arbitrary substrings in normal text.

- [ ] Add `tests/unit/test_testnet_evidence_sanitizer.py`.

Test cases:

- `test_sanitizer_redacts_account_update_in_jsonl`
- `test_sanitizer_redacts_account_update_in_timeline_csv`
- `test_sanitizer_preserves_order_trade_update_payload`
- `test_sanitizer_rejects_remaining_private_account_keys`

Each test should use `tmp_path` and call script functions directly. Do not mutate the real evidence directories in unit tests.

- [ ] Run the sanitizer tests.

```bash
uv run pytest -q tests/unit/test_testnet_evidence_sanitizer.py tests/unit/test_testnet_runner_evidence.py
```

Expected output contains:

```text
passed
```

- [ ] Sanitize the accepted evidence directories.

```bash
uv run python scripts/sanitize_testnet_evidence.py reports/evidence/testnet/chase/exec_3168600ee25b4193 reports/evidence/testnet/twap/exec_85bef3985ea3431a
```

Expected output:

```text
sanitized reports/evidence/testnet/chase/exec_3168600ee25b4193
sanitized reports/evidence/testnet/twap/exec_85bef3985ea3431a
```

- [ ] Verify that private account/position fields are gone from the accepted artifacts.

```bash
rg -n '"(cw|wb|bep|ep|iw|ma|mt|pa|ps|up)"|4999\.9875575|4999\.88820886|62212\.5|62224\.9425|62139\.178228|62116\.82' reports/evidence/testnet/chase/exec_3168600ee25b4193 reports/evidence/testnet/twap/exec_85bef3985ea3431a
```

Expected output: no matches.

- [ ] Verify that accepted exchange evidence is still present.

```bash
rg -n '"accepted_exchange_order_evidence": true|"exchange_order_ids":|16277695886|16277882286|16277899689|ORDER_TRADE_UPDATE' reports/evidence/testnet/chase/exec_3168600ee25b4193 reports/evidence/testnet/twap/exec_85bef3985ea3431a
```

Expected output contains matches in both accepted evidence directories.

- [ ] Commit the sanitizer and sanitized accepted evidence.

```bash
git add scripts/sanitize_testnet_evidence.py tests/unit/test_testnet_evidence_sanitizer.py reports/evidence/testnet/chase/exec_3168600ee25b4193 reports/evidence/testnet/twap/exec_85bef3985ea3431a
git commit -m "Add sanitized accepted Testnet evidence"
```

Expected output contains:

```text
Add sanitized accepted Testnet evidence
```

## Task 3: Update README, Manifest, Report, And Docs To Match The Accepted Evidence

- [ ] Update `README.md`.

Required changes:

- Keep the corrected Testnet endpoints:
  - REST: `https://demo-fapi.binance.com`
  - WebSocket root: `wss://demo-fstream.binance.com`
- Replace the pending-evidence paragraph with an accepted-evidence paragraph.
- List the accepted artifact directories:
  - `reports/evidence/testnet/chase/exec_3168600ee25b4193`
  - `reports/evidence/testnet/twap/exec_85bef3985ea3431a`
- State that accepted artifacts are sanitized and keep order/trade evidence while redacting `ACCOUNT_UPDATE` balances and positions.
- Keep the older `exec_85051310eb714ebe` and `exec_30ed2b4cac4346a1` runs only as rejected connectivity/error artifacts.

- [ ] Update `reports/submission_manifest.md`.

Required structure:

- Accepted Binance Testnet artifacts section with the two accepted directories.
- Rejected connectivity artifacts section with the two margin-rejected directories.
- Artifact privacy statement saying accepted raw logs redact account balance/position updates but preserve order/trade updates needed to prove exchange acceptance.

- [ ] Update `reports/report_draft.md`.

Required replacements:

- Executive summary must no longer say accepted Chase/TWAP evidence is pending.
- Requirement table must mark accepted Testnet evidence as covered.
- Testing evidence section must name the two accepted directories and their exchange order IDs.
- Limitations must move Testnet account funding from "current submission risk" to "external operational dependency for reruns".

- [ ] Update LaTeX report sections:

```text
reports/latex/sections/00-abstract.tex
reports/latex/sections/01-introduction.tex
reports/latex/sections/08-testing-evidence.tex
reports/latex/sections/11-limitations-future-work.tex
reports/latex/sections/b-artifact-checklist.tex
```

Required replacements:

- `Accepted Binance Testnet Chase/TWAP evidence` row changes from `\gap` to `\yes`.
- `08-testing-evidence.tex` names:
  - `reports/evidence/testnet/chase/exec_3168600ee25b4193`
  - `reports/evidence/testnet/twap/exec_85bef3985ea3431a`
  - `16277695886`
  - `16277882286`
  - `16277899689`
- The abstract says accepted Testnet artifacts are included and sanitized.
- The limitations section says Testnet reruns depend on account configuration, not that accepted evidence is missing.

- [ ] Update source documentation:

```text
docs/source/user_guide/binance_testnet.md
docs/source/user_guide/limitations.md
docs/source/user_guide/assignment_requirements.md
```

Required replacements:

- Remove pending accepted-evidence claims.
- Link or name both accepted artifact directories.
- Keep credential-gating and mainnet-hard-disable warnings.

- [ ] Update `AI_USAGE.md`.

Required replacement:

- Keep the claim that no secrets or private account data are included, and add that Testnet `ACCOUNT_UPDATE` balance/position fields are redacted from accepted evidence artifacts.

- [ ] Update `reports/external_code_review_summary.md`.

Add a top "Current status after final evidence cleanup" note stating that the previous accepted-evidence gap is superseded by:

```text
reports/evidence/testnet/chase/exec_3168600ee25b4193
reports/evidence/testnet/twap/exec_85bef3985ea3431a
```

Then update the gap rows under Testnet evidence so a reviewer does not see stale "missing accepted evidence" language.

- [ ] Search for stale claims.

```bash
rg -n "accepted.*pending|pending.*accepted|accepted-order.*pending|accepted Testnet order evidence remains pending|fstream\.binancefuture\.com|wss://fstream" README.md reports docs/source AI_USAGE.md
```

Expected output: no matches.

- [ ] Commit documentation/report source updates.

```bash
git add README.md reports/submission_manifest.md reports/report_draft.md reports/latex/sections docs/source AI_USAGE.md reports/external_code_review_summary.md src/exchanges/binance_usdm.py tests/unit/test_binance_order_mutations.py
git commit -m "Document accepted Testnet evidence"
```

Expected output contains:

```text
Document accepted Testnet evidence
```

## Task 4: Keep LaTeX Build Outputs Out Of Git

- [ ] Add LaTeX generated-file patterns to `.gitignore`.

Append near the docs/build ignores:

```gitignore
# LaTeX build artifacts
reports/latex/*.aux
reports/latex/*.blg
reports/latex/*.fdb_latexmk
reports/latex/*.fls
reports/latex/*.log
reports/latex/*.out
reports/latex/*.synctex.gz
```

Do not ignore `reports/latex/report.pdf`; it is part of the final submission.

- [ ] Remove generated untracked LaTeX files from the working tree.

```bash
rm -f reports/latex/report.aux reports/latex/report.blg reports/latex/report.fdb_latexmk reports/latex/report.fls reports/latex/report.out
```

Expected output: no output.

- [ ] Confirm they are ignored if regenerated.

```bash
git status --short --ignored reports/latex/report.aux reports/latex/report.blg reports/latex/report.fdb_latexmk reports/latex/report.fls reports/latex/report.out
```

Expected output: no staged or untracked entries for those files.

- [ ] Confirm `.env` is not tracked.

```bash
git ls-files .env
```

Expected output: no output.

- [ ] Commit `.gitignore`.

```bash
git add .gitignore
git commit -m "Ignore LaTeX build artifacts"
```

Expected output contains:

```text
Ignore LaTeX build artifacts
```

## Task 5: Harden Binance Reconciliation For Bounded Windows

- [ ] Add an in-memory order identity cache in `src/exchanges/binance_usdm.py`.

In `BinanceUsdmAdapter.__init__`, add:

```python
self._client_order_id_by_order_id: dict[str, str] = {}
```

- [ ] Add a private helper in `BinanceUsdmAdapter`.

```python
def _remember_order_identity(self, order: Order) -> None:
    if order.exchange_order_id is None:
        return
    self._client_order_id_by_order_id[order.exchange_order_id] = order.client_order_id
```

- [ ] Call `_remember_order_identity(order)` after every successful parse of an exchange order in:

```text
submit_limit_order
cancel_order
get_order
get_open_orders
reconcile_orders_and_fills
```

- [ ] In `reconcile_orders_and_fills`, when a user trade has `orderId` but no `clientOrderId` and the bounded `allOrders` response did not include that order, recover the client ID from `_client_order_id_by_order_id`.

Use this matching rule:

```python
if client_id is None and order_id is not None:
    client_id = self._client_order_id_by_order_id.get(order_id)
if client_id is None or not client_id.startswith(client_order_prefix):
    continue
```

- [ ] Add unit coverage in `tests/unit/test_binance_order_mutations.py`.

Required tests:

- `test_reconcile_uses_cached_order_identity_when_bounded_all_orders_omits_trade_order`
- `test_reconcile_ignores_cached_order_identity_for_other_execution_prefix`

The first test must:

- Parse or cache an order with `exchange_order_id="16277695886"` and `client_order_id="ce_abcdef123456_1"`.
- Return a bounded reconciliation response where `allOrders` is empty.
- Return a `userTrades` row with `orderId=16277695886` and no `clientOrderId`.
- Assert the resulting fill has `client_order_id == "ce_abcdef123456_1"`.

The second test must:

- Cache `orderId=16277695886` to `client_order_id="ce_otherexec_1"`.
- Reconcile with prefix `ce_abcdef123456_`.
- Assert no fill is returned.

- [ ] Run the focused adapter tests.

```bash
uv run pytest -q tests/unit/test_binance_order_mutations.py
```

Expected output contains:

```text
passed
```

- [ ] Commit the reconciliation hardening.

```bash
git add src/exchanges/binance_usdm.py tests/unit/test_binance_order_mutations.py
git commit -m "Harden bounded Binance fill reconciliation"
```

Expected output contains:

```text
Harden bounded Binance fill reconciliation
```

## Task 6: Make Simulator Demo Artifacts Read Cleanly

- [ ] Update `scripts/run_sim_chase.py` so the normal CHASE demo terminalizes.

After the first child is submitted:

```python
child = execution.child_orders[-1]
fill = await simulator.push_fill(child.client_order_id, child.remaining_quantity, child.price)
events.append(
    log_event(
        clock,
        execution,
        "simulator_fill",
        child=child,
        extra={"trade_id": fill.trade_id},
    )
)
execution = await service.reconcile_execution(execution.execution_id)
events.append(log_event(clock, execution, "filled_reconciled", child=execution.child_orders[-1]))
execution = await service.run_once(execution.execution_id)
```

At the end of the script, printed `status` should be `ExecutionStatus.COMPLETED` and `summary_snapshot(execution)["metrics"]["filled_quantity"]` should be `"0.01"`.

- [ ] Update `scripts/run_sim_twap.py` so the TWAP demo demonstrates multiple scheduled slices and terminal completion.

Use `ExecutionParameters(number_of_slices=5)` in the TWAP request. Advance the manual clock in 20-second increments, run one tick, fill the newest open child, reconcile, and continue until the execution is terminal.

Expected final properties:

```text
status=ExecutionStatus.COMPLETED
filled_quantity=0.01
child_order_count >= 2
```

- [ ] Add or update script-level evidence tests in `tests/simulation/test_required_scenarios.py`.

Assertions:

- `scripts/run_sim_chase.py` writes an `execution_summary.json` with `final_status == "COMPLETED"`.
- `scripts/run_sim_twap.py` writes an `execution_summary.json` with `final_status == "COMPLETED"`.
- TWAP artifact `twap_slice_ledger.csv` is non-empty.

- [ ] Regenerate committed simulator evidence if the repository keeps simulator artifacts under `reports/evidence/simulation`.

```bash
uv run python scripts/run_sim_chase.py --output-dir reports/evidence/simulation/chase
uv run python scripts/run_sim_twap.py --output-dir reports/evidence/simulation/twap
uv run python scripts/run_sim_cancel_race.py --output-dir reports/evidence/simulation/cancel-race
uv run python scripts/run_sim_create_timeout.py --output-dir reports/evidence/simulation/create-timeout
```

Expected output from the first two scripts contains:

```text
status=ExecutionStatus.COMPLETED
```

- [ ] Run focused scenario tests.

```bash
uv run pytest -q tests/simulation/test_required_scenarios.py
```

Expected output contains:

```text
passed
```

- [ ] Commit simulator demo cleanup and regenerated artifacts.

```bash
git add scripts/run_sim_chase.py scripts/run_sim_twap.py tests/simulation/test_required_scenarios.py reports/evidence/simulation
git commit -m "Refresh terminal simulator demos"
```

Expected output contains:

```text
Refresh terminal simulator demos
```

## Task 7: Rebuild Report And Documentation

- [ ] Rebuild the LaTeX PDF.

Run from `reports/latex`:

```bash
latexmk -pdf -interaction=nonstopmode report.tex
```

Expected output contains:

```text
Output written on report.pdf
```

- [ ] Rebuild Sphinx docs.

Run from the repository root:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Expected output contains:

```text
build succeeded
```

- [ ] Verify generated LaTeX auxiliary files remain ignored and the final PDF is tracked or modified.

```bash
git status --short reports/latex docs/_build
```

Expected output:

- `reports/latex/report.pdf` may be modified.
- No `reports/latex/*.aux`, `*.blg`, `*.fdb_latexmk`, `*.fls`, or `*.out` files appear.
- No `docs/_build` files appear because `docs/_build/` is ignored.

- [ ] Commit rebuilt final report if it changed.

```bash
git add reports/latex/report.pdf
git commit -m "Rebuild final report"
```

Expected output contains either:

```text
Rebuild final report
```

or:

```text
nothing to commit
```

## Task 8: Run Final Verification

- [ ] Run the full test suite.

```bash
uv run pytest -q
```

Expected output contains:

```text
passed
```

Current pre-plan baseline was:

```text
491 passed
```

The final count should increase if new sanitizer/reconciliation/script tests are added.

- [ ] Run the submission verifier.

```bash
uv run python scripts/verify_submission.py
```

Expected output contains:

```text
submission_verification=ok
```

- [ ] If Binance Testnet credentials are configured, run read-only integration tests.

```bash
uv run pytest -q tests/integration
```

Expected output when credentials are configured:

```text
passed
```

Expected output when credentials are absent: tests are skipped because `BINANCE_USDM_API_KEY` and `BINANCE_USDM_API_SECRET` are not configured.

- [ ] Verify no stale pending claims remain.

```bash
rg -n "accepted.*pending|pending.*accepted|accepted-order.*pending|accepted Testnet order evidence remains pending|fstream\.binancefuture\.com|wss://fstream" README.md reports docs/source AI_USAGE.md
```

Expected output: no matches.

- [ ] Verify private account/position fields remain absent from accepted Testnet evidence.

```bash
rg -n '"(cw|wb|bep|ep|iw|ma|mt|pa|ps|up)"|4999\.9875575|4999\.88820886|62212\.5|62224\.9425|62139\.178228|62116\.82' reports/evidence/testnet/chase/exec_3168600ee25b4193 reports/evidence/testnet/twap/exec_85bef3985ea3431a
```

Expected output: no matches.

- [ ] Verify accepted evidence is committed.

```bash
git ls-files reports/evidence/testnet/chase/exec_3168600ee25b4193 reports/evidence/testnet/twap/exec_85bef3985ea3431a
```

Expected output lists files from both accepted evidence directories, including:

```text
reports/evidence/testnet/chase/exec_3168600ee25b4193/evidence_manifest.json
reports/evidence/testnet/twap/exec_85bef3985ea3431a/evidence_manifest.json
```

- [ ] Verify `.env` is not staged or tracked.

```bash
git ls-files .env
git status --short .env
```

Expected output: no output.

## Task 9: Final Submission Archive

- [ ] Ensure the branch is clean after all commits.

```bash
git status --short
```

Expected output: no output.

- [ ] Create the submission archive from tracked files only. Do not zip the workspace directory directly because it contains ignored local files such as `.env`.

```bash
git archive --format=zip --output=/tmp/calais_execution_algorithm_submission.zip HEAD
```

Expected output: no output.

- [ ] Inspect archive contents for secrets and required artifacts.

```bash
unzip -l /tmp/calais_execution_algorithm_submission.zip | rg '(^|/)(\.env|report\.pdf|AI_USAGE\.md|evidence_manifest\.json|exec_3168600ee25b4193|exec_85bef3985ea3431a)'
```

Expected output:

- Contains `reports/latex/report.pdf`.
- Contains `AI_USAGE.md`.
- Contains both accepted evidence directories.
- Contains no `.env`.

- [ ] Run the verifier against the committed checkout one final time.

```bash
uv run python scripts/verify_submission.py
```

Expected output contains:

```text
submission_verification=ok
```

## Final Readiness Checklist

- [ ] Accepted CHASE evidence committed and sanitized.
- [ ] Accepted TWAP evidence committed and sanitized.
- [ ] `ACCOUNT_UPDATE` balances and positions redacted from accepted artifacts.
- [ ] `ORDER_TRADE_UPDATE` evidence preserved in accepted artifacts.
- [ ] README, report, manifest, AI disclosure, and docs no longer say accepted Testnet evidence is pending.
- [ ] Official Binance Testnet WebSocket root is `wss://demo-fstream.binance.com`.
- [ ] LaTeX auxiliary files are ignored.
- [ ] `.env` is untracked and absent from the archive.
- [ ] Normal simulator demos read cleanly as terminal or are explicitly documented as nonterminal probes.
- [ ] Full pytest suite passes.
- [ ] `scripts/verify_submission.py` returns `submission_verification=ok`.
- [ ] Final zip is produced with `git archive`, not from the raw workspace directory.
