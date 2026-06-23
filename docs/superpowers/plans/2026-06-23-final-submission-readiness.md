# Final Submission Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Calais execution algorithm repository submission-ready from an interviewer perspective by fixing two verified correctness risks, committing sanitized accepted Binance Testnet evidence, removing stale "accepted evidence pending" claims, preventing private account data from being written to artifacts, keeping generated build noise out of the repo, and rerunning the full verification path.

**Architecture:** Keep the current execution engine, API, simulator, and Binance adapter architecture intact. This plan touches only targeted runtime reconciliation fallback behavior, directional SELL min-notional prevalidation, artifact hygiene, Testnet runner evidence output, documentation/reporting, one bounded reconciliation hardening point, simulator demo presentation, formatting, and submission packaging.

**Tech Stack:** Python 3.11+, uv, pytest, Ruff, pydantic, httpx/websockets, Decimal arithmetic, LaTeX report source under `reports/latex`, Sphinx documentation under `docs/source`.

---

## Current Interviewer-Risk Ranking

1. Runtime user-event direct reconciliation can suppress bounded REST fallback when `apply_reconciliation_result()` fails or active-execution lookup fails.
2. SELL permanent min-notional prevalidation uses `target_price_upper` even though SELL price-bound validation is directional and only enforces `price >= target_price_lower`.
3. Accepted Testnet evidence is present in the working tree but untracked: `reports/evidence/testnet/chase/exec_3168600ee25b4193` and `reports/evidence/testnet/twap/exec_85bef3985ea3431a`.
4. Those accepted artifacts contain raw Binance `ACCOUNT_UPDATE` balances and position fields in `execution_log.jsonl` and `timeline.csv`.
5. README/report/manifest text still says accepted Binance Testnet evidence is pending.
6. Generated LaTeX files are untracked and not ignored: `reports/latex/report.aux`, `report.blg`, `report.fdb_latexmk`, `report.fls`, and `report.out`.
7. The corrected Binance Testnet WebSocket root is modified but not committed: `wss://demo-fstream.binance.com`.
8. Normal simulator CHASE/TWAP demos produce useful placement artifacts but currently read as nonterminal `RUNNING` demos.
9. `uv run ruff format --check .` reports 39 files would be reformatted; lint still passes.
10. Final archives produced directly from the workspace or plain `git archive HEAD` include internal planning/review material unless `.gitattributes` excludes it.
11. Bounded Binance reconciliation can miss fill attribution if a user-stream reconnect window returns `userTrades` with only `orderId` and the bounded `allOrders` response omits the matching order needed to recover `clientOrderId`.

## Updated File Structure

- Modify `src/api/runtime.py`: fix user-event direct reconciliation return semantics so failed direct application does not suppress bounded REST fallback.
- Create `tests/unit/test_runtime_user_event_fallback.py`: add focused runtime tests for direct event application failure, active lookup failure, and successful direct event application.
- Modify `src/execution/engine.py`: make permanent SELL min-notional prevalidation match directional SELL price-bound semantics.
- Modify `tests/unit/test_engine_lifecycle.py`: add regression coverage for a SELL target that is min-notional-valid only above `target_price_upper`.
- Modify `scripts/testnet_runner.py`: redact persisted `ACCOUNT_UPDATE` user-stream events while preserving order/trade events.
- Create `scripts/sanitize_testnet_evidence.py`: reproducibly sanitize existing accepted Testnet evidence bundles.
- Create `tests/unit/test_testnet_evidence_sanitizer.py`: test the evidence sanitizer without touching real evidence files.
- Modify `README.md`, `reports/submission_manifest.md`, `reports/report_draft.md`, `reports/latex/sections/*.tex`, `docs/source/user_guide/*.md`, and `AI_USAGE.md`: update accepted Testnet evidence, verification claims, and privacy claims.
- Modify `.gitignore`: ignore LaTeX auxiliary files.
- Create `.gitattributes`: exclude internal planning/review material from release archives.
- Modify `src/exchanges/binance_usdm.py`: cache order identity mappings for bounded reconciliation fallback.
- Modify `scripts/run_sim_chase.py`, `scripts/run_sim_twap.py`, and `tests/simulation/test_required_scenarios.py`: make normal simulator demos produce terminal summaries.

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

## Task 1: Fix Runtime User-Event REST Fallback Suppression

**Files:**
- Modify: `src/api/runtime.py`
- Create: `tests/unit/test_runtime_user_event_fallback.py`

- [ ] Write focused failing tests in `tests/unit/test_runtime_user_event_fallback.py`.

```python
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from api.runtime import ExecutionRuntime
from execution.ids import make_client_order_prefix
from execution.models import Environment, ExecutionStatus, Fill, ReconciliationResult


def runtime_record(execution_id: str = "exec_abcdef1234567890") -> Any:
    return SimpleNamespace(
        execution_id=execution_id,
        request=SimpleNamespace(environment=Environment.SIMULATION, symbol="BTCUSDT"),
        status=ExecutionStatus.RUNNING,
    )


def matching_result(execution_id: str = "exec_abcdef1234567890") -> ReconciliationResult:
    prefix = make_client_order_prefix(execution_id)
    return ReconciliationResult(
        orders=[],
        fills=[
            Fill(
                client_order_id=f"{prefix}1",
                cumulative_filled_quantity=Decimal("0.001"),
                last_filled_quantity=Decimal("0.001"),
                last_fill_price=Decimal("50000"),
                trade_id="trade-1",
                event_time_ms=1_000,
                transaction_time_ms=1_000,
            )
        ],
    )


class ParsingAdapter:
    def __init__(self, result: ReconciliationResult) -> None:
        self.result = result

    def reconciliation_from_user_event(self, event: object) -> ReconciliationResult:
        return self.result


class RuntimeService:
    def __init__(
        self,
        record: Any,
        *,
        apply_raises: bool = False,
        active_lookup_raises: bool = False,
    ) -> None:
        self.record = record
        self.apply_raises = apply_raises
        self.active_lookup_raises = active_lookup_raises
        self.apply_calls: list[str] = []
        self.active_calls = 0

    async def get_execution(self, execution_id: str) -> Any:
        return self.record

    async def active_executions(self) -> list[Any]:
        self.active_calls += 1
        if self.active_lookup_raises:
            raise RuntimeError("active lookup failed")
        return [self.record]

    async def apply_reconciliation_result(
        self,
        execution_id: str,
        result: ReconciliationResult,
    ) -> Any:
        self.apply_calls.append(execution_id)
        if self.apply_raises:
            raise RuntimeError("apply failed")
        return self.record


def install_runtime(runtime: ExecutionRuntime, service: RuntimeService, adapter: ParsingAdapter) -> None:
    runtime._services[Environment.SIMULATION] = service
    runtime._adapters[Environment.SIMULATION] = adapter
    runtime._execution_environments[service.record.execution_id] = Environment.SIMULATION


async def test_user_event_apply_failure_falls_back_to_bounded_rest_reconciliation() -> None:
    runtime = ExecutionRuntime()
    record = runtime_record()
    service = RuntimeService(record, apply_raises=True)
    install_runtime(runtime, service, ParsingAdapter(matching_result(record.execution_id)))
    fallback_calls: list[dict[str, int | None]] = []

    async def fallback(environment: Environment, *, start_time_ms: int | None, end_time_ms: int | None) -> None:
        fallback_calls.append({"start_time_ms": start_time_ms, "end_time_ms": end_time_ms})

    runtime._reconcile_active_executions_for_environment = fallback  # type: ignore[method-assign]

    await runtime._reconcile_active_executions_for_user_event(
        Environment.SIMULATION,
        {"event_time_ms": 20_000},
    )

    assert service.apply_calls == [record.execution_id]
    assert fallback_calls == [{"start_time_ms": 0, "end_time_ms": 20_000}]


async def test_active_lookup_failure_falls_back_to_bounded_rest_reconciliation() -> None:
    runtime = ExecutionRuntime()
    record = runtime_record()
    service = RuntimeService(record, active_lookup_raises=True)
    install_runtime(runtime, service, ParsingAdapter(matching_result(record.execution_id)))
    fallback_calls: list[dict[str, int | None]] = []

    async def fallback(environment: Environment, *, start_time_ms: int | None, end_time_ms: int | None) -> None:
        fallback_calls.append({"start_time_ms": start_time_ms, "end_time_ms": end_time_ms})

    runtime._reconcile_active_executions_for_environment = fallback  # type: ignore[method-assign]

    await runtime._reconcile_active_executions_for_user_event(
        Environment.SIMULATION,
        {"event_time_ms": 70_000},
    )

    assert fallback_calls == [{"start_time_ms": 10_000, "end_time_ms": 70_000}]


async def test_successful_user_event_application_avoids_extra_rest_reconciliation() -> None:
    runtime = ExecutionRuntime()
    record = runtime_record()
    service = RuntimeService(record)
    install_runtime(runtime, service, ParsingAdapter(matching_result(record.execution_id)))
    fallback_calls: list[dict[str, int | None]] = []

    async def fallback(environment: Environment, *, start_time_ms: int | None, end_time_ms: int | None) -> None:
        fallback_calls.append({"start_time_ms": start_time_ms, "end_time_ms": end_time_ms})

    runtime._reconcile_active_executions_for_environment = fallback  # type: ignore[method-assign]

    await runtime._reconcile_active_executions_for_user_event(
        Environment.SIMULATION,
        {"event_time_ms": 20_000},
    )

    assert service.apply_calls == [record.execution_id]
    assert fallback_calls == []
```

- [ ] Run the new tests and verify they fail against the current implementation.

```bash
uv run pytest -q tests/unit/test_runtime_user_event_fallback.py
```

Expected output before implementation contains failures for the two fallback tests.

- [ ] Update `_apply_user_event_reconciliation()` in `src/api/runtime.py`.

Replace the current `applied` block with this logic:

```python
        direct_reconciliation_succeeded = False
        fallback_required = active_lookup_failed
        for record in candidate_records:
            prefix = ids.make_client_order_prefix(record.execution_id)
            if not self._reconciliation_result_matches_prefix(result, prefix):
                continue
            try:
                updated = await service.apply_reconciliation_result(record.execution_id, result)
            except Exception as exc:
                self._record_runtime_error(record.execution_id, exc)
                fallback_required = True
                continue
            self._remember_execution(updated)
            self._cancel_background_loop_if_terminal(updated)
            direct_reconciliation_succeeded = True
        return direct_reconciliation_succeeded and not fallback_required
```

- [ ] Run the focused runtime tests again.

```bash
uv run pytest -q tests/unit/test_runtime_user_event_fallback.py
```

Expected output contains:

```text
passed
```

- [ ] Run nearby runtime/API tests.

```bash
uv run pytest -q tests/unit/test_runtime_user_event_fallback.py tests/unit/test_api.py
```

Expected output contains:

```text
passed
```

- [ ] Commit the runtime fallback fix.

```bash
git add src/api/runtime.py tests/unit/test_runtime_user_event_fallback.py
git commit -m "Fix user-event fallback reconciliation"
```

Expected output contains:

```text
Fix user-event fallback reconciliation
```

## Task 2: Fix Directional SELL Min-Notional Prevalidation

**Files:**
- Modify: `src/execution/engine.py`
- Modify: `tests/unit/test_engine_lifecycle.py`

- [ ] Replace the existing SELL min-notional regression in `tests/unit/test_engine_lifecycle.py`.

Replace `test_create_execution_rejects_sell_below_min_notional_after_rounding_upper_to_tick` with:

```python
async def test_create_execution_allows_sell_min_notional_above_upper_when_directional_bound_allows_it() -> None:
    service, simulator, _ = await fresh_service(
        position=Decimal("1"),
        bid=Decimal("101"),
        ask=Decimal("102"),
    )
    simulator.set_symbol_rules(
        SymbolRules(
            symbol=SYMBOL,
            tick_size=Decimal("1"),
            quantity_step=Decimal("1"),
            min_quantity=Decimal("1"),
            min_notional=Decimal("100.50"),
            status="TRADING",
            supported_time_in_force=frozenset({"GTC", "GTX", "IOC"}),
        )
    )

    execution = await service.create_execution(
        execution_request(target_position=Decimal("0"), lower=Decimal("1"), upper=Decimal("100.99"))
    )
    after_run = await service.run_once(execution.execution_id)

    assert execution.status is ExecutionStatus.RUNNING
    assert execution.side is Side.SELL
    assert execution.required_quantity == Decimal("1")
    assert after_run.status is ExecutionStatus.RUNNING
    assert after_run.final_reason is None
    assert len(after_run.child_orders) == 1
    assert after_run.child_orders[0].status is ChildOrderStatus.OPEN
    assert after_run.child_orders[0].price == Decimal("102")
```

- [ ] Run the focused test and verify it fails before implementation.

```bash
uv run pytest -q tests/unit/test_engine_lifecycle.py::test_create_execution_allows_sell_min_notional_above_upper_when_directional_bound_allows_it
```

Expected output before implementation contains:

```text
FAILED
```

- [ ] Update `_highest_legal_price_within_request_band()` in `src/execution/engine.py`.

Replace the SELL branch with:

```python
        if record.side is Side.SELL:
            return None
```

The full method should be:

```python
    def _highest_legal_price_within_request_band(
        self,
        record: ExecutionRecord,
        rules: SymbolRules,
    ) -> Decimal | None:
        if rules.tick_size <= Decimal("0"):
            return None
        if record.side is Side.BUY:
            return round_price(
                record.request.target_price_upper,
                rules.tick_size,
                Side.BUY,
                passive=True,
            )
        if record.side is Side.SELL:
            return None
        return None
```

- [ ] Run min-notional and lifecycle tests.

```bash
uv run pytest -q tests/unit/test_engine_lifecycle.py tests/unit/test_engine_review_regressions.py
```

Expected output contains:

```text
passed
```

- [ ] Commit the SELL prevalidation fix.

```bash
git add src/execution/engine.py tests/unit/test_engine_lifecycle.py
git commit -m "Align sell min-notional precheck with directional bounds"
```

Expected output contains:

```text
Align sell min-notional precheck with directional bounds
```

## Task 3: Redact Private User-Stream Account Data At Artifact Source

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

## Task 4: Sanitize The Existing Accepted Testnet Evidence

- [ ] Add `scripts/sanitize_testnet_evidence.py`.

The script must:

- Accept one or more evidence directory paths as positional arguments.
- Rewrite `execution_log.jsonl` in place.
- Rewrite `timeline.csv` in place.
- Apply the same `ACCOUNT_UPDATE` redaction shape from Task 3 to any JSON object or CSV cell containing a `user_event` object.
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

## Task 5: Update README, Manifest, Report, And Docs To Match The Accepted Evidence

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
- Replace any unconditional `491 passed` claim with environment-specific wording:
  - Non-live command: `uv run pytest -q tests/unit tests/simulation`
  - Current verified baseline before this plan: `489 passed`
  - Credentialed/network-enabled integration command: `uv run pytest -q tests/integration`
  - Full credentialed local review can report `491 passed` only when the two Binance integration tests run successfully.

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
- Verification section must distinguish non-live `489 passed` from credentialed integration results and must not use bare `uv run pytest -q` as the no-live command when a local `.env` may load Binance credentials.

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
- The testing section says the reproducible non-live command is `uv run pytest -q tests/unit tests/simulation` and records the verified pre-plan baseline as `489 passed`.

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

## Task 6: Keep Build Outputs And Internal Working Material Out Of The Release Archive

**Files:**
- Modify: `.gitignore`
- Create: `.gitattributes`

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

- [ ] Create `.gitattributes` so release archives exclude internal planning and review material.

Create `.gitattributes` with:

```gitattributes
docs/superpowers/ export-ignore
reports/external_code_review_summary.md export-ignore
.agents/ export-ignore
.codex/ export-ignore
.DS_Store export-ignore
__MACOSX/ export-ignore
```

- [ ] Verify archive-exclusion attributes.

```bash
git check-attr export-ignore -- docs/superpowers/plans/2026-06-23-final-submission-readiness.md reports/external_code_review_summary.md
```

Expected output:

```text
docs/superpowers/plans/2026-06-23-final-submission-readiness.md: export-ignore: set
reports/external_code_review_summary.md: export-ignore: set
```

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

- [ ] Confirm a dry-run archive omits internal working material.

```bash
git archive --format=tar HEAD | tar -tf - | rg '^docs/superpowers/|^reports/external_code_review_summary\.md$|^\.env$|^__MACOSX/'
```

Expected output: no matches.

- [ ] Commit `.gitignore` and `.gitattributes`.

```bash
git add .gitignore .gitattributes
git commit -m "Exclude build and planning artifacts from release"
```

Expected output contains:

```text
Exclude build and planning artifacts from release
```

## Task 7: Harden Binance Reconciliation For Bounded Windows

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

## Task 8: Make Simulator Demo Artifacts Read Cleanly

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

## Task 9: Format The Source Tree

**Files:**
- Modify: Python files reported by `ruff format --check .`

- [ ] Run the formatter check and capture the file count.

```bash
uv run ruff format --check .
```

Expected output before formatting currently contains:

```text
39 files would be reformatted
```

- [ ] Apply Ruff formatting in one mechanical pass.

```bash
uv run ruff format .
```

Expected output contains a count of reformatted files.

- [ ] Verify format and lint both pass.

```bash
uv run ruff format --check .
uv run ruff check .
```

Expected output:

```text
All checks passed!
All checks passed!
```

- [ ] Run the non-live tests after formatting.

```bash
uv run pytest -q tests/unit tests/simulation
```

Expected output contains:

```text
passed
```

- [ ] Commit formatting separately.

```bash
git add scripts src tests
git commit -m "Apply Ruff formatting"
```

Expected output contains:

```text
Apply Ruff formatting
```

## Task 10: Rebuild Report And Documentation

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

## Task 11: Run Final Verification

- [ ] Run the canonical non-live test suite.

```bash
uv run pytest -q tests/unit tests/simulation
```

Expected output contains:

```text
passed
```

Current pre-plan non-live baseline was:

```text
489 passed
```

The final count should increase after the new runtime fallback, sanitizer, reconciliation, and script tests are added.

- [ ] Run Ruff lint and formatting gates.

```bash
uv run ruff check .
uv run ruff format --check .
```

Expected output:

```text
All checks passed!
All checks passed!
```

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

Expected output when credentials and network access are configured:

```text
passed
```

Expected output when credentials are absent and local `.env` is absent: tests are skipped because `BINANCE_USDM_API_KEY` and `BINANCE_USDM_API_SECRET` are not configured.

If local `.env` contains credentials but the environment has no network access, this command is expected to fail with a Binance connectivity error. Do not report that as a non-live regression; report it separately as "credentialed integration unavailable in restricted network."

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

## Task 12: Final Submission Archive

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

- [ ] Inspect archive contents for excluded internal working material.

```bash
unzip -l /tmp/calais_execution_algorithm_submission.zip | rg 'docs/superpowers|external_code_review_summary|__MACOSX|\.DS_Store'
```

Expected output: no matches.

- [ ] Run the verifier against the committed checkout one final time.

```bash
uv run python scripts/verify_submission.py
```

Expected output contains:

```text
submission_verification=ok
```

## Final Readiness Checklist

- [ ] Runtime user-event application failures fall back to bounded REST reconciliation.
- [ ] Active-execution lookup failures during user-event handling no longer suppress bounded REST reconciliation.
- [ ] Successful user-event application still avoids unnecessary bounded REST reconciliation.
- [ ] SELL min-notional permanent-dust classification matches directional SELL price-bound semantics.
- [ ] Accepted CHASE evidence committed and sanitized.
- [ ] Accepted TWAP evidence committed and sanitized.
- [ ] `ACCOUNT_UPDATE` balances and positions redacted from accepted artifacts.
- [ ] `ORDER_TRADE_UPDATE` evidence preserved in accepted artifacts.
- [ ] README, report, manifest, AI disclosure, and docs no longer say accepted Testnet evidence is pending.
- [ ] Official Binance Testnet WebSocket root is `wss://demo-fstream.binance.com`.
- [ ] LaTeX auxiliary files are ignored.
- [ ] Internal plans and review notes are excluded from `git archive` output via `.gitattributes`.
- [ ] `.env` is untracked and absent from the archive.
- [ ] Normal simulator demos read cleanly as terminal or are explicitly documented as nonterminal probes.
- [ ] Ruff lint passes.
- [ ] Ruff format check passes.
- [ ] Non-live pytest suite passes.
- [ ] Credentialed Binance integration tests are either passed in a network-enabled environment or explicitly reported as network-gated.
- [ ] `scripts/verify_submission.py` returns `submission_verification=ok`.
- [ ] Final zip is produced with `git archive`, not from the raw workspace directory.
