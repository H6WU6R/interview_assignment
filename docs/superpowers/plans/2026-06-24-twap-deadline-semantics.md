# TWAP Deadline Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make TWAP deadline behavior explicit, testable, and reportable: passive TWAP scheduling stops at `target_duration_seconds`, while any later `AGGRESSIVE_WITHIN_RANGE` action is bounded deadline cleanup and is measured separately.

**Architecture:** Keep the current engine shape and avoid a large redesign. Add summary metrics that distinguish the decision/scheduling window from terminal cleanup, count post-deadline submissions by scheduled/passive versus deadline-aggressive type, and update report wording to explain the accepted Testnet TWAP artifact honestly. This is the best fit for the project brief because the brief requires a defined deadline policy and actual-duration reporting; it does not require pretending exchange cleanup completes exactly at the requested duration.

**Tech Stack:** Python 3.12, asyncio, Decimal arithmetic, pytest, Ruff, LaTeX report built with `latexmk`.

---

## Chosen Semantics

Use this contract throughout the implementation and report:

1. `target_duration_seconds` is the TWAP scheduling/decision window measured from `RUNNING` using the engine monotonic clock.
2. The engine must not create new passive scheduled TWAP children after that scheduling window.
3. If `deadline_policy == CANCEL_REMAINDER`, post-deadline behavior is cancellation/reconciliation only.
4. If `deadline_policy == AGGRESSIVE_WITHIN_RANGE`, post-deadline behavior may include one bounded aggressive marketable-limit child after exposure is safe. This is deadline cleanup, not an additional TWAP schedule slice.
5. Execution summaries must expose both:
   - `decision_deadline_elapsed_seconds`: elapsed time credited to the requested scheduling window.
   - `terminal_cleanup_duration_seconds`: additional time spent cancelling, processing stream events, applying fills, or reconciling.
6. Execution summaries must expose post-deadline order counts:
   - `orders_submitted_after_deadline`
   - `scheduled_orders_submitted_after_deadline`
   - `deadline_aggressive_orders_submitted_after_deadline`
7. The accepted Testnet TWAP run should be described as: requested duration `12s`, decision window `12s`, deadline cleanup `5.295300375s`, total lifecycle `17.295300375s`, scheduled post-deadline children `0`, deadline-aggressive post-deadline children `1`.

This avoids overclaiming. The current accepted TWAP artifact did submit the final IOC after the 12-second target, so the report must not say "no orders were submitted after deadline." It should say "no passive scheduled TWAP child was submitted after the scheduling deadline; one bounded aggressive deadline child was submitted during cleanup."

## File Structure

- Modify `src/execution/engine.py`
  - Add summary metrics for decision deadline, cleanup duration, total lifecycle duration, and post-deadline submission counts.
  - Add one private helper to count post-deadline submissions from `child_submitted_monotonic` and `aggressive_child_client_order_ids`.
- Modify `tests/simulation/test_required_scenarios.py`
  - Add regression tests for TWAP `AGGRESSIVE_WITHIN_RANGE` deadline cleanup.
  - Add regression tests for TWAP `CANCEL_REMAINDER` deadline behavior.
- Modify `reports/latex/sections/06-twap-design.tex`
  - Define scheduling window versus bounded cleanup phase.
- Modify `reports/latex/sections/09-results-metrics.tex`
  - Update the accepted Testnet metrics table and add honest wording for the current TWAP artifact.
- Modify `README.md`
  - Add a concise deadline semantics paragraph under the TWAP section or verification/Testnet evidence section.
- Modify `reports/latex/report.pdf`
  - Rebuild after LaTeX source changes.

No new module is needed. The existing `ExecutionRecord` already has enough data:

- `started_monotonic`
- `completed_monotonic`
- `child_submitted_monotonic`
- `aggressive_child_client_order_ids`
- `request.target_duration_seconds`
- `request.deadline_policy`

---

### Task 1: Add Failing TWAP Deadline Metric Tests

**Files:**
- Modify: `tests/simulation/test_required_scenarios.py`
- Test: `tests/simulation/test_required_scenarios.py`

- [ ] **Step 1: Add the TWAP aggressive deadline cleanup regression test**

Insert this test after `test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary` in `tests/simulation/test_required_scenarios.py`:

```python
async def test_twap_aggressive_deadline_reports_cleanup_separately() -> None:
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(
        request(
            algorithm=Algorithm.TWAP,
            target_position=Decimal("0.010"),
            duration=10,
            deadline_policy=DeadlinePolicy.AGGRESSIVE_WITHIN_RANGE,
            parameters=ExecutionParameters(number_of_slices=2),
        )
    )

    clock.advance(5)
    await push_fresh_market(
        clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00")
    )
    scheduled = await service.run_once(execution.execution_id)

    assert len(scheduled.child_orders) == 1
    assert scheduled.child_orders[0].status is ChildOrderStatus.OPEN
    assert scheduled.child_orders[0].client_order_id not in (
        scheduled.aggressive_child_client_order_ids
    )

    clock.advance(6)
    await push_fresh_market(
        clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00")
    )
    deadline_attempt = await service.run_once(execution.execution_id)

    assert len(deadline_attempt.child_orders) == 2
    passive_child, aggressive_child = deadline_attempt.child_orders
    assert passive_child.status is ChildOrderStatus.CANCELLED
    assert aggressive_child.status is ChildOrderStatus.OPEN
    assert aggressive_child.client_order_id in (
        deadline_attempt.aggressive_child_client_order_ids
    )

    await simulator.push_fill(
        aggressive_child.client_order_id,
        aggressive_child.submitted_quantity,
        aggressive_child.price,
    )
    completed = await service.reconcile_execution(execution.execution_id)

    assert completed.status is ExecutionStatus.COMPLETED
    assert completed.summary is not None
    metrics = completed.summary.metrics
    assert metrics["requested_duration_seconds"] == 10
    assert metrics["actual_duration_seconds"] == "11"
    assert metrics["total_lifecycle_duration_seconds"] == "11"
    assert metrics["decision_deadline_elapsed_seconds"] == "10"
    assert metrics["terminal_cleanup_duration_seconds"] == "1"
    assert metrics["orders_submitted_after_deadline"] == 1
    assert metrics["scheduled_orders_submitted_after_deadline"] == 0
    assert metrics["deadline_aggressive_orders_submitted_after_deadline"] == 1
```

- [ ] **Step 2: Add the TWAP cancel-remainder deadline regression test**

Insert this test immediately after the aggressive cleanup test:

```python
async def test_twap_cancel_remainder_deadline_has_no_post_deadline_submissions() -> (
    None
):
    clock, simulator, service = await simulator_service()
    execution = await service.create_execution(
        request(
            algorithm=Algorithm.TWAP,
            target_position=Decimal("0.010"),
            duration=10,
            deadline_policy=DeadlinePolicy.CANCEL_REMAINDER,
            parameters=ExecutionParameters(number_of_slices=2),
        )
    )

    clock.advance(5)
    await push_fresh_market(
        clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00")
    )
    scheduled = await service.run_once(execution.execution_id)

    assert len(scheduled.child_orders) == 1
    assert scheduled.child_orders[0].status is ChildOrderStatus.OPEN

    clock.advance(5)
    await push_fresh_market(
        clock, simulator, bid=Decimal("50000.00"), ask=Decimal("50001.00")
    )
    terminal = await service.run_once(execution.execution_id)

    assert terminal.status is ExecutionStatus.EXPIRED
    assert terminal.summary is not None
    metrics = terminal.summary.metrics
    assert metrics["requested_duration_seconds"] == 10
    assert metrics["actual_duration_seconds"] == "10"
    assert metrics["total_lifecycle_duration_seconds"] == "10"
    assert metrics["decision_deadline_elapsed_seconds"] == "10"
    assert metrics["terminal_cleanup_duration_seconds"] == "0"
    assert metrics["orders_submitted_after_deadline"] == 0
    assert metrics["scheduled_orders_submitted_after_deadline"] == 0
    assert metrics["deadline_aggressive_orders_submitted_after_deadline"] == 0
```

- [ ] **Step 3: Run the new tests and verify they fail for missing metrics**

Run:

```bash
uv run pytest -q \
  tests/simulation/test_required_scenarios.py::test_twap_aggressive_deadline_reports_cleanup_separately \
  tests/simulation/test_required_scenarios.py::test_twap_cancel_remainder_deadline_has_no_post_deadline_submissions
```

Expected result:

```text
FAILED tests/simulation/test_required_scenarios.py::test_twap_aggressive_deadline_reports_cleanup_separately - KeyError: 'total_lifecycle_duration_seconds'
FAILED tests/simulation/test_required_scenarios.py::test_twap_cancel_remainder_deadline_has_no_post_deadline_submissions - KeyError: 'total_lifecycle_duration_seconds'
```

- [ ] **Step 4: Commit the failing tests**

```bash
git add tests/simulation/test_required_scenarios.py
git commit -m "test: specify TWAP deadline cleanup metrics"
```

---

### Task 2: Implement Deadline Duration and Post-Deadline Submission Metrics

**Files:**
- Modify: `src/execution/engine.py`
- Test: `tests/simulation/test_required_scenarios.py`

- [ ] **Step 1: Import `decimal_string` in the engine**

Change the existing import near the top of `src/execution/engine.py` from:

```python
from observability.summary import execution_vwap, summary_metrics
```

to:

```python
from observability.summary import decimal_string, execution_vwap, summary_metrics
```

- [ ] **Step 2: Add summary metric calculations inside `_summary_metrics`**

In `src/execution/engine.py`, inside `_summary_metrics`, after the existing `actual_duration` calculation and before `metrics = summary_metrics(...)`, add:

```python
        requested_duration = Decimal(str(record.request.target_duration_seconds))
        decision_deadline_elapsed = min(actual_duration, requested_duration)
        terminal_cleanup_duration = actual_duration - decision_deadline_elapsed
        post_deadline_submission_counts = self._post_deadline_submission_counts(
            record,
            requested_duration=requested_duration,
        )
```

The surrounding block should become:

```python
        completed_at = (
            record.completed_monotonic
            if record.completed_monotonic is not None
            else self._now_decimal()
        )
        actual_duration = completed_at - record.started_monotonic
        if actual_duration < Decimal("0"):
            actual_duration = Decimal("0")

        requested_duration = Decimal(str(record.request.target_duration_seconds))
        decision_deadline_elapsed = min(actual_duration, requested_duration)
        terminal_cleanup_duration = actual_duration - decision_deadline_elapsed
        post_deadline_submission_counts = self._post_deadline_submission_counts(
            record,
            requested_duration=requested_duration,
        )

        metrics = summary_metrics(
```

- [ ] **Step 3: Add the new metrics to `metrics.update(...)`**

In `src/execution/engine.py`, inside the existing `metrics.update({ ... })` call in `_summary_metrics`, add these entries before `"twap_slice_ledger": self._twap_slice_ledger(record),`:

```python
                "decision_deadline_elapsed_seconds": decimal_string(
                    decision_deadline_elapsed
                ),
                "terminal_cleanup_duration_seconds": decimal_string(
                    terminal_cleanup_duration
                ),
                "total_lifecycle_duration_seconds": decimal_string(actual_duration),
                **post_deadline_submission_counts,
```

The end of the `metrics.update` dictionary should read:

```python
                "rate_limit_backoffs": record.metric_counts.get(
                    "rate_limit_backoffs", 0
                ),
                "rate_limit_backoff_blocks": record.metric_counts.get(
                    "rate_limit_backoff_blocks",
                    0,
                ),
                "decision_deadline_elapsed_seconds": decimal_string(
                    decision_deadline_elapsed
                ),
                "terminal_cleanup_duration_seconds": decimal_string(
                    terminal_cleanup_duration
                ),
                "total_lifecycle_duration_seconds": decimal_string(actual_duration),
                **post_deadline_submission_counts,
                "twap_slice_ledger": self._twap_slice_ledger(record),
```

- [ ] **Step 4: Add `_post_deadline_submission_counts` helper**

In `src/execution/engine.py`, insert this helper immediately before `_summary_vwap_inputs`:

```python
    def _post_deadline_submission_counts(
        self,
        record: ExecutionRecord,
        *,
        requested_duration: Decimal,
    ) -> dict[str, int]:
        deadline_monotonic = record.started_monotonic + requested_duration
        orders_after_deadline = 0
        scheduled_after_deadline = 0
        deadline_aggressive_after_deadline = 0

        for child in record.child_orders:
            submitted_at = record.child_submitted_monotonic.get(child.client_order_id)
            if submitted_at is None or submitted_at <= deadline_monotonic:
                continue

            orders_after_deadline += 1
            if child.client_order_id in record.aggressive_child_client_order_ids:
                deadline_aggressive_after_deadline += 1
            else:
                scheduled_after_deadline += 1

        return {
            "orders_submitted_after_deadline": orders_after_deadline,
            "scheduled_orders_submitted_after_deadline": scheduled_after_deadline,
            "deadline_aggressive_orders_submitted_after_deadline": (
                deadline_aggressive_after_deadline
            ),
        }
```

This helper intentionally counts `submitted_at > deadline`, not `>= deadline`. A child submitted exactly at the monotonic deadline belongs to the deadline instant, not the cleanup interval.

- [ ] **Step 5: Run the deadline tests**

Run:

```bash
uv run pytest -q \
  tests/simulation/test_required_scenarios.py::test_twap_aggressive_deadline_reports_cleanup_separately \
  tests/simulation/test_required_scenarios.py::test_twap_cancel_remainder_deadline_has_no_post_deadline_submissions
```

Expected result:

```text
2 passed
```

- [ ] **Step 6: Run the surrounding simulation tests**

Run:

```bash
uv run pytest -q tests/simulation/test_required_scenarios.py
```

Expected result:

```text
all tests in tests/simulation/test_required_scenarios.py pass
```

- [ ] **Step 7: Commit the implementation**

```bash
git add src/execution/engine.py tests/simulation/test_required_scenarios.py
git commit -m "feat: report TWAP deadline cleanup metrics"
```

---

### Task 3: Regenerate Simulator Evidence Affected by New Metrics

**Files:**
- Modify generated artifacts under:
  - `reports/evidence/simulation/chase/`
  - `reports/evidence/simulation/twap/`
  - `reports/evidence/simulation/cancel-race/`
  - `reports/evidence/simulation/create-timeout/`
- Test: simulator script tests in `tests/simulation/test_required_scenarios.py`

The metrics change affects all terminal summaries. Regenerate normal Chase and normal TWAP at minimum. If the separate terminal-evidence plan has not yet fixed cancel-race and create-timeout, keep their artifacts unchanged in this task to avoid mixing two review items.

- [ ] **Step 1: Regenerate normal Chase simulator evidence**

Run:

```bash
uv run python scripts/run_sim_chase.py --output-dir reports/evidence/simulation/chase
```

Expected output includes:

```text
SIMULATOR DEMO: Chase
status=ExecutionStatus.COMPLETED
artifact_dir=reports/evidence/simulation/chase/exec_<new id>
```

- [ ] **Step 2: Replace the committed normal Chase evidence path in docs**

Open the command output from Step 1 and copy the new `exec_<new id>` path. Replace the old normal Chase path everywhere it appears:

```text
reports/evidence/simulation/chase/exec_c8dc942476764355
```

with the new path in:

```text
README.md
reports/submission_manifest.md
reports/latex/sections/08-testing-evidence.tex
reports/latex/sections/09-results-metrics.tex
reports/latex/sections/b-artifact-checklist.tex
```

Use `rg` to verify there are no stale references:

```bash
rg "exec_c8dc942476764355"
```

Expected result:

```text
no output
```

- [ ] **Step 3: Regenerate normal TWAP simulator evidence**

Run:

```bash
uv run python scripts/run_sim_twap.py --output-dir reports/evidence/simulation/twap
```

Expected output includes:

```text
SIMULATOR DEMO: TWAP
status=ExecutionStatus.COMPLETED
artifact_dir=reports/evidence/simulation/twap/exec_<new id>
```

- [ ] **Step 4: Replace the committed normal TWAP evidence path in docs**

Open the command output from Step 3 and copy the new `exec_<new id>` path. Replace the old normal TWAP path everywhere it appears:

```text
reports/evidence/simulation/twap/exec_61fadac604f4440a
```

with the new path in:

```text
README.md
reports/submission_manifest.md
reports/latex/sections/06-twap-design.tex
reports/latex/sections/08-testing-evidence.tex
reports/latex/sections/09-results-metrics.tex
reports/latex/sections/b-artifact-checklist.tex
```

Use `rg` to verify there are no stale references:

```bash
rg "exec_61fadac604f4440a"
```

Expected result:

```text
no output
```

- [ ] **Step 5: Inspect regenerated summaries for new metrics**

Run:

```bash
python3 - <<'PY'
import json
from pathlib import Path

for path in sorted(Path("reports/evidence/simulation").glob("*/exec_*/execution_summary.json")):
    payload = json.loads(path.read_text())
    metrics = payload.get("metrics", {})
    if not metrics:
        continue
    print(path)
    print("  total_lifecycle_duration_seconds=", metrics.get("total_lifecycle_duration_seconds"))
    print("  decision_deadline_elapsed_seconds=", metrics.get("decision_deadline_elapsed_seconds"))
    print("  terminal_cleanup_duration_seconds=", metrics.get("terminal_cleanup_duration_seconds"))
    print("  orders_submitted_after_deadline=", metrics.get("orders_submitted_after_deadline"))
PY
```

Expected result for terminal regenerated summaries:

```text
total_lifecycle_duration_seconds is present
decision_deadline_elapsed_seconds is present
terminal_cleanup_duration_seconds is present
orders_submitted_after_deadline is present
```

- [ ] **Step 6: Run simulator script tests**

Run:

```bash
uv run pytest -q \
  tests/simulation/test_required_scenarios.py::test_normal_chase_script_writes_required_artifacts \
  tests/simulation/test_required_scenarios.py::test_normal_twap_script_writes_required_artifacts
```

Expected result:

```text
2 passed
```

- [ ] **Step 7: Commit regenerated simulator evidence**

```bash
git add \
  README.md \
  reports/submission_manifest.md \
  reports/latex/sections/06-twap-design.tex \
  reports/latex/sections/08-testing-evidence.tex \
  reports/latex/sections/09-results-metrics.tex \
  reports/latex/sections/b-artifact-checklist.tex \
  reports/evidence/simulation/chase \
  reports/evidence/simulation/twap
git commit -m "chore: refresh simulator summaries with deadline metrics"
```

---

### Task 4: Update TWAP Design Documentation

**Files:**
- Modify: `README.md`
- Modify: `reports/latex/sections/06-twap-design.tex`
- Modify: `reports/latex/sections/09-results-metrics.tex`

- [ ] **Step 1: Update README with the chosen semantics**

In `README.md`, add this paragraph after the existing TWAP description and before `## HTTP API`:

```markdown
### TWAP deadline semantics

`target_duration_seconds` is the TWAP scheduling window. Passive scheduled TWAP children are not created after that window. With `CANCEL_REMAINDER`, the engine cancels and reconciles remaining exposure. With `AGGRESSIVE_WITHIN_RANGE`, the engine may enter a bounded cleanup phase: it first makes exposure safe by cancelling/reconciling passive children, then may submit one marketable limit child inside the configured price range. Summaries report `decision_deadline_elapsed_seconds`, `terminal_cleanup_duration_seconds`, `total_lifecycle_duration_seconds`, and post-deadline submission counts so a reviewer can distinguish schedule behavior from terminal cleanup.
```

- [ ] **Step 2: Update the TWAP design section**

In `reports/latex/sections/06-twap-design.tex`, after the paragraph ending with:

```text
The final slice can absorb legal step-size rounding remainder, but the invariant still prevents exceeding the normalized target.
```

add:

```tex
The scheduling window ends at \code{target\_duration\_seconds}. After that point the engine does not create new passive scheduled TWAP children. Deadline handling is a separate bounded cleanup phase. Under \code{CANCEL\_REMAINDER}, cleanup cancels and reconciles remaining exposure. Under \code{AGGRESSIVE\_WITHIN\_RANGE}, cleanup may first cancel passive exposure and then submit one marketable limit child, but only within the configured price bound and only if the exposure invariant permits it. The summary therefore reports \code{decision\_deadline\_elapsed\_seconds}, \code{terminal\_cleanup\_duration\_seconds}, \code{total\_lifecycle\_duration\_seconds}, and post-deadline order counts.
```

- [ ] **Step 3: Update the accepted Testnet metrics table**

In `reports/latex/sections/09-results-metrics.tex`, extend Table `Accepted Testnet terminal metrics` by adding these rows before `Final status / reason`:

```tex
Exchange order IDs & \code{16277695886} & \code{16277882286}, \code{16277899689} \\
Requested / lifecycle duration & \(12s / 4.672s\) & \(12s / 17.295s\) \\
Decision / cleanup duration & \(4.672s / 0s\) & \(12s / 5.295s\) \\
Post-deadline scheduled / aggressive children & \(0 / 0\) & \(0 / 1\) \\
```

For Chase, `decision` is `4.672s` rather than `12s` because the target filled before the requested deadline. For TWAP, `decision` is capped at `12s` and cleanup is `17.295300375 - 12 = 5.295300375s`.

- [ ] **Step 4: Add the honest TWAP deadline explanation**

In `reports/latex/sections/09-results-metrics.tex`, after the accepted Testnet metrics table, add:

```tex
The accepted TWAP Testnet run used a \(12s\) requested scheduling window and a \(17.295s\) total lifecycle. The difference is deadline cleanup: the first passive child was cancelled, private user-stream events were applied, and a bounded IOC child completed the remaining target. No passive scheduled TWAP child was submitted after the \(12s\) scheduling deadline. One deadline-aggressive child was submitted during cleanup, inside the configured price range, and completed the target with \code{overfill\_quantity=0}.
```

- [ ] **Step 5: Verify wording does not make the false claim**

Run:

```bash
rg -n "No additional child order|no additional child order|orders submitted after deadline.*0|No order.*after" README.md reports/latex/sections
```

Expected result:

```text
no output
```

- [ ] **Step 6: Commit documentation source updates**

```bash
git add README.md reports/latex/sections/06-twap-design.tex reports/latex/sections/09-results-metrics.tex
git commit -m "docs: clarify TWAP deadline cleanup semantics"
```

---

### Task 5: Rebuild and Verify the Final Report

**Files:**
- Modify generated artifact: `reports/latex/report.pdf`
- Test: LaTeX build and submission verifier

- [ ] **Step 1: Rebuild the PDF**

Run:

```bash
cd reports/latex
latexmk -pdf -interaction=nonstopmode report.tex
cd ../..
```

Expected result:

```text
Output written on report.pdf
Latexmk: All targets (report.pdf) are up-to-date
```

- [ ] **Step 2: Run focused tests**

Run:

```bash
uv run pytest -q \
  tests/simulation/test_required_scenarios.py::test_twap_aggressive_deadline_reports_cleanup_separately \
  tests/simulation/test_required_scenarios.py::test_twap_cancel_remainder_deadline_has_no_post_deadline_submissions \
  tests/simulation/test_required_scenarios.py::test_normal_twap_script_writes_required_artifacts
```

Expected result:

```text
3 passed
```

- [ ] **Step 3: Run the non-live suite**

Run:

```bash
uv run pytest -q tests/unit tests/simulation
```

Expected result:

```text
all tests pass
```

- [ ] **Step 4: Run lint**

Run:

```bash
uv run ruff check .
```

Expected result:

```text
All checks passed!
```

- [ ] **Step 5: Run submission verification**

Run:

```bash
uv run python scripts/verify_submission.py
```

Expected result:

```text
submission_verification=ok
```

- [ ] **Step 6: Ensure no unexpected generated files are left dirty**

Run:

```bash
git status --short
```

Expected changed files are limited to:

```text
README.md
src/execution/engine.py
tests/simulation/test_required_scenarios.py
reports/evidence/simulation/chase/...
reports/evidence/simulation/twap/...
reports/latex/report.pdf
reports/latex/sections/06-twap-design.tex
reports/latex/sections/09-results-metrics.tex
reports/submission_manifest.md
reports/latex/sections/08-testing-evidence.tex
reports/latex/sections/b-artifact-checklist.tex
```

- [ ] **Step 7: Commit final report rebuild**

```bash
git add reports/latex/report.pdf
git commit -m "docs: rebuild report for TWAP deadline semantics"
```

---

## Self-Review

**Spec coverage**

- Project brief says `target_duration` starts when job enters `RUNNING` and duration uses monotonic time: Task 2 reports decision, cleanup, and lifecycle durations from `started_monotonic` and `completed_monotonic`.
- Project brief says deadline policy must be explicit: Task 4 defines `CANCEL_REMAINDER` and `AGGRESSIVE_WITHIN_RANGE` semantics in README and report.
- Project brief says TWAP must use absolute schedule and carry-forward: Task 4 preserves the existing TWAP design and clarifies that deadline cleanup is not another schedule slice.
- Project brief says final execution must report requested duration, actual duration, schedule deficit, open/filled/cancelled/unfilled, and final status/reason: Task 2 adds duration decomposition and order timing counts; existing summaries already report quantity and status metrics.
- External review concern says accepted TWAP `12s` requested versus `17.295s` lifecycle needs explanation: Task 4 adds the exact explanation and avoids the false "no orders after deadline" claim.

**Placeholder scan**

- Mechanical scan passed for the banned placeholder phrases from the writing-plans skill, and every test step includes concrete code.

**Type consistency**

- Tests use existing imports already present in `tests/simulation/test_required_scenarios.py`: `Decimal`, `Algorithm`, `ChildOrderStatus`, `DeadlinePolicy`, `ExecutionParameters`, and `ExecutionStatus`.
- Engine code uses existing `ExecutionRecord`, `ChildOrder`, `Decimal`, `child_submitted_monotonic`, and `aggressive_child_client_order_ids`.
- New metric keys are identical in tests, engine implementation, README, and report text:
  - `decision_deadline_elapsed_seconds`
  - `terminal_cleanup_duration_seconds`
  - `total_lifecycle_duration_seconds`
  - `orders_submitted_after_deadline`
  - `scheduled_orders_submitted_after_deadline`
  - `deadline_aggressive_orders_submitted_after_deadline`

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-24-twap-deadline-semantics.md`. Two execution options:

**1. Subagent-Driven (recommended)** - Dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
