# Review Remediation Plan

## Goal
Make the repository answer the Calais execution-algorithm project brief more completely before final PDF preparation. The work focuses on:

- Expanding `reports/report_draft.md` into a complete PDF-ready source document.
- Correcting code paths called out by review where they affect correctness, observability, or deployability.
- Preserving existing behavior and dirty worktree changes that are already present.
- Verifying with targeted tests plus the full local test suite.

## Tasks

1. Documentation and report draft
   - Rewrite `reports/report_draft.md` so it covers all PDF-required sections: algorithm overview, Chase, TWAP, execution lifecycle, Binance constraints, edge cases, simulator scenarios, Testnet evidence expectations, metrics, limitations, and final submission checklist.
   - Update stale test-count claims in docs after verification.
   - Expand `AI_USAGE.md` so it is specific about how AI was used, what was independently verified, and what was not delegated to AI.

2. Metrics hardening
   - Add explicit `reprices` metrics to execution summaries.
   - Add maker/taker fill accounting where exchange fills expose liquidity flags.
   - Keep metrics backward-compatible by treating absent maker/taker flags as unknown instead of guessing.

3. User-stream reconciliation and evidence logging
   - Parse Binance USD-M `ORDER_TRADE_UPDATE` user-stream events into internal reconciliation results.
   - Add a service/runtime path to apply reconciliation results from private stream events without relying only on REST polling.
   - Improve Testnet runner artifacts with UTC timestamps, monotonic timestamps, sanitized raw event context, and user-stream application records.

4. Mainnet configuration clarity
   - Remove or wire confusing environment toggles so `.env.example`, runtime configuration, and adapter safety gates agree.
   - Keep mainnet mutation disabled by default and require an explicit `ALLOW_MAINNET_TRADING=true` opt-in for live mainnet routing.

5. Tests
   - Add unit tests for maker/taker fill parsing and summary metrics.
   - Add unit tests for user-stream reconciliation parsing and runtime application.
   - Add unit tests for mainnet gating/configuration behavior.
   - Run full local verification: `uv run pytest -q` and the simulator scripts needed to prove Chase, TWAP, cancel-race, and create-timeout paths still work.
