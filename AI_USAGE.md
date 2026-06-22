# AI Usage

AI assistance was used during this take-home project for implementation planning, code review, test design, documentation drafting, and report preparation. The assisted areas included the execution engine safety model, deterministic simulator scenarios, Binance USD-M adapter behavior, API/runtime review, observability artifacts, and the final markdown report source.

AI-generated suggestions were treated as drafts, not accepted as authority. The candidate reviewed the design, checked it against the project brief, corrected unsafe or incomplete behavior, and verified the repository with local tests and simulator scripts. The main verification commands are:

```bash
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Important corrections and review focus areas included create-timeout handling, UNKNOWN order exposure, cancel/fill race safety, cumulative fill monotonicity, duplicate event handling, Decimal-only price/quantity paths, TWAP schedule carry-forward, and documentation alignment with the Calais project brief.

No secrets, credentials, request signatures, listen keys, or private Binance account data are included in the repository. Binance Testnet execution requires user-provided environment variables at runtime. The candidate remains responsible for understanding, explaining, modifying, and defending every submitted file.
