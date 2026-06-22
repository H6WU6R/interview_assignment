# Calais Execution Algorithm

Compact execution algorithm service for the Calais candidate project. The implementation focuses on one exchange family, one symbol, and two algorithms so that execution correctness, edge cases, and evidence remain inspectable.

## Quickstart

```bash
uv sync
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

## What To Review First

- Start with {doc}`user_guide/assignment_requirements` to see how the Calais brief maps to the repository.
- Read {doc}`user_guide/safety_invariants` for the core overfill-prevention model.
- Use {doc}`examples/index` to run deterministic proof cases.
- Use {doc}`api/index` when inspecting public module boundaries.

```{toctree}
:maxdepth: 2
:caption: User Guide

user_guide/index
user_guide/assignment_requirements
user_guide/architecture
user_guide/execution_lifecycle
user_guide/safety_invariants
user_guide/chase
user_guide/twap
user_guide/binance_testnet
user_guide/observability
user_guide/limitations
```

```{toctree}
:maxdepth: 2
:caption: Examples

examples/index
examples/normal_chase
examples/normal_twap
examples/cancel_fill_race
examples/create_timeout_reconciliation
examples/price_outside_range
examples/duplicate_fill_events
examples/cross_zero_position
```

```{toctree}
:maxdepth: 2
:caption: API Reference

api/index
```
