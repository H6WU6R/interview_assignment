# Architecture

The system is intentionally small and correctness-first. Runtime code supervises the environment, the engine owns trading state, algorithm modules compute narrow decisions, exchange adapters isolate external behavior, and observability code writes evidence.

## Layers

| Layer | Responsibility | Main Files |
| --- | --- | --- |
| API/runtime | HTTP app, runtime supervisor, background loops, stream supervision, graceful shutdown. | `src/api/app.py`, `src/api/runtime.py`, `src/api/schemas.py` |
| Service/engine | Execution records, child orders, exposure accounting, reconciliation, state transitions, terminal summaries. | `src/execution/engine.py`, `src/execution/service.py`, `src/execution/models.py`, `src/execution/state_machine.py` |
| Algorithms | Pure Chase and TWAP calculations. | `src/algorithms/chase.py`, `src/algorithms/twap.py` |
| Exchange adapters | Common adapter contract, deterministic simulator, Binance USD-M implementation. | `src/exchanges/base.py`, `src/exchanges/simulator.py`, `src/exchanges/binance_usdm.py` |
| Risk | Decimal rounding, price/quantity validation, exposure invariant checks. | `src/risk/decimal_math.py`, `src/risk/validation.py` |
| Observability | Sanitized logs, summaries, CSV/JSON/JSONL artifacts. | `src/observability/artifacts.py`, `src/observability/logging.py`, `src/observability/summary.py` |

## Boundary Rule

`ExecutionEngine` is the trading-correctness boundary. Runtime code may supervise streams and retries, but it does not own exposure math. Algorithm helpers compute decisions, but the engine decides whether a child order is safe to submit.

## Data Flow

1. API request or script creates an `ExecutionRequest`.
2. `ExecutionService` delegates to `ExecutionEngine`.
3. The engine reads position, symbol rules, and market data through `ExchangeAdapter`.
4. Chase or TWAP helpers compute desired price or scheduled quantity.
5. Risk checks and exposure accounting gate every child order submit.
6. Exchange responses, stream events, and reconciliation update child and parent state.
7. Observability writers emit the reviewable artifact bundle.
