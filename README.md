# Calais Execution Algorithm

Compact Binance USD-M execution service for the Calais take-home. The project is intentionally small, but the correctness path is explicit: API requests enter an `ExecutionService`, all order state is owned by `ExecutionEngine`, algorithm modules only compute narrow decisions, and exchange access is behind an `ExchangeAdapter` contract with both deterministic simulator and Binance USD-M implementations.

## Quickstart

```bash
uv sync
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

To run the simulation API locally:

```bash
uv run uvicorn api.app:create_app --factory --reload
```

The API factory wires a deterministic simulator by default. It does not send Binance orders.

## Project Layout

- `src/api/`: FastAPI app and Pydantic request/response schemas.
- `src/execution/`: service facade, execution engine, state machine, IDs, events, and domain models.
- `src/algorithms/`: narrow Chase and TWAP decision helpers.
- `src/exchanges/`: exchange contract, deterministic simulator, and Binance USD-M adapter.
- `src/risk/`: Decimal rounding and safety validation.
- `src/observability/`: JSONL/JSON/CSV artifact writers and sanitization helpers.
- `scripts/`: simulator demos and credential-gated Binance Testnet runners.
- `tests/`: unit, simulation, and optional Binance Testnet contract tests.
- `reports/`: report draft and failure-case log.

## Architecture

`ExecutionService` is the public application facade. It delegates to `ExecutionEngine`, which owns execution records, child orders, exposure accounting, reconciliation, and lifecycle transitions. Each execution is serialized through its own event actor so `create`, `run_once`, `cancel`, and `reconcile` calls cannot race each other inside one execution.

Algorithms are deliberately small:

- Chase computes the passive desired price from best bid/ask and decides whether to reprice an active order after the configured bps threshold and minimum interval.
- TWAP computes scheduled cumulative quantity from elapsed monotonic time and submits only the current schedule deficit after confirmed and reserved exposure. `number_of_slices` is plumbed into `ExecutionParameters` and Testnet scripts, but the current TWAP behavior is schedule-deficit based, not fixed equal-slice sleep.

The `ExchangeAdapter` interface is narrow enough for deterministic tests. The simulator supports market data, order creation, fills, cancel/fill races, create-timeout scenarios, reconciliation, and stream health scripting. The Binance adapter maps the same contract onto USD-M REST and WebSocket hooks.

## Risk and Invariants

The main safety invariant is enforced before every child submit:

```text
confirmed_filled + live_open + pending_submit + pending_cancel + unknown_order + new_child_quantity <= normalized_target_trade_quantity
```

This gate is shared by every submit path. Ambiguous create outcomes reserve `unknown_order_quantity` until reconciliation proves whether the child exists. Cancel ambiguity is tracked through `pending_cancel_quantity` until reconciliation refreshes reserved exposure.

Other guardrails:

- Quantities and prices are `Decimal`, not floats. API decimal fields must be JSON strings.
- Prices and quantities are rounded to exchange tick/step rules before order submission.
- Post-only orders are validated against current bid/ask and supported time-in-force.
- State changes go through `execution/state_machine.py` via engine helpers, not ad hoc status assignments.
- Reconciliation is scoped by the exact execution client-order prefix `ce_<short_exec>_`, never broad `ce_`.
- Monotonic time drives scheduling, repricing intervals, and simulator market-data freshness.

## API Usage

Create an execution:

```bash
curl -s -X POST http://127.0.0.1:8000/executions \
  -H 'Content-Type: application/json' \
  -d '{
    "environment": "simulation",
    "symbol": "BTCUSDT",
    "algorithm": "CHASE",
    "target_position": "0.010",
    "target_price_lower": "49000",
    "target_price_upper": "51000",
    "target_duration_seconds": 300,
    "deadline_policy": "AGGRESSIVE_WITHIN_RANGE",
    "parameters": {
      "reprice_threshold_bps": "2.0",
      "minimum_reprice_interval_ms": 500,
      "number_of_slices": 10,
      "child_order_timeout_seconds": 20,
      "repricing_mode": "ADVERSE_ONLY"
    }
  }'
```

Then progress and inspect the execution:

```bash
curl -s -X POST http://127.0.0.1:8000/executions/<execution_id>/run-once
curl -s http://127.0.0.1:8000/executions/<execution_id>
curl -s -X POST http://127.0.0.1:8000/executions/<execution_id>/reconcile
curl -s -X POST http://127.0.0.1:8000/executions/<execution_id>/cancel
```

The response includes status, final reason, required quantity, exposure buckets, child orders, original request, and summary metrics when terminal.

## Simulator Scripts

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

The first two print deterministic Chase and TWAP state. The cancel-race script writes artifacts under `/tmp/calais-sim-cancel-race` by default. The create-timeout script writes artifacts under `/tmp/calais-sim-create-timeout` and demonstrates that a create timeout blocks new submissions until exact reconciliation resolves the original child.

Artifacts contain:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`

## Binance Testnet

The Binance adapter uses:

- Testnet REST base `https://demo-fapi.binance.com`.
- Testnet WebSocket root `wss://fstream.binancefuture.com` with routed `/public/ws/...` and `/private/ws/...` paths.
- Signed REST requests with `timestamp`, `recvWindow`, and sanitized logging behavior.
- `POST /fapi/v1/order`, `DELETE /fapi/v1/order`, and `GET /fapi/v1/order` with `newClientOrderId` or `origClientOrderId`.
- Reconciliation via `GET /fapi/v1/openOrders`, `GET /fapi/v1/allOrders`, `GET /fapi/v1/userTrades`, and exact order lookup by client order ID.

HTTP 408 and transport timeouts on create/cancel are ambiguous mutation outcomes, not terminal rejects. Unknown create-timeout children are resolved with exact `GET /fapi/v1/order` lookup before the engine clears `unknown_order_quantity`.

The Testnet scripts are compact validation hooks, not production-grade WebSocket recovery loops. They require credentials and explicit consent:

```bash
export BINANCE_USDM_API_KEY=...
export BINANCE_USDM_API_SECRET=...

uv run python scripts/run_testnet_chase.py \
  --confirm-send-orders \
  --symbol BTCUSDT \
  --target-position 0.001 \
  --target-price-lower 90000 \
  --target-price-upper 120000

uv run python scripts/run_testnet_twap.py \
  --confirm-send-orders \
  --symbol BTCUSDT \
  --target-position 0.001 \
  --target-price-lower 90000 \
  --target-price-upper 120000 \
  --number-of-slices 5
```

They never fall back to the simulator. If credentials are absent, or `--confirm-send-orders` is missing, they exit before sending orders. Testnet artifacts are written under `/tmp/calais-binance-testnet` unless `--output-dir` is provided.

Mainnet is config-compatible only. Mutating mainnet requests are hard-disabled by default through `allow_mainnet_trading=False` and should not be used for demos.

## Verification

Primary local verification:

```bash
uv run pytest -q
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

Optional Testnet contract tests run only when `BINANCE_USDM_API_KEY` and `BINANCE_USDM_API_SECRET` are set. Without those variables, pytest skips them.

## Limitations

- This is a take-home execution service, not a production trading system.
- Persistence is in-memory; process restart loses execution state.
- The API factory is simulator-only.
- Testnet scripts do not implement robust reconnect, listen-key renewal scheduling, backoff orchestration, or operational alerting.
- Mainnet mutation is disabled by default and is not part of the demo contract.
- TWAP currently uses elapsed-time schedule deficit; it does not run an autonomous background scheduler.
