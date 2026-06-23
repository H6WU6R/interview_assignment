# Calais Execution Algorithm

Compact Binance USD-M execution service for the Calais take-home. The project is intentionally small, but the correctness path is explicit: API requests enter an `ExecutionService`, all order state is owned by `ExecutionEngine`, algorithm modules only compute narrow decisions, and exchange access is behind an `ExchangeAdapter` contract with both deterministic simulator and Binance USD-M implementations.

## Quickstart

```bash
uv sync
uv run pytest -q tests/unit tests/simulation
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

## Documentation

The repository includes a Sphinx documentation site under `docs/source/`, organized like a compact Python package manual:

- User Guide: assignment requirements, architecture, lifecycle, safety invariants, Chase, TWAP, Testnet evidence, observability, and limitations.
- Examples: deterministic simulator and scenario-test proof cases.
- API Reference: public modules for algorithms, execution, exchanges, risk, API/runtime, and observability.

Build the HTML docs locally with:

```bash
uv run sphinx-build -b html docs/source docs/_build/html
```

Open `docs/_build/html/index.html` in a browser after the build completes.

To run the simulation API locally:

```bash
uv run uvicorn api.app:create_app --factory --reload
```

The API factory wires a deterministic simulator by default. It does not send Binance orders. When run through FastAPI lifespan, the runtime starts background execution loops automatically; `/run-once` remains available for deterministic demos and debugging.

Optional container run for the simulation API:

```bash
docker build -t calais-execution-algorithm .
docker run --rm -p 8000:8000 calais-execution-algorithm
```

Do not bake Binance credentials into the image. Pass Testnet credentials at runtime only when explicitly running the Testnet scripts.

## Project Layout

- `src/api/`: FastAPI app, runtime supervisor, and Pydantic request/response schemas.
- `src/execution/`: service facade, execution engine, state machine, IDs, events, and domain models.
- `src/algorithms/`: narrow Chase and TWAP decision helpers.
- `src/exchanges/`: exchange contract, deterministic simulator, and Binance USD-M adapter.
- `src/risk/`: Decimal rounding and safety validation.
- `src/observability/`: JSONL/JSON/CSV artifact writers and sanitization helpers.
- `scripts/`: simulator demos and credential-gated Binance Testnet runners.
- `tests/`: unit, simulation, and optional Binance Testnet contract tests.
- `reports/`: report draft and failure-case log.
- `Dockerfile`, `.dockerignore`: optional container packaging for the simulation API.

## Architecture

`ExecutionRuntime` is the API-facing process supervisor. It constructs the correct environment adapter, rejects a second active execution for the same environment and symbol, advances nonterminal executions in the background, starts Binance market/user stream supervisors, renews listen keys, retries controlled runtime failures, and cancels/reconciles active executions during graceful shutdown.

`ExecutionService` is the public application facade. It delegates to `ExecutionEngine`, which owns execution records, child orders, exposure accounting, reconciliation, and lifecycle transitions. Each execution is serialized through its own event actor so `create`, `run_once`, `cancel`, and `reconcile` calls cannot race each other inside one execution.

Algorithms are deliberately small:

- Chase computes the passive desired price from best bid/ask and decides whether to reprice an active order after the configured bps threshold and minimum interval.
- TWAP uses absolute slice boundaries from `number_of_slices`; it never sleeps and does not accumulate scheduler drift. At each observed boundary, it computes scheduled cumulative quantity from elapsed monotonic time, subtracts confirmed fills and reserved exposure, then submits only the safe deficit.

The `ExchangeAdapter` interface is narrow enough for deterministic tests. The simulator supports market data, order creation, fills, cancel/fill races, create-timeout scenarios, reconciliation, and stream health scripting. The Binance adapter maps the same contract onto USD-M REST and WebSocket hooks.

## Risk and Invariants

The main safety invariant is enforced before every child submit:

```text
confirmed_filled + live_open + pending_submit + pending_cancel + unknown_order + new_child_quantity <= normalized_target_trade_quantity + permitted_tolerance
```

This gate is shared by every submit path. `permitted_tolerance` defaults to zero, so normal child sizing remains bounded by the normalized target quantity. Ambiguous create outcomes reserve `unknown_order_quantity` until reconciliation proves whether the child exists. Cancel ambiguity is tracked through `pending_cancel_quantity` until reconciliation refreshes reserved exposure.

Other guardrails:

- Quantities and prices are `Decimal`, not floats. API decimal fields must be JSON strings.
- Prices and quantities are rounded to exchange tick/step rules before order submission.
- Post-only orders are validated against current bid/ask and supported time-in-force.
- When the current quote is outside `target_price_range`, the engine submits no child order
  and keeps the execution running until the deadline. If the quote never becomes executable,
  the terminal summary reports `PRICE_OUTSIDE_RANGE` with filled and unfilled quantities.
- State changes go through `execution/state_machine.py` via engine helpers, not ad hoc status assignments.
- Reconciliation is scoped by the exact execution client-order prefix `ce_<short_exec>_`, never broad `ce_`.
- Monotonic time drives scheduling, repricing intervals, and simulator market-data freshness.
- Stale market data is handled as a controlled pause/final reason instead of an uncaught runtime exception.
- Per-child cumulative fills are monotonic; duplicate or lower cumulative snapshots cannot reduce or double-count parent fills.

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
EXECUTION_ID=exec_replace_me
curl -s -X POST "http://127.0.0.1:8000/executions/$EXECUTION_ID/run-once"
curl -s "http://127.0.0.1:8000/executions/$EXECUTION_ID"
curl -s -X POST "http://127.0.0.1:8000/executions/$EXECUTION_ID/reconcile"
curl -s -X POST "http://127.0.0.1:8000/executions/$EXECUTION_ID/cancel"
```

The response includes status, final reason, required quantity, exposure buckets, child orders, original request, and summary metrics when terminal.

## Simulator Scripts

```bash
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

All four scripts print deterministic simulator state and write structured artifacts. The committed simulator evidence for this submission is:

- Chase: `reports/evidence/simulation/chase/exec_c8dc942476764355`
- TWAP: `reports/evidence/simulation/twap/exec_61fadac604f4440a`
- Cancel/fill race: `reports/evidence/simulation/cancel-race/exec_669d47a536a94682`
- Create timeout: `reports/evidence/simulation/create-timeout/exec_3ddb8a47995348d0`

Pass `--output-dir` to write fresh bundles elsewhere; otherwise scripts use their documented local defaults.

Artifacts contain:

- `request_snapshot.json`
- `execution_log.jsonl`
- `execution_summary.json`
- `child_orders.csv`
- `fills.csv`
- `timeline.csv`
- `twap_slice_ledger.csv`

## Binance Testnet

The Binance adapter uses:

- Testnet REST base `https://demo-fapi.binance.com`.
- Testnet WebSocket root `wss://demo-fstream.binance.com` with routed `/public/ws/...` and `/private/ws/...` paths.
- Signed REST requests with `timestamp`, `recvWindow`, and sanitized logging behavior.
- `POST /fapi/v1/order`, `DELETE /fapi/v1/order`, and `GET /fapi/v1/order` with `newClientOrderId` or `origClientOrderId`.
- Reconciliation via `GET /fapi/v1/openOrders`, `GET /fapi/v1/allOrders`, `GET /fapi/v1/userTrades`, and exact order lookup by client order ID.
- Runtime stream supervisors keep public market and private user streams alive, renew listen keys, mark stream health degraded on disconnect, and trigger conservative reconciliation before safe continuation.

HTTP 408 and transport timeouts on create/cancel are ambiguous mutation outcomes, not terminal rejects. Unknown create-timeout children are resolved with exact `GET /fapi/v1/order` lookup before the engine clears `unknown_order_quantity`.

The Testnet scripts are compact validation hooks. They require credentials and explicit consent:

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

The Testnet runner writes the standard execution bundle plus Testnet-specific evidence:

- `symbol_rules.json`: exchange rule snapshot used for rounding and validation.
- `reconciliation_orders.csv`: final reconciliation order rows scoped to the execution.
- `evidence_manifest.json`: execution ID, order IDs, exchange-order evidence status, `accepted_exchange_order_evidence`, stream-event evidence flags, warning list, reconciliation counts, and rate-limit metadata.

Accepted Binance Testnet evidence for submission means both:

- at least one Chase run whose `evidence_manifest.json` has `accepted_exchange_order_evidence: true` and at least one non-empty `exchange_order_id`;
- at least one TWAP run whose `evidence_manifest.json` has `accepted_exchange_order_evidence: true` and at least one non-empty `exchange_order_id`.

Accepted sanitized Binance Testnet evidence is included in:

- `reports/evidence/testnet/chase/exec_3168600ee25b4193`
- `reports/evidence/testnet/twap/exec_85bef3985ea3431a`

These accepted artifacts preserve order/trade evidence needed to prove exchange acceptance while redacting `ACCOUNT_UPDATE` balance and position fields. The older runs `reports/evidence/testnet/chase/exec_85051310eb714ebe` and `reports/evidence/testnet/twap/exec_30ed2b4cac4346a1` are retained only as rejected connectivity/error artifacts because Binance rejected them before acceptance with `BINANCE_-2019:Margin is insufficient.`

Mainnet is config-compatible only. Mutating mainnet requests are hard-disabled by default through `allow_mainnet_trading=False` and should not be used for demos.

## Verification

Primary non-live local verification:

```bash
uv run pytest -q tests/unit tests/simulation
uv run python scripts/run_sim_chase.py
uv run python scripts/run_sim_twap.py
uv run python scripts/run_sim_cancel_race.py
uv run python scripts/run_sim_create_timeout.py
```

The submission verifier excludes live Binance Testnet integration tests by default:
`uv run python scripts/verify_submission.py`. To include credentialed/networked contract
tests, pass `--include-live-integration` explicitly.

Credentialed/network-enabled integration verification:

```bash
uv run pytest -q tests/integration
```

Optional Testnet contract tests run only when `BINANCE_USDM_API_KEY` and `BINANCE_USDM_API_SECRET` are set. Without those variables, pytest skips them.
These read-only contract tests validate connectivity and parsing only; they do not satisfy the required accepted-order Chase/TWAP Testnet evidence.

Current verified non-live baseline after the final evidence cleanup plan: `506 passed` with `uv run pytest -q tests/unit tests/simulation`. Credentialed/network-enabled integration tests are separate and should only be reported when run with Binance Testnet credentials.

## Known Limitations

- Persistence is in-memory; process restart loses execution state.
- Runtime stream supervision is compact and Testnet-focused; it is not a production operations system with durable recovery, alerting, or multi-process coordination.
- Binance Testnet scripts are credential-gated and require explicit send confirmation.
- Accepted Testnet order evidence requires a Testnet account that can pass Binance margin/risk checks.
- Mainnet is configuration-compatible but hard-disabled by default.
- The implementation targets BTCUSDT and One-way Mode.
- Deterministic simulator tests prove race conditions that Testnet may not reproduce reliably.
