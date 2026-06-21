# Binance API-Key Follow-Up Plan

This plan covers validation to perform after Binance USD-M Testnet API credentials are available. It intentionally stays separate from non-API correctness work.

Note: this file was reconstructed after the untracked working copy was removed. It is aligned with the current implementation and README.

## Scope

Use Binance Testnet to prove integration wiring:

- Exchange info parsing.
- Position query.
- Market data snapshot.
- Signed order create.
- Cancel request.
- Query by client order ID.
- Reconciliation artifacts.

Simulator tests remain the proof path for race conditions that Testnet may not reliably reproduce.

## Preconditions

Set credentials locally only:

```bash
export BINANCE_USDM_API_KEY=...
export BINANCE_USDM_API_SECRET=...
```

Do not commit credentials, listen keys, signatures, or authenticated payloads.

All mutating scripts must require explicit confirmation such as:

```bash
--confirm-send-orders
```

Mainnet remains configuration-compatible only. Assignment demo and validation should use simulator plus Testnet. Mutating mainnet orders stay hard-disabled by default.

## Task 1: Signed Request Timing

Validate signed REST requests include:

- Binance server-time offset handling.
- `timestamp`.
- Small bounded `recvWindow`, ideally 5000 ms or less.

Reason: local clock drift can cause signed order, cancel, and query requests to fail even when engine logic is correct.

## Task 2: Rate Limit Handling

Read rate-limit metadata from exchange info where practical.

Handle venue rate-limit outcomes conservatively:

- 429 should back off and avoid request storms.
- 418 should be treated as a serious venue-level block.
- Internal `minimum_reprice_interval_ms` reduces cancel storms but does not replace exchange-level rate-limit handling.

## Task 3: Error Classification

Keep exchange/API failures classified:

- Terminal reject.
- Retryable read failure.
- Ambiguous create outcome.
- Ambiguous cancel outcome.
- Stream health failure.

Important distinction:

- Ambiguous create outcome means the order may or may not exist, so reserve unknown child exposure.
- Ambiguous cancel outcome means the order was known to exist, so keep pending-cancel exposure until reconciliation.

## Task 4: Query Endpoint Safety

For reconciliation, prefer normal order status lookup by `origClientOrderId`:

```text
GET /fapi/v1/order
```

Do not rely only on a current-open-order endpoint, because a missing open order cannot distinguish filled, cancelled, expired, and never-created outcomes.

Use broader open-orders/all-orders/user-trades calls only as reconciliation support, and keep filtering scoped to the exact execution client-order prefix.

## Task 5: User-Data Stream

Implement or validate the minimal Testnet stream path:

- Create or renew listenKey before expiry.
- Treat 24-hour disconnect as expected lifecycle.
- Reconnect and reconcile after stream interruptions.
- Retain Binance event time and transaction time when available.
- Use event time for diagnostics, not as a replacement for conservative reconciliation.

Do not overclaim production-grade WebSocket recovery in the README or report.

## Task 6: Post-Only GTX

For Binance USD-M passive post-only orders:

- Map `post_only=True` to `timeInForce=GTX` when supported by symbol rules.
- If GTX support is missing or uncertain, reject the passive post-only order with a clear error.

The simulator should keep this path testable.

## Task 7: Testnet Evidence Scripts

Run credential-gated scripts only with explicit confirmation:

```bash
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

Each Testnet run should produce:

- Raw execution log.
- Order IDs and client order IDs.
- Parameter/request snapshot.
- Final execution summary.
- Reconciliation result.

## Acceptance Criteria

- Scripts exit before sending orders if credentials or confirmation are absent.
- Testnet run proves exchangeInfo, position query, market data, submit, cancel, order lookup, and reconciliation.
- Artifacts are linked by execution ID and client order ID.
- Logs are sanitized.
- README/report clearly state that deterministic simulator tests prove race behavior, while Testnet validates integration plumbing.
