# Failure Case Log

## Ambiguous Binance HTTP 408 and UNKNOWN create-timeout reconciliation

During development, Binance order creation timeouts needed stricter handling. An HTTP 408 or transport timeout on `POST /fapi/v1/order` is ambiguous: the exchange may have accepted the order even though the client did not receive the response. Treating that outcome as a terminal reject, or clearing UNKNOWN state from broad reconciliation alone, could release exposure and allow a duplicate replacement child.

Tests that exposed and locked down the behavior:

- `test_signed_request_http_408_maps_mutations_to_ambiguous_outcome`
- `test_exact_create_timeout_lookup_maps_found_order_to_live_open`
- `test_exact_create_timeout_lookup_not_found_clears_unknown_without_broad_warning`

Fix:

- Map HTTP 408 and transport timeout on create to `UnknownCreateOutcome`.
- Map HTTP 408 and transport timeout on cancel to `PendingCancelOutcome`.
- Keep create-timeout children in `UNKNOWN` status and reserve `unknown_order_quantity`.
- During reconciliation, query each UNKNOWN child exactly with `GET /fapi/v1/order` and `origClientOrderId`.
- If exact lookup finds the order, move the child back to live open exposure.
- If exact lookup returns Binance order-not-found, mark the child rejected and clear UNKNOWN exposure without relying on any broad `ce_` prefix warning.

Result: the submit gate continues to account for ambiguous children until exact reconciliation proves whether the original child exists, preserving the exposure invariant.
