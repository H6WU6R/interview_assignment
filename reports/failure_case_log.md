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

## Review-driven hardening cases

After external review, several implementation-level failures were made explicit and covered by regressions:

- Partial fill returned directly by create response did not have to update parent fills immediately. Fixed by applying cumulative child fills through the same monotonic fill ledger used by reconciliation.
- Lower or duplicate REST/user-stream cumulative snapshots could not be allowed to reduce or double-count parent fills. Fixed by treating cumulative fill quantity per child as monotonic and only applying positive deltas.
- A stale quote should pause or terminalize with a controlled reason, not escape as an uncaught exception from `run_once`.
- A runtime unknown-reconciliation failure should be recorded and retried, not silently kill the background execution task.
- User-stream events and disconnects should trigger execution-scoped reconciliation with bounded time windows before safe continuation.
- Graceful shutdown must stop new child creation, cancel/reconcile active execution-scoped orders, and leave no background execution task running.
- Simulator fills must update account position so later `target_position - current_position` calculations exercise the same accounting model as Binance.

Representative tests:

- `test_partial_fill_from_create_response_updates_parent_fills_immediately`
- `test_lower_rest_cumulative_snapshot_cannot_reduce_child_or_parent_fill`
- `test_background_loop_records_unknown_reconcile_failure_and_retries`
- `test_user_stream_event_reconciles_active_execution_with_event_time_bounds`
- `test_runtime_restarts_binance_user_stream_after_disconnect`
- `test_runtime_stop_cancels_and_reconciles_active_execution`
- `test_stale_market_data_pauses_execution_without_raising`
- `test_simulator_fill_updates_account_position_for_buy_and_sell`
