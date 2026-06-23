# Assignment Requirements

The Calais brief asks for a Python 3.11+ execution service for Binance USD-M Futures BTCUSDT Perpetual. The service receives a final target position, an allowed execution price range, and a target duration, then uses Chase or TWAP to move the account toward the target.

## Requirement Matrix

| Brief Area | Requirement | Repository Coverage | Proof |
| --- | --- | --- | --- |
| Target position | Compute `target_position - current_position`; support buy, sell, no action, and cross-zero. | `execution.models.required_trade`, `ExecutionEngine.create_execution`, API schemas. | `tests/simulation/test_required_scenarios.py::test_t10_cross_zero_position_uses_target_minus_current_absolute_quantity` |
| Price range | Never actively buy above upper bound or sell below lower bound. | `risk.validation.validate_child_order_safety`, engine price checks. | `tests/simulation/test_required_scenarios.py::test_t7_price_outside_range_waits_then_expires_without_invalid_order` |
| Duration | Use monotonic time for schedule and deadlines, while logging wall-clock timestamps. | `execution.clock`, engine lifecycle timestamps, artifact writer. | `tests/unit/test_engine_lifecycle.py` deadline and start-time tests |
| API/CLI | Provide create, query, cancel, and deterministic controls. | `api.app`, `api.runtime`, simulator scripts. | `tests/unit/test_api.py` and `scripts/run_sim_*.py` |
| Chase | Passive best bid/ask with threshold repricing and minimum interval. | `algorithms.chase`, engine cancel/replace path. | `tests/simulation/test_required_scenarios.py::test_t1_normal_chase_submits_passive_price_and_preserves_exposure_invariant`, `tests/simulation/test_required_scenarios.py::test_t2_chase_reprice_requires_threshold_and_minimum_interval` |
| TWAP | Absolute schedule, carry-forward deficit, and rounding-aware final quantity. | `algorithms.twap`, TWAP engine ledger. | `tests/simulation/test_required_scenarios.py::test_t5_twap_carry_forward_deficit_includes_previous_unfilled_quantity`, `tests/simulation/test_required_scenarios.py::test_t5b_twap_does_not_submit_before_first_absolute_slice_boundary` |
| Create timeout | Preserve UNKNOWN order exposure until exact reconciliation by client order ID. | `ExecutionEngine`, `ExchangeAdapter.get_order_by_client_order_id`, simulator timeout scripts. | `tests/simulation/test_required_scenarios.py::test_t4a_create_timeout_reconciles_to_open_order_without_new_client_order_id` |
| Cancel/fill race | A fill after cancel request reduces replacement quantity. | Exposure tracker and cancel/reconcile path. | `tests/simulation/test_required_scenarios.py::test_t3_cancel_fill_race_updates_confirmed_fills_before_replacement_sizing` |
| Decimal precision | Use `Decimal` and JSON decimal strings for order parameters. | `execution.models`, `api.schemas`, `risk.decimal_math`. | `tests/unit/test_models.py`, `tests/unit/test_api.py`, `tests/unit/test_decimal_math.py` |
| Logging/results | Write structured logs and result artifacts. | `observability.artifacts`, simulator and Testnet runners. | `tests/simulation/test_required_scenarios.py::test_cancel_race_script_writes_required_artifacts` |
| Binance Testnet accepted evidence | Provide Chase and TWAP Testnet order evidence with exchange order IDs. | Accepted sanitized artifacts redact `ACCOUNT_UPDATE` balance/position fields and preserve order/trade evidence. | `reports/evidence/testnet/chase/exec_3168600ee25b4193`, `reports/evidence/testnet/twap/exec_85bef3985ea3431a` |

## Direct Pass/Fail Risks

- Partial fill after cancel must not cause resubmission of the original total quantity.
- Create timeout must remain `UNKNOWN` until exact lookup resolves the client order ID.
- Price and quantity inputs must not be constructed from floats.
- TWAP must use absolute elapsed time rather than `sleep` drift.
- Price-out-of-range execution must report the real unfilled result.
- Tests and artifacts must be traceable by execution ID and client order ID.
