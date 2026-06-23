# Binance Testnet

The Binance USD-M adapter maps the same `ExchangeAdapter` contract used by the simulator onto Testnet REST and WebSocket behavior.

## Credentials And Consent

Testnet scripts require:

```bash
export BINANCE_USDM_API_KEY=<testnet-api-key>
export BINANCE_USDM_API_SECRET=<testnet-api-secret>
```

They also require `--confirm-send-orders`. Without credentials or explicit confirmation, the scripts exit before sending orders and do not fall back to the simulator.

## Endpoints And Evidence

- Testnet REST base: `https://demo-fapi.binance.com`
- Public/user stream root: `wss://demo-fstream.binance.com`
- Order mutation endpoints: `POST /fapi/v1/order`, `DELETE /fapi/v1/order`
- Reconciliation endpoints: `GET /fapi/v1/order`, `GET /fapi/v1/openOrders`, `GET /fapi/v1/allOrders`, `GET /fapi/v1/userTrades`

The Testnet runner writes `symbol_rules.json`, `reconciliation_orders.csv`, `execution_summary.json`, `execution_log.jsonl`, and `evidence_manifest.json`.

Accepted sanitized Testnet evidence is included in:

- `reports/evidence/testnet/chase/exec_3168600ee25b4193`
- `reports/evidence/testnet/twap/exec_85bef3985ea3431a`

Those artifacts preserve order/trade evidence and exchange order IDs while redacting `ACCOUNT_UPDATE` balance and position fields. The older insufficient-margin runs `reports/evidence/testnet/chase/exec_85051310eb714ebe` and `reports/evidence/testnet/twap/exec_30ed2b4cac4346a1` are retained only as rejected connectivity/error artifacts.

## Mainnet Guardrail

Mainnet is configuration-compatible but hard-disabled by default. Mutating mainnet requests require explicit configuration and should not be used for the assignment demo.

## Implementation And Proof

- Adapter: `src/exchanges/binance_usdm.py`
- Runtime supervision: `src/api/runtime.py`
- Scripts: `scripts/run_testnet_chase.py`, `scripts/run_testnet_twap.py`
- Contract tests: `tests/integration/test_binance_testnet_contract.py`
