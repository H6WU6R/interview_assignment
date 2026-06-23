# Limitations

The project is intentionally scoped to be small and inspectable for the assignment.

## Scope

- Persistence is in-memory; process restart loses execution state.
- The target exchange family is Binance USD-M Futures.
- The target symbol is BTCUSDT perpetual.
- The account mode is one-way mode.
- Runtime supervision is compact and Testnet-focused, not a production operations platform.
- Deterministic simulator tests prove races that Testnet may not reproduce reliably.
- Testnet reruns depend on account funding, permissions, and Binance risk checks.
- Mainnet mutations are hard-disabled by default.

## Submission Interpretation

Simulator evidence should not be presented as a replacement for accepted Testnet order evidence. Accepted sanitized Testnet artifacts are included at `reports/evidence/testnet/chase/exec_3168600ee25b4193` and `reports/evidence/testnet/twap/exec_85bef3985ea3431a`. If a future Testnet rerun is blocked before order acceptance, keep that raw rejected/error artifact only as connectivity evidence.
