# Limitations

The project is intentionally scoped to be small and inspectable for the assignment.

## Scope

- Persistence is in-memory; process restart loses execution state.
- The target exchange family is Binance USD-M Futures.
- The target symbol is BTCUSDT perpetual.
- The account mode is one-way mode.
- Runtime supervision is compact and Testnet-focused, not a production operations platform.
- Deterministic simulator tests prove races that Testnet may not reproduce reliably.
- Accepted Testnet order evidence depends on account funding, permissions, and Binance risk checks.
- Mainnet mutations are hard-disabled by default.

## Submission Interpretation

Simulator evidence should not be presented as a replacement for accepted Testnet order evidence. If Testnet order acceptance is blocked, keep the raw rejected/error artifact as connectivity evidence and label accepted-order evidence as pending account configuration.
