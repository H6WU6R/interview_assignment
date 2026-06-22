# Normal Chase

This example proves that Chase submits one passive child order at the best bid for a buy execution and reserves exposure for the normalized required quantity.

## Run

```bash
uv run python scripts/run_sim_chase.py
```

## Expected Evidence

Important output fields:

```text
SIMULATOR DEMO: Chase
execution_id=<generated-execution-id>
status=RUNNING
client_order_ids=[<generated-client-order-id>]
child_order id=<generated-child-order-id> clientOrderId=<generated-client-order-id> status=OPEN qty=0.010 price=50000.00
summary={... 'live_open_quantity': Decimal('0.010'), ... 'reserved_exposure': Decimal('0.010') ...}
```

The generated execution ID and client order ID change per run. The stable behavior is one open passive child order and live exposure equal to the required quantity.

## Related Test

`tests/simulation/test_required_scenarios.py::test_t1_normal_chase_submits_passive_price_and_preserves_exposure_invariant`
