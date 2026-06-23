# Safety Invariants

The engine enforces exposure safety before every child submit. The core rule is:

```text
confirmed_filled
+ live_open
+ pending_submit
+ pending_cancel
+ unknown_order
+ new_child_quantity
<= normalized_target_trade_quantity + permitted_tolerance
```

`permitted_tolerance` defaults to zero. It is an explicit accounting tolerance in the invariant gate, not extra quantity that normal Chase or TWAP sizing tries to submit.

## Reserved Buckets

| Bucket | Why It Counts |
| --- | --- |
| `confirmed_filled` | Quantity already filled toward the parent target. |
| `live_open` | Quantity still executable on the exchange. |
| `pending_submit` | Local intent sent to the exchange before the create response is known. |
| `pending_cancel` | Quantity that can still fill after a cancel request is sent. |
| `unknown_order` | Ambiguous create outcome until exact reconciliation resolves it. |
| `new_child_quantity` | Quantity proposed for the next child order. |

## Guarantees

- Replacement quantity is based on parent cumulative filled quantity and reserved exposure, not only the active order's remaining quantity.
- Duplicate or stale cumulative fill snapshots cannot reduce or double-count parent fills.
- A create timeout cannot be retried with a new client order ID while the original outcome is unknown.
- Price-out-of-range tasks may expire unfilled, but they do not fake completion.

## Implementation And Proof

- Implementation: `src/execution/engine.py`, `src/risk/validation.py`
- Unit proof: `tests/unit/test_engine_exposure.py`
- Scenario proof: `tests/simulation/test_required_scenarios.py`
