# Execution Lifecycle

An execution starts with a final target position and ends with an explicit terminal status. It does not imply guaranteed full fill, because price bounds, deadline policy, exchange failures, and remaining exposure are part of the result.

## Parent Execution States

| State | Meaning |
| --- | --- |
| `CREATED` | Request accepted and execution record built. |
| `VALIDATING` | Position, symbol rules, range, duration, and parameters are checked. |
| `RUNNING` | Engine may submit, cancel, reprice, or reconcile child orders. |
| `CANCELLING` | Manual cancel is draining active exposure. |
| `COMPLETED` | Required normalized quantity is filled. |
| `PARTIALLY_COMPLETED` | Some quantity filled and no more safe work remains. |
| `EXPIRED` | No-fill deadline or safety terminal outcome. |
| `CANCELLED` | Manual cancel completed with no filled quantity. |
| `FAILED` | Terminal validation or exchange failure prevents safe continuation. |

## Child Order States

| Current State | Allowed Next States |
| --- | --- |
| `PENDING_SUBMIT` | `OPEN`, `REJECTED`, `UNKNOWN` |
| `OPEN` | `PARTIALLY_FILLED`, `FILLED`, `PENDING_CANCEL` |
| `PARTIALLY_FILLED` | `FILLED`, `PENDING_CANCEL` |
| `PENDING_CANCEL` | `OPEN`, `PARTIALLY_FILLED`, `CANCELLED`, `FILLED` |
| `UNKNOWN` | `OPEN`, `PARTIALLY_FILLED`, `FILLED`, `CANCELLED`, `REJECTED` |
| `CANCELLED`, `FILLED`, `REJECTED` | Terminal states with no allowed next state. |

`UNKNOWN` means the create outcome is ambiguous. The engine reserves the unknown quantity until exact reconciliation proves whether the exchange accepted the order. It is not terminal; reconciliation must move it to the observed exchange state.

## Serialization

Each execution is serialized through its own event actor. `create`, `run_once`, `cancel`, and `reconcile` calls for one execution cannot interleave inside the engine in a way that corrupts child state or exposure accounting.

## Terminal Rule

Terminal execution states do not return to `RUNNING`. Terminal child states are not resurrected by stale reconciliation snapshots.
