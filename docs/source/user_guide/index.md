# User Guide

The User Guide is organized around evaluator questions from the Calais brief: what the service guarantees, where the guarantee is implemented, and which tests or scripts prove the behavior.

## Reading Path

1. {doc}`assignment_requirements` maps assignment requirements to repository coverage.
2. {doc}`architecture` explains the module boundaries.
3. {doc}`execution_lifecycle` explains parent and child order states.
4. {doc}`safety_invariants` explains why the engine cannot knowingly over-submit.
5. {doc}`chase` and {doc}`twap` explain algorithm behavior.
6. {doc}`binance_testnet`, {doc}`observability`, and {doc}`limitations` explain evidence and scope.

```{toctree}
:maxdepth: 1

assignment_requirements
architecture
execution_lifecycle
safety_invariants
chase
twap
binance_testnet
observability
limitations
```
