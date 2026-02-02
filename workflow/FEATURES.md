# News and noteworthy

## 26.01.2026

- Create initial implementation based on declarative DSL
- Provide integration with Axon Framework 5

## 01.02.2026

- Introduce `StepExecutionResult` (completed and future)
- Use `StepExecutionResult` as return value for the primitive calls
- Switch `WaitFor` primitive to use `QualifiedName` (no types anymore)
- Offload task execution from the `Execute` primitive to the dedicated `TaskManager`
- Implement `cancel` on `TaskManager` and `EventSubscriptionManager`

