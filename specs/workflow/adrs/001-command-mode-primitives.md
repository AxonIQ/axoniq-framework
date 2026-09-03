# ADR-001: Command-Mode Primitive API

- Status: Accepted
- Date: 2026-05-05

## Context

The runtime needs one durable primitive contract between DSL and execution. That contract is
`WorkflowStepResult`.

## Decision

Runtime primitives follow command mode.

Each primitive exposes one raw method from a base command to `WorkflowStepResult`.

```java
WorkflowStepResult execute(ExecuteCommand command);
WorkflowStepResult waitForEvent(WaitForCommand command);
WorkflowStepResult modifyPayload(ModifyPayloadCommand command);
```

Blocking and typed author-facing helpers are built above that contract in the DSL layer by waiting on and converting
the returned `WorkflowStepResult`.

## Naming

- Raw runtime verbs stay primitive-specific: `execute(...)`, `waitForEvent(...)`, `modifyPayload(...)`.
- Author-facing DSL names stay separate from runtime commands.

## Architecture

- Runtime command layer defines primitive commands and `WorkflowStepResult`.
- DSL step-definition layer builds commands from `ExecuteStepDefinition`, `WaitForStepDefinition`,
  `PayloadStepDefinition`, `FailWorkflowDefinition`, and `CancelStepDefinition`.
- `BaseWorkflowContext` is the inherited Java DSL base that owns the defaulted step-definition path.
- `SimpleWorkflowContext` may add convenience methods such as typed `awaitEvent`, `sleep`, or `setPayload`,
  but these remain adapters above `BaseWorkflowContext`.
- Delegate and execution layers implement only the raw runtime command path.

## Consequences

- There is one runtime execution path per primitive.
- `WorkflowStepResult` remains the only internal primitive result contract.
- Typed and convenience DSL methods are derived from that contract, not from separate runtime paths or typed runtime
  commands.
- Blocking and typed conversion logic lives in `AbstractDSLWorkflowContext`, `BaseWorkflowContext`,
  `SimpleWorkflowContext`, and `Kontext`.
