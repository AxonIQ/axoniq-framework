# ADR-002: DSL Primitive Definitions

- Status: Accepted
- Date: 2026-05-06

## Context

ADR-001 defines runtime command mode. This ADR defines the DSL DTO layer above it.

## Decision

The DSL uses these top-level DTOs in `io.axoniq.framework.workflow.runtime.api.execution.context`:

- `ExecuteStepDefinition`
- `WaitForStepDefinition`
- `PayloadStepDefinition`
- `FailWorkflowDefinition`
- `CancelWorkflowDefinition`
- `CancelStepDefinition`

Workflow termination uses `*WorkflowDefinition`.
Step primitives use `*StepDefinition`.

Reusable concerns shared across definitions are:

- `PrimitiveMetadata`
- `Timing`
- `PayloadMapping`

Primitive-specific concerns stay as direct fields on the corresponding definition.

## Structure

```java
public record PrimitiveMetadata(String stepName, EventNameCustomizer eventNameCustomizer) {}
public record Timing(Duration timeout) {}
public record PayloadMapping(PayloadReducer parameterPayloadReducer,
                             PayloadReducer resultPayloadReducer) {}

public record ExecuteStepDefinition(PrimitiveMetadata primitiveMetadata,
                                    Map<String, Object> inputPayload,
                                    PayloadProcessor action,
                                    PayloadMapping payloadMapping,
                                    Timing timing,
                                    RetryPolicy retryPolicy) {}
public record WaitForStepDefinition(PrimitiveMetadata primitiveMetadata,
                                    EventCondition eventCondition,
                                    PayloadMapping payloadMapping,
                                    Timing timing) {}
public record PayloadStepDefinition(PrimitiveMetadata primitiveMetadata,
                                    PayloadModification modification) {}
public record FailWorkflowDefinition(PrimitiveMetadata primitiveMetadata,
                                     Throwable cause) {}
public record CancelWorkflowDefinition(PrimitiveMetadata primitiveMetadata,
                                       Throwable cause) {}
public record CancelStepDefinition(PrimitiveMetadata primitiveMetadata,
                                   Throwable cause) {}
```

## Fluent Copy API

Definitions and reusable value objects may expose copy-style methods that return modified records.
Common nested properties may be flattened on top-level definitions for fluent authoring.

## API Shape

```java
void fail(FailWorkflowDefinition definition);
void cancel(CancelWorkflowDefinition definition);
void cancelStep(CancelStepDefinition definition);
WorkflowStepResult execute(ExecuteStepDefinition stepDefinition);
Map<String, Object> awaitExecute(ExecuteStepDefinition stepDefinition);
WorkflowStepResult waitForEvent(WaitForStepDefinition stepDefinition);
Map<String, Object> awaitEvent(WaitForStepDefinition stepDefinition);
WorkflowStepResult modifyPayload(PayloadStepDefinition stepDefinition);
void awaitModifyPayload(PayloadStepDefinition stepDefinition);
```

## Rules

- Do not introduce wrapper types for fields used by only one primitive.
- Do not use `StepDefinition` or `WorkflowDefinition` naming for reusable member types.
- Keep runtime primitive commands independent from this DSL composition.
- Definitions are handled directly in the DSL base and translated there into runtime commands.
- DSL convenience methods may exist only as adapters that build these definitions.

## Consequences

- `execute`, `waitForEvent`, and `modifyPayload` share one DSL composition model.
- `fail`, `cancel`, and `cancelStep` follow the same definition-based model.
- Shared concerns have one DSL representation.
- Definitions are the stable bridge between DSL convenience methods and runtime commands.
