# ADR-013: Workflow Engine Startup Ownership

- Name: Workflow Engine Startup Ownership
- Status: Proposed
- Date: 2026-07-28

## Context

[ADR-009](./009-event-sourced-workflow-state-rehydration.md) establishes event-sourced workflow rehydration.
Its startup sequence exposed individual engine operations to
`WorkflowEventProcessingRegistrationEnhancer`: seeding the safe point, loading workflow state, and starting restored
executions.

Those operations form one engine lifecycle transition. Exposing them separately lets configuration code choose an
incomplete or incorrectly ordered sequence. The former two-context arrangement also introduced a configuration-backed
execution context before there was a demonstrated need for one.

## Decision

`WorkflowEngine.start(TrackingToken, ProcessingContext)` owns startup of restored workflow executions. It:

1. Initializes the engine replay safe point.
2. Loads the running-workflow projection and each workflow's durable state.
3. Creates and initializes live workflow executions.
4. Starts those executions before replay catch-up so transient wait registrations are rebuilt.

The registration enhancer remains responsible for event-processor concerns. It determines the reset token, invokes
`WorkflowEngine.start(...)` inside its startup unit of work, then either resets the processor for replay or switches the
engine to live mode when replay is unnecessary.

Startup uses the same processing context for sourcing durable state and creating restored executions. A separate
execution context will be introduced only if a concrete lifetime or component-isolation requirement arises.

The individual state-load and restored-execution-start operations are implementation details of `WorkflowEngine`.

## Consequences

- The processor lifecycle hook has one engine-start operation instead of coordinating several engine internals.
- Restored workflows consistently rebuild transient state before replay catch-up.
- The startup path has one processing context and no speculative configuration-backed context.
- A future need for separate contexts can extend `WorkflowEngine.start(...)` with an evidence-backed requirement.
