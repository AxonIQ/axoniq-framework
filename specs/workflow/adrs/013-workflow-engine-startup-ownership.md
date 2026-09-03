# ADR-013: Workflow Engine Startup Ownership

- Name: Workflow Engine Startup Ownership
- Status: Accepted
- Date: 2026-07-28

## Context

[ADR-009](./009-event-sourced-workflow-state-rehydration.md) proposes event-sourced workflow rehydration.
Its startup sequence exposed a processing context from `WorkflowEventProcessingRegistrationEnhancer` to
`WorkflowEngine`. The enhancer consequently owned the startup unit of work, while the engine owned the work performed
inside it.

Those operations form one engine lifecycle transition. Exposing them separately lets configuration code choose an
incomplete or incorrectly ordered sequence. Passing the sourcing context across that boundary also obscures its
short-lived transaction and lifecycle ownership.

## Decision

`WorkflowEngine.start(TrackingToken, boolean)` owns the startup unit of work and startup of restored workflow
executions. It:

1. Creates the short-lived sourcing unit of work.
2. Prepares a child execution context from the processor token.
3. Loads the running-workflow projection and each workflow's durable state with the sourcing context.
4. Creates and initializes live workflow executions with the execution context.
5. Starts those executions before replay catch-up so transient wait registrations are rebuilt.
6. Switches to live mode within the sourcing unit of work when replay is unnecessary.

The registration enhancer remains responsible for event-processor concerns. It determines the processor and latest
tokens, initializes replay tracking, determines whether replay is required, and invokes `WorkflowEngine.start(...)`.
It does not create, receive, or reuse a workflow startup processing context.

Startup uses two different processing contexts for sourcing durable state and creating restored executions. The sourcing
context may carry event-store transactions and lifecycle handlers, so it must not be retained by asynchronously running
workflow bodies. The child execution context becomes the parent of restored workflow bodies and is retained for their
lifetime.
The individual state-load and restored-execution-start operations are implementation details of `WorkflowEngine`.

## Consequences

- The processor lifecycle hook has one engine-start operation and no startup-context plumbing.
- Restored workflows consistently rebuild transient state before replay catch-up.
- The sourcing context cannot outlive its unit of work or be reused by configuration code.
- The engine remains responsible for the required separation between sourcing and long-lived execution contexts.
