# ADR-004: Workflow Replay Safe-Point Tracking Tokens

- Status: Accepted
- Date: 2026-05-18

## Context

The feature sketch in [#175](https://github.com/AxonIQ/extension-workflow/issues/175) aims to avoid replaying the full
event stream on every application
start. Today `WorkflowEventProcessingRegistrationEnhancer` resets the workflow processor to the first token whenever
replay is needed, and `WorkflowExecutionRepository` only keeps in-memory executions for the current process.
We need one restart tracking token per active workflow execution and one durable safe-point tracking token per workflow
engine.

## Decision

We define two internal token concepts:

- Execution restart tracking token: the earliest `TrackingToken` a workflow execution needs for restart.
- Engine safe-point tracking token: the safe-point for one workflow event processor, stored separately from the
  processor's normal token store state.

Execution restart tracking token is an immutable attribute of `SimpleWorkflowExecution`, captured during construction
from the start event's `ProcessingContext`, and exposed through an internal getter on `WorkflowExecution`. Engine
safe-point tracking tokens are persisted through a dedicated workflow-safe-point store. The default implementation may be
backed by Axon `TokenStore`, but it must use a separate processor name or dedicated component so it never overwrites the
workflow processor's regular tracking token.

The engine safe-point tracking token is computed as follows:

- When the first execution starts, store its restart tracking token as the engine safe-point tracking token.
- When additional executions start, keep the lower bound of all active execution restart tracking tokens.
- When an execution terminates and is removed, recompute the lower bound across the remaining active executions.
- When no active executions remain, store the workflow engine's current tracking token as the engine safe-point tracking
  token.
  On startup, the replay reset hook fetches the engine safe-point tracking token and resets the workflow processor to
  that token.
  If no engine safe-point tracking token exists yet, startup falls back to the current behavior.

## Scenarios

```gherkin
Scenario: first workflow start initializes the safe-point
Given the workflow engine has no active executions
When a new workflow starts from event token 18
Then the execution stores restart tracking token 18
And the engine safe-point tracking token becomes 18

Scenario: additional workflow keeps the earliest restart tracking token
Given active workflow executions started at tokens 18 and 134
When another workflow starts at token 55
Then the engine safe-point tracking token remains 18

Scenario: finishing a non-earliest workflow keeps the current safe-point tracking token
Given active workflow executions started at tokens 18, 55, and 134
When the workflow that started at token 55 terminates
Then the engine safe-point tracking token remains 18

Scenario: finishing the earliest workflow advances to the next earliest active execution
Given active workflow executions started at tokens 18, 55, and 134
When the workflow that started at token 18 terminates
Then the engine safe-point tracking token becomes 55

Scenario: finishing the last workflow advances to the current engine tracking token
Given one active workflow execution started at token 18
And the workflow engine currently processes token 191
When that workflow terminates
Then the engine safe-point tracking token becomes 191

Scenario: stored safe-point tracking token is used for replay on startup
Given stored safe-point tracking token 18
When workflow engine starts and the latest token in the event store is 192
Then the engine executes a reset to token 18

Scenario: replay on startup falls back to the first event-store token if no safe-point tracking token exists
Given no stored safe-point tracking token
When workflow engine starts and the latest token in the event store is 192
Then the engine executes a reset to the first token from the event source
```

## Implementation Plan

1. Extend the internal workflow execution contract with a restart tracking token getter and implement it in
   `SimpleWorkflowExecution`.
2. Add an immutable restart tracking token field to `SimpleWorkflowExecution` and initialize it in the constructor
   from the supplied start-event `ProcessingContext`.
3. Introduce a `SafePointStore` abstraction with a default `TokenStore`-backed implementation and
   default registration in runtime and Spring Boot configuration.
4. Persist the first execution restart tracking token when the first workflow starts, update the persisted lower bound
   when more executions start, and recompute it when executions terminate.
5. Track the workflow engine's current processing `TrackingToken` so the last terminating execution can advance the
   engine safe-point tracking token to that current token instead of clearing it.
6. Add a safe-point update in `WorkflowEngine` that persists the engine safe-point tracking token after workflow creation,
   workflow removal, and engine shutdown.
7. Update `WorkflowEventProcessingRegistrationEnhancer` to reset from the stored engine safe-point tracking token and to
   fall back to the current behavior only when no engine safe-point tracking token exists yet.
8. Add tests for per-execution restart tracking token capture, safe-point recomputation, startup reset selection, and
   empty-engine behavior; verify with runtime tests plus `examples/simple`, and add Spring Boot coverage if token-store
   wiring changes.

## Consequences

- Restart replay cost becomes proportional to the oldest active workflow, not the full event history.
- The design stays additive: existing workflow definitions and DSL APIs do not change.
- Safe-point persistence remains replaceable because the engine depends on its own abstraction, not directly on one
  `TokenStore` layout.
- The engine safe-point tracking token is normally driven by active execution restart tracking tokens and advances to the
  current processor tracking token only when the last active workflow finishes.
- When a safe-point tracking token is stale, the system may replay more events than necessary, but it still reconstructs
  correct
  workflow state.
