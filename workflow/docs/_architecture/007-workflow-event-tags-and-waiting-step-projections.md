# ADR-007: Workflow Event Tags And Waiting-Step Projection Payloads

- Status: Accepted
- Date: 2026-07-08

## Context

[ADR-006](./006-serializable-association-strings.md) makes running wait conditions serializable and inspectable.
The next step is to make engine-published workflow events directly usable for event-sourced projections.

Two projections are the primary driver:

- `workflow executions`: all workflow instances that started and have not yet reached a terminal workflow state
- `waiting steps`: all `waitForEvent` steps that started and have not yet reached a terminal wait-step state

Today that information is only implicit in payloads and metadata. There is no stable event-store tagging contract for
querying the relevant subsets, and the `waitForEvent` started event does not yet carry enough replayable details for a
waiting-steps projection.

## Decision

All workflow-relevant events published by the engine get AF5 event-store tags through a central `TagResolver`.

### Base workflow tag

If an engine-published event carries `workflowId` metadata, it must also get this tag:

- `workflowId=<metadata workflowId>`

### Workflow lifecycle tag

Workflow lifecycle events get this marker tag:

- `workflowLifecycle=started`
- `workflowLifecycle=terminal`

This applies to:

- `WorkflowStarted`
- `WorkflowCompleted`
- `WorkflowFailed`
- `WorkflowTimedOut`
- `WorkflowCancelled`

### Wait-step lifecycle tag

`waitForEvent` step lifecycle events get this marker tag:

- `workflowWait=started`
- `workflowWait=terminal`

This applies to the started event of a `waitForEvent` step and to its terminal events.

### Wait-step started payload

The started event payload of a `waitForEvent` step must contain:

- `startTime`
- `eventQualifiedName`
- `serializedAssociations`
- `timeoutTime`

`timeoutTime` is the calculated absolute timeout instant, not just the relative duration.

### Scope

The tags are primarily for running-instance projections.
They may also be reused by configuration-repository or reporting use cases, but that reuse is optional.

## Consequences

- Workflow lifecycle and waiting-step projections can be built by reading tagged event subsets instead of replaying
  the full workflow stream.
- Tag resolution stays centralized in AF5 event-store configuration rather than being scattered across event emitters.
- `waitForEvent` started events become self-describing enough for a waiting-steps projection to rebuild outstanding
  waits, including event type, serialized associations, and deadline.
- Wait-step events now carry a small internal marker in metadata so the tag resolver can distinguish them from other
  step events.
