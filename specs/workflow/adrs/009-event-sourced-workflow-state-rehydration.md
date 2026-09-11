# ADR-009: Event-Sourced Workflow State Rehydration

- Status: Accepted
- Date: 2026-07-08

## Context

[ADR-004](./004-workflow-replay-safe-point-tokens.md) already established that workflow restart needs a durable replay
position.

[ADR-007](./007-workflow-event-tags-and-waiting-step-projections.md) introduced workflow event tags, and
[ADR-008](./008-running-workflows-event-sourced-entity.md) introduced `RunningWorkflows` as the abstraction for the
set of workflow identifiers that are still active. Its default implementation is event sourced.

The design of `SimpleWorkflowExecution` consists of transient and non-transient parts.

The transient runtime machinery includes task queues, futures, wait registrations, and timer scheduling state. Those
structures should be reconstructed, not serialized as the primary source of truth.

The durable state we actually need is the workflow execution state itself: payload, workflow status, definition
identity, step states, and other event-sourced facts owned by a single workflow instance.

## Decision

We persist workflow execution state as an AF5 event-sourced entity and rehydrate workflow executions from that sourced
state at startup.

### Durable state model

Each workflow instance has an `EventSourcedWorkflowState` entity loaded through AF5's declarative
`EventSourcedEntityModule` infrastructure.

The repository criteria select workflow-owned events by `workflowId`, using the workflow event tags introduced earlier.
This means the durable state of a workflow instance is reconstructed only from that workflow's own event stream slice.

`RunningWorkflows` remains the separate startup index of workflow identifiers that are still active. The default
`EventSourcedRunningWorkflows` implementation is a singleton event-sourced entity; the abstraction does not require
event sourcing. It is not the holder of per-workflow state.

### Workflow definition identity

Workflow lifecycle events carry the current workflow definition reference as an Axon Framework `VersionedType` containing:

- the workflow definition name
- the workflow definition version

`EventSourcedWorkflowState` stores this `VersionedType` as part of sourced state. Its version advances when a workflow
records a version migration, so it identifies the effective definition reference for the current execution state.
Startup uses it to resolve the workflow configuration that must recreate the execution.

### Startup order

Workflow startup follows this sequence:

1. Load `RunningWorkflows` from the workflow store. The default store obtains it from its event-sourced repository.
2. For each running workflow identifier, load `EventSourcedWorkflowState` from the per-workflow event-sourced
   repository.
3. Resolve the matching workflow configuration from the sourced workflow definition `VersionedType`.
4. Create a fresh workflow context and workflow execution.
5. Inject the sourced workflow state into that fresh execution and register it in `WorkflowExecutionRepository`.
6. Execute the restored workflow once before replay catch-up continues.
7. Let the streaming processor catch up from the stored safe point to the latest token.
8. Switch the engine to live mode after replay has completed.

### Rehydration rule

Rehydration restores only durable workflow facts. Transient execution parts are rebuilt by recreating a fresh execution
instance and re-entering the workflow definition.

This re-entry must happen before replay catch-up resumes, because that pass reconstructs:

- wait registrations for `waitForEvent`
- timeout scheduling decisions
- in-memory task queues and step execution bookkeeping
- other runtime-only delegates derived from the sourced step state

Executing restored workflows before catch-up ensures backlog events that arrived while the application was down are
routed to rebuilt waits instead of being lost behind an uninitialized transient runtime.

### Processing context

[ADR-013](./013-workflow-engine-startup-ownership.md) assigns the complete startup lifecycle to `WorkflowEngine`.
It creates a short-lived sourcing context for state loading and a separate child execution context for restored workflow
creation. The execution context remains valid after the sourcing unit of work completes.

### No checkpoint state images

This design does not store workflow state images, execution checkpoints, or projection snapshots as the primary restore
source.

Durability comes from event sourcing of `EventSourcedWorkflowState` plus the replay safe point used to resume the
streaming processor.

## Consequences

- Workflow restart is based on event-sourced workflow state, not serialized `SimpleWorkflowExecution` images.
- `RunningWorkflows` provides the list of active identifiers, while `EventSourcedWorkflowState` provides per-workflow
  durable state. The default `RunningWorkflows` implementation is `EventSourcedRunningWorkflows`.
- Transient runtime parts remain reconstructable from sourced step state and workflow re-entry.
- Backlog events that arrive during downtime can complete restored waits during catch-up before the engine switches to
  live mode.
- Startup now fails fast if `RunningWorkflows` references an identifier whose workflow state cannot be sourced or whose
  workflow definition `VersionedType` no longer resolves to a registered workflow definition.
