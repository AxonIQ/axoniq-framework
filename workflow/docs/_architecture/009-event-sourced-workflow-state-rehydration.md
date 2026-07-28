# ADR-009: Event-Sourced Workflow State Rehydration

- Status: Accepted
- Date: 2026-07-08

## Context

[ADR-004](./004-workflow-replay-safe-point-tokens.md) already established that workflow restart needs a durable replay
position.

[ADR-007](./007-workflow-event-tags-and-waiting-step-projections.md) introduced workflow event tags, and
[ADR-008](./008-running-workflows-event-sourced-entity.md) introduced `RunningWorkflows` as the event-sourced set of
workflow identifiers that are still active.

The design of `SimpleWorkflowExecution` contains of a transient and a non-transient part. 

The transient runtime machinery such as task queues, futures, wait registrations, and timer scheduling state. 
Those structures should be reconstructed, not serialized as the primary source of truth.

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

`RunningWorkflows` remains a separate singleton event-sourced entity that tracks which workflow identifiers are still
active. It is the startup index, not the holder of per-workflow state.

### Workflow definition identity

Workflow lifecycle events carry a stable `WorkflowDefinitionId` containing:

- the workflow definition name
- the workflow definition version

`EventSourcedWorkflowState` stores this identifier as part of sourced state. Startup uses it to resolve the exact
workflow configuration that must recreate the execution.

### Startup order

Workflow startup follows this sequence:

1. Load `RunningWorkflows` from its event-sourced repository.
2. For each running workflow identifier, load `EventSourcedWorkflowState` from the per-workflow event-sourced
   repository.
3. Resolve the matching workflow configuration from the sourced `WorkflowDefinitionId`.
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

The initial implementation used separate sourcing and execution contexts. [ADR-013](./013-workflow-engine-startup-ownership.md)
proposes consolidating startup ownership in `WorkflowEngine` and using the startup unit of work's processing context for
both state sourcing and restored execution creation.

### No checkpoint state images

This design does not store workflow state images, execution checkpoints, or projection snapshots as the primary restore
source.

Durability comes from event sourcing of `EventSourcedWorkflowState` plus the replay safe point used to resume the
streaming processor.

## Consequences

- Workflow restart is based on event-sourced workflow state, not serialized `SimpleWorkflowExecution` images.
- `RunningWorkflows` provides the list of active identifiers, while `EventSourcedWorkflowState` provides per-workflow
  durable state.
- Transient runtime parts remain reconstructable from sourced step state and workflow re-entry.
- Backlog events that arrive during downtime can complete restored waits during catch-up before the engine switches to
  live mode.
- Startup now fails fast if `RunningWorkflows` references an identifier whose workflow state cannot be sourced or whose
  `WorkflowDefinitionId` no longer resolves to a registered workflow definition.
