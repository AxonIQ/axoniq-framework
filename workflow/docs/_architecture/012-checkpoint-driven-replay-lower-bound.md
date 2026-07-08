# ADR-012: Checkpoint-Driven Replay Lower Bound

- Status: Accepted
- Date: 2026-07-08

## Context

[ADR-004](./004-workflow-replay-safe-point-tokens.md) introduced a separate engine safe-point store and defined the
replay lower bound as the earliest restart token of the active workflow executions.

[ADR-008](./008-running-workflows-event-sourced-entity.md) and
[ADR-009](./009-event-sourced-workflow-state-rehydration.md) changed startup in an important way:

- `RunningWorkflows` now provides the set of active workflow identifiers
- `EventSourcedWorkflowState` now provides the durable per-workflow state used for rehydration

That means restart no longer needs the oldest workflow start event in order to rebuild workflow state itself.

The remaining replay requirement is narrower. After downtime, the engine must only catch up the events after the last
durable processor position so restored waits can observe missed external events.

The old safe-point-store model is too pessimistic for this architecture. If one long-running workflow started far in the
past, replay restarts from that early token even when all durable workflow state is already reconstructable from
event-sourced workflow data. This can turn a short downtime backlog into a very long replay.

## Decision

The separate engine `SafePointStore` model from ADR-004 is deprecated.

The workflow processor's checkpointed tracking token becomes the sole replay lower bound.

### Durable sources

Startup uses two durable sources with distinct responsibilities:

- the processor token store provides the replay lower bound
- event-sourced workflow state provides the durable workflow state to restore

`RunningWorkflows` determines which workflow identifiers to restore. `EventSourcedWorkflowState` reconstructs the state
of each restored workflow. The processor checkpoint determines only how much backlog must still be replayed after those
executions are rebuilt.

### Startup flow

Workflow startup follows this sequence:

1. Read the workflow processor token from the normal processor token store.
2. Read the latest token from the event source.
3. Rehydrate active workflows from `RunningWorkflows` plus `EventSourcedWorkflowState`.
4. If the processor token already equals the latest token, switch directly to live mode.
5. Otherwise, start restored executions in checkpoint catch-up mode before replay resumes.
6. Replay only the events after the stored processor checkpoint until the latest token is reached.
7. Switch to live mode when catch-up reaches the startup latest token.

Checkpoint catch-up mode exists so restored waits and other transient runtime structures are recreated before backlog
events are delivered.

### Checkpoint advancement rule

The workflow engine advances the processor checkpoint only when workflow work is safe with respect to durable state.

For one execution, checkpoint advancement is safe when:

- no workflow task is currently active
- no workflow task is still queued
- no queued checkpoint intent remains

An execution waiting in `waitForEvent` with an empty queue is checkpoint-safe. Wait registrations, timeout futures, and
other transient runtime structures are rebuilt on restore and do not by themselves require replay from the workflow
start token.

If an external event has been observed but its workflow consequence is still in-flight through the task queue,
checkpoint advancement must wait. The engine should request another checkpoint attempt after the queue drains instead of
falling back to the earliest workflow start token.

## Consequences

- Replay cost becomes proportional to the backlog after the last confirmed processor checkpoint, not to the oldest
  active workflow start event.
- Event-sourced workflow rehydration remains the source of durable workflow state.
- The old safe-point-store architecture is no longer the target design for replay positioning.
- Checkpoint safety depends on queue drain and active task completion, not on workflow age.
- Catch-up mode remains necessary so restored waits can observe events that arrived during downtime before the engine
  becomes fully live.
