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

The separate engine `SafePointStore` model from ADR-004 is removed and the entire ADR-004 is deprecated.

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

The workflow engine requests processor checkpoints eagerly, but completes checkpoint advancement only when workflow work
is safe with respect to durable state.

The event-streaming framework guarantees at most one in-flight checkpoint advancement per segment: it does not invoke
`onCheckpointAdvanced(...)` again until the previously returned future has completed. The engine uses that contract to
simplify checkpointing:

- before a trigger is available, newer tracking tokens are coalesced in a local pending token
- once a trigger is available, checkpoint requests are forwarded immediately to the processor
- the processor, not the engine, serializes repeated checkpoint requests for the segment
- queue-drain waiting happens only inside `onCheckpointAdvanced(...)`

For one execution, checkpoint advancement is safe when:

- no workflow task is currently active
- no workflow task is still queued
- no queued checkpoint barrier remains

An execution waiting in `waitForEvent` with an empty queue is checkpoint-safe. Wait registrations, timeout futures, and
other transient runtime structures are rebuilt on restore and do not by themselves require replay from the workflow
start token.

The completion-side wait must still re-check queue safety after each barrier task runs. Consuming one barrier task does
not by itself prove quiescence, because an earlier workflow task may append more work behind that barrier. The engine
therefore repeats the barrier-and-recheck cycle until the workflow queues are actually drained.

### Checkpoint work index

Checkpoint advancement must not scan every live workflow execution to discover whether one has queued or active work.
With N live workflows, a scan makes the common checkpoint-safety check O(N). If work is pending, selecting executions
for barriers can cause a second O(N) scan. Many long-running workflows are normally waiting with empty task queues, so
the number of unsafe workflows is commonly much smaller than N. Repeated full scans would make processor checkpointing
the bottleneck as the number of concurrent workflows grows.

The engine therefore owns a transient checkpoint work index containing workflow identifiers that may still be unsafe
for checkpoint advancement. The repository remains responsible only for storing and looking up executions. Checkpoint
safety is an engine coordination concern and must not become repository state.

Each execution's checkpoint support updates the index when it transitions between safe and unsafe queue states. The
index is deliberately conservative:

- an execution must be indexed before it can expose queued, active, or barrier work
- an execution may remain indexed briefly after becoming safe
- an execution must never be absent while it has unsafe work

The common safety check is consequently O(1): the engine checks whether the index is empty. When a barrier is needed,
the engine snapshots the indexed identifiers and resolves them through the repository, producing O(P) work for P
unsafe executions. Stale identifiers are harmless because the execution is checked again before a barrier is appended.
Removed and shut-down executions are removed from the index.

## Consequences

- Replay cost becomes proportional to the backlog after the last confirmed processor checkpoint, not to the oldest
  active workflow start event.
- Event-sourced workflow rehydration remains the source of durable workflow state.
- The old safe-point-store architecture is no longer the target design for replay positioning.
- Checkpoint safety depends on queue drain and active task completion, not on workflow age.
- Engine-side in-flight checkpoint tracking is not needed; checkpoint waiting is concentrated in
  `onCheckpointAdvanced(...)`, and request serialization is delegated to the processor.
- Catch-up mode remains necessary so restored waits can observe events that arrived during downtime before the engine
  becomes fully live.
- Checkpoint coordination has O(1) common-path cost and O(P) barrier cost, where P is the number of unsafe workflow
  executions rather than the number of all live executions.
- Workflow execution queue transitions must notify the engine-owned checkpoint work index.
