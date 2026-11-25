# ADR-024: Workflow Snapshots by Default

- Name: Workflow Snapshots by Default
- Status: Proposed
- Date: 2026-11-24

## Context

The workflow engine sources three event-sourced entities:

- `EventSourcedWorkflowState`, the state of one workflow instance, see
  [ADR-009](./009-event-sourced-workflow-state-rehydration.md)
- `EventSourcedRunningWorkflows`, the list of running workflows, see
  [ADR-008](./008-running-workflows-event-sourced-entity.md)
- `EventSourcedWaitAssociations`, the list of open waits

Without a snapshot, every load of an entity replays its full event history. The engine loads the state of a workflow
when it restores it or wakes it from hibernation. A segment claim without a workflow index
([ADR-023](./023-workflow-index.md)) loads both lists. Both lists grow with every workflow the application runs.

Snapshots are an optimization. The events stay the source of truth, and a missing snapshot only costs time. An
application that has a `SnapshotStore` has what it takes to store them.

Axon Framework's snapshotting entity module fails the start of an application that has a snapshot policy and no
`SnapshotStore`. So the engine cannot give every entity a snapshot policy without a check.

## Decision

By default, the engine snapshots the state of each workflow instance when the application can take snapshots. It
snapshots the two lists only when snapshots are enabled.

Each list is 1 entity that holds every segment. A segment claim with a workflow index does not load it, so its snapshot
would only serve a claim without one.

### Three states of `enabled`

`WorkflowSnapshotConfiguration.enabled` has three states:

| `enabled` | Result |
|---|---|
| not set, the default | The workflow state is snapshotted when the application can take snapshots. Otherwise it is sourced without snapshots, and the application starts. The two lists are not snapshotted. |
| `true` | All three entities are snapshotted. An application that cannot take snapshots fails to start. |
| `false` | No snapshot is taken. |

The application can take snapshots of an entity when it has a `SnapshotStore` component and a `GeneralConverter`
component, and its `MessageTypeResolver` resolves the type of the entity.

`WorkflowConfigurer.snapshots(...)` sets the state and the `SnapshotPolicy` of each entity. In Spring Boot,
`axon.workflow.snapshots.enabled` has the same three states: not set, `true` and `false`. Each policy defaults to a
snapshot after 50 sourced events.

### Decided when the entity module is built

`WorkflowEntityModule` is the module of each of the three entities. It decides whether its entity is snapshotted when
it is built. At that point every `ConfigurationEnhancer` has run, so a `SnapshotStore` registered by any enhancer or
Spring bean counts, whatever its order. The check creates neither the `SnapshotStore` nor the `GeneralConverter`. It
creates only the `MessageTypeResolver`, to ask it.

The first case that fits wins:

1. `enabled` is `true`: the entity is snapshotted with its policy.
2. `enabled` is `false`: the entity is not snapshotted.
3. A `SnapshotPolicy` component is registered under the deprecated name of the entity in
   `WorkflowConfigurationDefaults`: the entity is snapshotted with that policy, and a warning is logged.
4. The entity is one of the two lists: it is not snapshotted.
5. The application can take snapshots: the workflow state is snapshotted with its policy.
6. Otherwise: the workflow state is not snapshotted.

### A failed snapshot never fails a load

A snapshotted entity stores its snapshots through `FailSafeSnapshotStore`. Axon Framework stores a snapshot while it
sources the entity, and logs a store failure that is reported through the returned future. `FailSafeSnapshotStore`
reports every failure through that future, also when the `SnapshotStore` throws. So a snapshot that fails to store
never fails the load of the entity.

### Alternatives considered

- **Snapshots only when the application enables them.** Rejected. An application with a `SnapshotStore` then replays
  its full history on every load, unless it knows the setting.
- **Snapshots always on.** Rejected. An application without a `SnapshotStore` then fails to start.
- **Deciding in the configuration enhancer.** Rejected. A `SnapshotStore` that another enhancer or a Spring bean
  registers is not visible to every enhancer, so the result depends on the order of the enhancers.
- **All three entities by default.** Rejected. A list snapshot holds every segment, so it grows with every running
  workflow and open wait of the application. With a workflow index, a claim reads the rows of its own segment and
  does not load the lists.

## Consequences

- An application that has a `SnapshotStore` stores snapshots of the workflow state without any setting. For such an
  application this is a change of behavior on upgrade. `snapshots(s -> s.enabled(false))` or
  `axon.workflow.snapshots.enabled=false` takes no snapshots.
- An application without a `SnapshotStore` starts and runs without snapshots.
- `enabled(true)` states that snapshots are required: an application that cannot take them fails to start.
- A snapshot is taken while an entity is sourced, so the first load of an entity with snapshots on still replays its
  full history.
- Snapshots are extra data in the `SnapshotStore`. Events, tokens and the rows of a workflow index are not affected.
- The three named `SnapshotPolicy` components of `WorkflowConfigurationDefaults` are deprecated. Each still applies
  while `enabled` is not set, and is ignored with a warning once `enabled` is `true` or `false`.
