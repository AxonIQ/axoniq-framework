# ADR-024: Workflow-State Snapshots by Default

- Name: Workflow-state snapshots by default
- Status: Proposed
- Date: 2026-11-21

## Context

[ADR-009](./009-event-sourced-workflow-state-rehydration.md) restores each running workflow instance by sourcing its
`EventSourcedWorkflowState` from its own events, and does not use state images as the primary restore source. Since
5.4.2, that entity can be snapshotted through Axon Framework's snapshotting entity module, but only when an application
enables snapshots explicitly (`WorkflowConfigurer#snapshots`, or `axon.workflow.snapshots.enabled=true`). Most
applications do not, so every claim, restart and (with hibernation) every wake replays an instance's full history.

Axon Framework's snapshotting entity module fails the start of an application unless it has a `SnapshotStore`, a
`GeneralConverter`, and a `MessageTypeResolver` that resolves the entity type. Axon Server and PostgreSQL register a
`SnapshotStore`; in-memory and plain JPA setups usually have none. A default must therefore never fail a start.

## Decision

The state of each workflow instance is snapshotted by default, with `SnapshotPolicy.afterEvents(50)`, when the
application can take snapshots. `WorkflowSnapshotConfiguration#enabled` becomes tri-state:

- Not set (the default): the workflow-state policy (`afterEvents(50)` unless configured) is used when the application
  has a `SnapshotStore`, a `GeneralConverter`, and a `MessageTypeResolver` that resolves `EventSourcedWorkflowState`.
  Otherwise the state is sourced without snapshots, and the application starts as usual.
- `true`: both the running-workflow list and the workflow state are snapshotted, as before. An application that cannot
  take snapshots fails to start, as before.
- `false`: no snapshot is taken.

The running-workflow list (`EventSourcedRunningWorkflows`) stays opt-in: with a workflow index
([ADR-023](./023-workflow-index.md)) claims rarely source it.

The `WorkflowSnapshotConfiguration` component is the single source of truth for both entities, apart from the
deprecated named policies below, which it only leaves room for while `enabled` is unset.
`WorkflowConfigurer#snapshots` only registers that component; a later call replaces the configuration of an earlier
one. Spring Boot's auto-configuration only provides it as a bean from the `axon.workflow.snapshots.*` properties, unless
the application defines its own.

The named `SnapshotPolicy` components that enabled snapshots in 5.4
(`WorkflowConfigurationDefaults#RUNNING_WORKFLOWS_SNAPSHOT_POLICY` and `#WORKFLOW_STATE_SNAPSHOT_POLICY`) are
deprecated, and are only read while the `WorkflowSnapshotConfiguration` leaves `enabled` unset. Then a component under
either name snapshots its entity with its policy, as in 5.4.2 (including the running-workflow list, and failing the
start when the application cannot take snapshots), and a warning points to `WorkflowConfigurer#snapshots`. Such a
component takes precedence over the configuration's policy of its entity, including one set through
`WorkflowConfigurer#snapshots` or the `axon.workflow.snapshots.*.after-events` properties, and the warning says so. An
application that registered them in 5.4 thus keeps its snapshots after upgrading. An explicit `enabled(true)` uses the
policies of the configuration, and an explicit `enabled(false)` takes no snapshot; in both cases, a named component is
ignored with a warning, so an explicit setting is never ambiguous.

The decision is taken when each entity module is built, after every configuration enhancer ran, so a `SnapshotStore`
registered by a connector, by multi-tenancy, by the application or as a Spring bean counts whatever its enhancer order.
Checking for the `SnapshotStore` and the `GeneralConverter` does not create them (creating an Axon Server snapshot
store opens a connection); the `MessageTypeResolver` is created to ask it.

A snapshotted entity stores its snapshots through a wrapper of the application's `SnapshotStore` that turns an
exception thrown while storing into a failed future. Axon Framework logs a failed future and keeps the loaded entity,
but an exception thrown from `store` (for example, by a store that converts the snapshot before returning) would fail
the load. A snapshot is an optimization, so failing to store one must never fail a load.

Spring Boot's `axon.workflow.snapshots.enabled` follows the same three states: unset, `true` or `false`. Any value
Spring converts to a `Boolean` (such as `yes` or `on`) counts. `WorkflowProperties.SnapshotProperties` keeps its 5.4
`isEnabled()` (deprecated, `true` only when explicitly enabled) and `setEnabled(boolean)`, next to the tri-state
`getEnabled()` / `setEnabled(Boolean)` that Spring Boot binds.

The snapshot key stays the plain workflow id under the resolved name of `EventSourcedWorkflowState`. State events are
tagged by workflow id only, so a snapshot is correct whichever workflow module sources it; a key per module would only
orphan the snapshots that 5.4.2 applications already stored.

ADR-009 remains accepted: event sourcing stays the source of truth, and a snapshot is only a shortcut AF's snapshotting
entity module takes while sourcing, followed by the events after it.

## Consequences

- Applications with a `SnapshotStore` restore and wake long-running workflows from a snapshot plus a short tail, without
  any configuration.
- Applications on Axon Server or PostgreSQL start writing workflow-state snapshots after upgrading. The first restore
  of an instance still replays its full history, and stores the first snapshot.
- Applications without a `SnapshotStore`, or whose resolver cannot name the workflow state, behave as before.
- `axon.workflow.snapshots.enabled=false`, or `snapshots(s -> s.enabled(false))`, restores the previous default.
- Sourcing from a snapshot keeps the DCB consistency marker of [ADR-015](./015-dcb-append-conditions.md): a node that
  restored an instance from a snapshot and a stale node running the same instance still record each fact once.
- An application that registered a named workflow `SnapshotPolicy` component in 5.4 keeps snapshotting with it, with a
  deprecation warning at startup, until it configures its snapshots through `WorkflowConfigurer#snapshots` or a
  `WorkflowSnapshotConfiguration` instead. While `enabled` is unset, the named component's policy wins over the
  configuration's policy of its entity. Setting `enabled` explicitly makes the named components ignored.
- A 5.4.2 application that disabled snapshots (`enabled(false)` or `axon.workflow.snapshots.enabled=false`) but also
  registered a named workflow `SnapshotPolicy` component was snapshotted with that policy in 5.4.2, and no longer is.
- A 5.4.2 Spring Boot application that set `axon.workflow.snapshots.enabled=true` and replaced the auto-configured
  named `SnapshotPolicy` bean with its own now gets the policy of the `axon.workflow.snapshots.*.after-events`
  properties instead of its bean's.
- The wrapper of the `SnapshotStore` is registered in each entity module, so decorators the application applies to
  every `SnapshotStore`, such as tracing, may wrap the workflow entities' snapshot store a second time.
