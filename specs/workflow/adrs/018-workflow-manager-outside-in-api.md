# ADR-018: Workflow Manager Outside-In API

- Name: Workflow manager outside-in API
- Status: Proposed
- Date: 2026-09-02

## Context

Workflow definitions operate from the inside out through `WorkflowContext`. Applications also need an outside-in
surface for support tooling, administration, and application services: find workflow instances, inspect their state,
and request an operation such as cancellation.

`WorkflowCancellationService` is currently a temporary internal, live-execution registry. It is not a user API and
cannot find historic workflows. It has no external dependants, so retaining it as a compatibility layer would add an
unnecessary public-looking indirection. Exposing internal `WorkflowState` or `WorkflowExecution` would leak mutable
runtime objects whose values may change while a caller inspects them. General Java `Predicate` filters would also make
the public query vocabulary opaque, non-serializable, and difficult to support consistently across live and historic
stores.

## Decision

Introduce a public `WorkflowManager` as the outside-in entry point. It is asynchronous and command-oriented. Its
methods return `CompletionStage`; invocation never blocks a caller or exposes a live workflow object.

The API is composed from narrow query and operation contracts. `WorkflowManager` is the convenience facade that
implements both, while applications that only read or only administer workflows can depend on the narrow contract.
The handles returned by queries expose the same split:

```java
interface WorkflowInstanceReader {
    CompletionStage<WorkflowInstanceSnapshot> snapshot();
}

interface WorkflowInstanceOperator {
    CompletionStage<WorkflowOperationResult> execute(WorkflowOperation command);
}

interface WorkflowInstance extends WorkflowInstanceReader, WorkflowInstanceOperator { }
```

The manager has distinct finder methods for one and many results, for example `findOne(WorkflowInstanceQuery)` and
`findAll(WorkflowInstancesQuery)`. They return a single `WorkflowInstance` handle or a `WorkflowInstances` batch
handle. A batch supports obtaining its snapshots and applying an operation to its selected instances, with an outcome
per instance. Not finding an instance is a normal query result, not an exceptional completion.

### Explicit value-based targeting

Public query methods never accept `Predicate`, `Function`, callbacks, or arbitrary filtering code. A query is an
immutable value object with dedicated factory methods for supported scopes, initially workflow identifier, workflow
definition identity, and association key/value. Composition is also value based, for example `and`, `or`, and `within`.
The manager may add a new scope only by adding a named query value and its catalog implementation; it must not add a
generic predicate escape hatch or overload a finder with ad-hoc filter arguments.

This makes scopes inspectable, serializable, auditable, and usable by every catalog implementation. Pagination and
ordering, when needed, are explicit value fields on `WorkflowInstancesQuery`, rather than callbacks.

### Snapshots and lifecycle neutrality

`WorkflowInstanceSnapshot` is a public immutable projection. It contains the workflow identity, definition identity,
status, step snapshots, and payload snapshot. It does not expose `WorkflowState`, `WorkflowStep`, mutable maps, or
references to a live execution. Collections are immutable copies. Payload data is copied into a value representation
through a configured snapshot mapper, so changing a runtime payload object cannot change an already returned snapshot.

The query catalog resolves both live execution state and retained history. A caller does not choose a "running" or
"historic" query path: the same query, handle, and snapshot shape apply to either. Whether an operation is currently
possible is evaluated only when the command is executed. This avoids a race where a workflow becomes terminal between
query and cancellation.

### Operations and initial cancellation commands

All active behavior is represented by a `WorkflowOperation` value. The first operation family is cancellation:

- cancel the workflow;
- cancel all currently running steps without terminating the workflow;
- cancel one named step.

Each cancellation command carries its explicit scope and optional reason. `WorkflowOperationResult` reports the
command outcome, including per-instance outcomes for a batch. A command against a historic, unknown, already terminal,
or otherwise ineligible target completes normally with a documented non-applied outcome. Infrastructure failures still
complete the stage exceptionally.

The manager owns live cancellation delivery and result translation. Its internal cancellation operation adapter
registers and routes valid requests to the workflow control thread, preserving the existing coordination semantics.
`WorkflowCancellationService` is removed as part of this change rather than made public or deprecated: it was a
temporary solution and has no external dependants. The registration and cleanup mechanics move to the manager's
private implementation boundary.

## Consequences

- Outside-in workflow administration has one public, asynchronous API that is separate from `WorkflowContext`.
- Query and command dependencies can be granted separately, while `WorkflowManager` remains convenient for callers
  that need both.
- A catalog capable of reading both active execution and retained history is required before broad searches can be
  implemented. The current live repository alone is insufficient.
- Snapshot mapping is a deliberate compatibility boundary. New internal state fields do not automatically become
  public API.
- `WorkflowCancellationService` is removed, with its live registration and control-thread routing behavior retained
  inside the manager implementation.
- Cancellation behavior remains cooperative and preserves the semantics in
  [ADR-013](./013-cancellation-and-termination-semantics.md).
- Later operations, such as retry, resume, or signal delivery, add a new `WorkflowOperation` value rather than new
  imperative manager methods.
