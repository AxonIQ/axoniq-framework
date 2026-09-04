# ADR-018: Workflow Manager Outside-In API

- Name: Workflow manager outside-in API
- Status: Accepted
- Date: 2026-09-02

## Context

Workflow definitions operate from the inside out through `WorkflowContext`. Applications also need an outside-in
surface for support tooling, administration, and application services: find workflow instances, inspect their state,
and request cancellation without exposing a mutable live `WorkflowExecution`.

The manager reads from two sources. The live execution repository has current state for executions owned by this
engine. The history repository retains projected state for instances that are no longer live. Callers must not need to
choose a source or supply Java code that only one source can interpret. Loading every history entry and filtering it in
the manager is not viable for a database-backed repository with a large history.

## Decision

`WorkflowManager` is the outside-in entry point:

```java
WorkflowInstances.Single findOne(WorkflowStateQuery query);
WorkflowInstances findMany(WorkflowStateQuery query);
```

`WorkflowInstances.Single` extends `WorkflowInstances`. It is therefore a zero-or-one collection view, not a
separate result abstraction: `single()` is its convenience operation, while `instances()`, `size()`, and the
collection cancellation operations remain available where uniform collection handling is useful.

```java
interface WorkflowInstances {
    Flow.Publisher<WorkflowInstance> instances();
    CompletableFuture<Integer> size();

    interface Single extends WorkflowInstances {
        CompletableFuture<@Nullable WorkflowInstance> single();
    }
}
```

Finders synchronously create lazy handles. Every state-reading or state-changing action on a handle is asynchronous,
returning a `CompletableFuture` or a `Flow.Publisher`. This makes the API two-stage asynchronous without requiring a
future merely to obtain a handle.

`WorkflowInstances.Single.single()` resolves the zero-or-one result of `findOne` and completes with `null` when no
instance matches. A resolved `WorkflowInstance` always exists, exposes a non-null detached state read and cancellation
operations. `WorkflowInstances.Single` also exposes the collection operations, constrained to zero or one instance. A
`WorkflowInstances` handle exposes an asynchronous publisher of resolved instances, an asynchronous count, and the same
cancellation operations for its live matches.

### Detached state

`WorkflowInstance.state()` returns `CompletableFuture<WorkflowState>`. The implementation returns a detached
`WorkflowState` value, not the state object owned by an execution or history projection. Its structural
containers are copied and immutable. Payload values and values embedded in a step remain application values, so the
manager does not attempt a generic deep copy.

`findOne(...).single()` completes with `null` when no instance matches and exceptionally with
`NonUniqueWorkflowInstanceMatchException` when more than one state matches. `findMany` represents zero or more
matches, and its publisher emits only resolved instances.

### Declarative state targeting

`WorkflowStateQuery` is the shared value-based criteria language in the neutral
`io.axoniq.framework.workflow.query.api` package. It targets state values, not a storage implementation or a live workflow
instance. Restrictions compose with logical AND. The initial vocabulary covers workflow ID, workflow definition ID,
workflow status, step existence and status, payload values, and version information.

The query contains data only. It does not accept `Predicate`, callbacks, or arbitrary filtering code, and it does not
evaluate itself. The same query is passed directly to both repositories:

```java
interface WorkflowExecutionRepository {
    Set<WorkflowExecution> findAll(WorkflowStateQuery query);
}

interface WorkflowHistoryRepository {
    List<WorkflowHistory> findAll(WorkflowStateQuery query);
}
```

Repository implementations must accept and apply `WorkflowStateQuery` directly. This is a required part of the
contract, not a manager convenience: a database-backed repository must translate the criteria to its native query
mechanism and must not load all records for manager-side filtering. The in-memory repositories use an internal
state-query matcher as their storage-specific implementation.

### Source composition and cancellation

The manager queries both sources with the same `WorkflowStateQuery`, then merges the returned state by workflow ID.
When both sources contain the same ID, the live state wins because it is authoritative. A caller therefore sees one
state regardless of whether it is live or historic.

State reads use this combined result. Cancellation is different: it is an operation on a live execution only. The
manager resolves cancellation targets from the live repository and never sends a cancellation request based on a
history entry. The existing internal `WorkflowCancellationService` remains the live delivery mechanism, but is not
part of the public manager API.

## Consequences

- Outside-in administration is separate from `WorkflowContext` and never returns a live execution object.
- The shared repository query contract enables storage-specific filtering and indexing for large data volumes.
- Historic state is readable, but does not become a cancellation target.
- The manager has a deterministic conflict rule: live state wins over history for the same workflow ID.
- Broad `WorkflowStateQuery.all()` searches can still be large. Paging or cursor-based repository results are a
  future extension needed to make the publisher end-to-end streaming for unbounded result sets.
- Cancellation remains cooperative and preserves the semantics in
  [ADR-013](./013-cancellation-and-termination-semantics.md).
