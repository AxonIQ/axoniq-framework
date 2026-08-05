# [Multi-Tenancy] Sourcing a per-tenant entity through its snapshot drops the events appended after it

### Basic information

* Axoniq Framework version: 5.3.0-RC1
* Axon Framework version (when different from Axoniq Framework version): affects `org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine`, in the core Axon Framework repo
* JDK version: 21
* Complete executable reproducer if available (e.g. GitHub Repo): [`integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SnapshotContentIsolationIT.java`](https://github.com/AxonIQ/axoniq-framework/blob/7afa9084d1dcefb7ba41552c99ef0f7b3e75a7c8/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SnapshotContentIsolationIT.java#L207)

### Steps to reproduce

1. Enable multi-tenancy with a real snapshot trigger active on an entity, e.g. `@Snapshotting(afterEvents = 3)`. Per-tenant event storage is composed via `MultiTenantEventStorageEngine.compose`, which wires `SnapshotCapableEventStorageEngine.decorate(engineFactory.engineFor(tenant), snapshotStoreFactory.storeFor(tenant))` directly per tenant — this bypasses `SnapshotSourcingConfigurationEnhancer` entirely (the multi-tenancy enhancer disables it). The per-tenant snapshot store here is a real, Converter-backed store (`AxonServerTenantSnapshotStoreFactory`), not `InMemorySnapshotStore`.
2. For one tenant, open the entity and apply 5 events that each increment a total by 1 (e.g. open a ledger, add `1` five times). The trigger fires at event 3, taking a snapshot mid-stream.
3. Await the snapshot being written to that tenant's own snapshot store.
4. Source the entity fresh for that tenant and read its total.

### Expected behaviour

The reconstructed total should be `5` — the snapshot is purely an optimization; sourcing through it must produce exactly the same state as sourcing from scratch.

### Actual behaviour

The reconstructed total is `2`: the snapshot is restored, but the events appended after it (events 4 and 5) never get applied — the tail is dropped.

Sourcing through a snapshot works like this in `SnapshotCapableEventStorageEngine` (`org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine`, core Axon Framework repo):

```java
return MessageStream.<EventMessage>just(new SnapshotEventMessage(snapshot))
    .concatWith(source(snapshot.position(), condition.criteria(), context));
...
private MessageStream<EventMessage> source(Position position, EventCriteria criteria, ProcessingContext context) {
    return delegate.source(SourcingCondition.conditionFor(position, criteria), context);
}
```

`conditionFor(position, criteria)` produces `SourcingStrategy.Absolute(position)`, resumed from `snapshot.position()`. Where that position comes from: `SnapshottingEntityLifecycleHandler.source(...)` sources via `transaction.source(condition, positionRef::set)`, then builds `new Snapshot(position, ...)` from whatever `positionRef` was set to. That value is filled from the stream's terminal `ConsistencyMarker` (`resumePositionCallback.accept(marker.position())`) — semantically an append/resume-safe *next* position, not necessarily "the position of the last consumed event."

On the read side, `InMemoryEventStorageEngine.source` treats `Absolute(position)` as an **inclusive** start index (`Math.max(0, GlobalIndexPosition.toIndex(position))`, then a `tailMap` from that index). Whether resuming from `snapshot.position()` is correct depends on whether that position is meant to be inclusive or exclusive of the next unconsumed event — and on this route, events 4 and 5 are being skipped entirely, consistent with resuming from a position that is already past them (or with the per-tenant composition passing/interpreting the resume position differently than the underlying engine expects).

**Why it's snapshot-specific, not a fixture artefact.** The same entity, same append-then-source path, without `@Snapshotting` active, reconstructs exactly — both [`PerTenantEventSourcingIsolationIT`](https://github.com/AxonIQ/axoniq-framework/blob/7afa9084d1dcefb7ba41552c99ef0f7b3e75a7c8/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PerTenantEventSourcingIsolationIT.java) and [`MultiTenantReadYourWritesIT`](https://github.com/AxonIQ/axoniq-framework/blob/7afa9084d1dcefb7ba41552c99ef0f7b3e75a7c8/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/MultiTenantReadYourWritesIT.java) pass on the same store and tenants; only adding `@Snapshotting` breaks it.

**What is not affected.** Tenant isolation holds regardless — neither tenant's total ever shows the other tenant's amount, and each tenant has its own snapshot in its own store, which [`PerTenantSnapshotIsolationIT`](https://github.com/AxonIQ/axoniq-framework/blob/7afa9084d1dcefb7ba41552c99ef0f7b3e75a7c8/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PerTenantSnapshotIsolationIT.java) covers separately and passes.

**Related but distinct.** A single-tenant control arm of the same hunt (`InMemorySnapshotStore` registered directly via `SnapshotSourcingConfigurationEnhancer`) produced a different wrong total (`8`, double-applying the head instead of dropping the tail). That turned out to have a separate, unrelated root cause specific to `InMemorySnapshotStore` storing entities by reference rather than by copy — see #322. It is not evidence of the same defect; it's a coincidence of two different bugs surfacing on the same test scenario.

### Suggested fix direction

Audit the `Position` written into the `Snapshot` when a trigger fires (`SnapshottingEntityLifecycleHandler`, sourced from the stream's terminal `ConsistencyMarker`) against exactly where `SnapshotCapableEventStorageEngine.source(...)` resumes sourcing from (`SourcingStrategy.Absolute(position)`), specifically for the per-tenant composition path used by `MultiTenantEventStorageEngine.compose`. Confirm whether the per-tenant `AxonServerEventStorageEngine` instance resuming sourcing treats `Absolute(position)` consistently with how `snapshot.position()` is recorded — a mismatch there would explain events being skipped rather than replayed. The fix belongs in `SnapshotCapableEventStorageEngine` or in how the multi-tenant composition invokes it, since that's where the per-tenant route diverges from the single-tenant `SnapshotSourcingConfigurationEnhancer` route.

### Possible Workarounds

None identified. Disabling snapshotting (no `@Snapshotting` trigger) avoids the defect entirely, at the cost of losing the performance benefit snapshots provide.


## Plan

1. Establish a deterministic, no-server reproduction in messaging/axoniq-multi-tenancy.
    - Extend PerTenantSnapshotSourcingIsolationTest (messaging/axoniq-multi-tenancy/src/test/java/io/axoniq/framework/messaging/multitenancy/eventsourcing/PerTenantSnapshotSourcingIsolationTest.java:48).
    - For tenant A: store a snapshot representing events 1–3 at its recorded resume position, append events 4–5, then source via SourcingStrategy.Snapshot.
    - Assert the stream is snapshot, event4, event5 and the rebuilt value is 5.
    - Use tenant B as an isolation control: it must neither load A’s snapshot nor receive A’s tail.

2. Add the real Axon Server regression test.
    - Add MultiTenantSnapshotSourcingIT under integrationtests/.../multitenancy.
    - Use two unique Axon Server contexts and the normal multi-tenant defaults; do not substitute InMemorySnapshotStore.
    - Produce events 1–3, load once to trigger and await the snapshot, append events 4–5, then load in a fresh unit of work.
    - Assert tenant A reconstructs 5 and tenant B remains independent.
    - Also retain a no-snapshot control arm; it separates general event sourcing failure from snapshot resumption failure.

3. Instrument the test boundary—not production code—until the discrepancy is visible.
    - Record snapshot payload and snapshot.position().
    - Record the subsequent SourceEventsRequest.fromSequence.
    - Record returned Axon Server event sequences and terminal consistency marker.
    - Expected sequence: snapshot after event sequence 2 stores resume position 3; the next request starts at 3 and returns sequences 3 and 4.

4. Fix only after the failing layer is identified.

Observation                                                       Fix location                                                                                                                                                         
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Single-tenant and multi-tenant paths both fail                    Core axon-framework: SnapshotCapableEventStorageEngine / snapshot lifecycle contract
────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────────────────
Request boundary is wrong for Axon Server                         ConditionConverter or connector stream marker conversion, with a protocol-contract test
────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────────────────
Request and response are correct, but only multi-tenancy fails    MultiTenantEventStorageEngine composition or tenant factory/wiring
────────────────────────────────────────────────────────────────  ─────────────────────────────────────────────────────────────────────────────────────────
Snapshot payload/position is wrong before resumption              Core SnapshottingEntityLifecycleHandler

5. Verification sequence:
    - Focused multi-tenancy unit test.
    - Connector condition/stream tests if the connector boundary changes.
    - ./mvnw -pl integrationtests -Dcoverage -Dit.test=MultiTenantSnapshotSourcingIT failsafe:integration-test failsafe:verify -q
    - If the core layer is at fault, implement and test there first; this checkout should only receive the dependency/update-side regression coverage.