# Multi-Tenancy Hunt -- Findings

Six findings, each with a test that fails on unfixed code. Hunted against a real multi-context Axon
Server (Testcontainers, licensed, DCB contexts) on branch
[`enhancement/208/multi-tenancy-hunt-tests`](https://github.com/AxonIQ/axoniq-framework/tree/enhancement/208/multi-tenancy-hunt-tests),
which branches off `feature/176/208-multi-tenant-queries` and adds only tests plus these documents.

All links are pinned to commit
[`a8ae66a`](https://github.com/AxonIQ/axoniq-framework/commit/a8ae66ac03944dd253f1275b27974ce0a7266f60)
(they need read access to the private repository).

**Run any test:**

```bash
./mvnw -Pintegration-test verify -pl integrationtests \
    -Dtest=zzzNone -Dsurefire.failIfNoSpecifiedTests=false \
    -Dit.test='<TestClassName>' -Dfailsafe.failIfNoSpecifiedTests=false
```

Requires Docker and an Axon Server license at
`integrationtests/src/test/resources/axon-server-test.license`. Note `-DskipTests` also skips
failsafe, so a run using it reports success having executed nothing.

| # | Finding | Impact | Failing test |
|---|---|---|---|
| 1 | Tenant context is lost on every wire hop | HIGH | [`TenantPropagationAcrossHopsIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/TenantPropagationAcrossHopsIT.java#L89) |
| 2 | Update from a rolled-back unit of work is delivered | HIGH | [`SubscriptionQueryUpdateDeliveryIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SubscriptionQueryUpdateDeliveryIT.java#L137) |
| 3 | Active subscription of a deleted tenant hangs silently | MEDIUM | [`TenantLifecycleIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/TenantLifecycleIT.java#L99) |
| 4 | Every multi-tenant shutdown blows the phase budget | MEDIUM-HIGH | [`ShutdownLatencyIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/ShutdownLatencyIT.java#L84) |
| 5 | No recovery of command dispatch after an Axon Server restart | HIGH | [`ChaosAxonServerRestartIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/ChaosAxonServerRestartIT.java#L102) |
| 6 | Sourcing through a snapshot returns wrong state, silently | HIGH | [`SnapshotContentIsolationIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SnapshotContentIsolationIT.java#L207) |

Findings 5 and 6 are **not multi-tenancy-specific**: their single-tenant control arms fail too. They
are reported here because multi-tenant deployments are disproportionately exposed to both.

---

## Finding 1 -- Tenant context is lost on every wire hop

**Impact: HIGH.** Claim MT-C1 refuted.

**Issue.** `MultiTenantAxonServerCommandBusConnector.resolveConnector` promises that "a command
dispatched while handling another message stays with the tenant of that message instead of having to
name its tenant again". Dispatch-side routing does follow the context, but the tenant lives ONLY in
the local `ProcessingContext`: nothing stamps it onto the outgoing message, and
`MultiTenancyConfigurationDefaults` registers no `CorrelationDataProvider` for the tenant key -- the
very mechanism `MetadataBasedTenantResolver`'s Javadoc names for propagation.

**How it manifests.** A follow-up message dispatched from inside a handler arrives carrying only
`causationId`/`correlationId`. The handler-side interceptor logs *"Tenant could not be resolved ...
Proceeding without tenant context"*, then `@TenantScoped` injection, event appends and sourcing all
fail with `TenantNotResolvedException`. Every process-manager-style flow is broken out of the box.

**Reproduce.** `-Dit.test=TenantPropagationAcrossHopsIT`

**Tests.**
[red arm](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/TenantPropagationAcrossHopsIT.java#L89)
fails; the
[candidate-fix arm](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/TenantPropagationAcrossHopsIT.java#L117)
registers `SimpleCorrelationDataProvider("tenantId")` and passes -- the verdict moves with the fix.

**Possible fix.** Register a `SimpleCorrelationDataProvider(tenantKey)` by default in
`MultiTenancyConfigurationDefaults`. Sturdier alternative covering custom (non-metadata) resolvers:
the per-tenant connectors know the resolved tenant at dispatch time, so stamp it into the outgoing
metadata there, and/or wrap each per-tenant connector's incoming `Handler` to attach that connector's
own `TenantDescriptor` to the context -- the receiving connector knows its tenant by construction and
today discards that knowledge.

---

## Finding 2 -- Subscription-query update from a rolled-back unit of work is delivered

**Impact: HIGH.** Claim MT-C6 refuted.

**Issue.** `SimpleQueryBus` gates `emitUpdate` / `completeSubscriptions*` on unit-of-work commit via
`runAfterCommitOrImmediately`. `DistributedQueryBus` -- the bus every Axon-Server-backed multi-tenant
application uses -- ignores its `context` parameter entirely and pushes straight to the wire.

**How it manifests.** A handler emits an update and then fails. The unit of work rolls back and no
event is persisted, but the subscriber has already received the update: read models and UIs observe
state that never happened. Observed: the subscriber's first update was `666`, from a command whose
dispatch completed exceptionally.

**Reproduce.** `-Dit.test=SubscriptionQueryUpdateDeliveryIT` and `-Dit.test=InMemoryRolledBackEmissionIT`

**Tests.** Axon Server arm
[`RolledBackEmission`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SubscriptionQueryUpdateDeliveryIT.java#L137)
is red; the
[in-memory arm](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/InMemoryRolledBackEmissionIT.java#L105)
runs the same flow on the local bus and is green. Vector **in-memory: PASS, axon-server: FAIL**, so
the defect is the distributed bus, not the application pattern.

**Possible fix.** In `DistributedQueryBus.emitUpdate` / `completeSubscriptions*`, defer through the
same after-commit hook the local bus uses (`context.runOnAfterCommit(...)` when a live, uncommitted
context is passed). `TenantAwareQueryBus` already forwards the context, so the fix is contained in
`axoniq-distributed-messaging`.

---

## Finding 3 -- Active subscription of a deleted tenant hangs silently forever

**Impact: MEDIUM.** Gap MT-M1 decided.

**Issue.** New queries for a deleted tenant are correctly rejected with `TenantNotResolvedException`.
A subscription-query stream that is already ACTIVE when the tenant's context is deleted gets nothing:
no completion, no error -- `completed=false, error=Optional.empty` after 30s. Nothing documents the
intended behaviour, and silence is the worst option, because a client cannot tell "no updates" from
"tenant gone".

**How it manifests.** Every client holding a subscription query for an offboarded tenant waits
forever; no cleanup signal reaches subscribers.

**Reproduce.** `-Dit.test=TenantLifecycleIT`

**Test.**
[`RemovalWithActiveSubscription`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/TenantLifecycleIT.java#L99)
(red in two runs). It first proves the stream is live by delivering a real update, then deletes the
context, so a silent hang cannot be confused with a stream that was never established.

**Possible fix.** On tenant removal, `MultiTenantAxonServerQueryBusConnector.removeTenant` should
complete (exceptionally) the subscription streams held against that tenant's connector before or
while disconnecting it. Likely coupled to Finding 4, where the per-tenant `connection.disconnect()`
that should tear those gRPC streams down never completes cleanly.

---

## Finding 4 -- Every multi-tenant application shutdown blows the phase budget and leaks live connectors

**Impact: MEDIUM-HIGH.** Claim MT-C8 / gap MT-M4.

**Issue.** Shutting down an IDLE two-tenant application hits the framework's 5s lifecycle-phase
timeout at phase `536870911` (`INBOUND_COMMAND_CONNECTOR` / `INBOUND_QUERY_CONNECTOR`) -- on every
shutdown, in every battery. `DefaultAxonApplication` logs `Timed out during shutdown phase
[536870911] after 5000ms. Proceeding to following phase` and abandons the handler.

A watchdog thread dump taken mid-stall shows `main` blocked in
`DefaultAxonApplication.invokeLifecycleHandlers` on the phase future, and a `grpc-default-executor`
thread blocked in `AxonServerManagedChannel.awaitTermination`: the disconnect chain
(`prepareDisconnect().thenCompose(...).thenRun(connection::disconnect)`) runs a BLOCKING
channel-termination await on the gRPC callback executor it needs. This branch compounds it --
`AxonServerQueryBusConnector.disconnect()` now also calls `connection.disconnect()` on the per-tenant
connection SHARED with the command connector and the tenant's event storage engine, and both
connectors race in the same phase.

**How it manifests.** Slow, noisy shutdowns, and abandoned handlers leaving "zombie" connectors that
keep their gRPC subscriptions. Because the tenant provider's context-update stream can also outlive
the shutdown, those zombies keep joining newly created contexts: during this hunt, applications from
earlier tests handled later tests' commands until the harness isolated tenants per test. In
production the same mechanism means a "stopped" node can keep consuming commands and queries.

**Reproduce.** `-Dit.test=ShutdownLatencyIT`

**Test.**
[`idleMultiTenantApplicationShutsDownWithinThePhaseBudget`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/ShutdownLatencyIT.java#L84)
(red) -- a watchdog prints all thread stacks when the shutdown exceeds 4s, so a red run carries its
own blocking frame.

**Possible fix.** Never run `AxonServerConnection.disconnect()`'s blocking await on the gRPC callback
executor: hop to another pool, or make the chain fully async. Longer term, per-tenant connectors
should not tear down the SHARED per-tenant connection at all -- ownership belongs to
`AxonServerConnectionManager`, which the tenant provider already calls via `disconnect(tenantId)` on
removal; connectors should stop at `prepareDisconnect()` plus drain.

---

## Finding 5 -- No recovery of command dispatching after an Axon Server restart

**Impact: HIGH.** Not tenancy-specific -- affects every Axon-Server-backed application on this stack;
multi-tenant applications inherit it per tenant.

**Issue.** When Axon Server restarts under load, the application never re-establishes working command
dispatch. Axon Server comes back healthy (container up, DCB contexts fast-forward their event logs,
admin API serving), but from the restart onward EVERY dispatch fails with `AxonServerException
[AXONIQ-4003] Received exception while dispatching command` -- for the full 3-minute observation
window, and in the first unbounded run for 12+ minutes until killed.

**How it manifests.** An Axon Server maintenance restart or crash-recovery turns every connected
instance into a brick for command handling until the APPLICATION is restarted. Multi-tenant arm:
61,059 commands accepted before the restart, 13,343 unknown outcomes during and after, zero accepted
afterwards. Single-tenant control: same shape, 6,247 consecutive failures.

**Attribution.** Multi-tenant FAIL, single-tenant FAIL identically, so the defect is in the
connector/framework layer, not the multi-tenancy module. A transient CONNECTION break, by contrast,
IS survived -- `PartialNetworkFaultIT` severs every established connection while the server stays up
and dispatching resumes with per-tenant conservation intact. What does not survive is the server
itself restarting, which is exactly the maintenance operation multi-tenant clusters perform most.

**Reproduce.** `-Dit.test=ChaosAxonServerRestartIT` and `-Dit.test=ChaosSingleTenantControlIT`
(both issue `docker restart` against the Axon Server container).

**Tests.**
[multi-tenant](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/ChaosAxonServerRestartIT.java#L102)
and
[single-tenant control](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/ChaosSingleTenantControlIT.java#L94),
both red; the failure message carries the accepted/unknown counters and the captured dispatch error.
The green transient-break differential is
[`PartialNetworkFaultIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PartialNetworkFaultIT.java).

**Possible fix.** Investigate whether command-handler registrations are replayed on the
re-established `CommandChannel` after the SERVER (not just the socket) went away. A consistent
`AXONIQ-4003` dispatch error rather than a "no handler" error, combined with the green
transient-break arm, points at post-restart handler re-registration rather than connection recovery.

**Oracle note.** The per-tenant conservation property under restart stays INCONCLUSIVE: unknown
outcomes cannot resolve while recovery is broken. Re-run that arm once recovery works.

---

## Finding 6 -- Sourcing through a snapshot returns the wrong state, silently

**Impact: HIGH.** Not tenancy-specific; both composition routes are wrong, in opposite directions.

**Issue.** With a real trigger active (`@Snapshotting(afterEvents = 3)`), an entity sourced after its
snapshot was taken does not reconstruct correctly. Minimal workload: open a ledger, add `1` five
times; the trigger fires at 3 events; the correct total is **5**.

| Arm | Total | Shape |
|---|---|---|
| Multi-tenant -- per-tenant `SnapshotCapableEventStorageEngine.decorate(engineFor(tenant), storeFor(tenant))` via `MultiTenantEventStorageEngine.compose` | **2** | snapshot restored, events appended after it never applied |
| Single-tenant control -- `InMemorySnapshotStore` registered directly, composition by `SnapshotSourcingConfigurationEnhancer` | **8** | snapshot restored AND its own events replayed on top |

**How it manifests.** No error, no warning: command handlers simply decide on wrong state. A balance
check, capacity check or idempotency check against a snapshotted entity can all be wrong.
Under-counting admits operations that should be rejected; over-counting rejects operations that
should be admitted.

**Why it is snapshot-specific, not a fixture artefact.** The same append-then-source path WITHOUT a
trigger reconstructs exactly:
[`PerTenantEventSourcingIsolationIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PerTenantEventSourcingIsolationIT.java)
(100 concurrent deposits, sourced balance equals accepted sum, two seeds) and
[`MultiTenantReadYourWritesIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/MultiTenantReadYourWritesIT.java)
both pass on the same store and tenants. Only adding `@Snapshotting` breaks it, and both numbers are
arithmetically consistent with an off-by-a-window position: `2` is the state as of the snapshot, `8`
counts the snapshot's own 3 events twice on top of the 5.

**What is NOT refuted.** Tenant isolation holds here -- neither arm shows the other tenant's amount
(1000) in its total, and each tenant has its own snapshot in its own store, which
[`PerTenantSnapshotIsolationIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PerTenantSnapshotIsolationIT.java)
covers separately and passes.

**Reproduce.** `-Dit.test=SnapshotContentIsolationIT` and `-Dit.test=SnapshotSingleTenantControlIT`

**Tests.**
[multi-tenant](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SnapshotContentIsolationIT.java#L207)
(red: 2 vs 5, two consecutive runs) and
[single-tenant control](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SnapshotSingleTenantControlIT.java#L91)
(red: 8 vs 5).

**Possible fix.** Audit the `Position` written into the `Snapshot` against the position post-snapshot
sourcing starts from in `SnapshotCapableEventStorageEngine.source(...)`. The two arms look like an
off-by-a-whole-window in each direction: one starts too late and drops the tail, the other starts at
the beginning and double-applies the head. The per-tenant route bypasses
`SnapshotSourcingConfigurationEnhancer` entirely (the multi-tenancy enhancer disables it), so the fix
belongs at the decorator, where both routes meet.

---

## Verified-good behaviour

These held under test, so a fix for the above must not regress them.

| Property | Test |
|---|---|
| Per-tenant event-store isolation and conservation: same entity id in 2 tenants, 100 concurrent deposits, two seeds | [`PerTenantEventSourcingIsolationIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PerTenantEventSourcingIsolationIT.java) |
| Read-your-writes and cross-application durability of acknowledged appends | [`MultiTenantReadYourWritesIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/MultiTenantReadYourWritesIT.java) |
| Subscription-update isolation: metadata-carrying AND context-routed subscriptions each get only their tenant's updates | [`SubscriptionQueryUpdateDeliveryIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/SubscriptionQueryUpdateDeliveryIT.java) |
| True cross-node emission: emitting handler pinned to node 2, subscription registered on node 1 | [`TrueCrossNodeUpdateEmissionIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/TrueCrossNodeUpdateEmissionIT.java) |
| Per-tenant snapshot store isolation: distinct, stable store per tenant | [`PerTenantSnapshotIsolationIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PerTenantSnapshotIsolationIT.java) |
| Tenant discovery: burst of 5 context creations; delete plus immediate recreate | [`TenantLifecycleIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/TenantLifecycleIT.java) |
| In-flight command at shutdown completes and its event is durable | [`GracefulShutdownInFlightWorkIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/GracefulShutdownInFlightWorkIT.java) |
| In-flight query at disconnect returns its value inside the connector's 5s drain budget | [`QueryDrainOnDisconnectIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/QueryDrainOnDisconnectIT.java) |
| Transient connection reset survived, per-tenant conservation intact | [`PartialNetworkFaultIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/PartialNetworkFaultIT.java) |
| Three reset rounds on a latency-degraded link, resume asserted per round | [`RepeatedFaultChaosIT`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/RepeatedFaultChaosIT.java) |

## Not covered

- **Streaming read side / projections** -- excluded: `MultiTenantEventStorageEngine.stream` /
  `firstToken` / `latestToken` / `tokenAt` throw `UnsupportedOperationException` on this branch
  (work in progress for #210). Everything projection-like here is driven from command/query contexts.
- **`LocalSegmentAdapter.awaitTermination`**, inspection-level: it parks to its full deadline once any
  query is in progress (the inner loop never re-checks the map), and its in-progress map is emptied at
  registration time -- `result.onClose(queriesInProgress.remove(id))` evaluates the remove immediately
  -- so the drain awaits nothing. `QueryDrainOnDisconnectIT` probes both and passes, so these stay
  latent code weaknesses rather than findings.
- **Snapshot version / schema evolution** -- a snapshot whose stored version no longer matches is
  silently ignored and the entity fully replayed; hard to judge until Finding 6 is fixed.
- **Long partitions and faults during tenant onboarding/offboarding** (context created or deleted
  while the link is down).

## Method and honesty notes

Claims were mined from Javadoc and ADRs into
[`docs/testing-plans/multi-tenancy-hunt.md`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/docs/testing-plans/multi-tenancy-hunt.md)
(claims MT-C1..C8, gaps MT-M1..M4, scenarios S1..S14); every test is tied to one of them. The full
triage trail is in
[`formal/MT-FINDINGS.md`](https://github.com/AxonIQ/axoniq-framework/blob/a8ae66ac03944dd253f1275b27974ce0a7266f60/formal/MT-FINDINGS.md).

Five harness defects were found and fixed before any verdict was accepted. Three had produced
convincing false findings -- missing `@EventTag` on fixture events (looked like "all appends lost"),
an emit/subscription type mismatch (looked like "updates never delivered"), and a tenant-prefix
predicate rejecting un-prefixed context names. Two only wedged runs. Each retraction is recorded with
the evidence that cleared the engine.

The multi-tenancy tests are silently disabled without the license file, and `-DskipTests` also skips
failsafe -- several early "green" runs were vacuous. Every verdict above comes from a run whose
per-class `Tests run:` line was checked.
