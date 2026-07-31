# Multi-Tenancy Hunt Report -- Axoniq Framework 5

**Scope**: the whole multi-tenancy feature on `feature/176/208-multi-tenant-queries`
(module `messaging/axoniq-multi-tenancy` + that branch's `connector/axon-server-connector` changes),
built against OSS Axon Framework `main` (5.3.0-SNAPSHOT, at `d234bed86f`).

**Method**: axon-hunt skill. Claims mined from Javadoc/ADRs into
`docs/testing-plans/multi-tenancy-hunt.md` (MT-C1..C8, gaps MT-M1..M4, scenarios S1..S14); every
test is tied to a claim or gap. All scenarios run against REAL infrastructure: multi-context Axon
Server (Testcontainers, licensed, DCB contexts), with in-memory and single-tenant differential arms
where they attribute a defect, three chaos arms (full Axon Server restart; one transient connection
reset; three reset rounds on a latency-degraded link -- the last two via an in-process TCP proxy), a
two-node arm with the emitting handler pinned to one node, a real snapshotting trigger, and a
concurrent same-entity-id workload across tenants. Working log with the full
triage trail (including five harness defects found, fixed and pinned before any verdict was
accepted): `formal/MT-FINDINGS.md`.

**Everything is local and uncommitted.** No GitHub issues were filed.

**Reproduce any test**:

```bash
cd ~/Projects/AxonFramework-mt-hunt
./mvnw -Pintegration-test verify -pl integrationtests \
    -Dtest=zzzNone -Dsurefire.failIfNoSpecifiedTests=false \
    -Dit.test='<TestClassName>' -Dfailsafe.failIfNoSpecifiedTests=false
```

Requires Docker and the Axon Server license at
`integrationtests/src/test/resources/axon-server-test.license` (gitignored, already in place).
Note: client-side entitlement enforcement is disabled in the test fixtures -- the local license is
an RSA-signed AxonServer license the entitlement-manager cannot validate (it kills the JVM
otherwise). Server-side licensing is untouched.

All tests live in
`integrationtests/src/test/java/io/axoniq/framework/integrationtests/multitenancy/hunt/`
(21 files).

## Findings at a glance

| # | Finding | Impact | Test (red) |
|---|---|---|---|
| 1 | Tenant context lost on every wire hop -- follow-up messages fail | HIGH | `TenantPropagationAcrossHopsIT` |
| 2 | Rolled-back unit of work's subscription update is delivered | HIGH | `SubscriptionQueryUpdateDeliveryIT$RolledBackEmission` |
| 3 | Active subscription of a deleted tenant hangs silently forever | MEDIUM | `TenantLifecycleIT$RemovalWithActiveSubscription` |
| 4 | Multi-tenant shutdown always blows the phase budget, leaks live connectors | MEDIUM-HIGH | `ShutdownLatencyIT` |
| 5 | No recovery of command dispatching after an Axon Server restart (not tenancy-specific) | HIGH | `ChaosAxonServerRestartIT`, `ChaosSingleTenantControlIT` |
| 6 | Sourcing through a snapshot returns the wrong state -- silently (not tenancy-specific) | HIGH | `SnapshotContentIsolationIT`, `SnapshotSingleTenantControlIT` |

---

## Finding 1 -- Tenant context is lost on every wire hop (claim MT-C1 refuted)

**Impact: HIGH**

**Description.** The per-tenant connector Javadoc promises: *"a command dispatched while handling
another message stays with the tenant of that message instead of having to name its tenant
again."* Routing indeed follows the context -- but the tenant travels ONLY in the local
`ProcessingContext`. Nothing stamps it onto the outgoing message, and
`MultiTenancyConfigurationDefaults` registers no `CorrelationDataProvider` for the tenant key
(the mechanism `MetadataBasedTenantResolver`'s own Javadoc names for propagation). After the
Axon Server round trip the message arrives carrying only `causationId`/`correlationId`; the
handler-side interceptor logs "Tenant could not be resolved ... Proceeding without tenant
context".

**How it manifests.** Any follow-up message dispatched from inside a handler without explicitly
naming its tenant fails on the handling side: `@TenantScoped` injection, event appends and
sourcing all throw `TenantNotResolvedException`. Observed:
`CommandExecutionException: TenantNotResolvedException: Tenant could not be resolved from the
processing context`. Every process-manager-style flow (command handler dispatching commands) is
broken out of the box.

**How to reproduce.** `-Dit.test=TenantPropagationAcrossHopsIT` -- the class carries both arms.

**Test.** `TenantPropagationAcrossHopsIT.followUpCommandDispatchedInsideHandlerStaysWithTheTenantOfTheHandledMessage`
(red on this branch). The candidate-fix arm
`followUpCommandKeepsItsTenantWhenATenantCorrelationProviderIsRegistered` registers
`SimpleCorrelationDataProvider("tenantId")` and is green -- the verdict moves with the fix, so
the cause is pinned.

**Possible fix.** Register a `SimpleCorrelationDataProvider(tenantKey)` by default in
`MultiTenancyConfigurationDefaults` (matches the resolver's documented design). Sturdier
alternative that also covers custom (non-metadata) resolvers: the per-tenant connectors know the
resolved tenant at dispatch time -- stamp it into the outgoing message's metadata there, and/or
wrap each per-tenant connector's incoming `Handler` to attach that connector's own
`TenantDescriptor` to the processing context (the connector receiving the message knows its
tenant by construction; today it discards that knowledge).

---

## Finding 2 -- Subscription-query update from a rolled-back unit of work is delivered (claim MT-C6 refuted)

**Impact: HIGH**

**Description.** The local `SimpleQueryBus` gates `emitUpdate`/`completeSubscriptions*` on unit-of-work
commit (`runAfterCommitOrImmediately`). `DistributedQueryBus` (the bus every multi-tenant,
Axon-Server-backed application uses) ignores its `context` parameter entirely and pushes the
update to the wire immediately.

**How it manifests.** A command handler emits an update and then fails; the unit of work rolls
back, no event is persisted -- but the subscriber has already received the update. Read models
and UIs observe state that never happened. Observed concretely: subscriber's first received
update was `666` from a command whose dispatch completed exceptionally.

**How to reproduce.** `-Dit.test=SubscriptionQueryUpdateDeliveryIT` (Axon Server arm),
`-Dit.test=InMemoryRolledBackEmissionIT` (in-memory arm).

**Test.** `SubscriptionQueryUpdateDeliveryIT$RolledBackEmission.updateEmittedByFailingUnitOfWorkIsNotDelivered`
(red). Differential vector: **in-memory: PASS, axon-server: FAIL**
(`InMemoryRolledBackEmissionIT`, same flow, green) -- the defect is in the distributed bus, not
in the application pattern.

**Possible fix.** In `DistributedQueryBus.emitUpdate`/`completeSubscriptions*`, defer the work via
the same after-commit hook the local bus uses (`context.runOnAfterCommit(...)` when a live,
uncommitted context is passed). Note: `TenantAwareQueryBus` forwards the context already, so the
fix is contained in `axoniq-distributed-messaging`.

---

## Finding 3 -- Active subscription of a deleted tenant hangs silently forever (gap MT-M1)

**Impact: MEDIUM**

**Description.** New queries for a deleted tenant are correctly rejected
(`TenantNotResolvedException`, covered by existing ITs). But a subscription-query stream that is
already ACTIVE when the tenant's context is deleted receives nothing: no completion, no error --
`completed=false, error=Optional.empty` after 30s (and counting). The documentation is silent on
what should happen; silence-as-behaviour is the worst of the options, because clients cannot
distinguish "no updates" from "tenant gone".

**How it manifests.** Every UI/client holding a subscription query for a tenant that gets
offboarded waits forever. No resource cleanup signal propagates to subscribers.

**How to reproduce.** `-Dit.test=TenantLifecycleIT`
(`RemovalWithActiveSubscription` nested class; the test first proves the stream is live by
delivering a real update, then deletes the context).

**Test.** `TenantLifecycleIT$RemovalWithActiveSubscription.activeSubscriptionStreamReachesTerminalStateWhenItsTenantIsDeleted` (red).

**Possible fix.** On tenant removal, `MultiTenantAxonServerQueryBusConnector.removeTenant` should
complete (exceptionally) the subscription streams held against that tenant's connector before/while
disconnecting it. Likely coupled to Finding 4: the per-tenant `connection.disconnect()` that should
tear the gRPC streams down never completes cleanly.

---

## Finding 4 -- Every multi-tenant application shutdown blows the phase budget and leaks live connectors (claim MT-C8 / gap MT-M4)

**Impact: MEDIUM-HIGH**

**Description.** Shutting down an IDLE two-tenant application runs into the framework's 5s
lifecycle-phase timeout at phase `536870911` (`INBOUND_COMMAND_CONNECTOR` /
`INBOUND_QUERY_CONNECTOR`) -- on every single shutdown, in every scenario battery (6/6, then
consistently after). `DefaultAxonApplication` logs
`Timed out during shutdown phase [536870911] after 5000ms. Proceeding to following phase` and
abandons the handler.

A watchdog thread dump taken mid-stall shows: `main` blocked in
`DefaultAxonApplication.invokeLifecycleHandlers` waiting on the phase future, and a
`grpc-default-executor` thread blocked in `AxonServerManagedChannel.awaitTermination` -- the
disconnect chain (`prepareDisconnect().thenCompose(...).thenRun(connection::disconnect)`) executes
the BLOCKING channel-termination await on the gRPC callback executor it itself needs. The branch
compounds this: this branch's `AxonServerQueryBusConnector.disconnect()` now also calls
`connection.disconnect()` on the per-tenant connection SHARED with the command connector and the
tenant's event storage engine, and both connectors race in the same phase.

**How it manifests.** Slow, noisy shutdowns (5s+ per timed-out phase); abandoned shutdown handlers
leave "zombie" connector instances that keep their gRPC subscriptions and -- because the tenant
provider's context-update stream may also outlive the shutdown -- keep joining newly created
contexts. In our batteries, zombie applications from earlier tests handled later tests' commands
until the harness isolated tenants per test. In production the same mechanism means a "stopped"
node can keep consuming commands/queries.

**How to reproduce.** `-Dit.test=ShutdownLatencyIT` (asserts an idle app's `shutdown()` stays
under the 5s phase budget; the watchdog prints the thread dump on stall).

**Test.** `ShutdownLatencyIT.idleMultiTenantApplicationShutsDownWithinThePhaseBudget` (red).

**Possible fix.** Never run `AxonServerConnection.disconnect()`'s blocking await on the gRPC
callback executor (hop to a dedicated/common pool, or make the disconnect chain fully async).
Longer term: per-tenant connectors should not tear down the SHARED per-tenant connection at all --
ownership belongs to `AxonServerConnectionManager` (which the tenant provider already calls via
`disconnect(tenantId)` on removal); connectors should stop at `prepareDisconnect()` + drain.

---

## Finding 5 -- No recovery of command dispatching after an Axon Server restart (chaos arm; not tenancy-specific)

**Impact: HIGH** (affects every Axon-Server-backed application on this stack; multi-tenant apps inherit it per tenant)

**Description.** When Axon Server restarts under load, the application never re-establishes
working command dispatch. Axon Server itself comes back healthy (container up, DCB contexts
fast-forward their event logs, admin API serving), but from the restart onward EVERY command
dispatch fails with `AxonServerException [AXONIQ-4003] Received exception while dispatching
command` -- for the full 3-minute observation window, and in the first (unbounded) run for 12+
minutes until killed.

**How it manifests.** An Axon Server maintenance restart or crash-recovery turns every connected
application instance into a brick for command handling until the APPLICATION is restarted.
Observed numbers: multi-tenant arm -- 61,059 commands accepted before the restart, 13,343 unknown
outcomes during/after, zero accepted after; single-tenant control -- same shape
(6,247 consecutive failures).

**Attribution (differential).** Multi-tenant arm: FAIL. Single-tenant arm on the default context,
plain application: FAIL, identically. The defect is in the connector/framework layer
(OSS main `d234bed86f` + this branch's `axon-server-connector`), NOT in the multi-tenancy module.
Reported here because multi-tenant deployments (many contexts, more frequent AS operations) are
disproportionately exposed.

**How to reproduce.** `-Dit.test=ChaosAxonServerRestartIT` (multi-tenant) and
`-Dit.test=ChaosSingleTenantControlIT` (single-tenant control). Both need Docker
(the test issues `docker restart` on the Axon Server container).

**Tests.** `ChaosAxonServerRestartIT.perTenantConservationHoldsAcrossAnAxonServerRestart` and
`ChaosSingleTenantControlIT.singleTenantApplicationRecoversCommandDispatchingAfterAxonServerRestart`
(both red; the failure message carries the accepted/unknown counters and the captured dispatch
error).

**Scope sharpener (differential vs a transient connection break).** `PartialNetworkFaultIT` runs the
same two-tenant workload through an in-process TCP proxy and severs every established connection
(SO_LINGER 0 reset) while Axon Server keeps running: dispatching resumes and per-tenant
conservation holds -- PASS. So plain reconnect works; what does not survive is the SERVER
restarting (its state/streams being re-created), which is exactly the maintenance operation
multi-tenant clusters perform most.

**Possible fix.** Needs connector-level investigation: whether command-handler registrations are
replayed on the re-established `CommandChannel` after the server (not just the socket) went away,
and whether the shared `AxonServerConnection` re-registers handlers at all in that case. The
consistent `AXONIQ-4003` (dispatch error) rather than a `no handler` error, combined with the
green transient-break arm, points at post-restart handler re-registration rather than at
connection recovery.

**Note on the chaos oracle.** The per-tenant conservation-under-restart property remains
INCONCLUSIVE (unknowns cannot resolve while recovery is broken); re-run the chaos arm once
recovery is fixed.

---

## Finding 6 -- Sourcing through a snapshot returns the wrong state, silently (not tenancy-specific)

**Impact: HIGH** (silent state corruption in any snapshotted entity; multi-tenant and single-tenant
applications are both affected, in opposite directions)

**Description.** With a real snapshotting trigger active (`@Snapshotting(afterEvents = 3)`), an
entity sourced after its snapshot was taken does not reconstruct to the correct state. The workload is
minimal: open a ledger, then add `1` five times; the trigger fires at 3 events; the correct total is
**5**.

- **Multi-tenant** (per-tenant composition via `MultiTenantEventStorageEngine.compose`, which applies
  `SnapshotCapableEventStorageEngine.decorate(engineFor(tenant), storeFor(tenant))`): total = **2**.
  The snapshot is restored and the events appended AFTER it are never applied.
- **Single-tenant control** (plain app, default context, `InMemorySnapshotStore` registered directly,
  snapshot composition by `SnapshotSourcingConfigurationEnhancer`): total = **8**. The snapshot is
  restored AND the events already contained in it are replayed on top.

Both are wrong, in opposite directions, so the defect is in the snapshot position handling of the
sourcing path -- not in multi-tenancy. The two composition routes get it wrong differently.

**How it manifests.** No error, no warning: the entity's command handlers simply decide on wrong
state. A balance check, a capacity check or an idempotency check made against a snapshotted entity
can all be wrong -- under-counting (multi-tenant) admits operations that should be rejected;
over-counting (single-tenant) rejects operations that should be admitted. Nothing in the logs marks it.

**Why this is snapshot-specific, not a fixture bug.** The same append-then-source path WITHOUT a
snapshotting trigger reconstructs exactly: `PerTenantEventSourcingIsolationIT` (100 concurrent
deposits, sourced balance == accepted sum, two seeds) and `MultiTenantReadYourWritesIT` both pass on
the same store, same tenants, same command flow. Only adding `@Snapshotting` breaks it. The two
observed numbers are also arithmetically consistent with an off-by-a-window position: `2` is exactly
the state as of the snapshot, `8` is the snapshot's own 3 events counted twice on top of the 5.

**What is NOT refuted.** Tenant isolation holds in this scenario: neither arm shows the other
tenant's amount (1000) inside its total, and each tenant does have its own snapshot in its own store
(`PerTenantSnapshotIsolationIT` covers that separately and passes).

**How to reproduce.** `-Dit.test=SnapshotContentIsolationIT` (multi-tenant, 2 tenants),
`-Dit.test=SnapshotSingleTenantControlIT` (single-tenant control). Reproduced in two consecutive runs.

**Tests.** `SnapshotContentIsolationIT.snapshotsAreWrittenPerTenantAndTheirContentNeverCrossesTenants`
(red: 2 vs 5) and
`SnapshotSingleTenantControlIT.eventsAppendedAfterASnapshotAreStillSourcedOnASingleTenantApplication`
(red: 8 vs 5).

**Possible fix.** Audit the `Position` written into the `Snapshot` against the position the
post-snapshot sourcing starts from in `SnapshotCapableEventStorageEngine.source(...)`: the two arms
look like an off-by-a-whole-window in each direction (one starts too late and drops the tail, the
other starts at the beginning and double-applies the head). The per-tenant route additionally
bypasses `SnapshotSourcingConfigurationEnhancer` entirely (the multi-tenancy enhancer disables it),
so both routes need the same fix applied at the decorator, not at the enhancer.

---

## What was tested and held (positive coverage)

| Property | Test | Verdict |
|---|---|---|
| Per-tenant event storage isolation + conservation: same entity id in 2 tenants, 100 concurrent deposits, sourced balance == per-tenant accepted sum | `PerTenantEventSourcingIsolationIT` | PASS (2 independent runs) |
| Read-your-writes and cross-application durability of acknowledged appends, per tenant | `MultiTenantReadYourWritesIT` | PASS |
| Same flow, single-tenant control on default context | `SingleTenantControlIT` | PASS |
| Subscription-query update isolation and delivery -- metadata-carrying AND context-routed subscriptions each receive exactly their tenant's updates | `SubscriptionQueryUpdateDeliveryIT$ContextRoutedSubscription` | PASS |
| Tenant discovery: burst of 5 context creations, all served; delete+immediate recreate serves again | `TenantLifecycleIT$RapidContextLifecycle` | PASS (see run log; one earlier red was a harness prefix bug, fixed) |
| In-flight command at shutdown completes and its event is durable (fresh app sources it) | `GracefulShutdownInFlightWorkIT` | PASS |
| Cross-instance update emission (subscription on app-1, emit dispatched via app-2) | `CrossInstanceUpdateEmissionIT` | PASS (weaker arm: Axon Server chooses the handling node) |
| **True cross-node** emission: emitting command handler exists ONLY on app-2; subscription registered through app-1, which has no command handlers at all | `TrueCrossNodeUpdateEmissionIT` | PASS -- gap MT-M2 closed positively: the update crosses nodes |
| Per-tenant snapshot isolation end to end on real Axon Server: distinct + stable `SnapshotStore` per tenant, no snapshot crossing tenants, per-tenant sourced balances | `PerTenantSnapshotIsolationIT` | PASS |
| Query in flight at shutdown reaches a terminal state (not abandoned), AND -- strict arm -- a 2s query inside the connector's 5s drain budget still produces its VALUE rather than being cancelled | `QueryDrainOnDisconnectIT` (2 tests) | PASS -- the drain does wait in practice, so the `awaitTermination` code weaknesses below stay latent |
| Per-tenant snapshot STORE isolation: distinct + stable store per tenant, per-tenant sourced balances (no snapshotting trigger) | `PerTenantSnapshotIsolationIT` | PASS |
| Repeated chaos: three connection-reset rounds, rounds 2-3 on a link with 25ms per-chunk injected latency; resume asserted per round + per-tenant conservation at the end | `RepeatedFaultChaosIT` | PASS |
| Chaos: Axon Server restart under two-tenant workload; per-tenant `accepted <= balance <= accepted+unknown`, no cross-tenant bleed, recovery | `ChaosAxonServerRestartIT` | FAIL -- became Finding 5; the conservation part stays INCONCLUSIVE until recovery works |
| Chaos: transient network fault (all established connections reset via an in-process TCP proxy, server stays up), two tenants under load | `PartialNetworkFaultIT` | PASS -- dispatch resumes, per-tenant conservation holds |

## Not covered -- what this hunt did NOT test

- **Streaming read side / projections**: excluded by design -- `MultiTenantEventStorageEngine.stream`
  / `firstToken` / `latestToken` / `tokenAt` throw `UnsupportedOperationException` on this branch
  (work in progress for #210, not testable yet). Everything projection-like in this hunt is driven
  from command/query contexts instead.
- **`LocalSegmentAdapter.awaitTermination`** (query connector), inspection-level and now
  test-probed: it parks to its FULL deadline once any query is in progress (the inner loop never
  re-checks the map), and its in-progress map is emptied at registration time --
  `result.onClose(queriesInProgress.remove(id))` evaluates the `remove` immediately and passes its
  return value to `onClose`, so the disconnect drain awaits nothing. `QueryDrainOnDisconnectIT`
  probes both, including a strict arm demanding that a 2s in-flight query (well inside the
  connector's 5s budget) still returns its value -- it PASSES. So the weaknesses are latent: no test
  makes them fail. Per the reporting bar they stay here, not among the findings.
- **Snapshot version/schema evolution**: a snapshot whose stored `version` no longer matches is
  silently ignored and the entity fully replayed. Not exercised -- and hard to judge until
  Finding 6 is fixed.
- **Partitions lasting past reconnect back-off**, and faults during tenant onboarding/offboarding
  (context created or deleted WHILE the link is down). Repeated resets with latency are covered by
  `RepeatedFaultChaosIT`; longer partitions are open, and full-restart recovery is blocked by
  Finding 5.

## Honest-reporting notes

- Five harness defects were found, fixed and pinned before any verdict was accepted
  (`formal/MT-FINDINGS.md`). Three had produced convincing false findings: missing `@EventTag` on
  fixture events (masqueraded as "all appends lost"), an emit/subscription type mismatch
  (masqueraded as "updates never delivered"), and a tenant-prefix predicate rejecting un-prefixed
  burst context names. Two wedged runs rather than faking findings: the chaos test's
  try-with-resources closed its pool before signalling the workers to stop, and the single-tenant
  chaos control lacked a `DepositMoney` handler. Every retraction is recorded with the evidence
  that cleared the engine.
- The multi-tenancy ITs are silently disabled without the license file; several early "green"
  runs were vacuous (`-DskipTests` skips failsafe too). All verdicts above come from runs whose
  per-class `Tests run:` lines were checked.
