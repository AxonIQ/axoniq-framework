# Multi-Tenancy Hunt -- Findings Log

Working log for the multi-tenancy hunt on `feature/176/208-multi-tenant-queries`.
Claims and scenario specs: `docs/testing-plans/multi-tenancy-hunt.md`.
Only findings with a reproducing test are eligible for the final report.

Status values: CONFIRMED (reproducing test red on this branch), HARNESS (was our bug),
CHARACTERISED (behaviour pinned, no doc claim violated), PENDING.

---

## F-MT-1 -- Tenant is lost on every wire hop: follow-up messages fail (CONFIRMED)

- Claim refuted: MT-C1 ("a command dispatched while handling another message stays with the
  tenant of that message instead of having to name its tenant again", per-tenant connector
  Javadoc), plus gap MT-M3.
- Test: `TenantPropagationAcrossHopsIT.followUpCommandDispatchedInsideHandlerStaysWithTheTenantOfTheHandledMessage`
- Observed: the follow-up command IS routed to tenant A's connection (dispatch-side context
  resolution works), but arrives with metadata `{causationId, correlationId}` only. The
  handler-side interceptor logs "Tenant could not be resolved ... Proceeding without tenant
  context"; the handler then fails at event append with
  `TenantNotResolvedException: Tenant could not be resolved from the processing context`.
- Cause: the tenant travels only in the local ProcessingContext; nothing stamps it onto the
  outgoing message, and `MultiTenancyConfigurationDefaults` registers no
  `CorrelationDataProvider` for the tenant key (the combination the
  `MetadataBasedTenantResolver` Javadoc itself describes as the propagation mechanism).
- Candidate-fix arm:
  `followUpCommandKeepsItsTenantWhenATenantCorrelationProviderIsRegistered` -- registering
  `SimpleCorrelationDataProvider("tenantId")` makes the identical chain pass. (verdict: see run log)

## F-MT-2 -- Update emitted by a rolled-back unit of work is delivered (CONFIRMED)

- Claim refuted: MT-C6 (code-inferred from OSS `SimpleQueryBus.runAfterCommitOrImmediately`);
  `DistributedQueryBus.emitUpdate` ignores its `context` parameter and pushes immediately.
- Test: `SubscriptionQueryUpdateDeliveryIT$RolledBackEmission.updateEmittedByFailingUnitOfWorkIsNotDelivered`
- Observed (run7): subscriber's first update after a failed EmitBalanceUpdate(666, failAfterEmit)
  followed by a committed EmitBalanceUpdate(1) was `666` -- the rolled-back emission reached the
  wire. Expected `1`.
- Differential arm: `InMemoryRolledBackEmissionIT` (local SimpleQueryBus) -- verdict pending.

## F-MT-3 -- Active subscription of a deleted tenant hangs silently (CONFIRMED)

- Gap MT-M1 decided; adjacent to MT-C3 (new queries for the deleted tenant ARE rejected -- existing
  IT -- but established streams learn nothing).
- Test: `TenantLifecycleIT$RemovalWithActiveSubscription.activeSubscriptionStreamReachesTerminalStateWhenItsTenantIsDeleted`
- Observed (run7): stream live (initial result + one delivered update), context deleted, then 30s of
  silence: `completed=false, error=Optional.empty`.

## F-MT-4 -- Multi-tenant application shutdown always exceeds the phase budget (CONFIRMED)

- Claim MT-C8 / gap MT-M4 territory.
- Test: `ShutdownLatencyIT.idleMultiTenantApplicationShutsDownWithinThePhaseBudget`
- Observed (run7): an IDLE 2-tenant application's `shutdown()` runs into the framework's 5s
  phase timeout at phase 536870911 (INBOUND_COMMAND/QUERY_CONNECTOR); every single multi-tenant
  shutdown in every battery logged the same warning. Watchdog thread dump: main blocked in
  `DefaultAxonApplication.invokeLifecycleHandlers` on a phase future; a
  `grpc-default-executor` thread blocked in `AxonServerManagedChannel.awaitTermination` --
  a connector disconnect chain (`prepareDisconnect().thenRun(connection::disconnect)`) executes
  the blocking channel await ON the gRPC callback executor.
- Consequence: abandoned shutdown handlers leak live per-tenant connectors ("zombie"
  applications that keep serving and dynamically join newly created contexts).

## Verification summary (as of run13)

- F-MT-1: red arm red in run4, run7, run12; candidate-fix arm green in run12 -- PINNED.
- F-MT-2: red in run7 (666 delivered); in-memory arm green in run7 -- vector
  in-memory:PASS axonserver:FAIL.
- F-MT-3: red in run7 and run13, identical silent-hang shape (completed=false, error=empty).
- F-MT-4: red in run7 (ShutdownLatencyIT, with thread dump); the same phase-timeout warning in
  every app shutdown of every battery.
- Positive: PerTenantEventSourcingIsolationIT green (run12 + run13 pending), ReadYourWrites +
  SingleTenantControl green (run11), ContextRoutedSubscription green (run7),
  GracefulShutdownInFlightWorkIT green (run12), CrossInstanceUpdateEmissionIT green-with-caveat
  (run7).

## F-MT-5 -- No recovery of command dispatching after an Axon Server restart (CONFIRMED, not tenancy-specific)

- Tests: `ChaosAxonServerRestartIT.perTenantConservationHoldsAcrossAnAxonServerRestart` (multi-tenant,
  2 tenants) and `ChaosSingleTenantControlIT.singleTenantApplicationRecoversCommandDispatchingAfterAxonServerRestart`
  (single-tenant differential).
- Observed: healthy workload before the restart (61,059 accepted multi-tenant; steady accepts
  single-tenant), fault lands (13,343 / 6,247 unknown outcomes), then ZERO newly accepted commands
  for the full 3-minute recovery window; every dispatch fails with
  `AxonServerException [AXONIQ-4003] Received exception while dispatching command`. First
  occurrence (run14) showed the same non-recovery for 12+ minutes.
- Vector: multi-tenant:FAIL single-tenant:FAIL -- the defect lives in the connector/framework
  stack, NOT in the multi-tenancy module; multi-tenant applications inherit it per tenant.
- Axon Server itself recovered (container healthy, contexts fast-forwarded their event logs,
  admin API serving) -- the application side never re-establishes working command dispatch.
- Consequence for the chaos oracle: the per-tenant conservation-under-restart verdict is
  INCONCLUSIVE until recovery works (the workload cannot produce post-restart accepted commands
  to check).

## F-MT-6 -- Sourcing through a snapshot returns the wrong state (CONFIRMED, both arms, opposite directions)

- Claim territory: MT-C5 / core event-sourcing correctness. Found by the scenario that closed the
  snapshot residual (a real `@Snapshotting(afterEvents = 3)` trigger, not just store-per-tenant).
- Tests: `SnapshotContentIsolationIT.snapshotsAreWrittenPerTenantAndTheirContentNeverCrossesTenants`
  (multi-tenant, 2 tenants) and
  `SnapshotSingleTenantControlIT.eventsAppendedAfterASnapshotAreStillSourcedOnASingleTenantApplication`
  (plain app, default context, directly registered `InMemorySnapshotStore`).
- Workload: ledger opened, then 5 `AddAmount(1)`. Snapshot trigger fires at 3 events. Correct total
  after sourcing = 5.
- Observed:
  - multi-tenant arm: total = **2** -- the snapshot is restored and the events appended AFTER it are
    NOT applied (2 = the state as of the snapshot). Reproduced in run20 and run21.
  - single-tenant arm: total = **8** -- the snapshot is restored AND the events it already contains
    are replayed on top (3 from the snapshot + all 5 events). run21.
- Vector: multi-tenant:FAIL (under-counts) single-tenant:FAIL (over-counts). Both wrong, in
  OPPOSITE directions, so this is not a multi-tenancy-only defect: the snapshot position handling in
  the sourcing path is wrong, and the two composition routes get it wrong differently
  (`SnapshotCapableEventStorageEngine.decorate` applied per tenant by
  `MultiTenantEventStorageEngine.compose` vs applied globally by
  `SnapshotSourcingConfigurationEnhancer`).
- Isolation itself was NOT refuted: neither arm shows one tenant's amount (1000) inside the other's
  total, and each tenant does have its own snapshot in its own store.
- Both `2` and `8` are silent data corruption: no error is raised, the entity simply decides on wrong
  state (a balance check, a capacity check, an idempotency check would all be wrong).

## Retracted candidates (harness, not engine)

- "Acknowledged appends never sourced / all sourcing empty" -- the fixture's events lacked
  `@EventTag`; untagged appends are legal and tag-based sourcing correctly finds nothing.
  Fixed; `MultiTenantReadYourWritesIT` + `SingleTenantControlIT` now green and kept as
  positive coverage of MT-C4 read-your-writes/durability.
- run7 `burstOfCreatedContextsAllBecomeServedTenants` red -- the per-test tenant-prefix
  predicate rejected the un-prefixed `burst-*` context names. Fixed.
- Subscription-update timeouts in run6/7 first pass -- fixture emitted for `BalanceQuery`
  while subscriptions watch `WatchAccount`. Fixed.

## Run log

| Run | What | Result |
|---|---|---|
| run4 | S2 red arm, first clean execution | FAIL as predicted (finding F-MT-1) |
| run5 | full battery | poisoned: test classes recompiled mid-run + leftover contexts; all errors were harness cascade, no engine signal. Harness fixed: setUp purges contexts, teardown null-guarded, no builds during runs. |
| run11 | ReadYourWrites + SingleTenantControl after @EventTag fix | all green -- retraction confirmed, MT-C4 read-your-writes/durability positively covered |
| run12 | S8 + S2 + S3 | S8 PASS; S2 red arm FAIL (F-MT-1) + fix arm PASS (pinned); S3 PASS |
| run13 | TenantLifecycle + S8 seed 2 | Removal FAIL again (F-MT-3, same silent-hang shape); RapidContext 2x PASS (run7 red was the prefix harness bug); S8 PASS (seed 2) |
| run14 | chaos, first bounded run | WEDGED by a harness bug: recovery await threw, try-with-resources closed the pool before `stop` was set, workers spun forever on instantly-failing dispatches. Killed. Notably: NO recovery within 3+ minutes of the AS restart -- workers' dispatches failed synchronously the whole time. Test hardened: stop in finally, failure capture, 50ms backoff. |
| run15 | chaos, hardened | FAIL: no recovery in 3 min after restart; accepted=61059 unknown=13343, lastFailure=AXONIQ-4003 -- F-MT-5 |
| run16 | single-tenant chaos differential, first attempt | INCONCLUSIVE: harness (ControlHandlers had no DepositMoney handler); fixed |
| run18 | residual scenarios promoted to real arms: S9 snapshot isolation on real AS, S10 true cross-node emission (emitting handler pinned to app-2 only), S11 query drain on disconnect | all three PASS -- MT-C5 covered end to end, MT-M2 gap closed positively, in-flight query at shutdown terminates |
| run19 | S12 partial network fault (in-process TCP proxy, SO_LINGER-0 reset of all established connections, server stays up) | PASS -- dispatch resumes and per-tenant conservation holds. Sharpens F-MT-5: a transient CONNECTION break is survived; the non-recovery is specific to a full Axon Server RESTART. |
| run20 | S13 snapshot content isolation (real trigger) + S11b strict drain oracle | drain STRICT arm PASS (2s in-flight query gets its value -> the `awaitTermination` weaknesses are latent, stay residuals); snapshot arm FAIL: tenant A total 2 instead of 5 |
| run22 | S14 repeated-fault chaos (3 reset rounds, rounds 2-3 with 25ms per-chunk latency) | PASS -- resume after every round, per-tenant conservation held |
| run21 | snapshot differential | multi-tenant FAIL (2, under-counts) + single-tenant control FAIL (8, over-counts) -> F-MT-6, not multi-tenancy-specific |
| run17 | single-tenant chaos differential | FAIL identically (accepted=12, unknown=6247, AXONIQ-4003) -- F-MT-5 attributed to connector/framework stack, not multi-tenancy |
| run6 | full battery, clean | Partials: F-MT-1 red arm reproduced again (39.97s class run, red as predicted). Workaround arm progressed past tenant resolution (correlation provider works: inner handler now names tenant A) but hit cross-test zombie interference. S6 rapid-context-lifecycle: both tests PASS (MT-C7 holds). SubscriptionQueryUpdateDelivery arms: all timed out -- HARNESS (fixture emitted for BalanceQuery while subscriptions watch WatchAccount); fixed. S5 evidence step poisoned by the same fixture bug. NEW SIGNAL: every one of 6 application shutdowns logged "Timed out during shutdown phase [536870911]" (inbound-connector phase) -- promoted to scenario ShutdownLatencyIT. Harness hardened: per-test unique tenant names + per-app tenant-prefix connect predicate (zombie apps cannot join later tests' contexts). |
