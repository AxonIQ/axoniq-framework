# Multi-Tenancy Hunt -- Claims, Gaps and Scenarios

Scope: the multi-tenancy feature of Axoniq Framework 5 as present on
`feature/176/208-multi-tenant-queries` (module `messaging/axoniq-multi-tenancy`, plus the
`AxonServerQueryBusConnector` changes that branch makes in `connector/axon-server-connector`).

Method: `axon-hunt` skill, part 2. Every scenario below is tied to a claim (MT-C*) or a gap
(MT-M*). Real infrastructure: multi-context Axon Server via `AxonServerTestInfrastructure`
(licensed, DCB contexts). In-memory arms where the property is expressible there. PostgreSQL
only for projections (not used yet; no streaming read side exists on this branch -- see MT-X1).

Out of scope on this branch (documented TODOs, not findings):

- MT-X1: `MultiTenantEventStorageEngine.stream/firstToken/latestToken/tokenAt` throw
  `UnsupportedOperationException` ("resolved with #210"). Pooled-streaming projections are
  therefore not testable on this branch, and every projection-like behaviour below is driven
  from command/query handling contexts instead.

## Claims

| # | Kind | Confidence | Statement (anchored) | Falsified by |
|---|---|---|---|---|
| MT-C1 | routing/semantics | documented | "a command dispatched while handling another message stays with the tenant of that message instead of having to name its tenant again" -- `MultiTenantAxonServerCommandBusConnector.resolveConnector` Javadoc (same wording on the query connector) | a follow-up message dispatched from a handler without naming its tenant failing, or being handled without the tenant of the triggering message |
| MT-C2 | isolation | documented | subscription-query updates are delivered exactly to the subscriptions of the emitting tenant: "a matching QueryMessage only fires when its tenantId metadata equals the tenant resolved from the given context" -- `TenantAwareQueryBus` class Javadoc; ADR `query-update-emitter-tenant-isolation-adr.md` constraint 2 asserts "a subscription only lives in tenant-A's context because it carried tenantId=A" | a subscription legitimately registered on tenant A's channel that never receives an update emitted for tenant A |
| MT-C3 | membership | documented | queries for a tenant that is not (or no longer) served are rejected with `TenantNotResolvedException` -- `TenantAwareQueryBus.query` Javadoc | such a query being answered |
| MT-C4 | isolation/durability | documented | "each tenant's events live in its own store"; appends and sourcing route to the one resolved tenant -- `MultiTenantEventStorageEngine` Javadoc | an event appended for tenant A observable when sourcing in tenant B, or cross-tenant append-condition interference on the same entity id |
| MT-C5 | isolation | documented | per-tenant snapshot composition: "Both stay within one tenant" -- `MultiTenantEventStorageEngine` Javadoc | a snapshot written for tenant A used when sourcing tenant B |
| MT-C6 | semantics | code-inferred (OSS `SimpleQueryBus` gates emits via `runAfterCommitOrImmediately`; `DistributedQueryBus.emitUpdate` ignores its `context` parameter entirely) | subscription-query updates emitted during message handling are delivered only if the unit of work commits | an update emitted by a handler that subsequently fails still reaching the subscriber |
| MT-C7 | membership | documented | contexts created/deleted at runtime are discovered and become/stop being tenants -- `AxonServerTenantProvider` Javadoc ("Subscribe to context updates to add/remove tenants at runtime") | a created context that never becomes a tenant, or a burst of creations leaving stragglers |
| MT-C8 | durability | documented (shutdown-phase Javadoc on `AxonServerMultiTenancyConfigurationDefaults`: disconnect first, "then its dispatching is shut down", implying orderly drain) | a command accepted before shutdown was initiated completes, and its events are durably appended | an accepted command failing mid-shutdown because a shared per-tenant connection was closed under it |

## Gaps (the docs are silent)

| # | Statement of silence | Evidence searched |
|---|---|---|
| MT-M1 | Nothing states what an ACTIVE subscription-query stream observes when its tenant's context is deleted at runtime: terminal signal, error, or silence | `TenantAwareQueryBus`, connector Javadoc, ADR, ITs (`sendingQueryToDeletedTenantFails` covers new queries only, not established streams) |
| MT-M2 | Nothing states whether an update emitted on instance 2 reaches a subscription registered through instance 1 (same tenant) | `DistributedQueryBus.updateRegistry` is process-local; `QueryBusConnector` has no broadcast; docs never say "same instance only" |
| MT-M3 | Nothing wires the tenant id into outgoing message metadata: `MetadataBasedTenantResolver` Javadoc says "Combined with a CorrelationDataProvider that propagates the same metadata key, this enables automatic tenant context propagation" -- but `MultiTenancyConfigurationDefaults` registers no such provider | `MultiTenancyConfigurationDefaults.enhance`, grep for `CorrelationDataProvider` in `axoniq-multi-tenancy` (zero hits) |
| MT-M4 | Nothing states the fate of the shared per-tenant `AxonServerConnection` when one connector disconnects: this branch adds `connection.disconnect()` to `AxonServerQueryBusConnector.disconnect()`, and command connector, event storage engine and query connector of one tenant share that connection via `AxonServerConnectionManager.getConnection(tenantId)` | branch diff of `AxonServerQueryBusConnector`; `createConnector` in both multi-tenant connectors; `AxonServerTenantEventStorageEngineFactory.perTenantEngine` |

## Scenarios

Template fields: ORACLE / WORKLOAD / EVIDENCE / AMBIGUITY / BUDGET. All run against real
multi-context Axon Server. Verdicts are recorded in `formal/MT-FINDINGS.md` as they land.

### S1 `context_routed_subscription_updates` (MT-C2)
- ORACLE: a subscription query routed to its tenant by the ProcessingContext (no `tenantId`
  metadata on the message) receives an update emitted from that same tenant's context within
  10s of emission. Compared arm: the identical subscription carrying `tenantId` metadata.
- WORKLOAD: 1 subscription per arm, 1 matching emit from a query handler of that tenant.
- EVIDENCE: initial result arrives on both arms (proves the subscription reached the tenant's
  context and the registry).
- AMBIGUITY: an initial result that never arrives is INCONCLUSIVE (routing failure, different
  defect class), not a pass or fail of the update oracle.
- BUDGET: single-shot; deterministic.

### S2 `tenant_propagation_across_hops` (MT-C1, MT-M3)
- ORACLE: a follow-up command dispatched from inside a tenant-A command handler, without
  naming its tenant, is handled with tenant A resolved: the handler observes the tenant in
  its context, and an event it appends lands in tenant A's store.
- WORKLOAD: 1 outer command (with metadata) -> handler dispatches 1 inner command (no
  metadata) via the context-provided command dispatcher.
- EVIDENCE: the outer command's handler resolves tenant A (already-passing IT behaviour).
- AMBIGUITY: inner-command failure is a FAIL of the claim, with the failure recorded; only an
  outer-command failure is INCONCLUSIVE.
- BUDGET: single-shot; deterministic.

### S3 `graceful_shutdown_inflight_command` (MT-C8, MT-M4)
- ORACLE: a command accepted (dispatch future obtained, handler entered) before
  `application.shutdown()` completes successfully, and its event is present in the tenant's
  store afterwards (verified by a fresh application sourcing that entity).
- WORKLOAD: 1 slow handler (300ms) per tenant, 2 tenants; shutdown initiated while handlers
  are in flight.
- EVIDENCE: handler-entered latch observed before shutdown is called.
- AMBIGUITY: a command that never entered its handler before shutdown is discarded from the
  oracle (not an accepted command).
- BUDGET: 5 repetitions; any repetition failing the oracle is a FAIL.

### S4 `precommit_update_emission_leak` (MT-C6)
- ORACLE: an update emitted by a tenant-A command handler that then fails its unit of work is
  never observed by tenant-A's subscriber. Control arm: the same handler succeeding delivers
  the update.
- WORKLOAD: 1 subscription, 1 failing command, 1 succeeding command.
- EVIDENCE: the failing command's dispatch future completes exceptionally; the control update
  arrives.
- AMBIGUITY: if the control update does not arrive, the run is INCONCLUSIVE (emission broken
  altogether, different defect).
- BUDGET: single-shot; deterministic.

### S5 `tenant_removal_active_subscription` (MT-M1, MT-C3)
- ORACLE: when the tenant's context is deleted, an active subscription stream reaches a
  terminal state (completed or error) within 30s. Silence is a FAIL of the silent-wedge kind.
- WORKLOAD: 1 subscription on tenant A; context A deleted mid-stream.
- EVIDENCE: an update delivered before deletion proves the stream was live.
- AMBIGUITY: none -- three-valued by construction.
- BUDGET: single-shot with a 30s ceiling.

### S6 `rapid_context_lifecycle_discovery` (MT-C7)
- ORACLE: after creating N=5 contexts back-to-back, all 5 are served tenants within 60s
  (command per tenant round-trips). After deleting and immediately recreating one context,
  the recreated tenant serves commands within 60s.
- WORKLOAD: context create x5 without pause; then delete+recreate of one.
- EVIDENCE: the provider's tenants() view and a served command per tenant.
- AMBIGUITY: Axon Server rejecting a create (name conflict window) is INCONCLUSIVE.
- BUDGET: 3 repetitions.

### S7 `cross_instance_update_emission` (MT-M2)
- ORACLE (characterisation, gap): an update emitted on app-2 for tenant A either reaches the
  subscription registered through app-1, or the behaviour is recorded as a documented-silence
  finding; the run also asserts the emit reports success (which, combined with non-delivery,
  is the silent-loss shape).
- WORKLOAD: two applications in one JVM, same Axon Server, same tenant; subscription on
  app-1, emit on app-2.
- EVIDENCE: app-1's own emit (control) reaches the subscription.
- AMBIGUITY: subscription registry routing is Axon Server's choice; the arm pins which app
  holds the registration by only subscribing handlers on app-1... the subscription query is
  dispatched THROUGH app-1's bus, whose connector holds the channel.
- BUDGET: single-shot.

### S8 `per_tenant_event_sourcing_isolation` (MT-C4, MT-C5)
- ORACLE: the same entity id used in tenants A and B concurrently: each tenant's sourced
  state equals the reference state computed from that tenant's own accepted commands
  (per-tenant conservation: balance == sum of that tenant's accepted deposits); no event of
  one tenant is observable in the other's sourced stream.
- WORKLOAD: 2 tenants x 1 entity id x 50 interleaved deposit commands per tenant, dispatched
  concurrently from 4 threads.
- EVIDENCE: both tenants accept >0 commands; concurrency observed (interleaved completion).
- AMBIGUITY: a command rejected by concurrency conflict is retried up to 3 times, then counted
  as not-accepted (excluded from the reference state); an unknown outcome (timeout) makes the
  run INCONCLUSIVE.
- BUDGET: 5 seeds (thread interleavings are non-deterministic; the oracle is
  interleaving-insensitive).

### S9 `per_tenant_snapshot_isolation` (MT-C5)
- ORACLE: the `TenantSnapshotStoreFactory` yields a distinct, stable `SnapshotStore` per tenant id
  (never the same instance for two tenants), and after activity on the same entity id in both
  tenants each tenant's sourced balance equals its own deposits only.
- WORKLOAD: open + 5 deposits per tenant on one shared account id, amounts 1 vs 1000.
- EVIDENCE: both tenants' stores resolve; both balances non-zero.
- AMBIGUITY: a failed command makes the run INCONCLUSIVE rather than failing the isolation oracle.
- BUDGET: single-shot.

### S10 `true_cross_node_update_emission` (MT-M2, decisive form)
- ORACLE: with the emitting command handler present on EXACTLY ONE instance (app-2) and the
  subscription registered through an instance with NO command handlers (app-1), the update emitted
  on app-2 reaches app-1's subscriber within 15s.
- WORKLOAD: 1 subscription on app-1; 1 `EmitBalanceUpdate` dispatched, routable only to app-2.
- EVIDENCE: app-1's initial result arrives (its bus holds the registration); the emitting command
  completes successfully.
- AMBIGUITY: a failed emitting command is INCONCLUSIVE, not a delivery failure.
- BUDGET: single-shot.

### S11 `query_drain_on_disconnect` (MT-C8 territory)
- ORACLE: a query whose handler has entered and is still running when `shutdown()` is called
  reaches a terminal state (value or error) within 45s -- never abandoned.
- WORKLOAD: 1 `SlowQuery` (2s) in flight; shutdown forced on handler entry.
- EVIDENCE: the handler-entered flag observed before shutdown.
- AMBIGUITY: none -- the outcome is one of value / error / abandoned.
- BUDGET: single-shot.

### S12 `partial_network_fault` (chaos, transient break)
- ORACLE: with the application connected through an in-process TCP proxy, severing every
  established connection (SO_LINGER 0 reset) while Axon Server stays UP must be survived:
  dispatching resumes within 2 minutes, and per-tenant conservation holds
  (`accepted <= balance <= accepted+unknown`, per tenant's own amount space).
- WORKLOAD: 2 tenants, continuous deposits (1 vs 1000) from 2 threads.
- EVIDENCE: accepts before the fault; unknown outcomes after it.
- AMBIGUITY: unknown-outcome deposits are counted separately and never folded into either bound.
- BUDGET: single-shot, 2-minute recovery ceiling.

### S13 `snapshot_content_isolation` (MT-C5, with a real trigger)
- ORACLE: with `@Snapshotting(afterEvents = 3)` on a dedicated entity, a snapshot exists in each
  tenant's own `SnapshotStore` (awaited -- the write is fire-and-forget and happens during sourcing),
  and the state sourced afterwards equals that tenant's own events only.
- WORKLOAD: same ledger id in 2 tenants, open + 5 adds each, amounts 1 vs 1000.
- EVIDENCE: both snapshots load non-null from their tenant's own store.
- AMBIGUITY: a snapshot that never appears is INCONCLUSIVE for the isolation oracle (trigger
  problem, different defect class) -- reported as such rather than as a leak.
- BUDGET: single-shot, 20s per snapshot await.
- NOTE: a dedicated entity is used, because `@Snapshotting` requires a `SnapshotStore` and the
  single-tenant control arms run the shared entity without one.

### S14 `repeated_fault_chaos` (multi-fault, degraded link)
- ORACLE: three successive connection-reset rounds (rounds 2-3 on a link with 25ms per-chunk
  injected latency) each resume dispatching within 90s, and after the last round per-tenant
  conservation holds in each tenant's own value space.
- WORKLOAD: 2 tenants, continuous deposits (1 vs 1000).
- EVIDENCE: per round, unknown-outcome count rises (fault landed) and accepted count then rises
  again (resumed) -- both asserted per round, not just at the end.
- AMBIGUITY: unknown outcomes are counted separately and never folded into either bound.
- BUDGET: 3 rounds, 90s resume ceiling each.

### S11b `query_drain_strict` (the drain-await weaknesses, independent oracle)
- ORACLE: a query needing 2s of handler time, already in flight when the disconnect starts, must
  produce its VALUE -- the connector's `queryInProgressAwait` budget is 5s, so a correct drain has
  time to finish it. An error or cancellation instead means the drain did not wait.
- WORKLOAD: 1 `SlowQuery(2000)`; shutdown forced on handler entry.
- EVIDENCE: the handler-entered flag observed before shutdown.
- AMBIGUITY: none -- value / error / abandoned are distinguishable.
- BUDGET: single-shot.

## Knowledge-usage notes

- Existing ITs (`MultiTenantCommandHandlingIT`, `MultiTenantQueryHandlingIT`,
  `AxonServerTenantProviderIT`) cover: metadata-carrying dispatch, dynamic tenant add,
  deleted-tenant rejection for NEW dispatches, tenant-scoped injection, subscription
  isolation for metadata-carrying subscriptions with query-handler-context emits. Nothing
  covers: follow-up dispatch, event-sourced entities on the multi-tenant AS store,
  subscription streams across tenant removal, shutdown, multi-instance, rollback gating.
- The unit tests in `axoniq-multi-tenancy` prove isolation at the routing layer with
  in-memory fakes; none cross the gRPC boundary.
