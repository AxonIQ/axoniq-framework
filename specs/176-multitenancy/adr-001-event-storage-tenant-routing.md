# ADR 001: Per-tenant event storage routing (issue [#209](https://github.com/AxonIQ/axoniq-framework/issues/209))

Date: 2026-07-23
Status: accepted
Related: [ADR 002, multi-tenant pooled streaming](adr-002-pooled-streaming.md) ([#210](https://github.com/AxonIQ/axoniq-framework/issues/210)), [#213](https://github.com/AxonIQ/axoniq-framework/issues/213) (per-tenant JPA storage), [#205](https://github.com/AxonIQ/axoniq-framework/issues/205) (no tenant data in stored events), [#206](https://github.com/AxonIQ/axoniq-framework/issues/206) (tenant-scoped components)

## Context

Every tenant gets its own event store in its own Axon Server context. Writing, sourcing, snapshotting, and the read stream that feeds projections must all reach the right tenant's store. The tenant is already known during handling. The `RegisterTenantDescriptorHandlerInterceptor` from [#206](https://github.com/AxonIQ/axoniq-framework/issues/206) puts it on the `ProcessingContext` (under `TenantDescriptor.RESOURCE_KEY`).

The design question is at which layer to route. The `EventStore` is the facade a command handler talks to. The `EventStorageEngine` is the storage the store delegates to.

## Options considered

Both options give each tenant its own store in its own context. They differ in which layer picks the tenant.

| | Option A: route at the `EventStore` | Option B: route at the `EventStorageEngine` |
|---|---|---|
| **How** | A custom `EventStore` replaces the default and delegates to a full per-tenant store. | A custom `EventStorageEngine` sits under the stock `StorageEngineBackedEventStore` and routes per tenant. |
| **Pros** | Nothing that lasts (see the decision). | Reuses the stock store, interceptors, tagging, and bus once. Snapshots route per tenant on the same engine. Smallest custom surface. Storage-agnostic, so [#213](https://github.com/AxonIQ/axoniq-framework/issues/213) is a different per-tenant factory on the same engine. |
| **Cons** | Re-implements the `EventStore` contract and duplicates the store machinery for every tenant. | The engine is multi-natured. It routes writes, merges reads, and is the `SnapshotStore`, which is coherent for a federating engine but worth knowing when reading it. |

## Decision: route at the `EventStorageEngine`, and merge reads there too

We choose option B. One `MultiTenantEventStorageEngine`, registered as `EventStorageEngine.class`, sits under the stock `StorageEngineBackedEventStore` with a dual, explicit contract.

- **writes** (`appendEvents` and `source`, which carry a `ProcessingContext`) resolve exactly one tenant and route to that tenant's engine.
- **reads** (`stream`, `firstToken`, `latestToken`, `tokenAt`, which carry no context) span all current tenants, merged.

![Full flow of the multi-tenant event storage design](adr-001-design-flow.svg)

Option A's one apparent advantage, needing no read-side coordination, does not hold. The read side builds on this engine either way, and a store facade is itself a `StreamableEventSource`, which only adds to the component-resolution ambiguity.

Verified against Axon Framework `main` (`5fca18d34e`). The `appendEvents` and `source` methods carry the context, and the sourcing path always runs inside the command's context. A pooled streaming processor's default source is the `EventStore`, which is the app's only `StreamableEventSource`, so no separate source needs registering.

## Reads merged natively

`stream()` folds the read side into the engine. For each current tenant it maps `engine.stream(condition)` to tag every entry with the tenant (`withResource(TenantDescriptor.RESOURCE_KEY, tenant)`), left-folds the per-tenant streams through core's `MergedMessageStream` (the primitive that does the concurrent merge, comparator selection, and completion), and positions the result with a module-owned `MultiTenantTrackingToken`.

This drops the separate `MultiTenantStreamableEventSource`, the engine-to-source adapter, and `DynamicSourcesTrackingToken`. That wrapper token only existed because `MultiStreamableEventSource.open()` rejects any token that is not a `MultiSourceTrackingToken`. Merging natively removes the constraint.

The token must still be union-tolerant. A source that only one side knows is treated as at its beginning, because the processor's coordinator compares persisted tokens outside the engine. That tolerance is confined to this module-owned token, so the shared `MultiSourceTrackingToken` guardrail stays intact (the reasoning is in the [ADR 002 addendum](adr-002-addendum-dynamic-sources-token.md)). Its class name lands in customer token stores, so a later rename needs a legacy type mapping.

## Snapshots

A `MultiTenantSnapshotStore` resolves the tenant from the context and routes `load` and `store` to the tenant's snapshot store, built by a `TenantSnapshotStoreFactory` (the AxonServer default uses the same cached connection as the engine). It is an internal collaborator of the routing engine rather than a component of its own. This meets the [#209](https://github.com/AxonIQ/axoniq-framework/issues/209) snapshot requirement.

Snapshot resolution has to stay below the fan-out. There are two ways an entity load reaches a snapshot. The slow route calls `SnapshotStore.load` and then sources the events that follow it. That is two round trips. The fast route passes `SourcingStrategy.Snapshot` to `source` and the engine resolves the snapshot within that one call, which an engine can only do when it is its own `SnapshotStore`, as `PostgresqlEventStorageEngine` is. The event sourcing defaults complement an engine that is not the configured `SnapshotStore` with the slow route, and that complement consumes the snapshot strategy. It resolves the snapshot itself and delegates a plain position-based sourcing inward. Applied above the fan-out it would resolve snapshots before a tenant is known, and no tenant engine could ever use its fast route.

Two things follow. First, the per-tenant engine has to arrive already able to resolve its tenant's snapshots, so `TenantEventStorageEngineFactory` decorates it while it is built, once per tenant, through `snapshotCapable`. The comparison there is by identity rather than a check for whether the engine implements `SnapshotStore`, mirroring the defaults. An engine that resolves snapshots from its own storage while its tenant's snapshots were written to a different store still needs decorating. Second, nothing may decorate the routing engine itself, so `MultiTenantEventStorageEngine` is a `SnapshotStore` as well, delegating `load` and `store` to the routing snapshot store, and both types resolve to one instance. The defaults leave an engine untouched when it is the configured snapshot store, so the strategy reaches the tenant's own engine intact. Both components stay lazy, built on first use, because resolving them pulls in the per-tenant factories. Resolving the engine therefore also verifies the two are one instance: an application registering a `SnapshotStore` of its own fails there rather than silently resolving snapshots above the fan-out. An application replacing the `EventStorageEngine` itself is not detected, since the registration simply backs off.

The routing engine therefore only routes, and [#213](https://github.com/AxonIQ/axoniq-framework/issues/213) can bring a snapshot resolving per-tenant engine that keeps its single round trip.

## Per-tenant lifecycle: lazy create, evict on removal

Segments are created lazily and cached, and evicted when the `TenantProvider` removes a tenant.

Eviction is a correctness requirement, not hygiene. `AxonServerTenantProvider.removeTenant()` disconnects the tenant's connection itself, so a create-only cache would keep serving an engine bound to a dead connection after a re-add. An inactivity or TTL trigger is not usable here. While any streaming processor runs, every live tenant's engine stream needs to be held open.

Each factory holds a small `TenantScopedCache`. It is a `ConcurrentHashMap` keyed by tenant that creates a component lazily on first use (`computeIfAbsent`) and drops it when the `Registration` returned to the `TenantProvider` is cancelled. This mirrors the intent of the existing `TenantComponentProvider` (lazy create, drop on removal, subscribed to the tenant lifecycle), rather than reusing it, because the cached value here is a storage component (an event storage engine or snapshot store) rather than a user component. On removal the restarter re-opens the stream without the tenant, so its per-tenant stream closes around the eviction. Eviction while a stream is briefly still open is tolerable, and is covered with the read side in [#283](https://github.com/AxonIQ/axoniq-framework/pull/283).

## The nullable `ProcessingContext`

`appendEvents` and `source` take a `@Nullable ProcessingContext`, so where it is absent matters.

- `source` only runs inside a transaction bound to the command's context, so it always has the tenant.
- `appendEvents` is context-bound on the command path. The only context-less path is a direct publish, where the tenant is resolved from event metadata. It fails as unresolved when none is found, or as ambiguous when a batch spans tenants (mixed-tenant batches are unsupported).
- The read methods take no context by contract, so they never depend on one.

## Responsibility across the two PRs

This ADR describes the whole design, but each PR keeps its own responsibility, so the read side is built in #283 rather than pulled into #209.

- #209 (this ADR) delivers the write and storage side. That is the `MultiTenantEventStorageEngine` routing for `appendEvents` and `source`, the `MultiTenantSnapshotStore`, the factories, and the per-tenant lifecycle.
- #283 ([ADR 002](adr-002-pooled-streaming.md)) delivers the read side on top of that engine. That is the merge in the engine's `stream` and token methods, the `MultiTenantTrackingToken`, the `MultiTenantStreamingProcessorRestarter`, and the read-side documentation and tests.

Keeping the read work in #283 leaves each PR at its own responsibility. #283 also renumbers its ADR and addendum to 002.

Two consequences of this split are worth stating so they are not read as gaps in #209.

- **Release gating.** #209 and #283 ship in the same milestone. Until #283 lands, the read-side methods throw, so #209 is never released on its own with an unusable read side. The split is for reviewability across two pull requests, not across two releases.
- **Cross-tenant tests.** The two-tenant integration test that proves per-tenant isolation end to end needs the read side to assert what landed in which tenant. It therefore lives in #283, next to the merge it exercises.

## From the proof of concept to this plan

Two things predate this plan, and both are scaffolding rather than a baseline. The `_multitenancy_poc/` directory holds an older, broad prototype (an Axon Framework 4 style multi-tenant event store, snapshot store, and per-tenant processor and query segments). This branch's module then has a reworked slice already committed, the `TenantRoutingEventStore` and its `TenantEventSegmentFactory`. Both route at the `EventStore`. The plan routes at the `EventStorageEngine` instead.

| | Proof of concept (poc directory and current branch) | This plan |
|---|---|---|
| **Routing layer** | The `EventStore` facade, registered under its own type. | The `EventStorageEngine`, under the stock `StorageEngineBackedEventStore`. |
| **Per-tenant unit** | A full `StorageEngineBackedEventStore` per tenant. | A per-tenant `EventStorageEngine` plus a per-tenant `SnapshotStore`. |
| **Snapshots** | Not handled in the committed slice. | Per tenant, resolved by the same engine. |
| **Reads** | Not in the committed slice. | Merged inside the engine. |
| **Lifecycle** | Segments are cached but never follow the tenant lifecycle. | Segments are created lazily and evicted when a tenant is removed. |

## Implementation

New, all `@Internal`: `MultiTenantEventStorageEngine`, `MultiTenantTrackingToken`, `MultiTenantSnapshotStore`, `TenantEventStorageEngineFactory`, `TenantSnapshotStoreFactory`. Deleted: `TenantRoutingEventStore`, `TenantEventSegmentFactory`, `AxonServerTenantEventSegmentFactory`.

The routing engine registers in `AxonServerMultiTenancyConfigurationDefaults` (order `MIN_VALUE + 7`) under both `EventStorageEngine` and `SnapshotStore` as one instance, before `AxonServerConfigurationEnhancer` (`MIN_VALUE + 10`), whose `registerIfNotPresent` for both types then backs off. Naming uses the `MultiTenant*` prefix, matching `MultiTenantAxonServerCommandBusConnector`. The single-tenant SPI types (`TenantDescriptor`, `TenantProvider`, `TenantResolver`) keep their names.

Tests:

- `MultiTenantTrackingToken` serialization round-trip across `TestConverter.all()`, plus union-tolerant comparison tests.
- `stream()` tests and write-routing tests ported from the existing suites.
- lifecycle tests (remove evicts the cached component, re-add gets a fresh one). Eviction under an open stream lands with the read side in #283.
- a two-tenant snapshot integration test, delivered in #283 with the read side that lets it assert per-tenant isolation.
- the enhancer-ordering test adjusted.

## Scope

Slice 2 (persistent streams) bypasses the `StreamableEventSource` path and needs its own tenant labelling, so it is unaffected. Slice 3 (per-tenant processors and token stores) is deferred. The per-tenant engines stay available from the factory for it to build on.

## Consequences

- One federating engine, the standard `EventStore`, and the standard processor wiring. Reads and writes share one path.
- Snapshots per tenant, resolved below the fan-out, so a snapshot resolving per-tenant engine keeps its single round trip.
- [#213](https://github.com/AxonIQ/axoniq-framework/issues/213) becomes a JPA per-tenant factory on the same engine.
- A module-owned token means a tenant-set change replays the affected tenant from the start (the specified behaviour), and the class name is a permanent commitment in token stores.
