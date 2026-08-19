# ADR 006: Tenant-aware dead-letter queue integration (issue [#214](https://github.com/AxonIQ/axoniq-framework/issues/214))

Date: 2026-08-19
Status: work in progress

> This ADR records the intended direction and outstanding review questions. It is subject to change and must be
> updated as the implementation, tests, and documentation progress.

## Context

Dead-letter queues are generic framework infrastructure. Multi-tenancy needs two additional guarantees around them:

- Enqueuing a dead letter must select the queue and its storage resources for the tenant handling the event.
- Processing a dead letter later must establish that tenant before the queue invokes event handling.

The multi-tenancy module must not make dead-letter-queue support mandatory for applications that do not use it. At the
same time, applications using JDBC or JPA queues may need a different `DataSource` and therefore a different queue
factory for every tenant. A generic `DeadLetterQueueConfiguration#factory()` cannot be assumed to make that selection.

## Decision: optional, auto-detected DLQ integration

The multi-tenancy module has an optional dependency on the DLQ module. Its dedicated DLQ configuration enhancer checks
whether the DLQ classes are present and applies tenant-aware DLQ wiring only when they are available. When the DLQ
module is absent, the enhancer does nothing.

This retains a composable module boundary: enabling multi-tenancy alone neither loads nor requires dead-letter-queue
infrastructure, while an application that includes both modules receives the tenant-aware integration automatically.

A dedicated multi-tenancy-DLQ bridge module was considered. It would also keep the two dependencies separate, but the
direct optional dependency plus the missing-class guard already provides that optionality. Introducing another module
only to connect these capabilities would add structure without adding a distinct runtime or configuration boundary, so
it is deliberately not used.

## Decision: configuration-owned DLQ tenant lifecycle

The tenant-aware DLQ integration uses a configuration-owned registry as the owner of tenant lifecycle and cached
queues:

1. The multi-tenancy enhancer registers a DLQ registry as a configuration component.
2. `TenantComponentProviderSubscriber` subscribes that registry to the `TenantProvider` at application startup and
   retains the resulting registration for shutdown.
3. The registry owns the `TenantScopedCache` instances that lazily create tenant-specific DLQs and evict them when a
   tenant is removed.
4. `TenantRoutingSequencedDeadLetterQueue` remains a thin adapter: it resolves the tenant from the processing context
   and asks the registry for that tenant's DLQ. It neither owns a cache nor subscribes itself.
5. `TenantRoutingSequencedDeadLetterQueueFactory` passes the registry to each routing queue it creates.

The registry keys its lazily created `TenantScopedCache` instances by processing group, configuration, and delegate
factory. It registers every known tenant with a new cache before using it, and cancels the tenant's registrations in
every cache when that tenant is removed. Re-adding a tenant consequently creates fresh tenant queues.

This reuses the established subscription lifecycle: a configuration-owned component receives tenant registration and
removal events, and its subscription is explicitly cancelled at application shutdown. It prevents dynamically created
queues from subscribing in their constructors and leaving a subscription with no owner.

Alternatives considered and rejected for now:

- **Self-subscribing routing queues:** small implementation, but each dynamically created queue owns no retained
  registration and has no explicit cancellation path.
- **A factory that subscribes itself and forwards tenant changes to queues:** avoids per-queue subscriptions, but the
  factory must retain and coordinate registrations for every dynamically created queue. This duplicates the registry's
  lifecycle responsibility without integrating with the established subscriber.
- **Keeping the local `ConcurrentHashMap`:** avoids lifecycle wiring but cannot evict tenant queues when a tenant is
  removed, leaving stale tenant-specific resources reachable.

Queue resolution belongs to the asynchronous DLQ API. An unresolved tenant must complete the returned
`CompletableFuture` exceptionally instead of throwing before a future is returned.

## Implementation status

The decision is implemented by `TenantRoutingSequencedDeadLetterQueueRegistry`, the tenant-routing queue and factory,
the DLQ configuration enhancer, and `TenantComponentProviderSubscriber`.

Focused tests cover:

- enhancer detection of both the present and absent optional DLQ dependency, including registration of the registry;
- subscriber registration of the registry with the `TenantProvider`;
- lazy cache creation after a tenant is registered and cache eviction across multiple processing groups; and
- factory-created queues routing an operation to the queue of the tenant in the processing context.

## Open question: tenant-aware queue factories

The implementation deliberately leaves the factory contract unchanged and passes
`DeadLetterQueueConfiguration#factory()` to the registry. This may be insufficient for JDBC and JPA queue factories:
they can need a tenant-specific `DataSource`, while a shared factory has no tenant input with which to choose it.

Determine an application extension point that derives a queue factory, or a queue, from the `TenantDescriptor`. The
chosen contract must allow storage to remain physically tenant-specific and must be proven with a tenant-specific
datasource-backed queue test.

## Consequences

- Applications that do not include the DLQ module retain multi-tenancy without any DLQ classes or behaviour.
- Applications that include both modules receive tenant-aware queue routing without duplicating tenant-context setup.
- Tenant queue instances are evicted on removal and recreated on re-registration, rather than remaining reachable in
  a routing queue cache.
- The current delegate factory remains shared; physical datasource isolation is pending the open factory-contract
  decision.
- Framework and application enhancers retain ordering space, so applications can override defaults when necessary.

## Follow-up work

- Resolve and test the tenant-aware factory contract, including a datasource-backed queue.
- Add Antora/AsciiDoc documentation for multi-tenancy DLQ configuration and storage.
- Update this work-in-progress ADR if the factory contract or implementation changes.
