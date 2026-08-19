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

The tenant-routing queue will use the established `TenantScopedCache`. It therefore follows the same tenant
registration and eviction semantics as the other tenant-aware infrastructure components.

Queue resolution belongs to the asynchronous DLQ API. An unresolved tenant must complete the returned
`CompletableFuture` exceptionally instead of throwing before a future is returned.

## Open question: tenant-aware queue factories

The final factory contract remains open. Passing `DeadLetterQueueConfiguration#factory()` through unchanged may be
incorrect: JDBC and JPA queue factories can need a tenant-specific `DataSource`, and a shared factory has no tenant
input with which to choose it.

While implementing the review feedback, determine an application extension point that derives a queue factory, or a
queue, from the `TenantDescriptor`. The chosen contract must allow storage to remain physically tenant-specific and
must be proven with a tenant-specific datasource-backed queue test.

## Consequences

- Applications that do not include the DLQ module retain multi-tenancy without any DLQ classes or behaviour.
- Applications that include both modules receive tenant-aware queue routing without duplicating tenant-context setup.
- One tenant's queue and datasource selection remains independent of every other tenant's.
- Framework and application enhancers retain ordering space, so applications can override defaults when necessary.

## Follow-up work

- Add a configuration test proving that the enhancer applies tenant-aware wiring when the DLQ module is present, as
  well as the existing no-op behaviour when it is absent.
- Add focused tests for tenant cache lifecycle, asynchronous unresolved-tenant failure, and tenant-specific queue
  factory selection.
- Document how applications configure tenant-aware DLQ processing and storage.
- Update this ADR with the final factory API, implementation locations, and verification evidence.
