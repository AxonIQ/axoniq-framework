# Tenant Projections Plan

Goal: support users who do not use persistent streams, but instead run their own tenant-specific event processors with a tenant-specific datasource for projections and token storage.

## Context

- `_old` shows the AF4 approach: tenant routing through a thread-local transaction wrapper and datasource routing.
- `_tmp` shows the AF5 direction: per-tenant event processor segments, per-tenant token stores, and tenant-specific transactional executor providers.
- The current `axoniq-multi-tenancy` module already covers command, query, event-store, and persistent-stream routing.
- The missing piece is the projection path for custom event processors that use JDBC or JPA per tenant.

## Implementation Progress

- Copied the `_tmp` tenant-projection event-processing tree into `src/main/java/io/axoniq/framework/messaging/multitenancy/eventhandling/...`.
- Added the missing `eventhandling/processing/streaming` and `eventhandling/processing/streaming/token` package markers.
- Normalized the copied classes to the `io.axoniq` module namespace and aligned their `@since` / author javadocs.

## Desired Shape

- Each tenant gets its own processor segment.
- Each processor segment owns its token store, projection persistence resources, and executors.
- Tenant identity comes from message metadata and `ProcessingContext`, not from thread-local state.
- The transaction boundary must be explicit and tenant-local.

## Plan

1. Add a tenant-scoped persistence factory abstraction.
- Resolve a tenant-specific `DataSource` or `EntityManagerFactory`.
- Build a tenant-specific `TransactionalExecutorProvider` or equivalent transaction executor.
- Cache the result per tenant.

2. Reuse the tenant-scoped executor across the projection stack where possible.
- If token store and projection share one datasource, use the same tenant-local transaction provider for both.
- If they use separate datasources, create separate tenant-local transaction providers and document that atomicity is limited to each datasource.

3. Model the event processor path after the `_tmp` multi-tenant processor design.
- Create tenant-specific processor segments lazily.
- Use tenant-specific naming such as `processorName@tenantId`.
- Replay subscriptions and processor configuration when a tenant appears after startup.

4. Keep the tenant resolution flow explicit.
- Resolve tenant from message metadata and `ProcessingContext`.
- Do not introduce a thread-local tenant carrier.
- Use tenant-aware parameter resolution if handler code needs direct access to the tenant or tenant-scoped components.

5. Wire token store and projection together per tenant.
- For JDBC, create one `JdbcTokenStore` per tenant with a tenant-specific `JdbcTransactionalExecutorProvider`.
- For JPA, create one `JpaTokenStore` per tenant with a tenant-specific `JpaTransactionalExecutorProvider`.
- Use the same tenant-local persistence setup for the projection side when it shares the same datasource.

6. Keep transaction semantics clear.
- A successful event batch should commit projection changes and token advancement together when both share a datasource and transaction manager.
- If projection and token store use different datasources, the system must be treated as at-least-once between those resources.
- Projection handlers must remain idempotent in the split-datasource case.

7. Add tests that prove tenant isolation and transaction behavior.
- Verify that each tenant gets its own token store and processor segment.
- Verify that token advancement and projection writes are consistent for the same datasource.
- Verify that tenant-specific failures do not leak across tenants.
- Verify that dynamic tenant registration creates a usable processor segment without restart.

## Non-Goals

- Do not restore the AF4 thread-local transaction model.
- Do not duplicate the full infrastructure graph per tenant.
- Do not make persistent streams the only supported event-processing mode.

## Open Questions

- Should the tenant-scoped persistence factory live in the core multi-tenancy module or in a dedicated event-processing submodule?
- Should JDBC and JPA share one common abstraction for tenant-local transactional execution, or should each stay with its own provider type?
- Do we need a tenant-aware projection component resolver in the same release, or can that follow the processor and token-store wiring?
