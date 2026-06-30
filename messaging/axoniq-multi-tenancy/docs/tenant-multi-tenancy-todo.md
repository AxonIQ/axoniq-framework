<!--
Copyright (c) 2010-2026. AxonIQ B.V.

Licensed under the AXONIQ TERMS OF SERVICE,
Version 29 April 2026 (the "License");
-->

# Tenant Multi-Tenancy Todo

This list compares the current `axoniq-multi-tenancy` module against the local `_tmp` and `_old` reference trees under `messaging/axoniq-multi-tenancy/`.

## Feature Work

- Spring Boot auto-configuration
    - Provide a Spring Boot starter that auto-registers the tenant multi-tenancy infrastructure.
    - Expose tenant-aware beans through auto-configuration so applications do not need manual wiring.
    - Keep the auto-configuration aligned with the existing non-Spring configuration path.



## What We Already Have

- [x] Tenant model and registry APIs
  - `TenantDescriptor`
  - `TenantProvider`
  - `TenantResolver` and `TenantResolverRegistry`
  - `TenantComponentRegistry` and `TenantComponentFactory`
- [x] Default tenant resolution and component registration
  - metadata-based tenant resolution
  - default registry implementations
  - configuration enhancer hooks
- [x] Axon Server tenant bootstrap and context management
  - `AxonServerTenantProvider`
  - Axon Server-specific context discovery / creation support
- [x] Multi-tenant command/query/event routing for the Axoniq Framework integration
  - `MultiTenantAxonServerCommandBusConnector`
  - `MultiTenantAxonServerQueryBusConnector`
  - `TenantRoutingEventStore`
- [x] Event processor support
  - `MultiTenantEventProcessor`
  - `MultiTenantEventProcessorModule`
  - `MultiTenantPooledStreamingEventProcessorModule`
- [x] Streaming token store factories
  - in-memory
  - JDBC
  - JPA
- [x] Tenant-local JDBC transaction bridging
  - `TenantTransactionManagerFactory`
  - `JdbcTenantTransactionManager`
- [x] Tenant-aware persistent stream message source support
  - `MultiTenantPersistentStreamMessageSource`
- [x] Working example coverage in `examples/multi-tenancy-jdbc-java`
  - tenant-specific H2 datasources
  - tenant-specific projections
  - tenant-specific JDBC token store wiring
  - tenant-specific transaction manager wiring

## What Is In `_tmp` or `_old` and Not Yet Covered

- [ ] Snapshot store tenant routing
  - `TenantRoutingSnapshotStore`
  - `TenantSnapshotStoreSegmentFactory`
- [ ] Processing-context tenancy helpers
  - tenant-aware processing context wrapper
  - processing-context resolvers and factories
  - annotation-based tenant component resolution
- [ ] Event processor control and scheduling from `_old`
  - scheduler support
  - event processor control segments
  - dead-letter queue support
- [ ] Embedded / integration-level compatibility tests from `_tmp` and `_old`
  - multi-tenant embedded integration tests
  - tenant resolver registry tests beyond the current defaults
  - full factory wiring tests for command/query/event routing
- [ ] Documentation parity from `_old`
  - reference docs for configuration
  - disable/enable guides
  - multi-tenant component catalog

## Won't Do - Solved Differently

- [x] Command bus routing on the full legacy surface
  - `TenantRoutingCommandBus`
  - `TenantCommandSegmentFactory`
  - legacy command interception helpers
  - solved by the tenant-aware bus connector layer instead
- [x] Query bus routing on the full legacy surface
  - `TenantRoutingQueryBus`
  - `TenantQuerySegmentFactory`
  - legacy query update emitter helpers
  - solved by the tenant-aware bus connector layer instead

## Likely Next Technical Gaps

- [ ] Add coverage for tenant-specific `PooledStreamingEventProcessor` startup in the example module.
- [ ] Add coverage for tenant-specific projection transactions with JPA as well as JDBC.
- [ ] Add coverage for command/query connector behavior when multiple tenants are active at once.
- [ ] Decide which legacy `_old` features are intentionally out of scope for the Axoniq Framework port.
