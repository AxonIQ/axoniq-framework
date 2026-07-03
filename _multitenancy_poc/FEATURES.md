# Multi-Tenancy Scope

## Constraints

* Focus on Axon Server
* Main goal is to support separating tenant data in transit (dispatch, handle) and at rest (event storage, projections)
* Opposed to the AF4 approach we want to avoid duplication of infrastructure components (gateways, buses) - where possible we split on the lowest possible level (Axon Server Connection)
* The AF4 multi-tenancy extension (https://github.com/AxonFramework/extension-multitenancy) is a reference; this integration should be a lot more minimal
* Out of scope: running one application instance per tenant - this does not require this feature at all, and is already achievable today via static configuration (e.g. pointing an instance at a single default context)
* Out of scope: mixing single-tenant and multi-tenant-aware components within the same application instance is not supported

## Base Idea 
* in storage a tenant is represented by an Axon Server context
* Components are multi-tenant-aware and fan out on connections internally
* outbound messages carry tenant information that is resolved into the appropriate context connection
* inbound messages are enriched with the appropriate tenant information provided by the context connection (and/or metadata)
* Multi-tenancy will become a regular module of the AxoniqFramework repo (like dead-letter-queue), not remain a separate repo as for AF4

## Requirements Overview
* Infrastructure: registration and removal of tenants, resolving and providing tenant information for message handling components
* Commands: multi-tenant-aware command dispatching, fanning out to per-tenant Axon Server connections without changing the CommandGateway/CommandBus API; plus reestablishing tenant information on the handling side
* Event Storage: one Axon Server context (event store) per tenant, with routing to the correct store resolved from the message
* Event Handling/Sourcing: multi-tenant support for subscribing, pooled, and persistent-stream-based event processors; plus reestablishing tenant information on the handling side
* Queries: multi-tenant-aware query dispatching, mirroring the approach taken for commands; plus reestablishing tenant information on the handling side
* Tenant-aware components: allow application-specific, tenant-scoped components (e.g. a per-tenant SQL datasource) to be registered and injected into message handling methods
* Dead Letter Queue: tenant-aware datasource selection when enqueueing dead letters, and correct tenant context when processing them
* Data Protection: tenant-aware crypto store selection for the data-protection extension
* Spring Boot Autoconfiguration: wire up multi-tenancy support out of the box, while allowing it to be disabled/excluded and application-specific components to be configured

### Infrastructure

#### Requirements

* Tenants must be known to the system, both via static (startup-time) configuration and dynamic (runtime) discovery/removal
* The system must be able to filter out discovered contexts that should not be treated as tenants
* New or removed tenants must trigger (un)registration hooks on all tenant-aware components (connectors, event store, event processors, component registries)
* The tenant associated with a message must be resolvable when processing it, either from the message itself or from the connection it arrived on
* Outgoing messages must resolve which tenant's connection to dispatch/publish on, based on the tenant carried in the message
* Inbound/sourced messages must be enriched with the tenant determined by the connection they arrived on, written into their metadata
* The tenant id must be propagated into the metadata of events appended during processing, so any downstream consumer can determine the tenant purely from the message
* Axon Framework's transaction facilities must be tenant-aware, providing tenant-specific transactional resources (e.g. a per-tenant JDBC connection) to message processing (see `### Event Handling/Sourcing` `#### Implementation` for the current `JdbcTenantTransactionManager`)
* A server API to create/remove tenants is needed to test dynamic tenant registration/removal in integration tests

#### Questions

* Tenant name resolution: today tenant discovery is 1:1 with the Axon Server context name. A tenant spanning multiple contexts (e.g. multiple bounded contexts for one application) would need a naming scheme (e.g. `[tenant]-[context]`) and a resolver deriving the tenant id from it - not yet implemented
* Tenant information: Only Metadata or needed in ProcessingContext (or both if not MetadataBasedTenantResolver)
  * con: map lookup is cheap, we have everything we need 
  * Resolved by the current implementation: metadata-only. The tenant-aware `ParameterResolverFactory` (`TenantComponentResolver` inside `MultiTenancyConfigurationDefaults`) reads tenantId directly from message metadata at resolution time - nothing is cached on `ProcessingContext`
  * Reasoning: metadata is unavoidable regardless - it's what survives the wire and is available before a ProcessingContext exists (dispatch interceptors, connector-level routing); a second cached value on ProcessingContext would risk drifting from the message's actual metadata; and it's ultimately about getting messages to the correct tenant, everything else (projections, etc.) derives from the message itself
  * Exception to reconsider: transaction manager hooks may only receive `ProcessingContext`, not the original message - if so, tenant-specific transactional resource selection might require tenant information to be available on `ProcessingContext` as well, unlike the parameter-resolution case above

#### Implementation

* `TenantDescriptor` (record: `tenantId` + `properties` map) - `api/TenantDescriptor.java`
* `TenantProvider` - `api/TenantProvider.java`; sole impl `AxonServerTenantProvider` (`configuration/AxonServerTenantProvider.java`, `@Internal`) discovers tenants from static context names or the Axon Server Admin API, and can subscribe to runtime context add/remove events
* `TenantConnectPredicate` - `api/TenantConnectPredicate.java`, functional predicate deciding whether a discovered tenant should be wired in, with a default `alwaysTrue()`
* `TenantResolver<M>` - `api/TenantResolver.java`, resolves the tenant of a message; sole impl `MetadataBasedTenantResolver` (`configuration/MetadataBasedTenantResolver.java`) reads the `tenantId` metadata key (configurable), throws `NoSuchTenantException` if missing
* `TenantResolverRegistry` - `api/TenantResolverRegistry.java`, holds resolvers per message type (command/query/event) plus a global fallback; sole impl `DefaultTenantResolverRegistry` (`configuration/DefaultTenantResolverRegistry.java`, `@Internal`)
* `MultiTenantAwareComponent` - `api/MultiTenantAwareComponent.java`, the `registerTenant`/`unregisterTenant` contract implemented by every tenant-fanout component
* `NoSuchTenantException` - `api/NoSuchTenantException.java`
* Metadata propagation: `MultiTenancyConfigurationDefaults` decorates the `CorrelationDataProviderRegistry` with `SimpleCorrelationDataProvider(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY)`
* Central wiring: `MultiTenancyConfigurationDefaults` (`configuration/MultiTenancyConfigurationDefaults.java`) - a `ConfigurationEnhancer` registering `TenantConnectPredicate.alwaysTrue()`, `AxonServerTenantProvider`, and `DefaultTenantResolverRegistry` by default

### Commands

#### Requirements

* Multi-tenancy must not require changes to the existing CommandGateway/CommandBus public API
* Command dispatching must route each command to the Axon Server context belonging to its resolved tenant
* Command handlers must be able to resolve tenant-specific resources (e.g. a per-tenant datasource) needed during handling, based on the tenant information carried in the message's metadata (see `### Infrastructure`)

#### Questions

* Sequencing: is it required that sequencing must not affect other tenants, or should the application developer be able to configure a single full-sequential policy across all tenants? Likely answer: ship an additional per-tenant sequencing policy with the multi-tenancy extension that the application developer can configure - not yet implemented. Sequencing policies are built at the generic message level, not per message type, so the same policy applies to `### Event Handling/Sourcing` as well

#### Implementation

* Local handler registration is unaffected - handlers still register on the plain `SimpleCommandBus` (`org.axonframework.messaging.commandhandling.SimpleCommandBus`, AF5 core, unchanged)
* `MultiTenantAxonServerCommandBusConnector` (`commandhandling/MultiTenantAxonServerCommandBusConnector.java`) - extends `AbstractAxonServerCommandBusConnector`, implements `MultiTenantAwareComponent`; keeps a per-tenant `TenantState` (own Axon Server connection, local subscription map, in-flight tracker), replays known subscriptions to newly-registered tenants, resolves the target tenant via the injected `TenantResolver`
* The `TenantResolver` is injected into the connector as a hard dependency at construction (via the enhancer), rather than resolved from a registry/context per call
* Events appended by the handler get their tenantId written back to metadata via the `SimpleCorrelationDataProvider` mechanism described under `### Infrastructure`
* Registered as the `CommandBusConnector` by `MultiTenancyConfigurationDefaults`


### Events Storage

#### Requirements

* Each tenant must have its own isolated event store, backed by a dedicated Axon Server context
* Events must be appended to and read from the correct tenant's event store, based on the tenant resolved from the message

#### Questions

* Could this be solved at the `EventStorageEngine` level instead of `EventStore`?
* Should the aggregate-based storage engine be supported per tenant as well, or is the tag-based `AxonServerEventStorageEngineFactory` sufficient for all use cases? Not currently wired in for multi-tenancy.
* Snapshot support: the analogous `SnapshotStore` decoration for multi-tenancy is currently disabled (commented out) in `MultiTenancyConfigurationDefaults` - do we need a tenant-routing snapshot store, and if so when?

#### Implementation

* `TenantRoutingEventStore` (`eventsourcing/TenantRoutingEventStore.java`) - implements `EventStore` and `MultiTenantAwareComponent`; resolves the tenant-specific `EventStore` segment from event metadata and delegates to it; `open(StreamingCondition, ProcessingContext)` intentionally throws `UnsupportedOperationException` (cross-tenant streaming is handled at a higher level, see `### Event Handling/Sourcing`)
* `TenantEventSegmentFactory` (`api/TenantEventSegmentFactory.java`) - `Function<TenantDescriptor, EventStore>` used by `TenantRoutingEventStore` to build/obtain each tenant's segment
* Default per-tenant segment (`MultiTenancyConfigurationDefaults.defaultEventStoreSegment(...)`) uses `AxonServerEventStorageEngineFactory.constructForContext(tenant.tenantId(), config)` wrapped in a `StorageEngineBackedEventStore`, falling back to an in-memory event storage engine when no `AxonServerConnectionManager` is present
* Registered as an `EventStore` decorator (`DECORATION_ORDER = Integer.MIN_VALUE + 75`) by `MultiTenancyConfigurationDefaults`

### Event Processing

#### Requirements

* Event processing must support per-tenant pooled streaming processors
* Event processing must support per-tenant persistent-stream-based processors
* Event handlers must be able to resolve tenant-specific resources (e.g. a per-tenant datasource) needed during handling, based on the tenant information carried in the message's metadata (see `### Infrastructure`), so results are written to the correct tenant-specific projection
* Sequencing uses the same per-tenant policy as commands (see `### Commands` `#### Questions`) - sequencing policies operate at the generic message level, not per message type

#### Questions

* Is a subscribing-style multi-tenant event processor required, or is pooled/persistent-stream sufficient? No `MultiTenantSubscribingEventProcessor` (or equivalent) currently exists.
* Can a single event processor multiplex on projection/token per tenant, instead of one processor per tenant? What are the consequences?
* Do we need a dedicated `MultiTenantEventProcessorModule`/`MultiTenantPooledStreamingEventProcessorModule` at all, or can multi-tenancy be achieved by decorating/wrapping the existing (single-tenant) event processor components, the same way `TenantRoutingEventStore` decorates `EventStore`?

#### Implementation

* `MultiTenantPersistentStreamMessageSource` (`eventstreaming/MultiTenantPersistentStreamMessageSource.java`) - implements `SubscribableEventSource` and `MultiTenantAwareComponent`; fans a single consumer out to one `PersistentStreamMessageSource` per tenant, built via `TenantPersistentStreamMessageSourceFactory` (`api/TenantPersistentStreamMessageSourceFactory.java`)
* `MultiTenantEventProcessor` (`eventhandling/processing/MultiTenantEventProcessor.java`) - implements `StreamingEventProcessor` and `MultiTenantAwareComponent`; decorator wrapping one `StreamingEventProcessor` per tenant, aggregating/fanning-out segment operations (`splitSegment`, `mergeSegment`, `releaseSegment`, `resetTokens`, `processingStatus`, `maxCapacity`)
* `MultiTenantEventProcessorModule` (`eventhandling/processing/MultiTenantEventProcessorModule.java`) - static factory, the multi-tenant counterpart of `EventProcessorModule` (e.g. `MultiTenantEventProcessorModule.pooledStreaming("name")`)
* `MultiTenantPooledStreamingEventProcessorModule` (`eventhandling/processing/streaming/pooled/...`) - fluent module producing per-tenant `PooledStreamingEventProcessor`s, each with an isolated event source, token store, executor threads, and dead letter queue
* `TenantEventProcessorSegmentFactory` (`eventhandling/processing/TenantEventProcessorSegmentFactory.java`) - `Function<TenantDescriptor, EventProcessor>`
* Per-tenant token stores: `TenantTokenStoreFactory` (interface) with `InMemoryTenantTokenStoreFactory`, `JdbcTenantTokenStoreFactory` (+ `TenantConnectionProviderFactory`), `JpaTenantTokenStoreFactory` - under `eventhandling/processing/streaming/token/store/`
* Per-tenant transaction management: `TenantTransactionManagerFactory` (interface), `JdbcTenantTransactionManager` (`eventhandling/processing/transaction/`) - opens one JDBC connection per processing lifecycle, tied to the tenant's own datasource

### TenantAware general components support

#### Requirements

* Application developers must be able to register tenant-scoped components (e.g. a per-tenant SQL datasource) that get injected into message handling methods, so the correct tenant's resource is used

#### Questions

* Naming issue: `TenantComponentRegistry<T>`'s name suggests a registry that can hold many different tenant-aware component types, but it's actually generic over a single type `T` - each instance only holds per-tenant instances of that one component type. To support multiple component types, multiple separate `TenantComponentRegistry` instances are needed (one per type). Naming needs to be fixed to reflect this. Known issue, not yet fixed in code (`TenantComponentRegistry.java`).
* Spring Boot autoconfiguration: how convenient should registration of application-specific components (`TenantComponentRegistry`) be made? Currently there is no autoconfiguration for this - only the `MultiTenancyConfigurationDefaults` enhancer bean is provided (see `### Spring Boot Autoconfiguration`)
  * Idea: automatically wire any Spring bean implementing a designated tenant-component interface (e.g. `TenantComponentFactory`) so it becomes resolvable as a message-handling parameter without manual registration

#### Implementation

* `TenantComponentRegistry<T>` (`api/TenantComponentRegistry.java`) - extends `MultiTenantAwareComponent`; caches component instances per tenant, lazily created via a `TenantComponentFactory`, and cleans them up (if `AutoCloseable`) on tenant removal
* `TenantComponentFactory<T>` (`api/TenantComponentFactory.java`) - user-implemented factory (`TenantDescriptor -> T`), e.g. building a per-tenant JDBC repository
* `DefaultTenantComponentRegistry<T>` (`configuration/DefaultTenantComponentRegistry.java`, `@Internal`) - sole implementation
* Injection: a `ParameterResolverFactory` (`TenantComponentResolver`, private nested class in `MultiTenancyConfigurationDefaults`) reads the tenantId from message metadata and resolves the component via `registry.getComponent(tenant)`
* Demonstrated in the examples: `examples/multi-tenancy-java`, `examples/multi-tenancy-jdbc-java` (`CourseStatsConfiguration.java`, `JdbcCourseStatsRepositoryTest`), `examples/multi-tenancy-spring-boot-4` (`MultiTenancyConfiguration.java`)


### Queries

#### Requirements

* Query dispatching must route each query to the Axon Server context belonging to its resolved tenant, mirroring the approach used for commands
* Query handlers must be able to resolve tenant-specific resources (e.g. a per-tenant datasource) needed to read from the correct projection, based on the tenant information carried in the message's metadata (see `### Infrastructure`)

#### Implementation

* `MultiTenantAxonServerQueryBusConnector` (`query/MultiTenantAxonServerQueryBusConnector.java`) - extends `AbstractAxonServerQueryBusConnector`, implements `MultiTenantAwareComponent`; structurally mirrors `MultiTenantAxonServerCommandBusConnector` (per-tenant connection, subscription map, in-flight tracker, replay on tenant registration)
* Registered as the `QueryBusConnector` by `MultiTenancyConfigurationDefaults`

### Dead Letter Queue

#### Requirements

* Enqueueing dead letters must select tenant-aware datasource resources, and should be covered by `### Event Handling/Sourcing` out of the box (per-tenant pooled processors already build per-tenant handling components with dead letter queues, per the module's own Javadoc)
* Processing dead letters is a separate flow: it needs a means to establish the correct tenant context before invoking the DLQ for processing

#### Questions

* Do we want the multi-tenancy module to depend on the DLQ module (to build tenant-aware DLQ support), or should DLQ integration remain optional/pluggable?

#### Implementation

* Not yet implemented - no tenant-aware DLQ class exists (e.g. no `TenantAwareSequencedDeadLetterQueue`)
* `MultiTenantPooledStreamingEventProcessorModule`'s Javadoc already mentions "dead letter queues per tenant" as part of the per-tenant handling component decoration it builds (see `### Event Handling/Sourcing`), but this is not backed by a dedicated tenant-aware DLQ processing mechanism
* Generic (non-tenant-aware) DLQ building blocks exist in `dependency-injection/spring/spring-boot-autoconfigure/`: `DeadLetterQueueAutoConfiguration`, `JdbcDeadLetterQueueAutoConfiguration`, `JpaDeadLetterQueueAutoConfiguration`, `DeadLetterQueueProcessorProperties` - not multi-tenancy-specific

### Data Protection

* Support multi-tenancy for the data-protection extension: select the correct crypto store per tenant
* AF4 has no special support for this; likely needs a tenant-aware crypto component, since a multitude of stores may be provided


### Spring Boot Autoconfiguration

* provide enhancer as bean
* disable feature/exclude enhancer
* How to configure application specific components (TenantComponentRegistry)?




## General Questions

* The PoC's `TenantDescriptor`'s equals/hashCode include `properties`, so it doesn't behave as identity based purely on `tenantId` - since it's used as a map key, two descriptors for the same tenant with differing properties won't match as the same key. Only `tenantId` should determine identity. Known issue, not yet fixed in code (`TenantDescriptor.java`).
* For integration testing and examples we want an offline license file to run CI

## References

* AxoniqFramework issue: https://github.com/AxonIQ/axoniq-framework/issues/176
* AxonFramework issue: https://github.com/AxonIQ/AxonFramework/issues/4324
* Data Protection extension issue: https://github.com/AxonIQ/extension-data-protection/issues/8
* AF4 multi-tenancy extension (prior art): https://github.com/AxonFramework/extension-multitenancy

