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
* Design principle: when introducing new APIs that accept a message, also consider whether `ProcessingContext` should be
  accepted alongside it - several tenant-aware resolution paths in this feature need both together (see
  `### Infrastructure` `#### Questions` for the metadata-vs-`ProcessingContext` discussion)

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
* Dynamic tenant registration/removal must be exercised in integration tests using Axon Server's existing Admin API to create/remove contexts (no new server-side API needs to be built)
* For integration testing and examples, an offline Axon Server license file is needed to run CI
* Provide reference documentation section

#### Questions

* Known issue: the PoC's `TenantDescriptor`'s equals/hashCode include `properties`, so it doesn't behave as identity based purely on `tenantId` - since it's used as a map key, two descriptors for the same tenant with differing properties won't match as the same key. Only `tenantId` should determine identity. Not yet fixed in code (`TenantDescriptor.java`).
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
* Extend the AxonServerContainer to create/remove contexts

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
* Should the aggregate-based storage engine be supported per tenant as well, or is the tag-based
  `AxonServerEventStorageEngineFactory` sufficient for all use cases? Not currently wired in for multi-tenancy - tracked
  as a separate deferred issue (see `## Planning`), not part of the main Events Storage issue
* Snapshot support: the analogous `SnapshotStore` decoration for multi-tenancy is currently disabled (commented out) in `MultiTenancyConfigurationDefaults` - do we need a tenant-routing snapshot store, and if so when?

#### Implementation

* `TenantRoutingEventStore` (`eventsourcing/TenantRoutingEventStore.java`) - implements `EventStore` and `MultiTenantAwareComponent`; resolves the tenant-specific `EventStore` segment from event metadata and delegates to it; `open(StreamingCondition, ProcessingContext)` intentionally throws `UnsupportedOperationException` (cross-tenant streaming is handled at a higher level, see `### Event Handling/Sourcing`)
* `TenantEventSegmentFactory` (`api/TenantEventSegmentFactory.java`) - `Function<TenantDescriptor, EventStore>` used by `TenantRoutingEventStore` to build/obtain each tenant's segment
* Default per-tenant segment (`MultiTenancyConfigurationDefaults.defaultEventStoreSegment(...)`) uses `AxonServerEventStorageEngineFactory.constructForContext(tenant.tenantId(), config)` wrapped in a `StorageEngineBackedEventStore`, falling back to an in-memory event storage engine when no `AxonServerConnectionManager` is present
* Registered as an `EventStore` decorator (`DECORATION_ORDER = Integer.MIN_VALUE + 75`) by `MultiTenancyConfigurationDefaults`

### Event Processing

#### Requirements

* Event processing must support per-tenant pooled streaming processors
    * Support database transactions to span token and projection persistence
* Event processing must support per-tenant persistent-stream-based processors
* Event handlers must be able to resolve tenant-specific resources (e.g. a per-tenant datasource) needed during handling, based on the tenant information carried in the message's metadata (see `### Infrastructure`), so results are written to the correct tenant-specific projection
* Sequencing uses the same per-tenant policy as commands (see `### Commands` `#### Questions`) - sequencing policies operate at the generic message level, not per message type

#### Questions

* Is a subscribing-style multi-tenant event processor required, or is pooled/persistent-stream sufficient? No `MultiTenantSubscribingEventProcessor` (or equivalent) currently exists.
* Can a single event processor multiplex on projection/token per tenant, instead of one processor per tenant? What are the consequences?
    * consequences would be that we cannot have tenant-specific projection data sources that share the database
      transaction with the token store to make sure the token is only persisted when the event is processed
* Do we need a dedicated `MultiTenantEventProcessorModule`/`MultiTenantPooledStreamingEventProcessorModule` at all, or can multi-tenancy be achieved by decorating/wrapping the existing (single-tenant) event processor components, the same way `TenantRoutingEventStore` decorates `EventStore`?
* Do we need to support event replays per tenant?

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
* Do we need to support dynamic tenant aware components (dynamically adding a new tenant adds a new repository instance
  in the config connecting to the relevant database)?

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

#### Requirements

* Support multi-tenancy for the data-protection extension: select the correct crypto store per tenant

#### Questions

* AF4 has no special support for this; likely needs a tenant-aware crypto component, since a multitude of stores may be provided - details tracked separately (see `## References` - Data Protection extension issue)
* Is it feasible to fan out multi-tenancy at the crypto-store (`CryptoEngine`) level, or does the `FieldEncryptingConverter` itself need to become a multi-tenant-aware component?
  * `FieldEncryptingConverter.convert(Object, Type)` has no access to `Message`/`ProcessingContext`/tenant info at all - making the converter itself tenant-aware would mean changing the core `Converter` SPI
  * `CryptoEngine` (`getOrCreateKey(String id)`, `getKey`, `deleteKey`) is the natural fan-out seam instead: either namespace the key id with the tenant id inside a wrapping `CryptoEngine`, or use the same `TenantComponentRegistry`-style pattern used elsewhere in the POC to resolve a distinct `CryptoEngine` instance per tenant (needed for full physical isolation, e.g. separate Vault paths/JDBC schemas per tenant)
  * Where would tenant info come from at that point? `CryptoEngine.getOrCreateKey/getKey` receives only a single opaque `String` (`prefix + @DataSubjectId field value`, both fixed by the object being converted and its class annotation) - no `Message`, `ProcessingContext`, or ambient context reaches that call, and `CryptoEngine` is a fixed singleton wired once at bootstrap, not resolved per call. So tenant info can only reach it via (a) making the `@DataSubjectId` value itself tenant-qualified (a data-model change), or (b) a tenant-aware `CryptoEngine` implementation consulting some ambient state set up at the edge around the conversion call (would need to respect the "no ThreadLocals except at edges" principle) - there is no existing plumbing to thread tenant identity through the conversion call chain

#### Implementation

* Not yet implemented - no crypto/data-protection code exists anywhere in this module; the data-protection extension itself lives in the separate `extension-data-protection` repo


### Spring Boot Autoconfiguration

#### Requirements

* Multi-tenancy support must be automatically wired in Spring Boot applications by default (property-driven, no manual bean registration required)
* It must be possible to disable multi-tenancy support entirely via configuration
* Application-specific tenant-aware components (e.g. `TenantComponentRegistry`) must be configurable from Spring Boot - not yet implemented (see `### TenantAware general components support` `#### Questions`)

#### Implementation

* `MultiTenancyAutoConfiguration` (`dependency-injection/spring/spring-boot-autoconfigure/.../MultiTenancyAutoConfiguration.java`) - the sole Spring glue for multi-tenancy, keeping the core module Spring-free:
  * `multiTenancyConfigurationDefaults()` bean - registers `MultiTenancyConfigurationDefaults` when `axon.multitenancy.enabled` is true/absent and no bean already exists (`@ConditionalOnProperty(matchIfMissing = true)` + `@ConditionalOnMissingBean`)
  * `disableMultiTenancyConfigurationEnhancer()` bean - when `axon.multitenancy.enabled=false`, registers a `ConfigurationEnhancer` that disables `MultiTenancyConfigurationDefaults`
* Test: `MultiTenancyAutoConfigurationTest` covers all three states (auto-registered, disabled via property, user-provided enhancer preserved)

## References

* AxoniqFramework issue: https://github.com/AxonIQ/axoniq-framework/issues/176
* AxonFramework issue: https://github.com/AxonIQ/AxonFramework/issues/4324
* Data Protection extension issue: https://github.com/AxonIQ/extension-data-protection/issues/8
* AF4 multi-tenancy extension (prior art): https://github.com/AxonFramework/extension-multitenancy

## Planning

Incremental delivery plan for graduating `_multitenancy_poc` into a regular AxoniqFramework module - moving and reworking code from the POC into its final location issue by issue, rather than one big-bang port. One issue per feature area (mirrors the `###` sections above); each issue also resolves that area's open `#### Questions` as part of its own acceptance criteria (no separate design-spike issues). Dead Letter Queue and Data Protection are deferred past the end-of-July milestone. Proposed issues are children of AxoniqFramework issue #176 (see `## References`).

Each bold-titled bullet below is a separate GitHub issue and PR. "Phase" headings are dependency/sequencing groups, not units of work - e.g. Phase 3 lists three independent issues that can be worked in parallel once Phase 1 and 2 land, not one combined issue.

#176 becomes the tracking issue to complete once all required work is done with a github task list referencing
sub-issues.

Each issue also adds or updates the corresponding Antora reference documentation for that feature area (see
`docs/CLAUDE.md`) as part of its own PR - documentation ships with the code, not as a follow-up.

### Phase 0 - Prerequisites

* **"Add an offline Axon Server license file for CI"** - unblocks integration tests for every subsequent issue (see `### Infrastructure` `#### Requirements`)
* **"Scaffold the `axoniq-multi-tenancy` module in the main repo"** - empty module skeleton, POM/BOM wiring, CI pipeline
  wiring, package structure; no functional code moved yet, create documentation skeleton for multitenancy feature (
  create sections required)

### Phase 1 - Infrastructure (foundation; everything else depends on it)

* **"Infrastructure: tenant registration, resolution and wiring skeleton"**
  - Ports `TenantDescriptor`, `TenantProvider`/`AxonServerTenantProvider`, `TenantConnectPredicate`, `TenantResolver`/`MetadataBasedTenantResolver`/`TenantResolverRegistry`, `MultiTenantAwareComponent`, `NoSuchTenantException`, metadata propagation (`SimpleCorrelationDataProvider`), and the `MultiTenancyConfigurationDefaults` skeleton
  - Fixes the `TenantDescriptor` equals/hashCode bug while porting (identity must be `tenantId`-only)
  - Resolves: the `[tenant]-[context]` naming scheme for tenants spanning multiple contexts (implement, or explicitly scope out for July with a follow-up note); whether transaction-manager hooks need tenant info on `ProcessingContext`
  - Wires integration tests to create/remove tenants dynamically via Axon Server's existing Admin API (no new server-side API needed)

### Phase 2 - Tenant-aware component support (needed by Commands/Queries/Event Processing's handling side)

* **"Tenant-aware components: per-tenant component registry and parameter injection"**
  - Ports `TenantComponentRegistry`, `TenantComponentFactory`, `DefaultTenantComponentRegistry`, and the `TenantComponentResolver` `ParameterResolverFactory`
  - Resolves: the `TenantComponentRegistry` naming fix (it holds many tenants' instances of one component type, not many component types) and clarifies the multiple-registries-per-type-T story

### Phase 3 - Message dispatch & storage (can proceed in parallel once Phase 1+2 land)

* **"Commands: multi-tenant command dispatching and handling"**
  - Ports `MultiTenantAxonServerCommandBusConnector`; reconciles the POC's modified `AxonServerCommandBusConnector` with the mainline `axon-server-connector` module
  - Resolves: the per-tenant sequencing policy design and implementation (shared with the Event Processing issue below - implement once here, reuse there)
* **"Queries: multi-tenant query dispatching and handling"**
  - Ports `MultiTenantAxonServerQueryBusConnector`; reconciles the POC's modified `AxonServerQueryBusConnector` with mainline
* **"Events Storage: per-tenant event store routing"**
  - Ports `TenantRoutingEventStore`, `TenantEventSegmentFactory`; reconciles the POC's modified `AxonServerEventStorageEngineFactory` with mainline
  - Resolves: `EventStorageEngine`-vs-`EventStore`-level routing, and the disabled multi-tenant snapshot store
    decoration
  - Does not include aggregate-based storage engine support - tracked separately (see "Deferred past July" below)

### Phase 4 - Event processing (depends on Events Storage, Infrastructure, and Tenant-aware components)

* **"Event Processing: per-tenant streaming, pooled and persistent-stream processors"**
  - Ports `MultiTenantPersistentStreamMessageSource`, `MultiTenantEventProcessor`, `MultiTenantEventProcessorModule`, `MultiTenantPooledStreamingEventProcessorModule`, per-tenant token stores, per-tenant transaction managers
  - Resolves: whether a subscribing-style processor is required, whether a single processor can multiplex per-tenant projections/tokens, and whether a dedicated module is needed vs. decorating existing components
  - Reuses the sequencing policy implemented in the Commands issue
  - Likely the largest issue in this plan given the number of open questions and classes involved - flag for a possible mid-review split if it grows too large for one PR

### Phase 5 - Spring Boot wiring (depends on the phases above existing)

* **"Spring Boot Autoconfiguration: wire up multi-tenancy support"**
  - Ports `MultiTenancyAutoConfiguration` (enable-by-default / disable-via-property)
  - Resolves: how to autoconfigure application-specific `TenantComponentRegistry` beans - implement the "auto-wire any Spring bean implementing a tenant-component interface" idea

### Phase 6 - Documentation

* **"Migration guide: AF4 `extension-multitenancy` to AF5 built-in multi-tenancy"** - reference documentation walking
  existing AF4 extension users through migrating to the new built-in AF5 module (see `## Constraints` and
  `## References` for the AF4 extension prior art)

### Deferred past July

* **"Dead Letter Queue: tenant-aware DLQ support"** - resolves whether the multi-tenancy module should depend on the DLQ module
* **"Data Protection: tenant-aware crypto store fan-out"** - resolves the `CryptoEngine`-level fan-out approach identified above
* **"Aggregate-based event storage engine: per-tenant support"** - should-have, not must-have; resolves whether
  `AggregateBasedAxonServerEventStorageEngine` needs to be wired in per tenant alongside the tag-based
  `AxonServerEventStorageEngineFactory`

