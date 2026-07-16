# Multi-Tenancy Scope

## Constraints

* Focus on Axon Server for now — PostgreSQL-native multi-tenancy (e.g. schema/row-per-tenant) could be a future option, not addressed here
* Main goal is to support separating tenant data in transit (dispatch, handle) and at rest (event storage, projections)
* Opposed to the AF4 approach we want to avoid duplication of infrastructure components (gateways, buses) - where possible we split on the lowest possible level (Axon Server Connection)
* The AF4 multi-tenancy extension (https://github.com/AxonFramework/extension-multitenancy) is a reference; this integration should be a lot more minimal
* Mixing single-tenant and multi-tenant-aware components within the same application instance is technically feasible via the modular Axon configuration, but we do not advertise or document it as a supported feature
* There are different levels of tenancy:
  * Single-tenant application — already possible today without this feature (out of scope); achievable via static configuration, e.g. pointing an instance at a single default context
  * Logical multi-tenancy — tenant is part of the domain model, no physical data separation; already possible today without this feature
  * Logical + physical multi-tenancy — this feature's actual target: separation both in the domain and at rest (own Axon Server context per tenant)
* Supporting only logical multi-tenancy (handling tenant information without physical separation) as a dedicated, lighter-weight feature could be a future requirement — not addressed now

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
* Axoniq Workflows: multi-tenancy must work with Axoniq Workflows, similar in nature to the Data Protection requirement - workflows rely on pooled streaming event processors, which may need adjustment; deferred, not required for 5.3.0
* Spring Boot Autoconfiguration: wire up multi-tenancy support out of the box, while allowing it to be disabled/excluded and application-specific components to be configured

### Infrastructure

#### Requirements

* Tenants must be known to the system, both via static (startup-time) configuration and dynamic (runtime) discovery/removal — a concrete use-case for static configuration is quality-of-service tiering, confining specific tenants to specific instances
* The system must be able to filter out discovered contexts that should not be treated as tenants — the Axon Server default `_admin` context is never a tenant and is filtered out by default
* New or removed tenants must trigger (un)registration hooks on all tenant-aware components (connectors, event store, event processors, component registries)
* The tenant associated with a message must be resolvable when processing it, either from the message itself or from the connection it arrived on
* Outgoing messages must resolve which tenant's connection to dispatch/publish on, based on the tenant carried in the message
* Inbound/sourced messages must be enriched with the tenant determined by the connection they arrived on, written into their metadata
* The tenant id must be propagated into the metadata of events appended during processing, so any downstream consumer can determine the tenant purely from the message
* Axon Framework's transaction facilities must be tenant-aware, providing tenant-specific transactional resources (e.g. a per-tenant JDBC connection) to message processing (see `### Event Handling/Sourcing` `#### Implementation` for the current `JdbcTenantTransactionManager`)
* Dynamic tenant registration/removal must be exercised in integration tests using Axon Server's existing Admin API to create/remove contexts (no new server-side API needs to be built)
* For integration testing and examples, an offline Axon Server license file is needed to run CI
* Provide reference documentation section
* For now, the multi-tenancy module is only indirectly restricted through Axon Server itself; integrating the existing entitlement module (e.g. to restrict by number of tenants) is deferred

#### Questions

* Known issue: the PoC's `TenantDescriptor`'s equals/hashCode include `properties`, so it doesn't behave as identity based purely on `tenantId` - since it's used as a map key, two descriptors for the same tenant with differing properties won't match as the same key. Only `tenantId` should determine identity. Not yet fixed in code (`TenantDescriptor.java`).
* Tenant name resolution: today tenant discovery is 1:1 with the Axon Server context name. A tenant spanning multiple contexts (e.g. multiple bounded contexts for one application) would need a naming scheme (e.g. `[tenant]-[context]`) and a resolver deriving the tenant id from it - not yet implemented
   * Resolved: multi-context support for a single tenant is a convenience feature, not required now - deferred to after 5.3.0. Idea to explore when picked up: represent the tenant via the Axon Server replication group instead of a context-naming scheme
* Tenant information: Only Metadata or needed in ProcessingContext (or both if not MetadataBasedTenantResolver) - **reopened for reconsideration**, see below
  * con: map lookup is cheap, we have everything we need 
  * Previously resolved by the current implementation: metadata-only. The tenant-aware `ParameterResolverFactory` (`TenantComponentResolver` inside `MultiTenancyConfigurationDefaults`) reads tenantId directly from message metadata at resolution time - nothing is cached on `ProcessingContext`
  * Reasoning for that resolution: metadata is unavoidable regardless - it's what survives the wire and is available before a ProcessingContext exists (dispatch interceptors, connector-level routing); a second cached value on ProcessingContext would risk drifting from the message's actual metadata; and it's ultimately about getting messages to the correct tenant, everything else (projections, etc.) derives from the message itself
  * Exception previously flagged: transaction manager hooks may only receive `ProcessingContext`, not the original message - if so, tenant-specific transactional resource selection might require tenant information to be available on `ProcessingContext` as well, unlike the parameter-resolution case above
  * Reopened: Metadata is user-owned; where possible we should avoid putting tenant information in persisted metadata (i.e. event metadata). Points to work through:
    * On dispatch, the tenant should be resolvable via the `TenantResolver` from the `Message` itself
    * On handling, the tenant is known from the connection the message arrived on; that information still needs to be transported somehow - candidates are the `MessageStream.Entry` and/or the `ProcessingContext`, as an alternative to persisted event metadata
    * For commands there is currently no `ProcessingContext` created - introducing one (mirroring events) may be needed to carry this
    * Correlating tenant information from an inbound command to an emitted event could still happen via an interceptor and the `ProcessingContext`, without requiring the tenant id to live in persisted metadata
    * A general helper for resolving tenant information (from message and/or context) is likely needed regardless of which carrier wins

#### Implementation

* `TenantDescriptor` (record: `tenantId` + `properties` map) - `api/TenantDescriptor.java`
* `TenantProvider` - `api/TenantProvider.java`; sole impl `AxonServerTenantProvider` (`configuration/AxonServerTenantProvider.java`, `@Internal`) discovers tenants from static context names or the Axon Server Admin API, and can subscribe to runtime context add/remove events
* `TenantConnectPredicate` - `api/TenantConnectPredicate.java`, functional predicate deciding whether a discovered tenant should be wired in, with a default `alwaysTrue()`
  * Known gap: the default should exclude the Axon Server `_admin` context (never a valid tenant), but `alwaysTrue()` currently includes it - the default predicate needs to change while porting
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

* Sequencing: is it required that sequencing must not affect other tenants, or should the application developer be able to configure a single full-sequential policy across all tenants? Sequencing policies are built at the generic message level, not per message type, so the same policy applies to `### Event Handling/Sourcing` as well
  * Resolved: no tenant-specific sequencing policy is needed for 5.3.0 - deferred (see "Deferred past July")

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
* Snapshotting must be supported from the beginning - it is tightly intertwined with event storage, not a follow-up concern

#### Questions

* Could this be solved at the `EventStorageEngine` level instead of `EventStore`?
* Should the aggregate-based storage engine be supported per tenant as well, or is the tag-based
  `AxonServerEventStorageEngineFactory` sufficient for all use cases? Not currently wired in for multi-tenancy
  * Resolved: yes, support for the aggregate-based `AggregateBasedAxonServerEventStorageEngine` per tenant is a must-have for 5.3.0, part of the main Events Storage issue (not a separate deferred issue)
* Snapshot support: the analogous `SnapshotStore` decoration for multi-tenancy is currently disabled (commented out) in `MultiTenancyConfigurationDefaults` - do we need a tenant-routing snapshot store, and if so when?
  * Resolved: yes, needed from the beginning - snapshotting is a 5.3.0 must-have, re-enabling the tenant-routing `SnapshotStore` decoration as part of the Events Storage issue

#### Implementation

* `TenantRoutingEventStore` (`eventsourcing/TenantRoutingEventStore.java`) - implements `EventStore` and `MultiTenantAwareComponent`; resolves the tenant-specific `EventStore` segment from event metadata and delegates to it; `open(StreamingCondition, ProcessingContext)` intentionally throws `UnsupportedOperationException` (cross-tenant streaming is handled at a higher level, see `### Event Handling/Sourcing`)
* `TenantEventSegmentFactory` (`api/TenantEventSegmentFactory.java`) - `Function<TenantDescriptor, EventStore>` used by `TenantRoutingEventStore` to build/obtain each tenant's segment
* Default per-tenant segment (`MultiTenancyConfigurationDefaults.defaultEventStoreSegment(...)`) uses `AxonServerEventStorageEngineFactory.constructForContext(tenant.tenantId(), config)` wrapped in a `StorageEngineBackedEventStore`, falling back to an in-memory event storage engine when no `AxonServerConnectionManager` is present
* Registered as an `EventStore` decorator (`DECORATION_ORDER = Integer.MIN_VALUE + 75`) by `MultiTenancyConfigurationDefaults`

### Event Processing

#### Requirements

* Default (non-multi-tenant) layout for reference: a single shared pooled streaming event processor with one shared token store, writing to a single shared projection data source for all tenants
* Event processing must support per-tenant pooled streaming processors
    * Support database transactions to span token and projection persistence
    * Full physical separation (separate token store and connection per tenant) is a "Should" for 5.3.0, not a "Must" - see `#### Questions`
* Event processing must support per-tenant persistent-stream-based processors
    * Persistent streams are the simplest db-per-tenant solution: no per-tenant token store is needed (Axon Server tracks the position), only a per-tenant projection data source (via the tenant-aware components feature) is required client-side; persistent streams also support writing to a single shared data source just like pooled streaming does, so per-tenant projections and shared projections are not mutually exclusive
* Event handlers must be able to resolve tenant-specific resources (e.g. a per-tenant datasource) needed during handling, based on the tenant information carried in the message's metadata (see `### Infrastructure`), so results are written to the correct tenant-specific projection
* No tenant-specific sequencing policy is required for 5.3.0 (see `### Commands` `#### Questions`) - sequencing policies operate at the generic message level, not per message type, so this applies equally here

#### Questions

* Is a subscribing-style multi-tenant event processor required, or is pooled/persistent-stream sufficient? No `MultiTenantSubscribingEventProcessor` (or equivalent) currently exists.
* Can a single event processor multiplex on projection/token per tenant, instead of one processor per tenant? What are the consequences?
    * consequences would be that we cannot have tenant-specific projection data sources that share the database
      transaction with the token store to make sure the token is only persisted when the event is processed
    * Resolved: full per-tenant pooled-streaming (separate token store and connection per tenant, via `MultiTenantPooledStreamingEventProcessorModule`) is a "Should" for 5.3.0, not a "Must". The 5.3.0 default keeps a single shared pooled streaming processor and shared token store, with per-tenant projection datasources resolved via tenant-aware components at the handler level (logical separation of projections, not physical separation of the processor/token-store infrastructure). Persistent-stream-based per-tenant processors remain the 5.3.0 must-have db-per-tenant solution (see `#### Requirements`)
* Do we need a dedicated `MultiTenantEventProcessorModule`/`MultiTenantPooledStreamingEventProcessorModule` at all, or can multi-tenancy be achieved by decorating/wrapping the existing (single-tenant) event processor components, the same way `TenantRoutingEventStore` decorates `EventStore`?
* Do we need to support event replays per tenant?
    * Resolved: no, not for 5.3.0 - only add documentation explaining the implications of a replay (a replay affects all tenants)

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

* Support multi-tenancy for the data-protection extension which essentially boils down to tenant-aware key resolution

#### Questions

* AF4 has no special support for this; likely needs a tenant-aware crypto component, since a multitude of stores may be provided - tracked in https://github.com/AxonIQ/extension-data-protection/issues/8
* Is it feasible to fan out multi-tenancy at the crypto-store (`CryptoEngine`) level, or does the `FieldEncryptingConverter` itself need to become a multi-tenant-aware component?
  * `FieldEncryptingConverter.convert(Object, Type)` has no access to `Message`/`ProcessingContext`/tenant info at all - making the converter itself tenant-aware would mean changing the core `Converter` SPI
  * `CryptoEngine` (`getOrCreateKey(String id)`, `getKey`, `deleteKey`) is the natural fan-out seam instead: either namespace the key id with the tenant id inside a wrapping `CryptoEngine`, or use the same `TenantComponentRegistry`-style pattern used elsewhere in the POC to resolve a distinct `CryptoEngine` instance per tenant (needed for full physical isolation, e.g. separate Vault paths/JDBC schemas per tenant)
  * Where would tenant info come from at that point? `CryptoEngine.getOrCreateKey/getKey` receives only a single opaque `String` (`prefix + @DataSubjectId field value`, both fixed by the object being converted and its class annotation) - no `Message`, `ProcessingContext`, or ambient context reaches that call, and `CryptoEngine` is a fixed singleton wired once at bootstrap, not resolved per call. So tenant info can only reach it via (a) making the `@DataSubjectId` value itself tenant-qualified (a data-model change), or (b) a tenant-aware `CryptoEngine` implementation consulting some ambient state set up at the edge around the conversion call (would need to respect the "no ThreadLocals except at edges" principle) - there is no existing plumbing to thread tenant identity through the conversion call chain
  * Likely simplest answer (per Allard Buijze, unvalidated): fan out one layer below the `CryptoEngine`/`Converter` SPI, at datasource routing. If `JpaCryptoEngine` is combined with a tenant-routing `EntityManager`/`DataSource`, the crypto engine's per-message key load/decrypt should transparently hit the correct tenant's datasource with no code changes to the Data Protection module. `JdbcCryptoEngine` can achieve the same by wrapping its datasource in an `AbstractRoutingDataSource` keyed by an ambient "current tenant" lookup (see https://www.baeldung.com/multitenancy-with-spring-data-jpa)
  * This needs a `TenantIdentifierResolver`-style bridge that sets the ambient "current tenant" lookup key (consumed by the routing datasource) from the tenant already resolved for the message being processed - inherently ThreadLocal-shaped (matches Hibernate's `CurrentTenantIdentifierResolver` pattern), which fits the framework's "ThreadLocals only at the edges" principle as long as it's scoped tightly to message processing. Not yet validated with a POC.

#### Implementation

* Not yet implemented - no crypto/data-protection code exists anywhere in this module; the data-protection extension itself lives in the separate `extension-data-protection` repo

### Axoniq Workflows

#### Requirements

* Axoniq Workflows must keep working with multi-tenancy enabled, similar in nature to the Data Protection requirement
* Workflows rely on event processors (pooled streaming event processors specifically), so the per-tenant event processing design (see `### Event Processing`) needs to accommodate this use-case

#### Questions

* Not required for 5.3.0 - deferred (see `## Planning`). For 5.4.0 this becomes a MUST feature
* Which parts of the per-tenant pooled streaming event processor design (see `### Event Processing`) need to change to support workflows? Not yet investigated

#### Implementation

* Not yet implemented

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

Incremental delivery plan for graduating `_multitenancy_poc` into a regular AxoniqFramework module - moving and reworking code from the POC into its final location issue by issue, rather than one big-bang port. One issue per feature area (mirrors the `###` sections above); each issue also resolves that area's open `#### Questions` as part of its own acceptance criteria (no separate design-spike issues). Dead Letter Queue and Data Protection are "Should" for the end-of-July milestone (in scope, lower priority than the "Must" phases, may slip). Proposed issues are children of AxoniqFramework issue #176 (see `## References`).

Each bold-titled bullet below is a separate GitHub issue and PR. "Phase" headings are dependency/sequencing groups, not units of work - e.g. Phase 3 lists three independent issues that can be worked in parallel once Phase 1 and 2 land, not one combined issue.

#176 becomes the tracking issue to complete once all required work is done with a github task list referencing
sub-issues.

Each issue also adds or updates the corresponding Antora reference documentation for that feature area (see
`docs/CLAUDE.md`) as part of its own PR - documentation ships with the code, not as a follow-up.

### Phase 0 - Prerequisites

* **"Scaffold the `axoniq-multi-tenancy` module in the main repo"** - empty module skeleton, POM/BOM wiring, CI pipeline
  wiring, package structure; no functional code moved yet, create documentation skeleton for multitenancy feature (
  create sections required); also adds an offline Axon Server license file for CI, unblocking integration tests for
  every subsequent issue

### Phase 1 - Infrastructure (foundation; everything else depends on it)

* **"Infrastructure: tenant registration, resolution and wiring skeleton"**
  - Ports `TenantDescriptor`, `TenantProvider`/`AxonServerTenantProvider`, `TenantConnectPredicate`, `TenantResolver`/`MetadataBasedTenantResolver`/`TenantResolverRegistry`, `MultiTenantAwareComponent`, `NoSuchTenantException`, metadata propagation (`SimpleCorrelationDataProvider`), and the `MultiTenancyConfigurationDefaults` skeleton
  - Fixes the `TenantDescriptor` equals/hashCode bug while porting (identity must be `tenantId`-only)
  - Changes the default `TenantConnectPredicate` to exclude the Axon Server `_admin` context by default (currently `alwaysTrue()` includes it)
  - Resolves: the `[tenant]-[context]` naming scheme for tenants spanning multiple contexts is explicitly scoped out for July (see "Deferred past July" below); whether transaction-manager hooks need tenant info on `ProcessingContext`
  - Wires integration tests to create/remove tenants dynamically via Axon Server's existing Admin API (no new server-side API needed)

### Phase 2 - Tenant-aware component support (needed by Commands/Queries/Event Processing's handling side)

* **"Tenant-aware components: per-tenant component registry and parameter injection"**
  - Ports `TenantComponentRegistry`, `TenantComponentFactory`, `DefaultTenantComponentRegistry`, and the `TenantComponentResolver` `ParameterResolverFactory`
  - Resolves: the `TenantComponentRegistry` naming fix (it holds many tenants' instances of one component type, not many component types) and clarifies the multiple-registries-per-type-T story

### Phase 3 - Message dispatch & storage (can proceed in parallel once Phase 1+2 land)

* **"Commands: multi-tenant command dispatching and handling"**
  - Ports `MultiTenantAxonServerCommandBusConnector`; reconciles the POC's modified `AxonServerCommandBusConnector` with the mainline `axon-server-connector` module
* **"Queries: multi-tenant query dispatching and handling"**
  - Ports `MultiTenantAxonServerQueryBusConnector`; reconciles the POC's modified `AxonServerQueryBusConnector` with mainline
* **"Events Storage: per-tenant event store routing"**
  - Ports `TenantRoutingEventStore`, `TenantEventSegmentFactory`; reconciles the POC's modified `AxonServerEventStorageEngineFactory` with mainline
  - Re-enables the disabled multi-tenant `SnapshotStore` decoration - snapshotting is a 5.3.0 must-have
  - Wires in per-tenant support for the aggregate-based `AggregateBasedAxonServerEventStorageEngine` alongside the tag-based `AxonServerEventStorageEngineFactory` - a 5.3.0 must-have
  - Resolves: `EventStorageEngine`-vs-`EventStore`-level routing

### Phase 4 - Event processing (depends on Events Storage, Infrastructure, and Tenant-aware components)

* **"Event Processing: per-tenant streaming, pooled and persistent-stream processors"**
  - Must-have for 5.3.0: ports `MultiTenantPersistentStreamMessageSource`, per-tenant transaction managers, and the shared-pooled-processor/tenant-aware-projection-datasource path (single shared token store, per-tenant projection datasource resolved via tenant-aware components)
  - Should-have for 5.3.0 (may slip if time-constrained): full per-tenant pooled-streaming with physical separation - `MultiTenantEventProcessor`, `MultiTenantEventProcessorModule`, `MultiTenantPooledStreamingEventProcessorModule`, per-tenant token stores
  - Resolves: whether a subscribing-style processor is required, whether a single processor can multiplex per-tenant projections/tokens, and whether a dedicated module is needed vs. decorating existing components
  - Likely the largest issue in this plan given the number of open questions and classes involved - flag for a possible mid-review split if it grows too large for one PR

### Phase 4b - Should-have for 5.3.0 (lower priority, may slip)

* **"Dead Letter Queue: tenant-aware DLQ support"** - resolves whether the multi-tenancy module should depend on the DLQ module
* **"Data Protection: tenant-aware crypto store fan-out"** - validates and documents a datasource-routing based approach (`JpaCryptoEngine`/`JdbcCryptoEngine` combined with a tenant-routing datasource and a `TenantIdentifierResolver`-style bridge) rather than changes to the `CryptoEngine`/`Converter` SPI; tracked in https://github.com/AxonIQ/extension-data-protection/issues/8

### Phase 5 - Spring Boot wiring (depends on the phases above existing)

* **"Spring Boot Autoconfiguration: wire up multi-tenancy support"**
  - Ports `MultiTenancyAutoConfiguration` (enable-by-default / disable-via-property)
  - Resolves: how to autoconfigure application-specific `TenantComponentRegistry` beans - implement the "auto-wire any Spring bean implementing a tenant-component interface" idea

### Phase 6 - Documentation

* **"Migration guide: AF4 `extension-multitenancy` to AF5 built-in multi-tenancy"** - reference documentation walking
  existing AF4 extension users through migrating to the new built-in AF5 module (see `## Constraints` and
  `## References` for the AF4 extension prior art)

### Deferred past July

* **"Axoniq Workflows support"** - not required for 5.3.0, but becomes a MUST feature for 5.4.0; requires the per-tenant
  pooled streaming event processor design to accommodate workflows
* **"Multi-context support for a single tenant"** - convenience feature; requires the `[tenant]-[context]` naming
  scheme (or an alternative, e.g. representing the tenant via the Axon Server replication group) and a resolver
  deriving the tenant id from it
* **"Entitlement integration for the multi-tenancy module"** - wire up the existing entitlement module (e.g. restricting by number of tenants); currently the multi-tenancy module is only indirectly restricted through Axon Server itself
* **"Per-tenant sequencing policy"** - an additional sequencing policy, configurable by the application developer, that scopes sequential processing to a single tenant; shared between Commands and Event Processing since sequencing policies are built at the generic message level

