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
* A server API to create/remove tenants is needed to test dynamic tenant registration/removal in integration tests

#### Questions

* Tenant name resolution: today tenant discovery is 1:1 with the Axon Server context name. A tenant spanning multiple contexts (e.g. multiple bounded contexts for one application) would need a naming scheme (e.g. `[tenant]-[context]`) and a resolver deriving the tenant id from it - not yet implemented
* Tenant information: Only Metadata or needed in ProcessingContext (or both if not MetadataBasedTenantResolver)
  * con: map lookup is cheap, we have everything we need 
  * Resolved by the current implementation: metadata-only. The tenant-aware `ParameterResolverFactory` (`TenantComponentResolver` inside `MultiTenancyConfigurationDefaults`) reads tenantId directly from message metadata at resolution time - nothing is cached on `ProcessingContext`
  * Reasoning: metadata is unavoidable regardless - it's what survives the wire and is available before a ProcessingContext exists (dispatch interceptors, connector-level routing); a second cached value on ProcessingContext would risk drifting from the message's actual metadata; and it's ultimately about getting messages to the correct tenant, everything else (projections, etc.) derives from the message itself

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

* Sequencing: is it required that command sequencing must not affect other tenants, or should the application developer be able to configure a single full-sequential policy across all tenants? Likely answer: ship an additional per-tenant sequencing policy with the multi-tenancy extension that the application developer can configure - not yet implemented

#### Implementation

* Local handler registration is unaffected - handlers still register on the plain `SimpleCommandBus` (`org.axonframework.messaging.commandhandling.SimpleCommandBus`, AF5 core, unchanged)
* `MultiTenantAxonServerCommandBusConnector` (`commandhandling/MultiTenantAxonServerCommandBusConnector.java`) - extends `AbstractAxonServerCommandBusConnector`, implements `MultiTenantAwareComponent`; keeps a per-tenant `TenantState` (own Axon Server connection, local subscription map, in-flight tracker), replays known subscriptions to newly-registered tenants, resolves the target tenant via the injected `TenantResolver`
* The `TenantResolver` is injected into the connector as a hard dependency at construction (via the enhancer), rather than resolved from a registry/context per call
* Events appended by the handler get their tenantId written back to metadata via the `SimpleCorrelationDataProvider` mechanism described under `### Infrastructure`
* Registered as the `CommandBusConnector` by `MultiTenancyConfigurationDefaults`


### Events Storage

Each tenant has its own AxonServer event store, TenantRoutingEventStore resolves tenantId/context from message and routes to the correct one.

* AxonServer only
* StorageEngineBackedEventStore via AxonServerEventStorageEngineFactory - resolved per tenant
* We also have the AggregateBasedAxonServerEventStorageEngine to consider

#### Questions

* could this be solved on the level of EventStorageEngine instead of EventStore?

### Event Handling/Sourcing

Need to support

* Subcribing
* Pooling 
* Streaming -> MultiTenantPersistentStreamMessageSource

#### Handling

* Reestablish tenant information on the ProcessingContext when an event is handled on a tenant-specific connection (open question - see `### Infrastructure` `#### Questions`; metadata alone may be sufficient)
* Select the tenant-specific resources (e.g. datasource) needed by the handler, so it writes to the correct (tenant-specific) projection - AF4 only did this for the datasource

#### Questions

* Can a single event processor multiplex on projection/token per tenant, instead of one processor per tenant? What are the consequences?

#### Todo

* Sequencing for event handling shares the sequencing-policy concept with command handling (see `### Commands`), but works differently - needs its own per-tenant design

### TenantAware general components support

* Generally: User can use a custom component registry to register components that are aware of the tenant context and inject them into Message Handling methods, so we get f.e. the correct sql datasource for storing the tenants data. 

#### Questions

* More than one?
* Spring boot autoconfiguration? how convenient?


### Queries

* Dispatching: solved same as Command - MultiTenantAxonServerQueryBusConnector (mirrors MultiTenantAxonServerCommandBusConnector, wrapping AxonServerQueryBusConnector) is multi-tenant aware

#### Handling

* Reestablish tenant information on the ProcessingContext when a query is received on a tenant-specific connection (open question - see `### Infrastructure` `#### Questions`; metadata alone may be sufficient)
* Select the tenant-specific resources (e.g. datasource) needed to read from the correct projection - AF4 only did this for the datasource

### Dead Letter Queue

* Enqueueing dead letters is part of the event processing flow, so tenant-aware datasource selection should be covered by `### Event Handling/Sourcing` out of the box
* Processing dead letters is a separate flow: it needs a means to establish the correct tenant context before invoking the DLQ for processing

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

