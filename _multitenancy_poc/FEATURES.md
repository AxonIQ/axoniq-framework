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

Tenants need to be known by the system 

* TenantDescriptor - id+meta
* TenantProvider - knows all tenants, calls register hooks
  * Registration/removal must support both static (startup-time) and dynamic (runtime) tenants
* TenantConnectPredicate - should I use that as a tenant
* Tenant name resolution: maps an Axon Server context name to a tenant id. Currently 1:1 (context name == tenant id), but a tenant may span multiple contexts (e.g. to represent multiple bounded contexts for one application), so we need a naming scheme (e.g. `[tenant]-[context]`) and a resolver that can derive the tenant id from it

Tenant association must be known when processing messages (derived from the message or it's source)

* TenantResolver - resolves tenant from message used 
* TenantResolverRegistry - holds resolvers per concrete message type (C,Q,E) or global (? needed)
* Propagate metadata tenantId via SimpleCorrelationDataProvider: sets tenantId in Metadata of appended event
* Direction differs by flow: on dispatch/publish, the tenant resolved from metadata selects the outbound connection; on inbound/sourced messages, the connection that delivered the message determines the tenant, which is then written to metadata

#### Questions

* Tenant information: Only Metadata or needed in ProcessingContext (or both if not MetadataBasedTenantResolver)
  * con: map lookup is cheap, we have everything we need 
  * Leaning: metadata as the sole source of truth; avoid caching resolved tenant identity in ProcessingContext unless a concrete hot path proves the lookup cost matters
    * Metadata is unavoidable regardless - it's what survives the wire and is available before a ProcessingContext exists (dispatch interceptors, connector-level routing)
    * A second, cached tenant value in ProcessingContext risks drifting from the message's actual metadata mid-processing - two sources of truth for the same fact
    * Tenant-aware component resolution (e.g. per-tenant datasource, see `### TenantAware general components support`) will likely be a ParameterResolverFactory, which already receives both the message and the ProcessingContext at resolution time - so it can read tenantId from metadata directly without anything pre-populated in ProcessingContext
    * In the end it's all about getting messages to the correct tenant; everything else (projections, etc.) derives from the message itself

#### Todo

* Server API to create/remove tenants in integration tests, needed to test dynamic tenant registration/removal

### Commands

* CommandGateway and CommandBus: No changes
* Handler registrations on local segment (SimpleCommandBus - org.axonframework.messaging.commandhandling.SimpleCommandBus, AF5 core class from the AxonFramework repo)
* New: MultiTenantAxonServerCommandBusConnector - uses same methods as AxonServerCommandBusConnector but internally holds map of tenant-specific connections
* TenantResolver works on commandMessage because it is passed as a hard dependency in enhancer, not resolved from context 

#### Handling

* Reestablish tenant information on the ProcessingContext when a command is received on a tenant-specific connection (open question - see `### Infrastructure` `#### Questions`; metadata alone may be sufficient)
* Select the tenant-specific resources (e.g. datasource) needed by the handler based on that tenant information - AF4 only did this for the datasource
* Events appended by the handler get their tenantId written back to metadata via the `SimpleCorrelationDataProvider` mechanism described under `### Infrastructure`

#### Todo

* Sequencing for commands - full sequential processing - policy per tenant


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

