# Features

Constraint: 

* Only Axon Server is supported
* No copying of infrastructure components (gateways, buses) - where possible
  * split on the lowest level

Base Idea:

* Components can become MultiTenantAwareComponent and be registered via hooks so that they can be used in multi-tenant mode

## Register Tenants

If I have tenants, I need to make them known to the system 

* TenantDescriptor - id+meta
* TenantProvider - knows all tenants, calls register hooks
* TenantConnectPredicate - should I use that as a tenant

## Resolve Tenants / Provide tenant information

Derive tenant from message

* TenantResolver - resolves tenant from message used 
* TenantResolverRegistry - holds resolvers per concrete message type (C,Q,E) or global (? needed)
* Propagate metadata tenantId via SimpleCorrelationDataProvider: sets tenantId in Metadata of appended event

### Questions

* Tenant information: Only Metadata or needed in ProcessingContext (or both if not MetadataTenantResolver)
  * con: map lookup is cheap, we have everything we need 

## Commands

* CommandGateway and CommandBus: No changes
* Handler registrations on local segment (SimpleCommandBus)
* New: MultiTenantCommandBusConnector - uses same methods as AxonServerCommandBusConnector but internally holds map of tenant-specific connections
* TenantResolver works on commandMessage because it is passed as a hard dependency in enhancer, not resolved from context 


 
### Todo

* Sequencing for commands - full sequential processing - policy per tenant


## Events Storage

Each tenant has its own AxonServer event store, TenatRoutingEventStore reolves tenantId/context from message  and routes to the orrect one.

* AxonServer only
* StorageEngineBackedEventStore via AxonServerEventStorageEngineFactory - resolved per tenant

### Questions

* could this be solved on the level of EventStorageEngine instead of EventStore?

## Event Handling/Sourcing

Need to support

* Subcribing
* Pooling 
* Streaming -> MultiTenantPersistentStreamMessageSource

## TenantAware general components support

* Generally: User can use a custom component registry to register components that are aware of the tenant context and inject them into Message Handling methods, so we get f.e. the correct sql datasource for storing the tenants data. 

### Questions

* More than one?
* Spring boot autoconfiguration? how convenient?


## Queries

* solved same as Command: QueryServerBisConnector is multi tenant aware


## Spring Boot Autoconfiguration

* provide enhancer as bean
* disable feature/exclude enhancer
* How to configure application specific components (TenantComponentRegistry)?




### General Questions

* TenantDescriptor is the core Key - but only tenantId is under our control - does that always match?


