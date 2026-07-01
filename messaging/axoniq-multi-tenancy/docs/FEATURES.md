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


## Events

* AxonServer only
* StorageEngineBackedEventStore via AxonServerEventStorageEngineFactory - resolved per tenant


## Queries





### General Questions

* TenantDescriptor is the core Key - but only tenantId is under our control - does that always match?


