# Axoniq Multi Tenancy

* [Issue-4324](https://github.com/AxonIQ/AxonFramework/issues/4324)

## Purpose

Brings multi-tenancy-support to Axoniq Framework. AF4 supported this via the extensions-multitenancy module.

Multi-Tenancy aims for strict data isolation between tenants. 

## Scope

* Multi-Tenancy is only supported for Axon Server use cases. Each tenant must be configured in its own axon-server context.
  * Note: This mixes the terms "Bounded Context" and "Tenant". If you design a DDD system with multiple bounded contexts (Customer Management, Billing, Warehouse) and use multiple tenants, this would mean that your contexts are "billing-customerA", "billing-customerB".
* The framework keeps one shared Axon Server configuration and one shared multi-tenancy infrastructure setup.
* Tenants differ by connection target only: each tenant gets a dedicated Axon Server connection for its context, but not a separate Axon Server configuration object or copied infrastructure graph.
* To avoid multiple datasource connections for token storage, we will focus on the new persistent stream feature only for the first PoC.

## Assumption

* We are working with one cluster of axon servers, and each tenant has its own axon server context following the naming conventions stated above.
* Tenant ID is the default context name unless a dedicated mapping is introduced later.
* A feature/use case implemented for multiple tenants will structurally be a singleton use case, so routing and storage must be tenant-specific, business logic must be tenant agnostic.

## Considerations

* The AF4 extension basically copied all infrastructure components for each tenant, giving you n+1 (n=tenants, 1= localSegment) instances for each module. This should be avoided.
* For the Axon Server path, the intended model is n connections for n tenant contexts, not n copies of the full infrastructure stack.
* The connector is single-context per instance, so tenant-specific Axon Server wiring should be implemented as one connector/command bus segment per tenant context, not as a context-switching shared connector.

## Questions

* How are tenants (aka server contexts with special naming convention) are configured? 

## Setup Flow

The intended Axon Server multitenancy setup is:

* one shared `AxonServerConfiguration`
* one shared `AxonServerConnectionManager`
* one tenant registry/provider that knows which tenants exist
* one `MultiTenantCommandBus` at the application level
* one dedicated command-bus segment per tenant context

The tenant-specific segment is not a copied application stack. It is only:

* one `AxonServerConnection` for the tenant context
* one `AxonServerCommandBusConnector`
* one local `SimpleCommandBus`
* one `DistributedCommandBus`

### How Tenants Become Known

Tenants are made known to the system by a tenant provider or registry. Typical sources are:

* a static list from configuration
* contexts discovered from Axon Server
* an external source of truth such as a database or configuration service

Once a tenant is known, it is registered with the multi-tenant command bus:

```java
multiTenantCommandBus.registerAndStartTenant(TenantDescriptor.tenantWithId("tenant-a"));
```

### Command Dispatch Flow

1. Application code dispatches a command to `MultiTenantCommandBus`.
2. `TenantResolver` resolves the tenant for the command.
3. `MultiTenantCommandBus` looks up the tenant segment.
4. The tenant segment is a `DistributedCommandBus`.
5. The distributed bus uses its connector.
6. The connector uses the tenant-specific Axon Server connection.
7. Axon Server receives the command in the tenant context.
8. Axon Server routes the command to the matching handler in that context.

### Mermaid Overview

```mermaid
flowchart TD
    A[Application] --> B[One AxonServerConfiguration]
    B --> C[One AxonServerConnectionManager]
    A --> D[TenantProvider / Tenant registry]
    D --> E[Register tenant A]
    D --> F[Register tenant B]
    E --> G[Register tenant A in bus]
    F --> H[Register tenant B in bus]

    A --> I[Register command handlers]
    I --> J[Subscribe handler on multi-tenant bus]
    J --> K[Handler registered on all tenant segments]

    A --> L[Dispatch command]
    L --> M[TenantResolver resolves tenant from message]
    M --> N[MultiTenantCommandBus finds tenant segment]
    N --> O[Dedicated DistributedCommandBus for that tenant]
    O --> P[AxonServerCommandBusConnector]
    P --> Q[Get tenant Axon Server connection]
    Q --> R[Axon Server context for tenant]
    R --> S[Correct handler in that context]
```

```mermaid
sequenceDiagram
    participant App as Application
    participant MT as MultiTenantCommandBus
    participant TR as TenantResolver
    participant TS as Tenant Segment
    participant CM as AxonServerConnectionManager
    participant CX as AxonServerConnection
    participant AS as Axon Server
    participant H as Command Handler

    App->>MT: dispatch(command)
    MT->>TR: resolve tenant from command
    TR-->>MT: tenantDescriptor
    MT->>TS: resolve tenant segment
    TS->>CM: getConnection(tenantContext)
    CM-->>TS: connection for that context
    TS->>CX: send command
    CX->>AS: command in tenant context
    AS->>H: invoke matching handler
    H-->>AS: result
    AS-->>App: command result
```

### Practical Summary

* Tenants are registered through a provider/registry.
* Handlers are registered once on `MultiTenantCommandBus`.
* `TenantResolver` selects the tenant per command.
* The tenant segment uses the tenant context connection.
* The system keeps one shared Axon Server configuration and one shared infrastructure setup.
* The only tenant-specific variation is the connection target, not a copied configuration graph.
