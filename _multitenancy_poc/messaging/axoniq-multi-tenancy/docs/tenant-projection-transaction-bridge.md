# Tenant Projection Transaction Bridge

This note explains the gap between the historic multi-tenancy implementations in `_old` and `_tmp` and the current pooled-streaming JDBC projection path.

## Short Answer

The framework already provides the JDBC/JPA executor pieces:

- `JdbcTransactionalExecutorProvider`
- `JpaTransactionalExecutorProvider`
- `TransactionalUnitOfWorkFactory`
- `ConnectionExecutor`

What it does not provide is a tenant-local transaction manager that binds the correct executor into the processing context for a specific tenant datasource.

That is the missing bridge for tenant-specific pooled streaming processors.

## What `_old` Did

The `_old` implementation used `TenantWrappedTransactionManager`.

Its strategy was:

- store the current tenant in a `ThreadLocal`
- wrap `startTransaction()`, `executeInTransaction()`, and `fetchInTransaction()`
- let tenant-aware infrastructure ask `TenantWrappedTransactionManager.getCurrentTenant()`

That worked well for the older model because tenant routing could rely on thread-local state.

### Why that mattered

The old `MultiTenantDataSourceManager` could resolve the tenant datasource by looking at the active transaction context or the current unit of work.

So the tenant identity was effectively carried by the transaction wrapper itself.

## What `_tmp` Did

The `_tmp` implementation moved closer to the current Axon Framework model.

It used framework-native executor providers directly:

- `JdbcTenantTokenStoreFactory` created a `JdbcTokenStore` with `new JdbcTransactionalExecutorProvider(tenantDataSource)`
- `JpaTenantTokenStoreFactory` created a `JpaTokenStore` with `new JpaTransactionalExecutorProvider(emf)`
- `JpaTenantEventSegmentFactory` used `JpaTransactionalExecutorProvider` for tenant event storage

### What `_tmp` solved

This solved tenant-specific store creation.

Each tenant got its own:

- token store
- event store or storage engine
- datasource or entity manager factory

### What `_tmp` did not solve

It did not add a tenant-local transaction bridge for pooled streaming processor startup.

That is why the current JDBC pooled-streaming path still needs extra wiring.

## Why the Current JDBC Pooled-Streaming Path Fails Without the Bridge

`PooledStreamingEventProcessor.start()` touches the token store early during startup.

For `JdbcTokenStore`, startup expects a connection executor to be present in the `ProcessingContext`.

That executor is normally supplied by the transaction manager lifecycle.

If the tenant processor is started without a tenant-local `TransactionManager` attached to the unit of work, the token store cannot resolve:

`JdbcTransactionalExecutorProvider.SUPPLIER_KEY`

The result is the startup failure we saw:

`A connection executor must be present in the processing context.`

## Practical Interpretation

So the situation is:

- the framework already has the low-level JDBC/JPA transaction primitives
- `_tmp` already uses those primitives for tenant-specific stores
- the pooled processor still needs a tenant-specific unit-of-work bridge so the processor lifecycle exposes the right executor resource

## Minimal Design Rule

Do not reimplement framework behavior in the multi-tenancy module.

Only add the tenant-specific glue that the framework does not yet know about:

- resolve the tenant datasource or entity manager factory
- create a tenant-local `TransactionManager`
- attach the transaction to the processing lifecycle
- let the existing framework providers read the executor from the processing context

## Recommendation

For JDBC tenant projections, keep the custom code as small as possible:

- one tenant-local transaction adapter
- one tenant-specific token store factory
- one tenant-specific processor wiring path

Everything else should remain delegated to Axon Framework.
