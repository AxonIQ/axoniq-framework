# ADR 003: Following the routing engine, and owning its registration (issue [#210](https://github.com/AxonIQ/axoniq-framework/issues/210))

Date: 2026-07-30
Status: accepted
Extends: [ADR 002, multi-tenant pooled streaming](adr-002-pooled-streaming.md)

## Context

[ADR 002](adr-002-pooled-streaming.md) decided that a tenant change restarts the running streaming event processors, so each re-opens its merged stream over the current tenants. It did not decide *how* the restarter learns about a tenant change. The first implementation had it follow the `TenantProvider`, and that turned out to be wrong in a way an integration test caught intermittently: a tenant added at runtime went missing from the read model.

The `TenantProvider` notifies its subscribers in turn. The `MultiTenantEventStorageEngine` is one of those subscribers, and it registers the tenant when its turn comes. The restarter was another. When the restarter's turn came first, it restarted the processors, the merged stream re-opened over the tenants the engine held *at that moment*, and the new tenant was not among them. No further tenant change followed, so nothing re-opened the stream again and that tenant's events were never read.

Measured in a failing run: the restart ran at `12:29:21.736`, and the engine reported the tenant at `12:29:21.768`. A 32 millisecond gap decided whether a tenant was visible to the read side until the next application restart.

The fix has to make the restart follow something that cannot be ahead of the engine. That is the engine itself.

## Situation before

Two separate problems, one per commit below.

### 1. The restarter resolved a `TenantChangeSource` from the configuration

The engine implemented a `TenantChangeSource` interface, and was registered twice so a follower had a name to resolve that decorators of `EventStorageEngine` would not match:

```java
componentRegistry.registerComponent(
        subscribedComponent(TenantChangeSource.class,
                            AxonServerMultiTenancyConfigurationDefaults::routingEngine));
componentRegistry.registerComponent(
        ComponentDefinition.ofType(EventStorageEngine.class)
                           .withBuilder(AxonServerMultiTenancyConfigurationDefaults::routingEngineFrom));
```

with the second registration reaching back for the first instance:

```java
private static EventStorageEngine routingEngineFrom(Configuration config) {
    return (EventStorageEngine) config.getComponent(TenantChangeSource.class);
}
```

The restarter then resolved that name, degrading to a log line when it found nothing:

```java
Optional<TenantChangeSource> tenantChangeSource = configuration.getOptionalComponent(TenantChangeSource.class);
if (tenantChangeSource.isEmpty()) {
    logger.info("No tenant change source is configured, ...");
    return null;
}
return tenantChangeSource.get().subscribe(this::requestRestart);
```

This worked, but it rested on an invariant a reader had to reconstruct: two registrations must always be made together, and must always yield one instance. A reviewer said plainly that the code was hard to understand.

### 2. An application-supplied `EventStorageEngine` was tolerated

Registering the routing engine was skipped when the application had registered an `EventStorageEngine` of its own:

```java
if (!componentRegistry.hasComponent(EventStorageEngine.class, SearchScope.ALL)) {
    // register the routing engine
}
```

Nothing said so. Everything else in the method still ran: both per-tenant factories were registered, `SnapshotSourcingConfigurationEnhancer` was disabled, and `MultiTenantSnapshotStore` was registered as the `SnapshotStore`. The only related signal came from elsewhere, when the restarter found no `TenantChangeSource` to follow and logged that at INFO.

## Situation now

### 1. The engine announces, and is handed to the restarter at startup

The engine keeps its own listeners and announces only after the change is visible through its own `tenants()`:

```java
private Registration announcing(Registration registration) {
    announceTenantsChanged();
    return () -> { ... };
}
```

`registerTenant` returns `announcing(composedEngines.registerTenant(tenantDescriptor))`, so the inner registration has already taken effect by the time any listener runs.

Nothing resolves the engine. The enhancer that builds it hands that instance over from a start handler on the engine's own `ComponentDefinition`:

```java
componentRegistry.registerComponent(
        subscribedComponent(EventStorageEngine.class,
                            AxonServerMultiTenancyConfigurationDefaults::routingEngine)
                .onStart(MultiTenancyConfigurationDefaults.TENANT_COMPONENT_SUBSCRIBER_PHASE,
                         AxonServerMultiTenancyConfigurationDefaults::followRoutingEngine));
```

```java
private static void followRoutingEngine(Configuration config, EventStorageEngine engine) {
    config.getComponent(MultiTenantStreamingProcessorRestarter.class)
          .follow((MultiTenantEventStorageEngine) engine);
}
```

A component's own start handler receives the instance that definition built, never a decorator of the type it is registered under. `MultiTenantAxonServerCommandBusConnector` in the same class already relies on this, for the same reason.

### 2. An application-supplied `EventStorageEngine` is rejected

```java
private static void rejectForeignEventStorageEngine(ComponentRegistry componentRegistry) {
    if (componentRegistry.hasComponent(EventStorageEngine.class, SearchScope.ALL)) {
        throw new AxonConfigurationException("""
                A multi-tenant application stores events per tenant, so it cannot use an EventStorageEngine that \
                serves every tenant from one place, but one is already registered. Register a \
                TenantEventStorageEngineFactory to control how each tenant's event storage engine is built, \
                instead of registering an EventStorageEngine of your own.""");
    }
}
```

Called next to the `SnapshotStore` rejection that already existed, before any registration happens, so nothing is mutated before the failure.

## The differences

| | Before | Now |
|---|---|---|
| What the restart follows | the `TenantProvider`, which notifies subscribers in turn | the engine, which announces after its own registration took effect |
| How the follower reaches the engine | resolves `TenantChangeSource` from the configuration | is handed the built instance by the enhancer |
| Registrations for one engine | two, which had to agree | one |
| Types involved | `TenantChangeSource` + `TenantChangeListener` | `TenantChangeListener` |
| A restarter with nothing to follow | logged at INFO from the restarter | logged at WARN from the restarter, one phase after any handover |
| An application-supplied `EventStorageEngine` | silently skipped, application starts | `AxonConfigurationException`, application does not start |

## Why this over the alternatives

### Why hand over instead of resolve

A resolved `EventStorageEngine` can be a decorator. Applying the "register `EventStorageEngine` normally and resolve it for the `TenantChangeSource`" variant that came up in review fails outright:

```
ClassCastException: SnapshotCapableEventStorageEngine cannot be cast to MultiTenantEventStorageEngine
```

Worse than the exception is the version that does not throw: a follower subscribed to a decorator gets no announcements at all, and reports itself as subscribed while doing nothing. Handing over the built instance removes the lookup, so decoration cannot come between the announcer and its follower.

### Why not keep the interface and register it under its own type

That is the situation before. It works only while every future maintainer keeps two registrations in step, and it made the code hard to read for the reviewer who had to.

### Why not follow the `TenantProvider` and re-check afterwards

Considered and rejected on measurement, not taste. A restart takes about two milliseconds while the engine's registration landed 32 milliseconds later, so a convergence loop exits before there is anything to notice.

### Why reject an application-supplied engine rather than tolerate it

The tolerated configuration was not "multi-tenancy doing less". It was three halves that do not fit:

- events in one shared store,
- snapshots still written per tenant through `MultiTenantSnapshotStore`,
- snapshot sourcing composed nowhere, because `SnapshotSourcingConfigurationEnhancer` had already been disabled.

Tenant propagation also differs by message type. Commands and queries take their tenant from `RegisterTenantDescriptorHandlerInterceptor`, resolved from message metadata, so those kept working. Events consumed from the stream take theirs from the routing engine, which tags each entry with `withResource(TenantDescriptor.RESOURCE_KEY, tenant)`. Without that engine, streamed events carry no tenant, so a `@TenantScoped` component in an event handler has nothing to resolve from and every tenant's read model collapses into one.

That is a silent loss of the isolation the module exists for. A foreign `SnapshotStore` already threw for a milder version of the same problem, so tolerating the engine case was an inconsistency rather than a decision.

## Disadvantages, and how they are mitigated

### A startup failure where there used to be a warning

*Mitigation.* The message names the supported alternative, `TenantEventStorageEngineFactory`, which is the mirror of the `TenantSnapshotStoreFactory` the existing rejection already points at. Three routes remain open:

- per-tenant engines: register a `TenantEventStorageEngineFactory`, which the enhancer registers with `registerIfNotPresent`, so an application's own wins;
- adding behaviour to the engine: `registerDecorator(EventStorageEngine.class, ...)` is unaffected, since the check looks for a registered component and a decorator is not one;
- opting out entirely: `disableEnhancer(AxonServerMultiTenancyConfigurationDefaults.class)` stands the whole enhancer down rather than half of it.

Nothing released is affected. Every type here is `@since 5.3.0` and the latest tag is `axoniq-5.2.2`.

### The rejection could break every default event-sourcing setup

*Mitigation.* It cannot, and that is pinned rather than argued. This enhancer runs at order `Integer.MIN_VALUE + 7`, while `EventSourcingConfigurationDefaults` runs at `MessagingConfigurationDefaults.ENHANCER_ORDER - 100`, which resolves to `Integer.MAX_VALUE - 100`, and registers its `InMemoryEventStorageEngine` with `registerIfNotPresent`. The default is therefore neither present when the check runs, nor able to overwrite the routing engine afterwards. `acceptsTheDefaultEventSourcingSetupAndYieldsTheRoutingEngine` asserts a default `EventSourcingConfigurer` still yields the routing engine.

### The restarter is inert if nothing hands it an engine

*Mitigation.* It says so. `warnWhenFollowingNothing` runs at `TENANT_HANDOVER_CHECK_PHASE`, one phase after `TENANT_COMPONENT_SUBSCRIBER_PHASE`, because a handover happens in that phase and a phase has no order within itself. `warnsAtStartupWhenNothingHandedOverARoutingEngine` asserts the warning appears, and a sibling test asserts it stays silent once an engine was handed over, so a correctly wired application gets no noise.

### `follow` is public, so it can be called twice

*Mitigation.* It is single-shot. A second call cancels its own subscription and throws `AxonConfigurationException`, leaving the first engine followed, since a second engine's tenants decide nothing about the merged stream the first one spans.

### `TenantEventStorageEngineFactory` is `@Internal`

Not mitigated, and worth stating. The exception points users at an internal type. Every type in this module is internal for now, so it is consistent, but if the module becomes publicly consumable this factory is the first type that should become public. Until then the supported route is officially unsupported.

### The engine grew a listener list and a fan-out

*Mitigation by scope.* Only the object that owns `tenants()` can promise that a change is announced after it is visible there, so this is the one place the behaviour can live. If a second component needs the same thing, `announcing(Registration)` plus the listener list lift out into a collaborator inside the `eventsourcing` package. Doing that now would add an indirection for one announcer and one follower.

## Tests

- `picksUpATenantAddedAtRuntimeWhileASubscriberAheadOfTheEngineIsStillRegisteringIt`: the ordering regression. It fails if the restart follows the provider again.
- `subscribesAndFollowsTheRoutingEngineItselfWhenTheEventStorageEngineIsDecorated`: with a decorator registered, both the tenant subscription and the restarter's listener land on the routing engine. Swapping the handover for a lookup makes it error.
- `subscribesTheFactoriesBeforeTheRoutingEngineThatComposesFromThem`: the factories are subscribed a phase before the engine, so a factory always holds a tenant the engine announces.
- `rejectsAnEventStorageEngineRegisteredByTheApplication` and `rejectsASnapshotStoreRegisteredByTheApplication`: both storage components are rejected the same way.
- `acceptsTheDefaultEventSourcingSetupAndYieldsTheRoutingEngine`: the rejection does not trip on framework defaults.
- `warnsAtStartupWhenNothingHandedOverARoutingEngine`: the inert path is reported, and silent when wired.
- `rejectsASecondEngineAndKeepsFollowingTheFirst`, `rejectsANullEngine`, `describesTheEngineItFollows`: the `follow` contract.
