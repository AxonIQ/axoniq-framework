# ADR: Tenant Isolation for Subscription-Query Update Emission

**Status**: Implemented, 2026-07-24 — Option A (tenant-aware `QueryBus` decorator). See [Decision](#decision).
**Date**: 2026-07-24
**Context tickets**: #176 (multi-tenancy), #208 (multi-tenant queries), #263 (tenant available in the event-handling `ProcessingContext`).

## Problem

Subscription-query *dispatch* is already tenant-routed: `DistributedQueryBus.subscriptionQuery` → `MultiTenantAxonServerQueryBusConnector.resolveConnector(query)` picks the per-tenant `AxonServerQueryBusConnector`, so the subscription channel lives on the resolved tenant's Axon Server context.

Update *emission* is not tenant-scoped. The emit path is:

```
QueryUpdateEmitter.forContext(ctx)                     // SimpleQueryUpdateEmitter
  → emit(type|name, payloadPredicate, update)
    → queryBus.emitUpdate(Predicate<QueryMessage>, supplier, ctx)
      → DistributedQueryBus.emitUpdate                 // iterates a process-wide updateRegistry
```

On a handler node the `MultiTenantAxonServerQueryBusConnector` holds one `AxonServerQueryBusConnector` per tenant, so the single process-wide `DistributedQueryBus.updateRegistry` (`Map<QueryMessage, UpdateCallback>`) mixes subscription registrations from *all* connected tenants. `emitUpdate` applies the caller's predicate across **every** entry regardless of tenant. If tenant-A and tenant-B each hold a subscription whose stored message matches the same predicate, an emit intended for A also fires B's callback → **cross-tenant update leak**.

Delivery itself is already tenant-correct — each `UpdateCallback` is bound to the Axon Server channel it arrived on. Only **matching** is un-scoped.

## Forces / constraints

1. **The emitter's predicates are payload-only.** `SimpleQueryUpdateEmitter` invokes the caller's filter with the *payload*, never the message:
   - `queryNameFilter`: `filter.test(message.payload())`
   - `queryTypeFilter`: `filter.test(message.payloadAs(queryType, converter))`

   The tenant is in `message.metadata()` (`tenantId`), invisible to the caller's predicate. A tenant clause can therefore only be added at the `Predicate<QueryMessage>` level — either inside the emitter (where that predicate is constructed) or at the `QueryBus.emitUpdate` boundary. It cannot be bolted onto the caller's predicate. This is equally true for the type-based and name-based variants.

2. **`tenantId` survives the Axon Server round-trip** and is present on the registry key. `QueryConverter.convertQueryMessage` serializes metadata onto the nested `QueryRequest`; `convertSubscriptionQueryMessage` reads it back. So a `Predicate<QueryMessage>` in `emitUpdate` can read `message.metadata().get("tenantId")`. This is self-consistent: a subscription only lives in tenant-A's context because it carried `tenantId=A`, and that same metadata is on the registry key.

3. **The emit's tenant is resolvable from the `ProcessingContext`.** With #263, the tenant is present in the context during event handling (the dominant projector-driven emit case) as well as command/query handling, readable via `TenantUtils.tenantDescriptorFrom(context)` / `TenantUtils.TENANT_ID_KEY`.

4. **`QueryUpdateEmitter.forContext(ProcessingContext)` is a hardcoded static factory** returning `new SimpleQueryUpdateEmitter(...)`, pulling `QueryBus` / `MessageTypeResolver` / `MessageConverter` from the context. Both the parameter-injection path (`QueryUpdateEmitterParameterResolverFactory` → `forContext`) and any direct application call to `forContext` go through it. A solution that scopes only one of those two paths leaves the other leaking; **tenant isolation must not depend on how the emitter was obtained.**

Both options below satisfy constraints 1–4 and are functionally equivalent in coverage. They differ in *where* the tenant clause is applied and in *blast radius*.

---

## Option A — Tenant-aware `QueryBus` decorator

Add a `QueryBus` decorator in `axoniq-multi-tenancy` that overrides only the emit/complete trio, ANDing a tenant-metadata clause onto the already-built `Predicate<QueryMessage>` before delegating.

```java
// axoniq-multi-tenancy
final class TenantAwareQueryBus implements QueryBus {
    private final QueryBus delegate;

    // query / subscriptionQuery / subscribe / subscribeToUpdates: pure delegation

    @Override
    public CompletableFuture<Void> emitUpdate(Predicate<QueryMessage> filter,
                                              Supplier<SubscriptionQueryUpdateMessage> supplier,
                                              ProcessingContext ctx) {
        TenantDescriptor tenant = TenantUtils.tenantDescriptorFrom(ctx);
        return delegate.emitUpdate(
                m -> tenant.tenantId().equals(m.metadata().get(TenantUtils.TENANT_ID_KEY)) && filter.test(m),
                supplier, ctx);
    }
    // completeSubscriptions / completeSubscriptionsExceptionally: same one-line composition
}
```

Wired through the existing config decoration mechanism:

```java
componentRegistry.registerDecorator(QueryBus.class, ORDER,
        (config, name, delegate) -> new TenantAwareQueryBus(delegate, ...));
```

**Why it covers both paths:** `SimpleQueryUpdateEmitter` (whether created by parameter injection or by a direct `forContext` call) resolves `context.component(QueryBus.class)`, which is the decorated bus. The tenant clause applies universally without touching `forContext`.

**Pros**
- No core (`axon-messaging`) change; entirely inside `axoniq-multi-tenancy`.
- No code duplication — reuses `SimpleQueryUpdateEmitter`'s payload/type filter as-is and only ANDs one message-level clause.
- Uses the AF5-preferred `ComponentRegistry`/decorator model; scoped to the configuration, easy to disable/vary per app or test.
- Symmetric with how the command/query connectors are already composed (decoration over reimplementation, per the connector-decorator refactor note).

**Cons**
- Adds another wrapper to the `QueryBus` chain (alongside `InterceptingQueryBus` / `DistributedQueryBus`); decorator ordering must be defined so the tenant clause reliably reaches `DistributedQueryBus.emitUpdate`.
- The tenant-isolation logic sits at the bus layer, one step removed from the "emitter" concept — a reader looking at emission starts at the emitter and has to follow the delegation down to the bus.
- Only the emit/complete trio is tenant-aware; the decorator is otherwise a pass-through, which can read as an incomplete `QueryBus` implementation.

---

## Option B — Pluggable `QueryUpdateEmitter`, multi-tenancy supplies a preceding `MultiTenantQueryUpdateEmitter` via config

Make emitter creation pluggable in core, defaulting to today's behavior, and let the multi-tenancy module register a tenant-aware emitter that wins.

**Core (`axon-messaging`) change** — resolve the emitter factory from configuration, defaulting to `SimpleQueryUpdateEmitter`:

```java
public interface QueryUpdateEmitterFactory {
    QueryUpdateEmitter create(ProcessingContext context);
}

// QueryUpdateEmitter
static QueryUpdateEmitter forContext(ProcessingContext context) {
    return context.component(QueryUpdateEmitterFactory.class, SimpleQueryUpdateEmitterFactory::new)
                  .create(context);
}
```

The default `SimpleQueryUpdateEmitterFactory` builds exactly the `SimpleQueryUpdateEmitter(queryBus, messageTypeResolver, converter, context)` constructed today — behavior-preserving when multi-tenancy is absent.

**Multi-tenancy module** registers a preceding factory via config so `forContext` resolves a `MultiTenantQueryUpdateEmitter`. That emitter reimplements the emit/complete methods, building the `Predicate<QueryMessage>` itself and ANDing the tenant clause (both variants):

```java
// axoniq-multi-tenancy — MultiTenantQueryUpdateEmitter
@Override public <Q> void emit(Class<Q> type, Predicate<? super Q> filter, Supplier<Object> supplier) {
    TenantDescriptor tenant = TenantUtils.tenantDescriptorFrom(context);
    queryBus.emitUpdate(
            m -> resolvedName(type).equals(m.type().qualifiedName())
              && tenant.tenantId().equals(m.metadata().get(TenantUtils.TENANT_ID_KEY))
              && filter.test(m.payloadAs(type, converter)),
            () -> asUpdateMessage(supplier.get()), context).join();
}
// name-based emit + complete/completeExceptionally: same shape
```

```java
componentRegistry.registerComponent(QueryUpdateEmitterFactory.class,
        cfg -> ctx -> new MultiTenantQueryUpdateEmitter(ctx, ...));   // precedes / overrides the core default
```

**Why it covers both paths:** the change is in `forContext` itself, so parameter-injected and direct `forContext` callers both receive the `MultiTenantQueryUpdateEmitter`.

**Pros**
- Tenant-isolation logic is co-located with the emitter concept.
- Config-scoped (not classpath/ServiceLoader), consistent with AF5's declarative-configuration direction and overridable per app/test.

**Cons**
- Requires a core `axon-messaging` public-API change (new `QueryUpdateEmitterFactory` SPI + reworked `forContext`) that must ship in core, with the compatibility surface that implies.
- The new SPI's only known consumer is multi-tenancy. "Reusable beyond multi-tenancy" is not a demonstrated benefit — no other use case has been identified — so the core public-API change is being justified by, and its cost paid for, a single requirement. If no second consumer materializes, this is a permanent core extension point maintained for one caller.
- Duplicates `SimpleQueryUpdateEmitter`'s currently-`private` helpers (`queryTypeFilter`, `queryNameFilter`, `asUpdateMessage`) into `MultiTenantQueryUpdateEmitter`, unless they are extracted/exposed for reuse — creating two emitter implementations to keep in sync.
- Larger blast radius: core + multi-tenancy module, vs. multi-tenancy module only.

---

## Comparison

| | Option A — QueryBus decorator | Option B — pluggable emitter |
|---|---|---|
| Core (`axon-messaging`) change | None | New `QueryUpdateEmitterFactory` SPI + `forContext` rework |
| Blast radius | `axoniq-multi-tenancy` only | core + `axoniq-multi-tenancy` |
| Covers injected `QueryUpdateEmitter` param | Yes | Yes |
| Covers direct `forContext(ctx)` | Yes | Yes |
| Code duplication | None | Reimplements 3 private emitter helpers |
| Where the tenant clause lives | Bus (`emitUpdate` boundary) | Emitter (predicate construction) |
| Reusable beyond multi-tenancy | No (tenancy-specific wrapper) | Unproven — SPI shape is general, but multi-tenancy is the only identified consumer |
| Config-scoped / test-overridable | Yes | Yes |
| Conceptual locality | Isolation logic away from emitter | Isolation logic on the emitter |

## Open questions (independent of the choice)

- **Behavior when no tenant resolves from context.** Given #263 this should not occur for handler-driven emits; define the contract regardless — throw `TenantNotResolvedException` (strict, surfaces misconfiguration) vs. pass through unscoped (lenient, risks silent leak). Recommend strict, but decide explicitly.
- **Ordering.** Option A: decorator order relative to `InterceptingQueryBus` / `DistributedQueryBus`. Option B: factory precedence over the core default component.
- **Helper reuse (Option B).** Whether to extract `queryTypeFilter` / `queryNameFilter` / `asUpdateMessage` into a shared internal utility rather than duplicate them.
- **Testing.** Either way, strengthen the isolation IT to use a *non*-distinguishing predicate (or identical payloads across tenants) so isolation is proven by the framework, not by the test's payload predicate; add a focused unit test with two registry entries differing only by `tenantId` metadata.

## Decision

**Option A — the tenant-aware `QueryBus` decorator — is adopted.**

Both options are functionally equivalent in tenant-isolation coverage (see constraints 1–4); the deciding factors were delivery timeline and blast radius, not capability:

- **Deadline.** #176/#208 is time-constrained. Option A ships entirely within `axoniq-multi-tenancy` — no core (`axon-messaging`) public-API addition, so no core-maintainer review/release coordination sits on the critical path.
- **Lower blast radius.** The change is contained to one module, with no new public SPI and no compatibility surface added to core.
- **Reversible later, at low cost.** Because A and B only differ in *where* the tenant clause is applied (bus boundary vs. emitter), not in behavior, switching from A to B afterwards is an internal refactor within `axoniq-multi-tenancy` — it does not require undoing anything exposed to applications. If a concrete second consumer for a pluggable `QueryUpdateEmitter` ever materializes (the gap noted in Option B's cons), that's the point to revisit B — not before.

Implementation proceeds against Option A; Option B remains documented above as the fallback/future path, not as an alternative still open for this PR.
