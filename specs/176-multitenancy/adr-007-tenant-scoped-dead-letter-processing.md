# ADR 007: Tenant-scoped dead-letter processing (issue [#176](https://github.com/AxonIQ/axoniq-framework/issues/176))

Date: 2026-09-02
Status: accepted
Extends: [ADR 006, tenant-aware dead-letter queue integration](adr-006-tenant-aware-dead-letter-queue.md)

## Context

ADR 006 makes the queue tenant-aware. Every queue operation resolves `TenantDescriptor.RESOURCE_KEY` from the
supplied `ProcessingContext` before selecting the concrete queue for that tenant. This works while an event is being
handled: `DeadLetteringEventHandlingComponent.handle(event, context)` receives the tenant-bearing context, stores a
snapshot of it in the `DeadLetter`, and enqueues through the routing queue.

Retry starts at a different boundary. The public `SequencedDeadLetterProcessor` exposes only
`process(Predicate<DeadLetter<? extends M>>)` and `processAny()`. Its current implementation calls
`queue.process(sequenceFilter, processingTask::process, null)`. Consequently the tenant-routing queue has to choose a
physical queue with no tenant and rejects the operation. The saved context cannot repair this: it is merged into the
fresh retry `ProcessingContext` by `DeadLetteredEventProcessingTask` only after the queue has selected and loaded a
dead letter.

This is therefore not an event-handler interceptor problem. It is a missing tenant-selection input at the retry entry
point. `processAll` is not an operation on this API; the documented form is a loop that repeatedly calls `processAny()`.
Any solution must make every iteration tenant-scoped, or deliberately define an all-tenants operation.

## Decision drivers

- The selected queue, the retry `ProcessingContext`, and the event handler must all use the same tenant.
- Tenant identity must be explicit at an asynchronous API boundary. `ThreadLocal` state is not acceptable because
  completion and retry work can move to another thread.
- The normal single-tenant `SequencedDeadLetterProcessor` API should remain uncomplicated where possible.
- A predicate can select a sequence only after a queue is selected. It cannot route an operation to an unknown physical
  tenant queue.
- Tenant add/remove lifecycle and queue eviction must remain owned by the ADR 006 registry.

## Decision

Choose option 3: add nullable `ProcessingContext` overloads to `SequencedDeadLetterProcessor`. This exposes the
input that retry routing already needs without adding a tenant-specific processor hierarchy. Options 1 and 2 remain
below as reference alternatives and are not selected.

The ADR 006 queue integration is merged. Processor changes are tracked in follow-up issue
[#406](https://github.com/AxonIQ/axoniq-framework/issues/406).

The public multi-tenant interaction is explicit and small: a caller replaying dead letters creates a processing context
that contains `TenantDescriptor.RESOURCE_KEY` and invokes the context-aware processor method. The queue reached by
`DeadLetteringEventHandlingComponent` is already a `TenantRoutingSequencedDeadLetterQueue`; its `queueFor(context)`
method derives the physical tenant queue from that resource. This usage must be documented with the DLQ retry examples.

Follow up with the Platform team on how a dashboard-triggered dead-letter replay identifies the tenant and supplies the
tenant-bearing processing context. This is a compatibility and integration question only; it does not change this
decision or block issue #406.

## Options

### Option 1: an extension-level tenant-bound retry API

Keep the standard one-parameter processor contract and make tenant selection an explicit multi-tenancy extension API.
Options 1a and 1b provide the same two-argument retry capability in curried and uncurried forms; they are API-shape
variants, not independent routing designs.

#### Option 1a: a `TenantComponentLookup` of tenant-bound processor views

Expose the existing `TenantComponentLookup<SequencedDeadLetterProcessor<EventMessage>>` under the same component name
as the ordinary dead-letter processor. Looking up the tenant returns the ordinary `SequencedDeadLetterProcessor` view
for that tenant. The returned view supplies a tenant-bearing context whenever it delegates to the actual
processor/queue. Thus callers write `lookup.componentFor(tenant).processAny()` or use `process(...)` as today.

Yes: conceptually the dispatcher selects the concrete processor for the tenant. It should not construct a complete
event-handling component per tenant. There is one DLQ-decorated event-handling component; the tenant-bound view fixes
the queue selection and retry context for that invocation. The registry continues to own the physical tenant queues.
The dispatcher may cache lightweight views, but it must invalidate them with tenant removal and must not retain tenant
resources outside the registry lifecycle.

This is a curried form of the missing two-argument operation:

```text
(tenant, predicate) -> retry one matching sequence
tenant -> (predicate -> retry one matching sequence)
```

Smallest framework wiring change (the component factory makes a named lookup available beside the existing named
processor):

```java
// Registered by the multi-tenancy DLQ enhancer for each DLQ-decorated component name.
TenantComponentLookup<SequencedDeadLetterProcessor<EventMessage>> lookup = tenant ->
        new TenantBoundProcessor(delegate, contextFor(tenant));

final class TenantBoundProcessor<M extends Message> implements SequencedDeadLetterProcessor<M> {
    private final ContextAwareDeadLetterProcessor<M> delegate;
    private final ProcessingContext tenantContext;

    public CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends M>> filter) {
        return delegate.process(filter, tenantContext);
    }
}
```

The lookup is a framework-owned adapter, not an application-supplied `TenantComponentProvider`. It creates lightweight
views and owns no tenant resources, so the ADR 006 registry remains the owner of physical queue lifecycle. The
framework should resolve the named `TenantComponentLookup` where tenant-aware retry is requested; the existing named
`SequencedDeadLetterProcessor` remains available for the single-tenant path.

`TenantComponentProviderUtil.find(configuration, type)` is not the right discovery call here. It finds an application
registered `TenantComponentProvider` by raw component type, whereas dead-letter processors are framework-created and
their component name identifies the event-handling component. A single provider for
`SequencedDeadLetterProcessor.class` could not distinguish `invoiceProjection` from another component. The existing
`TenantComponentLookup` interface is the reusable part; the correct discovery mechanism is a named component factory.

Declarative configuration and retrieval would look like this:

```java
// Application configuration: enable DLQ and the existing multi-tenancy enhancer installs the lookup.
configurer.eventProcessing(eventProcessing -> eventProcessing.pooledStreaming(pooled -> pooled
        .processor("orders", processor -> processor.customized((cfg, processorConfig) -> processorConfig
                .extend(DeadLetterQueueConfiguration.class,
                        () -> new DeadLetterQueueConfiguration().enabled())))
));

// Retry endpoint/job: resolve the named tenant lookup instead of the ordinary processor.
var componentName = "EventHandlingComponent[orders][invoiceProjection]";
configuration.getModuleConfiguration("orders")
             .flatMap(module -> module.getOptionalComponent(TenantComponentLookup.class, componentName))
             .ifPresent(lookup -> lookup.componentFor(tenant).processAny());
```

The important configuration change is in the multi-tenancy DLQ enhancer: when it finds an enabled DLQ, it registers
the named `TenantComponentLookup` factory beside the existing `SequencedDeadLetterProcessor` factory. No application
`TenantComponentProvider<SequencedDeadLetterProcessor<?>>` registration is required or desirable, because the
processor name is part of the component identity and the factory must reuse the framework's DLQ-decorated handler.

Advantages:

- Keeps `SequencedDeadLetterProcessor` source and binary compatible for single-tenant consumers.
- Makes the tenant visible where retry work is scheduled, while retaining the familiar `process` and `processAny` API.
- Makes a per-tenant “process all” loop unambiguous: repeatedly call
  `lookup.componentFor(tenant).processAny()`.

Costs and open details:

- Adds a multi-tenancy-specific type and component lookup path alongside the existing processor lookup.
- The adapter needs a clean construction seam to provide the tenant context to the existing dead-letter processor;
  merely holding the existing processor does not help unless it can accept that context.
- An explicit all-tenants operation is a separate policy decision: ordering and fairness across tenant queues cannot be
  represented as one globally oldest sequence without scanning and comparing the tenant queues.

#### Option 1b: an explicit two-parameter retry gateway

Instead of returning a tenant-bound standard processor, expose an extension-level gateway with
`retry(TenantDescriptor, Predicate)` and `retryAny(TenantDescriptor)`. It creates a `ProcessingContext` containing
`TenantDescriptor.RESOURCE_KEY`, routes to the tenant queue, and executes the normal dead-letter processing task with
the same tenant in its retry context.

This is the uncurried form of option 1a. A factory such as
`Function<TenantDescriptor, SequencedDeadLetterProcessor<M>>` is sufficient only when its implementation owns that
context propagation; a factory which only returns the existing no-context processor recreates the original gap.

Smallest design-shaped change:

```java
interface TenantDeadLetterRetryGateway<M extends Message> {
    CompletableFuture<Boolean> retry(TenantDescriptor tenant,
                                     Predicate<DeadLetter<? extends M>> filter);

    default CompletableFuture<Boolean> retryAny(TenantDescriptor tenant) {
        return retry(tenant, letter -> true);
    }
}

public CompletableFuture<Boolean> retry(TenantDescriptor tenant, Predicate<DeadLetter<? extends M>> filter) {
    ProcessingContext context = new StubProcessingContext()
            .withResource(TenantDescriptor.RESOURCE_KEY, tenant);
    return queue.process(filter, processingTask::process, context);
}
```

Advantages:

- No ambient state and no change to the core `SequencedDeadLetterProcessor` signature.
- The multi-tenancy module owns its API and can define tenant lifecycle and all-tenants semantics deliberately.

Costs:

- Requires a new public extension API and a documented way to retrieve it.
- Does not preserve the familiar `process` / `processAny` API as directly as option 1a.

### Option 2: copy tenant identity into message metadata and filter by it

An interceptor on normal event handling could copy tenant identity into message metadata before dead-lettering. A retry
predicate could then inspect that metadata and select letters for a tenant.

Smallest design-shaped change:

```java
MessageDispatchInterceptor<EventMessage> persistTenantMetadata = message ->
        message.andMetaData(Map.of("tenant-id", tenantFromContext().tenantId()));

processor.process(letter -> tenantId.equals(letter.message().getMetaData().get("tenant-id")));
```

This does not solve routing with the ADR 006 design. The routing queue must first select one physical tenant queue in
order to obtain a letter on which it can run the predicate; it has no queue to inspect when no tenant context was
provided. It only becomes viable if the implementation scans every registered tenant queue itself. That changes the
operation from “process one queue” into a cross-queue coordinator and still needs a policy for `processAny()` and the
documented repeated-`processAny()` process-all loop.

It also makes tenant identity part of persisted message metadata, despite it being routing information rather than an
event contract. This creates migration, visibility, and compatibility questions for all existing events and storage
implementations.

Conclusion: reject as the primary solution. It may be useful as an application-level diagnostic or filter after a
tenant queue is explicitly selected, but it must not be the routing mechanism.

### Option 3: add `ProcessingContext` overloads to `SequencedDeadLetterProcessor` (chosen)

Add `process(filter, context)` and `processAny(context)`, and have `DeadLetteringEventHandlingComponent` forward that
context to `SequencedDeadLetterQueue.process(...)`. The existing methods can remain and delegate with `null`, retaining
current single-tenant behavior. The multi-tenant caller constructs a context with
`TenantDescriptor.RESOURCE_KEY` and uses the overload.

This is the most direct representation of the real input and naturally supports predicates, `processAny`, and a
per-tenant process-all loop. It expands a widely used core API and requires every implementation to make a deliberate
choice about the new overload, so it should be treated as an API evolution even if compatibility defaults can avoid an
immediate binary break.

Smallest backward-compatible API change:

```java
public interface SequencedDeadLetterProcessor<M extends Message> {
    // Existing abstract method: retained for source and binary compatibility.
    CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends M>> filter);

    // New callers can supply context; existing implementations retain their old behavior and ignore it.
    default CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends M>> filter,
                                               @Nullable ProcessingContext context) {
        return process(filter);
    }

    // Existing no-context method is retained.
    default CompletableFuture<Boolean> processAny() {
        return process(letter -> true);
    }

    // New callers can supply context; existing implementations retain their old behavior and ignore it.
    default CompletableFuture<Boolean> processAny(@Nullable ProcessingContext context) {
        return processAny();
    }
}

// Framework implementation: old methods redirect to the new overloads with null.
public CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends EventMessage>> filter,
                                          @Nullable ProcessingContext context) {
    return queue.process(filter, processingTask::process, context);
}

public CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends EventMessage>> filter) {
    return process(filter, null);
}

public CompletableFuture<Boolean> processAny() {
    return processAny(null);
}

public CompletableFuture<Boolean> processAny(@Nullable ProcessingContext context) {
    return process(letter -> true, context);
}
```

The direction of the default is important. The new overload may default to the retained old abstract method, which
keeps existing third-party implementations loadable and callable. The old method must not default to a new abstract
overload: an already-compiled implementation would then fail when the default invokes a method it does not implement.
Both methods also cannot safely default to each other. An old implementation will ignore the supplied context through
the compatibility default; only implementations that override the new overload promise context-aware behavior.

## Consequences

- The old processor methods retain their current signatures and behavior. The new overloads default to them, so
  existing implementations remain binary and source compatible and simply ignore a supplied context.
- `DeadLetteringEventHandlingComponent` overrides the new overloads. Its no-context methods call those overloads with
  `null`; the context-aware `process(...)` implementation passes the given nullable context to
  `SequencedDeadLetterQueue.process(...)`.
- A tenant replay caller must add `TenantDescriptor.RESOURCE_KEY` to its processing context and call
  `process(filter, context)` or `processAny(context)`. Omitting that resource leaves a tenant-routing queue unable to
  select a physical queue and fails through its asynchronous API.
- “Process all” remains a caller-owned loop over `processAny(context)`, scoped to the one tenant in that context. A
  cross-tenant scheduler is not introduced.
- The remaining options are retained for architectural reference, but are not implementation candidates for issue
  #406.

## Verification required after the decision

- A two-tenant integration test retries tenant A through the public selected API and proves that tenant B's physical
  queue and handler are untouched.
- A retry handler observes the same `TenantDescriptor` in its `ProcessingContext` as the queue selected for the
  retry.
- A predicate and repeated `processAny()` operate only within the selected tenant.
- Missing and removed tenants fail through the asynchronous API without selecting another tenant.
- Existing single-tenant `SequencedDeadLetterProcessor` behavior remains unchanged.
