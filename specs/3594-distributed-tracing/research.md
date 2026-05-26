# Phase 0 — Research

**Feature**: 3594 — Distributed Tracing Support

**Branch**: `feat/3594-DistributedTracing`

This file resolves every `NEEDS CLARIFICATION` carried into planning and records the key design decisions that drive the contracts in Phase 1. It is **not** a data model — there is no domain data here, only design rationale.

---

## 1. AF4 → AF5 Concern Mapping

The single most important design move is the collapse of AF4's nine per-component span-factory interfaces into one public `SpanFactory` plus private per-component **decorators**. The table below is the **one-row-per-concern summary**; the **exhaustive method-by-method audit** (44 AF4 factory methods × every AF4 production caller × AF5 decorator mapping × cross-check against AF4 reference guide) lives in [`af4-span-inventory.md`](./af4-span-inventory.md) and is the authoritative artifact for Story 4 acceptance criterion 3 ("the consolidation does not regress the observable trace shape").

| Concern (what is being traced) | AF4 — per-component factory + consumer | AF5 — what replaces it in AxoniqFramework |
|---|---|---|
| Command dispatch + handling | `CommandBusSpanFactory` (interface) + `DefaultCommandBusSpanFactory`, consumed by `SimpleCommandBus.Builder#spanFactory(...)` | `internal.TracingCommandBus implements CommandBus` registered via `DecoratorDefinition.forType(CommandBus.class)`. Span names: `"CommandBus.dispatchCommand <commandName>"` (kind `CLIENT` when distributed, `INTERNAL` otherwise) and `"CommandBus.handleCommand <commandName>"` (kind `SERVER`). |
| Event publication | `EventBusSpanFactory` + `DefaultEventBusSpanFactory`, consumed by `SimpleEventBus.Builder#spanFactory(...)`. AF4's `AbstractEventBus#publish` creates **two** spans: (a) `createPublishEventSpan(e)` per event around `propagateContext(...)` at publish-entry (`AbstractEventBus.java:121-122`), and (b) `createCommitEventsSpan()` spanning the surrounding UoW's `prepareCommit` → `commit` → `afterCommit` → `cleanup` phases (`AbstractEventBus.java:160-202`, plus the no-UoW path at `:144`). | `internal.TracingEventSink implements EventSink` registered via `DecoratorDefinition.forType(EventSink.class)`. Produces **two spans per call**, equivalent to AF4: (a) `"EventBus.publishEvent <eventName>"` (kind `PRODUCER`) — one per event, synchronously at publish-entry, around `SpanFactory#propagateContext(event)`, BEFORE delegating; (b) `"EventBus.commitEvents"` — one per `publish(...)` call, bound to the supplied `ProcessingContext` via `ProcessingContextSpanBinding` (opened in `runOnPrepareCommit`, errored in `onError`, closed in `whenComplete`). When `ProcessingContext` is `null`, the commit-events span falls back to a synchronous `Span.run(...)` wrapper around `delegate.publish(...)`. Both spans are creatable from the public `EventSink#publish(ProcessingContext, List<EventMessage>)` signature alone — no event-bus internals are touched. |
| Event handling (incl. async / pooled streaming) | `EventProcessorSpanFactory` + `DefaultEventProcessorSpanFactory`, consumed by `TrackingEventProcessor.Builder` / `PooledStreamingEventProcessor.Builder`. Produces TWO nested spans: `StreamingEventProcessor.batch` (root per batch) wrapping `EventProcessor.process` (child per event), via `AbstractEventProcessor.processInUnitOfWork`'s `createBatchSpan(...).runCallable(() -> createProcessEventSpan(...).runCallable(handler))` pattern. | `internal.TracingEventHandlingComponent implements EventHandlingComponent` registered via `DecoratorDefinition.forType(EventHandlingComponent.class)`. Produces the same TWO nested spans via lazy-open on the shared `ProcessingContext`: `"StreamingEventProcessor.batch"` (kind `INTERNAL`) opened by `ctx.computeResourceIfAbsent(BATCH_SPAN_KEY, …)` on the first per-event `handle()` call, guarded by `ctx.getResource(Segment.RESOURCE_KEY).isPresent()`; and `"EventProcessor[<processor>].process <eventName>"` (kind `CONSUMER`) per event with the producer's W3C context extracted from metadata. AF4 batch-span coverage parity (token-store write + status update enclosed) verified; rejected alternatives (`TracingEventProcessor` decorator, upstream `BatchInterceptor` SPI, `UnitOfWorkFactory` decoration, drop-batch-span) documented separately. **See [`research-batch-tracing.md`](./research-batch-tracing.md) for full design, AF4 vs Option C coverage matrix, and rejected alternatives.** |
| Query dispatch + handling | `QueryBusSpanFactory` + `DefaultQueryBusSpanFactory`, consumed by `SimpleQueryBus.Builder#spanFactory(...)` | `internal.TracingQueryBus implements QueryBus` registered via `DecoratorDefinition.forType(QueryBus.class)`. Span name `"QueryBus.query <queryName>"` (kind `CLIENT`/`INTERNAL`) on dispatch, `"QueryBus.handle <queryName>"` (kind `SERVER`) on handling. |
| Subscription-query updates | `QueryUpdateEmitterSpanFactory` + `DefaultQueryUpdateEmitterSpanFactory`, consumed by `SimpleQueryUpdateEmitter.Builder` | `internal.TracingQueryUpdateEmitter implements QueryUpdateEmitter` registered via `DecoratorDefinition.forType(QueryUpdateEmitter.class)`. Span name `"QueryUpdateEmitter.emit <updateType>"`. |
| Aggregate / entity load + save | `RepositorySpanFactory` + `DefaultRepositorySpanFactory`, consumed by `AbstractRepository.Builder` | `internal.TracingRepository implements Repository` + `internal.TracingStateManager implements StateManager`, registered via `DecoratorDefinition.forType(Repository.class)` / `…StateManager.class`. Span names `"Repository.load <entityType> <id>"` and `"Repository.save <entityType> <id>"`. |
| Snapshot creation + read | `SnapshotterSpanFactory` + `DefaultSnapshotterSpanFactory`, consumed by `AbstractSnapshotter.Builder` | `internal.TracingSnapshotter implements Snapshotter` registered via `DecoratorDefinition.forType(Snapshotter.class)`. Span names `"Snapshotter.create <entityType> <id>"` and `"Snapshotter.read <entityType> <id>"`. `separateTrace` and `aggregateTypeInSpanName` knobs come back as `TracingProperties.snapshotter.*`. |
| ~~Deadline schedule + fire~~ | ~~AF4: `DeadlineManagerSpanFactory` + `DefaultDeadlineManagerSpanFactory`~~ | **Out of scope.** `DeadlineManager` does not exist in Axon Framework 5 (clarification 2026-05-26). No `TracingDeadlineManager`, no enhancer registration, no `TracingProperties` group. |
| ~~Saga / process-manager invocation~~ | ~~AF4: `SagaManagerSpanFactory` + `DefaultSagaManagerSpanFactory`~~ | **Out of scope.** Sagas / process-managers do not exist in Axon Framework 5 today (clarification 2026-05-26). No `TracingSagaManager`, no enhancer registration, no `TracingProperties` group, no `@SagaEventHandler` wrapping. FR-012 is withdrawn. |
| `@*Handler` annotation handlers | `TracingHandlerEnhancerDefinition` (already a `HandlerEnhancerDefinition` in AF4) | `internal.TracingHandlerEnhancerDefinition` — direct port of the AF4 class shape, registered as a `HandlerEnhancerDefinition` bean / SPI. Wraps `@CommandHandler`, `@EventHandler`, `@QueryHandler`, `@EventSourcingHandler`. (`@DeadlineHandler` and `@SagaEventHandler` are excluded — see deadline and saga rows above.) |
| Custom user span attributes | `SpanAttributesProvider` (already a generic SPI in AF4) | `SpanAttributesProvider` — kept as public SPI in `axoniq-tracing-core`. Six built-in providers ported verbatim into `attributes/` (message id, message name, message type, payload type, metadata, aggregate identifier). |
| OpenTelemetry implementation | `OpenTelemetrySpanFactory` + `OpenTelemetrySpan` + `MetadataContextSetter` + `MetadataContextGetter` in `tracing-opentelemetry` | Same four classes, ported into `axoniq-tracing-opentelemetry`. Class shape is essentially unchanged; only the imports and `Span` interface they implement are AF5 ones. |
| Spring Boot wiring | `AxonTracingAutoConfiguration` + `TracingProperties` + `OpenTelemetryAutoConfiguration` in `spring-boot-autoconfigure` | `TracingAutoConfiguration` + `TracingProperties` + `OpenTelemetryTracingAutoConfiguration` added **inside the existing** `dependency-injection/spring/spring-boot-autoconfigure` module. Three `@Bean` methods exposing per-component `*SpanFactory` beans in AF4 collapse to **one** `SpanFactory` `@Bean`. |

**Decision**: No public `*BusSpanFactory` / `*ManagerSpanFactory` / `*ProcessorSpanFactory` / `*EmitterSpanFactory` / `RepositorySpanFactory` / `SagaManagerSpanFactory` / `SnapshotterSpanFactory` interface is exposed (FR-016). `DeadlineManagerSpanFactory` is also implicitly forbidden — both deadlines and sagas / process-managers are out of scope (clarifications 2026-05-26).

**Rationale**: Each of those AF4 interfaces had three responsibilities glued together — building a span name string, choosing a span kind, and (sometimes) deciding what to do for the distributed case. None of those responsibilities benefit from being polymorphic on the user's side: there are no real implementations of `EventBusSpanFactory` outside the framework. They exist as interfaces in AF4 only because the consumer-side builder needed an interface to inject; AF5's `DecoratorDefinition` removes that need entirely.

**Alternatives considered**:
- **Keep AF4 interfaces 1:1, just port them.** Rejected — duplicates the API surface, locks span shapes behind public types that no one implements, and forces every new tracked concern to add a new public interface.
- **One public `SpanFactory` + a public `MetadataContextPropagator` / `SpanNames` / `OperationKinds` mini-SPI for advanced authors.** Recognised in spec §DFI-001 as a future graduation path; explicitly deferred. Today, those helpers stay package-private. Graduating them later is purely additive.

---

## 2. Decoration Approach (DecoratorDefinition + ConfigurationEnhancer)

The wiring uses two AF5 extension points:

1. **`org.axonframework.common.configuration.DecoratorDefinition<C, D extends C>`** — declarative wrapper registration against a `ComponentRegistry`.
2. **`org.axonframework.common.configuration.ConfigurationEnhancer#enhance(ComponentRegistry)`** — single entrypoint where all tracing decorators are registered.

### 2.1 Wrapper class shape

Every tracing wrapper follows AF5's existing `TracingCommandBus` pattern verbatim:

```java
@Internal
final class TracingCommandBus implements CommandBus {

    private final CommandBus delegate;
    private final SpanFactory spanFactory;

    TracingCommandBus(CommandBus delegate, SpanFactory spanFactory) {
        this.delegate = Objects.requireNonNull(delegate, "delegate may not be null");
        this.spanFactory = Objects.requireNonNull(spanFactory, "spanFactory may not be null");
    }

    @Override
    public CompletableFuture<? extends CommandResultMessage<?>> dispatch(
            CommandMessage<?> command, @Nullable ProcessingContext context) {

        Span dispatchSpan = spanFactory.createDispatchSpan(SpanNames.CMD_DISPATCH, command);
        CommandMessage<?> propagated = spanFactory.propagateContext(command);

        if (context != null) {
            ProcessingContextSpanBinding.bind(context, dispatchSpan);     // open on pre-invocation, close on whenComplete
            return delegate.dispatch(propagated, context);
        }
        return dispatchSpan.runSupplierAsync(() -> delegate.dispatch(propagated, null));
    }

    @Override
    public CommandBus subscribe(QualifiedName name, CommandHandler handler) {
        delegate.subscribe(name, handler);
        return this;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("spanFactory", spanFactory);
    }
}
```

**Decision**: Wrappers are `final` classes in a package-private / `@Internal` package, never `public`. They are constructed only by `TracingConfigurationEnhancer`. Users who want a custom wrapper write their own `DecoratorDefinition` on top of `SpanFactory` — but they do not extend ours.

**Rationale**: This is the AF5-blessed shape (see `TracingCommandBus.java` in upstream — even though it's dead code, the *shape* is right). It composes with interceptors, metrics, distributed transport, and any other decorator because every wrapper just calls `delegate.<op>(...)` and contributes its own `describeWrapperOf` link to the chain.

**Alternatives considered**:
- **Direct implementation without delegation** (AF4's `DistributedCommandBus` style) — explicitly rejected by FR-013 and Story 4. It makes stacking impossible.
- **Static subclassing** (`class TracingCommandBus extends SimpleCommandBus`) — rejected. Couples tracing to one concrete bus implementation and breaks composition with `InterceptingCommandBus`.

### 2.2 `TracingConfigurationEnhancer`

```java
public final class TracingConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        // The user MUST register a SpanFactory bean themselves (or rely on the no-op default).
        // Each decorator pulls SpanFactory from the registry at construction time.
        registry.registerIfNotPresent(SpanFactory.class, c -> NoOpSpanFactory.INSTANCE);

        DecoratorDefinition.forType(CommandBus.class)
                .with((cfg, name, delegate) -> new TracingCommandBus(delegate, cfg.get(SpanFactory.class)))
                .order(TracingOrders.DECORATOR_ORDER)
                .registerWith(registry);

        DecoratorDefinition.forType(EventSink.class)
                .with((cfg, name, delegate) -> new TracingEventSink(delegate, cfg.get(SpanFactory.class)))
                .order(TracingOrders.DECORATOR_ORDER)
                .registerWith(registry);

        DecoratorDefinition.forType(EventHandlingComponent.class)
                .with((cfg, name, delegate) -> new TracingEventHandlingComponent(delegate, cfg.get(SpanFactory.class)))
                .order(TracingOrders.DECORATOR_ORDER)
                .registerWith(registry);

        DecoratorDefinition.forType(QueryBus.class)
                .with((cfg, name, delegate) -> new TracingQueryBus(delegate, cfg.get(SpanFactory.class)))
                .order(TracingOrders.DECORATOR_ORDER)
                .registerWith(registry);

        DecoratorDefinition.forType(QueryUpdateEmitter.class)
                .with((cfg, name, delegate) -> new TracingQueryUpdateEmitter(delegate, cfg.get(SpanFactory.class)))
                .order(TracingOrders.DECORATOR_ORDER)
                .registerWith(registry);

        DecoratorDefinition.forType(Repository.class)
                .with((cfg, name, delegate) -> new TracingRepository<>(delegate, cfg.get(SpanFactory.class)))
                .order(TracingOrders.DECORATOR_ORDER)
                .registerWith(registry);

        DecoratorDefinition.forType(Snapshotter.class)
                .with((cfg, name, delegate) -> new TracingSnapshotter(delegate, cfg.get(SpanFactory.class)))
                .order(TracingOrders.DECORATOR_ORDER)
                .registerWith(registry);

        // Deadline tracing is permanently out of scope — DeadlineManager does not exist in AF5
        // (clarification 2026-05-26). No `ifClassPresent("org.axonframework.deadline.DeadlineManager", …)`.

        // Saga / process-manager tracing is permanently out of scope — sagas / process-managers do not
        // exist in AF5 (clarification 2026-05-26). No TracingSagaManager registration here.

        // Annotation-handler enhancer (covers @EventSourcingHandler etc. — orthogonal to the bus-level decorators above).
        registry.registerComponent(HandlerEnhancerDefinition.class, "tracingHandlerEnhancer",
                cfg -> new TracingHandlerEnhancerDefinition(cfg.get(SpanFactory.class),
                                                            cfg.getAll(SpanAttributesProvider.class)));
    }
}
```

**Decision**: One enhancer registers everything. `order(TracingOrders.DECORATOR_ORDER)` puts tracing **outside** interception so that the dispatcher span captures the full handler invocation including interceptors. (Concrete numeric order is fixed in `TracingOrders.java` and documented in `public-api.md`.)

**Rationale**: A single enhancer is one bean / one ServiceLoader entry, and one place to evolve. Future component tracing (e.g., if a scheduler or saga / process-manager ever lands in AF5) is "add one more `DecoratorDefinition` here" — zero public-API churn (Story 4 AS-2).

**Alternatives considered**:
- **One `ConfigurationEnhancer` per concern (`TracingCommandsConfigurationEnhancer`, `TracingEventsConfigurationEnhancer`, …)** — rejected: nine beans to register, nine ServiceLoader entries, no benefit because they all share the same `SpanFactory` resolution.

---

## 3. UnitOfWork → ProcessingContext: Span Lifecycle Binding

In AF4, tracing relied on `UnitOfWork.getCurrent().getResource(...)` and a `ThreadLocal`-driven active span. That model breaks under AF5's async/reactive paradigm and is explicitly forbidden by Constitution §V.

**Decision**: Active span lives on `ProcessingContext` under a `ResourceKey<SpanScope>`, and span open/close is bound to `ProcessingLifecycle` callbacks.

```java
@Internal
final class ProcessingContextSpanBinding {

    static final ResourceKey<SpanScope> ACTIVE_SPAN = ResourceKey.create("axoniq.tracing.activeSpan");

    /**
     * Opens {@code span} when the processing context enters its invocation phase, and closes it
     * (or marks it errored, then closes) when the context completes or fails.
     */
    static void bind(ProcessingContext context, Span span) {
        context.runOnPreInvocation(ctx -> {
            SpanScope scope = span.start();
            ctx.putResource(ACTIVE_SPAN, scope);
        });
        context.onError((ctx, phase, error) -> {
            SpanScope scope = ctx.getResource(ACTIVE_SPAN);
            if (scope != null) {
                scope.span().recordException(error);
            }
        });
        context.whenComplete(ctx -> {
            SpanScope scope = ctx.removeResource(ACTIVE_SPAN);
            if (scope != null) {
                scope.close();
            }
        });
    }

    private ProcessingContextSpanBinding() { }
}
```

**Key properties of this binding**:

| Property | How it's preserved |
|---|---|
| No `ThreadLocal` in framework code | The `ACTIVE_SPAN` resource key is the only source of truth. `OpenTelemetry.Context.makeCurrent()` is called inside `SpanScope.close()` / `SpanScope.start()` only when the user explicitly opts into the imperative `Span.run...` helper. |
| Works across reactive boundaries | `ProcessingLifecycle` hooks fire on the right phases regardless of which thread the continuation runs on. The span is owned by the context, not by the dispatch thread. |
| Survives commit and rollback | `onError` records the exception; `whenComplete` always closes the span. Closing is idempotent in the OTel adapter. |
| Composable | Multiple decorators (tracing + metrics + interception) can all bind their own resources on the same context without colliding (each uses a distinct `ResourceKey<T>`). |

**Decision (fallback)**: When a wrapped operation has no `ProcessingContext` parameter (e.g., a programmatic, out-of-band snapshot creation), the decorator falls back to `Span.runSupplier(...)` / `Span.runSupplierAsync(...)`. This is the direct AF4 style and is acceptable here per FR-013a because there is genuinely no lifecycle to ride on.

### 3.1 Worked example — `TracingEventSink` reproduces AF4's two-span pattern

AF4's `AbstractEventBus.publish(...)` opens two spans (see the mapping row above): one per event around context propagation, and one across the UoW commit phases. Both are reproducible from the AF5 `EventSink#publish(ProcessingContext, List<EventMessage>)` signature alone — the decorator never touches the bus internals.

```java
@Internal
final class TracingEventSink implements EventSink {

    private final EventSink delegate;
    private final SpanFactory spanFactory;

    TracingEventSink(EventSink delegate, SpanFactory spanFactory) {
        this.delegate = Objects.requireNonNull(delegate, "delegate may not be null");
        this.spanFactory = Objects.requireNonNull(spanFactory, "spanFactory may not be null");
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                           List<? extends EventMessage<?>> events) {

        // (a) per-event publish span — synchronous, around propagateContext (mirrors AbstractEventBus.java:121-122)
        List<EventMessage<?>> propagated = events.stream()
                .map(event -> spanFactory.createDispatchSpan(SpanNames.EVT_PUBLISH, event)
                                         .runSupplier(() -> spanFactory.propagateContext(event)))
                .toList();

        // (b) commit-events span — bound to ProcessingContext lifecycle (mirrors AbstractEventBus.java:160-202)
        //     or, when no context is active, run synchronously around the delegate call (mirrors :144).
        if (context != null) {
            Span commitSpan = spanFactory.createInternalSpan(SpanNames.EVT_COMMIT);
            ProcessingContextSpanBinding.bind(context, commitSpan);
            return delegate.publish(context, propagated);
        }
        return spanFactory.createInternalSpan(SpanNames.EVT_COMMIT)
                          .runSupplierAsync(() -> delegate.publish(null, propagated));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("spanFactory", spanFactory);
    }
}
```

**Why the decorator is sufficient**: every input AF4 needed to open both spans is part of the AF5 public method signature: the `events` list (for the per-event span) and the `ProcessingContext` (for the commit-events span). The AF4 implementation reached into its own `UnitOfWork` because it had to — `UnitOfWork` was the carrier. In AF5, that carrier is the `ProcessingContext` parameter — already in the decorator's hands. **No AF5 event-bus internals are touched.**

**The same pattern applies to other components** whose AF4 counterparts opened both a per-message dispatch span and a UoW-scoped commit span (none currently in scope beyond event publication, but the pattern is the template).

### 3.2 Sourcing the aggregate identifier in AF5 (Aggregate → Entity / DCB shift)

AF4's `AggregateIdentifierSpanAttributesProvider` casts the inbound `Message` to `DomainEventMessage` and calls `getAggregateIdentifier()` (see `AxonFramework4/messaging/src/main/java/org/axonframework/tracing/attributes/AggregateIdentifierSpanAttributesProvider.java:39-43`). That path is **closed** in AF5:

- `DomainEventMessage` is **completely removed** from AxonFramework 5 production code (verified by grep across `messaging/`, `modelling/`, `eventsourcing/`, `extensions/` — zero matches, stash excluded).
- The "aggregate" concept itself has been replaced by **Entity / Dynamic Consistency Boundary (DCB)**. New code uses `@EventSourcedEntity(tagKey = "...")` plus `@EventTag` on event fields/records; the event store resolves entities by tag rather than by aggregate identifier embedded in the message. See AF5's migration guide: `docs/reference-guide/modules/migration/pages/paths/aggregates.adoc`.
- For the migration window, AF5 ships a small bridge: `org.axonframework.messaging.core.LegacyResources`. The aggregate-based event storage engines (`AggregateBasedJpaEventStorageEngine`, etc.) populate three `ResourceKey<?>` values on the `ProcessingContext` while sourcing or persisting a legacy aggregate stream:

  ```java
  public static final Context.ResourceKey<String> AGGREGATE_IDENTIFIER_KEY = Context.ResourceKey.withLabel("aggregateIdentifier");
  public static final Context.ResourceKey<String> AGGREGATE_TYPE_KEY       = Context.ResourceKey.withLabel("aggregateType");
  public static final Context.ResourceKey<Long>   AGGREGATE_SEQUENCE_NUMBER_KEY = Context.ResourceKey.withLabel("aggregateSequenceNumber");
  ```
- AF5's idiomatic readers of these values are `AggregateTypeParameterResolverFactory.AggregateTypeParameterResolver` and `SourceIdParameterResolverFactory.SourceIdParameterResolver`. Both do exactly:

  ```java
  var value = context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY);
  if (value != null) return CompletableFuture.completedFuture(value);
  ```

**Decision**: the new `AggregateIdentifierSpanAttributesProvider` (and any future legacy-bridge attribute provider) MUST read from `ProcessingContext.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)`. It MUST NOT reference `DomainEventMessage` (forbidden by constitution v2.1.0).

```java
@NullMarked
public final class AggregateIdentifierSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public Map<String, String> provideForMessage(Message<?> message, @Nullable ProcessingContext context) {
        if (context == null) {
            return Map.of();
        }
        String aggregateId = context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY);
        return aggregateId == null ? Map.of() : Map.of("axoniq.aggregate.identifier", aggregateId);
    }
}
```

**Consequence — SPI shape**: the `SpanAttributesProvider` SPI single method is `Map<String, String> provideForMessage(Message<?> message, @Nullable ProcessingContext context)`. Providers that do not need the context simply ignore it (e.g., `MessageIdSpanAttributesProvider` takes the id off the `Message` itself). The decision rejects the multi-method / default-fallback alternative — every implementation pays the cost of one `@Nullable` parameter; the SPI stays a single SAM and there is no ambiguity about which method the factory calls.

**Consequence — `SpanFactory.create*Span` signatures**: the two message-aware factory methods gain a `@Nullable ProcessingContext` parameter so that decorators (which always have a `ProcessingContext` from the AF5 bus signatures) feed it through to providers. The non-message `createInternalSpan(operationName)` stays as-is — decorators (e.g., `TracingSnapshotter`, `TracingRepository`) attach their own attributes via `Span#addAttribute(key, value)` from local method parameters; the existing `AggregateIdentifierSpanAttributesProvider` picks up `LegacyResources` data from the `ProcessingContext` when one is active. There is no `provideForSubject(Object)` SPI method and no `createInternalSpan(String, Object)` overload — see §5 below.

**Consequence — behavior on DCB / entity operations**: the `axoniq.aggregate.identifier` attribute is intentionally absent on traces produced inside DCB / entity-based event streams (`LegacyResources.AGGREGATE_IDENTIFIER_KEY` is simply not populated by the storage engines for those streams). Trace consumers MUST treat the attribute as optional. Edge case captured in `spec.md` "DCB / entity-based operation" bullet.

**Out of scope / deferred**: a sister `EntityTagSpanAttributesProvider` that contributes one or more `axoniq.entity.tag.<key>` attributes from the inbound event's `@EventTag`-derived tags is acknowledged as a deferred future improvement (similar to DFI-001 for the propagator SPI). It is a pure addition — register a new `SpanAttributesProvider` bean in the autoconfig — and does not affect any public shape this feature ships.

**Alternatives considered**:
- **Port `AggregateIdentifierSpanAttributesProvider` verbatim and depend on a stash bridge for `DomainEventMessage`** — rejected; trips constitution v2.1.0 and breaks the moment the stash drops the bridge.
- **Add a public `MetadataKey<String> AGGREGATE_IDENTIFIER` and propagate aggregate-id through message metadata** — rejected; duplicates the responsibility of `LegacyResources` and forces every event-store implementation to repeat the work the resource already does.
- **Add the SPI overload with a default body that falls back to a no-context call** — rejected per the SPI-shape question; one method, one signature.

**Alternatives considered**:
- **OpenTelemetry's `io.opentelemetry.context.Context.current()` everywhere** — rejected: pulls a `ThreadLocal` back into the framework. We rely on it only at imperative-edge `Span.runSupplier(...)` call sites.
- **A new `SpanLifecycleInterceptor`** — rejected: introduces a new interceptor level which Constitution §Interceptor Levels forbids (only `MessageDispatchInterceptor` / `MessageHandlerInterceptor` are blessed; tracing plugs in via `DecoratorDefinition` + `HandlerEnhancerDefinition` instead).

---

## 4. Cross-Process Trace Context Propagation

**Decision**: Trace context travels in `MetaData` under W3C-standard keys (`traceparent`, `tracestate`, `baggage`). Serialisation/deserialisation is handled by `MetadataContextSetter` / `MetadataContextGetter` (W3C `TextMapSetter` / `TextMapGetter` implementations against `MetaData`). These classes are `@Internal` in `axoniq-tracing-opentelemetry`; users get propagation for free without seeing the propagator SPI.

**Send side** (inside `TracingCommandBus.dispatch` and friends):
```java
CommandMessage<?> propagated = spanFactory.propagateContext(command);
```
`SpanFactory#propagateContext(Message)` returns a new `Message` with metadata enriched with the current trace context (`OpenTelemetrySpanFactory` calls `W3CTraceContextPropagator.inject(...)` with `MetadataContextSetter`). `NoOpSpanFactory#propagateContext` returns the message unchanged.

**Receive side** (inside `TracingEventHandlingComponent` and the handler side of `TracingCommandBus`):
```java
Span handleSpan = spanFactory.createHandlerSpan(SpanNames.CMD_HANDLE, command);
```
`SpanFactory#createHandlerSpan(...)` extracts trace context from the message metadata first (`W3CTraceContextPropagator.extract(...)` with `MetadataContextGetter`) and uses it as the parent of the new handler span.

**Metadata key collisions**: The W3C keys are namespaced enough in practice that real-world apps don't collide. The behaviour on collision is documented in `docs/.../tracing/pages/index.adoc`: the tracing decorator overwrites the existing values on the outgoing message (because trace context only makes sense if it actually matches the active span). Inbound, if a user metadata key happens to be `traceparent`, the receive-side propagator will read it — that is by design (it's the W3C contract). No-op factory simply does not touch metadata.

**Alternatives considered**:
- **Expose `MetadataContextPropagator` as public SPI now** — deferred to DFI-001 in spec.
- **Custom non-W3C key scheme** — rejected: would silo AxoniqFramework traces from the rest of the ecosystem (Jaeger, Tempo, Datadog, etc., all speak W3C).

---

## 5. SpanFactory Coverage of Non-Message Operations

A handful of operations (snapshot creation, repository load / save) are not `Message`s but still need spans. AF4 modelled this with bespoke per-component factory methods like `SnapshotterSpanFactory.createSnapshotSpan(aggregateType, aggregateIdentifier)` and `RepositorySpanFactory.createLoadAggregateSpan(...)` — typed parameters, dedicated methods per operation.

**Decision**: The consolidated `SpanFactory` exposes **one** non-message builder:

```java
Span createInternalSpan(String operationName);
```

Decorators handling non-message operations own the attribute attachment locally. Example — `TracingSnapshotter`:

```java
@Override
public CompletableFuture<Void> scheduleSnapshot(String entityType, Object identifier) {
    Span span = spanFactory.createInternalSpan(SpanNames.SNAPSHOT_CREATE + " " + entityType);
    span.addAttribute("axoniq.entity.type", entityType);
    span.addAttribute("axoniq.aggregate.identifier", String.valueOf(identifier));
    return span.runSupplierAsync(() -> delegate.scheduleSnapshot(entityType, identifier));
}
```

When the operation runs inside an active `ProcessingContext` (typical during event sourcing replay), the standard providers — in particular `AggregateIdentifierSpanAttributesProvider` reading `LegacyResources.AGGREGATE_IDENTIFIER_KEY` per §3.2 — contribute their attributes automatically through the message/context path. The decorator's local `addAttribute(...)` calls handle the no-context case.

**Rationale**:
- AF4 didn't have a generic `Object subject` parameter either — its per-component factories had typed methods. A single-typed-parameter `Object` is a worse abstraction than AF4's bespoke methods, not a better one.
- Every decorator that opens a non-message span already has the relevant typed data as its own method parameters — there is no point routing it through `Object` and an `instanceof` switch on the provider side.
- The simplification fits Constitution §I (Simplicity First) and §II (Minimal Impact — "three similar lines beats a premature abstraction").
- The `SpanNames` table still fixes the name strings, so the observable trace shape stays equivalent to AF4 (SC-003a).

**Snapshot is not a `Message` in AF5** — confirmed and accommodated. Snapshot creation runs against an entity stream identified by `tagKey` + tag value; there is no inbound `Message` to feed a provider. `TracingSnapshotter` opens the span via `createInternalSpan(...)` and attaches `axoniq.entity.type` / `axoniq.aggregate.identifier` directly from its decorator parameters. No new SPI method is required for this; AF4's `provideForSubject`-style hook would also have been unnecessary.

> **Worked sequence diagrams** for command dispatch, async event publication + handler-side cross-thread W3C propagation, and snapshot creation (with the explicit "providers do NOT fire here" annotation) live in [`flows.md`](./flows.md), together with a "When does my `SpanAttributesProvider` fire?" cheat sheet that summarises the table above.

**Alternatives considered**:
- **Keep `Span createInternalSpan(String, Object subject)` + `SpanAttributesProvider#provideForSubject(Object)`** — rejected (this clarification). Premature abstraction with no concrete callers in this feature; `Object`-typed parameter forces every provider to `instanceof`-switch; AF4 did not have it.
- **Re-introduce per-component span factories (`SnapshotterSpanFactory`, `RepositorySpanFactory`)** — explicitly forbidden by FR-016 and the consolidation goal of Story 4.
- **Pass typed records (e.g., `SnapshotSubject(String type, Object id)`)** — rejected; same `instanceof` ergonomics under a different name, and still no concrete need.

---

## 6. Default-Off Behavior + Backend Discovery

**Decision**:
- `axoniq-tracing-core` defaults to `NoOpSpanFactory.INSTANCE` if nothing is registered (FR-005). No SLF4J warnings, no OTel reflection — if the user doesn't add OpenTelemetry, they get the no-op.
- `axoniq-tracing-opentelemetry` does **not** auto-replace the no-op. The replacement happens in `OpenTelemetryTracingAutoConfiguration` (Spring) or in user code (`MessagingConfigurer.componentRegistry(cr -> cr.registerComponent(SpanFactory.class, c -> new OpenTelemetrySpanFactory(otel)))`).
- The Spring autoconfig is gated by `@ConditionalOnClass(io.opentelemetry.api.OpenTelemetry.class)` — if OTel isn't on the classpath, no OTel bean is created and the no-op default stays in place.

**Rationale**: Eliminates the "I depended on the core module and now I get spans I don't want" failure mode (SC-008). Makes the OpenTelemetry module a true opt-in.

---

## 7. AxonFramework 5 Cleanup Inventory (P9 Checklist)

After AxoniqFramework P1–P8 lands, the following dead code is deleted from `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5`. Paths are relative to that repo root.

**Messaging tracing core** (`messaging/src/main/java/org/axonframework/messaging/tracing/`)
- `SpanFactory.java`, `Span.java`, `SpanScope.java`, `SpanUtils.java`, `SpanAttributesProvider.java`, `HandlerSpanFactory.java`, `MultiSpanFactory.java`, `NoOpSpanFactory.java`, `TracingHandlerEnhancerDefinition.java`, `package-info.java`
- `attributes/` — all six provider classes + `package-info.java`

**Command-handling tracing** (`messaging/src/main/java/org/axonframework/messaging/commandhandling/tracing/`)
- `CommandBusSpanFactory.java`, `DefaultCommandBusSpanFactory.java`, `TracingCommandBus.java`, `QueryBusSpanFactory.java`, `DefaultQueryBusSpanFactory.java`, `QueryUpdateEmitterSpanFactory.java`, `DefaultQueryUpdateEmitterSpanFactory.java`, `package-info.java`
- *(yes — the file listing has query factories living under `commandhandling/tracing/`, a leftover from the relocation; they go too.)*

**Event-handling tracing** (`messaging/src/main/java/org/axonframework/messaging/eventhandling/tracing/`)
- `EventBusSpanFactory.java`, `DefaultEventBusSpanFactory.java`, `EventProcessorSpanFactory.java`, `DefaultEventProcessorSpanFactory.java`, `TracingEventHandlingComponent.java`, `package-info.java`

**Query-handling tracing** (`messaging/src/main/java/org/axonframework/messaging/queryhandling/tracing/`)
- `QueryBusSpanFactory.java`, `DefaultQueryBusSpanFactory.java`, `QueryUpdateEmitterSpanFactory.java`, `DefaultQueryUpdateEmitterSpanFactory.java`, `TracingQueryBus.java`, `package-info.java`

**Stash / todo** (`stash/todo/src/main/java/org/axonframework/tracing/`)
- `LoggingSpanFactory.java`, `package-info.java`

**Stash spring-boot autoconfig** (`stash/todo/src/main/java/org/axonframework/springboot/autoconfig/`)
- `AxonTracingAutoConfiguration.java`
- (`OpenTelemetryAutoConfiguration.java` if present — search at cleanup time)

**Extensions** (`extensions/tracing/`)
- The entire `extensions/tracing/` directory including `tracing-opentelemetry/` submodule.
- The `extensions/tracing` `<module>` entry in the upstream parent / aggregator pom.

**Tests** (all):
- `messaging/src/test/java/org/axonframework/messaging/tracing/*`
- `messaging/src/test/java/org/axonframework/messaging/{command,event,query}handling/tracing/*`
- `extensions/tracing/tracing-opentelemetry/src/test/**`

**Verification after deletion** (SC-007 / FR-029):
- `grep -r "org.axonframework.tracing\|org.axonframework.messaging.tracing\|extensions/tracing" .` returns **zero source matches**.
- `./mvnw clean verify` is green at the upstream AF5 repository root.

---

## 8. Open Items (None Block Phase 1)

- **OpenTelemetry BOM version**: pin to the latest stable line (1.x). Concrete version chosen at P1 implementation time; the BOM keeps API/SDK aligned.
- **`examples/` host app**: an empty `examples/` subtree exists at plan time. P7 picks whichever example app is present, or scaffolds a minimal one.
- **AxonServer connector availability in integration-tests**: SC-009 prefers a real AxonServer round-trip; if the test profile can't run AxonServer (no Docker, no Testcontainers), P6 falls back to an in-process two-bus simulation that still validates metadata-based propagation (FR-015).

None of the above require new public API decisions. They are surfaced in `tasks.md` as TODO comments on the relevant tasks, not as Phase-0 unknowns.
