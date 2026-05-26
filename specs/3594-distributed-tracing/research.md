# Phase 0 — Research

**Feature**: 3594 — Distributed Tracing Support

**Branch**: `feat/3594-DistributedTracing`

This file resolves every `NEEDS CLARIFICATION` carried into planning and records the key design decisions that drive the contracts in Phase 1. It is **not** a data model — there is no domain data here, only design rationale.

---

## 1. AF4 → AF5 Concern Mapping

The single most important design move is the collapse of AF4's nine per-component span-factory interfaces into one public `SpanFactory` plus private per-component **decorators**. The table below is the authoritative mapping the port works against.

| Concern (what is being traced) | AF4 — per-component factory + consumer | AF5 — what replaces it in AxoniqFramework |
|---|---|---|
| Command dispatch + handling | `CommandBusSpanFactory` (interface) + `DefaultCommandBusSpanFactory`, consumed by `SimpleCommandBus.Builder#spanFactory(...)` | `internal.TracingCommandBus implements CommandBus` registered via `DecoratorDefinition.forType(CommandBus.class)`. Span names: `"CommandBus.dispatchCommand <commandName>"` (kind `CLIENT` when distributed, `INTERNAL` otherwise) and `"CommandBus.handleCommand <commandName>"` (kind `SERVER`). |
| Event publication | `EventBusSpanFactory` + `DefaultEventBusSpanFactory`, consumed by `SimpleEventBus.Builder#spanFactory(...)`. AF4's `AbstractEventBus#publish` creates **two** spans: (a) `createPublishEventSpan(e)` per event around `propagateContext(...)` at publish-entry (`AbstractEventBus.java:121-122`), and (b) `createCommitEventsSpan()` spanning the surrounding UoW's `prepareCommit` → `commit` → `afterCommit` → `cleanup` phases (`AbstractEventBus.java:160-202`, plus the no-UoW path at `:144`). | `internal.TracingEventSink implements EventSink` registered via `DecoratorDefinition.forType(EventSink.class)`. Produces **two spans per call**, equivalent to AF4: (a) `"EventBus.publishEvent <eventName>"` (kind `PRODUCER`) — one per event, synchronously at publish-entry, around `SpanFactory#propagateContext(event)`, BEFORE delegating; (b) `"EventBus.commitEvents"` — one per `publish(...)` call, bound to the supplied `ProcessingContext` via `ProcessingContextSpanBinding` (opened in `runOnPrepareCommit`, errored in `onError`, closed in `whenComplete`). When `ProcessingContext` is `null`, the commit-events span falls back to a synchronous `Span.run(...)` wrapper around `delegate.publish(...)`. Both spans are creatable from the public `EventSink#publish(ProcessingContext, List<EventMessage>)` signature alone — no event-bus internals are touched. |
| Event handling (incl. async / pooled streaming) | `EventProcessorSpanFactory` + `DefaultEventProcessorSpanFactory`, consumed by `TrackingEventProcessor.Builder` / `PooledStreamingEventProcessor.Builder` | `internal.TracingEventHandlingComponent implements EventHandlingComponent` registered via `DecoratorDefinition.forType(EventHandlingComponent.class)`. Span name `"EventProcessor[<processor>].process <eventName>"` (kind `CONSUMER`), with the producer's W3C context extracted from metadata and linked as parent. |
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

A handful of operations (snapshot creation, repository load/save) are not `Message`s but still need spans. AF4 modelled this by giving each per-component factory bespoke methods like `createSaveAggregateSpan(Aggregate)`, etc.

**Decision**: The consolidated `SpanFactory` exposes **two** generic builders for the non-message case:

1. `Span createInternalSpan(String name)` — for purely internal operations with no associated message.
2. `Span createInternalSpan(String name, Object subject)` — same, but the `subject` (an aggregate id, an entity descriptor) is passed through to every `SpanAttributesProvider` so attribute keys remain pluggable.

The four built-in providers (aggregate id, message id, message name, payload type) inspect the `subject` reflectively (one short `instanceof` switch on the known internal subject types) so we don't need a parallel `SpanAttributesProvider` hierarchy.

**Rationale**: Two generic methods replace ~12 bespoke per-component span-creation methods from AF4 without losing fidelity. The internal `SpanNames` table fixes the name strings so the observable trace shape stays equivalent to AF4 (SC-003a).

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
