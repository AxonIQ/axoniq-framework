# Public API Contract — Distributed Tracing

**Feature**: 3594 — Distributed Tracing Support

**Modules**: `axoniq-tracing-api` (public SPI + decorator-authoring helpers), `axoniq-tracing-messaging` / `axoniq-tracing-modelling` / `axoniq-tracing-eventsourcing` (per-concern decorators — all `@Internal`, no public surface), `axoniq-tracing-opentelemetry`, `axoniq-spring-boot-autoconfigure` (new classes only). The public API below lives in **`axoniq-tracing-api`** except the OTel `SpanFactory` impl. See spec.md clarification 2026-05-26 (five-module structure).

This document is the authoritative description of the **public** API surface shipped by this feature. Anything not listed here is `@Internal` or package-private, and not subject to the framework's binary-compatibility promise.

The shape below is **the only acceptable public surface** — see SC-003. A CI architectural test (set up in P3) enumerates public types under `io.axoniq.framework.tracing` and fails the build if a `*BusSpanFactory` / `*ManagerSpanFactory` / `*ProcessorSpanFactory` / `*EmitterSpanFactory` / `RepositorySpanFactory` / `SagaManagerSpanFactory` / `SnapshotterSpanFactory` ever shows up. (`DeadlineManagerSpanFactory` is implicitly forbidden — deadlines and sagas / process-managers are out of scope per clarifications 2026-05-26.)

---

## Module 1 — `axoniq-tracing-api`

### 1.1 `SpanFactory` (interface)

```java
package io.axoniq.framework.tracing;

/**
 * The sole public abstraction for creating tracing spans in AxoniqFramework.
 * <p>
 * One {@code SpanFactory} is registered against {@link org.axonframework.configuration.MessagingConfigurer}
 * (or its {@code ComponentRegistry}); all messaging components are wrapped with tracing decorators
 * by the single {@link TracingConfigurationEnhancer}. There is intentionally no per-bus or
 * per-component {@code SpanFactory} interface — per-component span shapes (names, kinds, attributes,
 * cross-process metadata propagation) are implementation details of internal decorators registered
 * by the enhancer.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
// NB: AF5 `Message` is non-generic. NO ThreadLocal: parents resolve from message metadata
// (cross-boundary) and from the active span recorded on the passed ProcessingContext (in-process);
// never from Context.current(). See spec.md clarification 2026-05-27 (ProcessingContext-based active span).
public interface SpanFactory {

    /**
     * Creates a {@link Span} for an outbound (dispatch / producer) operation on the given {@link Message}.
     * Parent = the active span on {@code context} (in-process nesting) else a root. The {@code context}
     * (when non-null) is forwarded to every {@link SpanAttributesProvider}.
     */
    Span createDispatchSpan(String operationName, Message message, @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} for an inbound (handler / consumer) operation. Parent = the context propagated
     * in {@code message}'s metadata (cross-thread / cross-process) else the active span on {@code context}
     * else a root. Never reads a thread-bound current span.
     */
    Span createHandlerSpan(String operationName, Message message, @Nullable ProcessingContext context);

    /**
     * Like {@link #createHandlerSpan} but with an additional OTel {@code SpanLink} to {@code linkedMessage}'s
     * span context (clickable cross-trace navigation, not a parent-of). Implementations MUST extract the W3C
     * span context from {@code linkedMessage.metadata()} and attach it as a link; when none can be extracted
     * the span is still created without the link — never throws.
     */
    Span createLinkedHandlerSpan(String operationName, Message message, Message linkedMessage,
                                 @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} for an internal operation not directly tied to a {@link Message}. Parent = the
     * active span on {@code context} (so it nests under the operation that opened it, e.g. a handler span)
     * else a root. Non-message attributes are attached by the calling decorator via
     * {@link Span#addAttribute(String, String)}.
     */
    Span createInternalSpan(String operationName, @Nullable ProcessingContext context);

    /**
     * Creates a {@link Span} that always starts a new trace (a root), ignoring any active span when resolving
     * its own parent — for batch / snapshot boundaries on pooled threads that must not attach to a stale
     * parent. When {@code context} is non-null, starting the span still records it as that context's active
     * span so children nest under it.
     */
    Span createRootSpan(String operationName, @Nullable ProcessingContext context);

    /** Registers a {@link SpanAttributesProvider} that contributes attributes to every span this factory produces. */
    void registerAttributesProvider(SpanAttributesProvider provider);
}
```

> **Propagation moved to `Span`.** AF4's `SpanFactory.propagateContext(message)` injected the *thread-current* span — impossible without a ThreadLocal. It is therefore replaced by `Span.propagateContext(message)` (§1.2): a span injects **its own** context, with no ambient lookup. The dispatch decorator creates + starts the dispatch span, then calls `span.propagateContext(command)`.

### 1.2 `Span` (interface)

```java
package io.axoniq.framework.tracing;

/**
 * Represents one unit of traced work. A {@code Span} is opened ({@link #start()}) and closed via
 * {@link SpanScope#close()}.
 * <p>
 * NO ThreadLocal: when a span is created with a {@link ProcessingContext}, {@link #start()} records it as
 * that context's active span (so spans created next with that context nest under it) and restores the
 * previous active span on close — never via {@code makeCurrent()}. Framework code binds the span to the
 * context lifecycle through {@code ProcessingContextSpanBinding}; the {@code run*} helpers are the
 * imperative edge (no active-span tracking).
 */
public interface Span {

    /** Opens this span (recording it active on its creation {@link ProcessingContext}) and returns its {@link SpanScope}. */
    SpanScope start();

    /** Sets a key/value attribute on the span. */
    Span addAttribute(String key, String value);

    /** Records the given exception against the span and marks it as errored. The span is NOT closed. */
    Span recordException(Throwable t);

    /**
     * Returns a copy of {@code message} with THIS span's context injected into its metadata, so a remote /
     * asynchronous handler can continue the trace. Replaces the old factory-level {@code propagateContext}
     * (which needed a thread-current span); the span propagates itself. No-op factories return the input.
     */
    <M extends Message> M propagateContext(M message);

    /** Runs the given block inside the span (imperative-edge convenience; no active-span tracking). */
    void run(Runnable runnable);

    /** Runs the given block inside the span and returns the result. */
    <T> T runSupplier(Supplier<T> supplier);

    /** Runs the given async block inside the span; the span is closed when the future completes. */
    <T> CompletableFuture<T> runSupplierAsync(Supplier<CompletableFuture<T>> supplier);
}
```

### 1.3 `SpanScope` (interface)

```java
package io.axoniq.framework.tracing;

/**
 * The active-span scope returned by {@link Span#start()}. A {@code SpanScope} is held on the
 * {@link ProcessingContext} via a {@link ResourceKey} (never via {@code ThreadLocal}), and is closed when
 * the context completes.
 */
public interface SpanScope extends AutoCloseable {
    Span span();

    @Override
    void close();
}
```

### 1.4 `SpanAttributesProvider` (interface — pluggable SPI)

```java
package io.axoniq.framework.tracing;

/**
 * Contributes attributes to a {@link Span} based on the {@link Message} (or non-message {@code subject})
 * the span is created for. Multiple providers compose; the order in which they are invoked is unspecified.
 * <p>
 * The {@code context} parameter on {@link #provideForMessage(Message, ProcessingContext)} is {@code @Nullable}
 * because some span-creation points (e.g., an out-of-band snapshot trigger) have no
 * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext} available. Providers that don't need
 * the context simply ignore it. Providers that DO need it (e.g.,
 * {@link io.axoniq.framework.tracing.attributes.AggregateIdentifierSpanAttributesProvider}, which reads from
 * {@code LegacyResources.AGGREGATE_IDENTIFIER_KEY}) MUST handle {@code null} gracefully.
 * <p>
 * Implementations MUST NOT reference AF4-era types that have been removed from AxonFramework 5 (e.g.,
 * {@code DomainEventMessage}) — see constitution v2.1.0 §"Relationship to AxonFramework Upstream".
 */
@FunctionalInterface
public interface SpanAttributesProvider {

    Map<String, String> provideForMessage(Message<?> message, @Nullable ProcessingContext context);
}
```

### 1.5 `TracingConfigurationEnhancer` (the only public `ConfigurationEnhancer`)

```java
package io.axoniq.framework.tracing;

/**
 * The single {@link ConfigurationEnhancer} that wires tracing into AxoniqFramework. Registers
 * {@link DecoratorDefinition}s for every traced component type (command bus, event sink,
 * event handling component, query bus, query update emitter, repository, state manager,
 * snapshotter) and a {@link HandlerEnhancerDefinition} for
 * annotation-based handlers.
 * <p>
 * Programmatic usage:
 * <pre>{@code
 * MessagingConfigurer.create()
 *         .componentRegistry(cr -> cr
 *                 .registerComponent(SpanFactory.class, c -> new OpenTelemetrySpanFactory(otel))
 *                 .registerEnhancer(new TracingConfigurationEnhancer())
 *         );
 * }</pre>
 * Spring Boot users get this enhancer registered automatically by {@code TracingAutoConfiguration}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class TracingConfigurationEnhancer implements ConfigurationEnhancer {

    public TracingConfigurationEnhancer() { }

    @Override
    public void enhance(ComponentRegistry registry) { /* see research.md §2.2 */ }
}
```

### 1.6 Built-in `SpanFactory` implementations

```java
package io.axoniq.framework.tracing;

/** A {@link SpanFactory} that produces no spans. Default when no other factory is configured. */
public final class NoOpSpanFactory implements SpanFactory {
    public static final NoOpSpanFactory INSTANCE = new NoOpSpanFactory();
    private NoOpSpanFactory() { }
    /* … */
}

/** Composes multiple {@link SpanFactory} instances (e.g., OpenTelemetry + Logging for dev). */
public final class MultiSpanFactory implements SpanFactory {
    public MultiSpanFactory(List<SpanFactory> delegates) { /* … */ }
    /* … */
}

/** SLF4J-backed {@link SpanFactory} for debug / local development. */
public final class LoggingSpanFactory implements SpanFactory {
    public static final LoggingSpanFactory INSTANCE = new LoggingSpanFactory();
    private LoggingSpanFactory() { }
    /* … */
}
```

### 1.7 Built-in `SpanAttributesProvider`s — package `io.axoniq.framework.tracing.attributes`

Attribute keys follow **Option B** (see spec.md clarification 2026-05-27): OpenTelemetry-style dotted namespaces (lowercase, snake_case leaf segments), prefixed `axoniq.` to match the new `io.axoniq.framework` group. This modernizes the Axon Framework 4 flat-underscore keys (`axon_message_name`, …). The exact AF4 keys can be restored per provider via the key-override constructor (see below) — e.g. `new MessageNameSpanAttributesProvider("axon_message_name")`.

| Class | Default attribute key | AF4 key (restore via constructor) |
|---|---|---|
| `MessageIdSpanAttributesProvider` | `axoniq.message.id` | `axon_message_id` |
| `MessageNameSpanAttributesProvider` | `axoniq.message.name` (qualified name) | `axon_message_name` |
| `MessageTypeSpanAttributesProvider` | `axoniq.message.type` = `COMMAND` / `EVENT` / `QUERY` | `axon_message_type` |
| `PayloadTypeSpanAttributesProvider` | `axoniq.message.payload_type` | `axon_payload_type` |
| `MetadataSpanAttributesProvider` | `axoniq.metadata.<key>` (prefix overridable; optional allowlist) | `axon_metadata_` prefix |
| `AggregateIdentifierSpanAttributesProvider` | `axoniq.aggregate.identifier` — sourced from `LegacyResources.AGGREGATE_IDENTIFIER_KEY` on the `ProcessingContext`. Best-effort: present only when a legacy aggregate-based event storage engine populated the resource. Absent on DCB / entity-based operations. MUST NOT reference the removed `DomainEventMessage` type (constitution v2.1.0). | `axon_aggregate_identifier` |

Each is a `public final class` with a no-arg constructor (default key) **plus a constructor accepting the attribute key** (for `MetadataSpanAttributesProvider`: a `String prefix` and/or a `Set<String>` allowlist), so the key can be overridden — including restoring the AF4 key for drop-in dashboard compatibility.

**Future improvement (Option C, deferred — see spec.md clarification 2026-05-27):** map onto OpenTelemetry's official messaging *semantic conventions* (`messaging.message.id`, `messaging.message.conversation_id`, `messaging.operation.name`, …) where they exist, keeping `axoniq.*` only for Axon-specific attributes. Most standards-aligned, but the most divergent from AF4; can be added later without breaking changes (it is just different default keys behind the same providers).

### 1.8 `@Internal` types (NOT public surface; listed for reviewer reference)

Under `io.axoniq.framework.tracing.internal`:
- `TracingCommandBus`, `TracingEventSink`, `TracingEventHandlingComponent`, `TracingQueryBus`, `TracingQueryUpdateEmitter`, `TracingRepository`, `TracingStateManager`, `TracingSnapshotStore` (decorates the real AF5 `SnapshotStore` — AF5 has no `Snapshotter` component; see `af4-span-inventory.md` §1.8)
- `TracingHandlerEnhancerDefinition`
- `SpanNames` (constants table: `CMD_DISPATCH = "CommandBus.dispatchCommand"`, `EVT_PUBLISH = "EventBus.publishEvent"`, `EVT_COMMIT = "EventBus.commitEvents"`, …)
- `ProcessingContextSpanBinding`
- `TracingOrders` (decorator-order constants)

---

## Module 2 — `axoniq-tracing-opentelemetry`

### 2.1 `OpenTelemetrySpanFactory` (only public class)

```java
package io.axoniq.framework.tracing.opentelemetry;

/**
 * {@link SpanFactory} backed by OpenTelemetry's {@code Tracer} and {@code TextMapPropagator}.
 * <p>
 * Construction:
 * <pre>{@code
 * OpenTelemetry otel = /* injected or global */;
 * SpanFactory factory = new OpenTelemetrySpanFactory(otel);
 * MessagingConfigurer.create()
 *         .componentRegistry(cr -> cr.registerComponent(SpanFactory.class, c -> factory)
 *                                    .registerEnhancer(new TracingConfigurationEnhancer()));
 * }</pre>
 *
 * @author AxonIQ
 * @since 5.2.0
 */
public final class OpenTelemetrySpanFactory implements SpanFactory {

    public OpenTelemetrySpanFactory(OpenTelemetry openTelemetry) { /* … */ }

    /** Convenience constructor that uses {@link GlobalOpenTelemetry#get()}. */
    public OpenTelemetrySpanFactory() { /* … */ }

    /* … */
}
```

`OpenTelemetrySpan`, `MetadataContextSetter`, `MetadataContextGetter` are `@Internal` (NOT public).

---

## Module 3 — `axoniq-spring-boot-autoconfigure` (new classes only)

### 3.1 `TracingProperties`

```java
package io.axoniq.framework.springboot;

@ConfigurationProperties(prefix = "axon.tracing")
public class TracingProperties {

    /** Master switch. Default {@code true}. */
    private boolean enabled = true;

    private final ComponentToggles components = new ComponentToggles();
    private final SnapshotterOptions snapshotter = new SnapshotterOptions();
    private final EventProcessorOptions eventProcessor = new EventProcessorOptions();
    private final CommandBusOptions commandBus = new CommandBusOptions();
    private final QueryBusOptions queryBus = new QueryBusOptions();
    private final RepositoryOptions repository = new RepositoryOptions();
    private final AttributeProviderToggles attributeProviders = new AttributeProviderToggles();
    /* getters / setters */

    public static class ComponentToggles {
        private boolean commands = true;
        private boolean events = true;
        private boolean queries = true;
        private boolean queryUpdates = true;
        private boolean snapshotting = true;
        private boolean repository = true;
        /* getters / setters */
    }

    public static class SnapshotterOptions {
        private boolean separateTrace = false;
        private boolean aggregateTypeInSpanName = false;
        /* getters / setters */
    }

    public static class EventProcessorOptions {
        private boolean disableBatchTrace = false;
        private boolean distributedInSameTrace = true;
        private Duration distributedInSameTraceTimeLimit = Duration.ofMinutes(2);
        /* getters / setters */
    }

    public static class CommandBusOptions {
        private boolean distributedInSameTrace = true;
        /* getters / setters */
    }

    public static class QueryBusOptions {
        private boolean distributedInSameTrace = true;
        /* getters / setters */
    }

    public static class RepositoryOptions {
        private String aggregateIdAttributeName = "axon.aggregate.identifier";
        /* getters / setters */
    }

    public static class AttributeProviderToggles {
        private boolean messageId = true;
        private boolean messageName = true;
        private boolean messageType = true;
        private boolean payloadType = true;
        private boolean metadata = true;
        private boolean aggregateIdentifier = true;
        /* getters / setters */
    }
}
```

### 3.2 `TracingAutoConfiguration`

```java
package io.axoniq.framework.springboot.autoconfig;

@AutoConfiguration
@ConditionalOnProperty(prefix = "axon.tracing", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(TracingProperties.class)
public class TracingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(SpanFactory.class)
    public SpanFactory spanFactory() {
        return NoOpSpanFactory.INSTANCE;
    }

    @Bean
    public TracingConfigurationEnhancer tracingConfigurationEnhancer() {
        return new TracingConfigurationEnhancer();
    }

    @Bean @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "message-id", havingValue = "true", matchIfMissing = true)
    public MessageIdSpanAttributesProvider messageIdSpanAttributesProvider() { return new MessageIdSpanAttributesProvider(); }

    @Bean @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "message-name", havingValue = "true", matchIfMissing = true)
    public MessageNameSpanAttributesProvider messageNameSpanAttributesProvider() { return new MessageNameSpanAttributesProvider(); }

    @Bean @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "message-type", havingValue = "true", matchIfMissing = true)
    public MessageTypeSpanAttributesProvider messageTypeSpanAttributesProvider() { return new MessageTypeSpanAttributesProvider(); }

    @Bean @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "payload-type", havingValue = "true", matchIfMissing = true)
    public PayloadTypeSpanAttributesProvider payloadTypeSpanAttributesProvider() { return new PayloadTypeSpanAttributesProvider(); }

    @Bean @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "metadata", havingValue = "true", matchIfMissing = true)
    public MetadataSpanAttributesProvider metadataSpanAttributesProvider() { return new MetadataSpanAttributesProvider(); }

    @Bean @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "aggregate-identifier", havingValue = "true", matchIfMissing = true)
    public AggregateIdentifierSpanAttributesProvider aggregateIdentifierSpanAttributesProvider() {
        return new AggregateIdentifierSpanAttributesProvider();
    }

    @Bean
    public SpanAttributesProviderRegistrar spanAttributesProviderRegistrar(SpanFactory spanFactory,
                                                                          ObjectProvider<SpanAttributesProvider> providers) {
        return new SpanAttributesProviderRegistrar(spanFactory, providers.orderedStream().toList());
    }
}
```

`SpanAttributesProviderRegistrar` is a tiny `@Internal` Spring component that loops over the discovered providers and calls `spanFactory.registerAttributesProvider(...)` on startup.

### 3.3 `OpenTelemetryTracingAutoConfiguration`

```java
package io.axoniq.framework.springboot.autoconfig;

@AutoConfiguration(before = TracingAutoConfiguration.class)
@ConditionalOnClass(io.opentelemetry.api.OpenTelemetry.class)
@ConditionalOnProperty(prefix = "axon.tracing", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OpenTelemetryTracingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(SpanFactory.class)
    public SpanFactory openTelemetrySpanFactory(ObjectProvider<OpenTelemetry> openTelemetry) {
        OpenTelemetry otel = openTelemetry.getIfAvailable(GlobalOpenTelemetry::get);
        return new OpenTelemetrySpanFactory(otel);
    }
}
```

### 3.4 Registration in `AutoConfiguration.imports`

Two new lines appended to `dependency-injection/spring/spring-boot-autoconfigure/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`:
```
io.axoniq.framework.springboot.autoconfig.TracingAutoConfiguration
io.axoniq.framework.springboot.autoconfig.OpenTelemetryTracingAutoConfiguration
```

---

## Architectural Test (P3, runs on every build)

A unit test under `axoniq-tracing-api/src/test/java/io/axoniq/framework/tracing/PublicApiSurfaceTest.java` enumerates classes via classpath scanning and asserts:

1. `io.axoniq.framework.tracing.*` contains EXACTLY `SpanFactory`, `Span`, `SpanScope`, `SpanAttributesProvider`, `NoOpSpanFactory`, `MultiSpanFactory`, `LoggingSpanFactory`, `TracingConfigurationEnhancer`.
2. No public type in any sub-package matches the forbidden patterns `*BusSpanFactory`, `*ManagerSpanFactory`, `*ProcessorSpanFactory`, `*EmitterSpanFactory`, `RepositorySpanFactory`, `SagaManagerSpanFactory`, `SnapshotterSpanFactory`. (`DeadlineManagerSpanFactory` is also implicitly forbidden — both deadlines and sagas / process-managers are out of scope.)
3. Every class under `io.axoniq.framework.tracing.internal` is either package-private OR annotated with `@Internal`.

This is the executable form of SC-003 / FR-016.
