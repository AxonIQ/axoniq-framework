# Quickstart — Distributed Tracing in AxoniqFramework

**Feature**: 3594 — Distributed Tracing Support

Two routes — Spring Boot and plain Java. Both end with a working command → event → query trace visible in your OpenTelemetry exporter.

---

## Route A — Spring Boot (≤ 1 dependency + 1 property)

### 1. Add the starter

```xml
<dependency>
    <groupId>io.axoniq.framework</groupId>
    <artifactId>axoniq-tracing-opentelemetry</artifactId>
</dependency>

<!-- The Spring Boot autoconfig classes live in the existing axoniq-spring-boot-autoconfigure module,
     which you already pull in via axoniq-spring-boot-starter. -->
```

If you use the AxoniqFramework BOM, the version is pinned for you.

### 2. Add an OpenTelemetry exporter

Add whatever OTel exporter you use — OTLP, Jaeger, Zipkin, logging. Example with the OTLP exporter:

```xml
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-sdk</artifactId>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-otlp</artifactId>
</dependency>
<dependency>
    <groupId>io.opentelemetry.instrumentation</groupId>
    <artifactId>opentelemetry-spring-boot-starter</artifactId>
</dependency>
```

The OTel Spring Boot starter wires `OpenTelemetry` into the Spring context. AxoniqFramework's `OpenTelemetryTracingAutoConfiguration` picks it up automatically and registers an `OpenTelemetrySpanFactory`.

### 3. Configure your exporter

```yaml
# application.yml
otel:
  service:
    name: my-app
  exporter:
    otlp:
      endpoint: http://localhost:4317
```

### 4. Run

Start the app, dispatch a command, and look at your trace backend. You'll see a connected trace:

```
CommandBus.dispatchCommand RegisterStudent          [PRODUCER]
└─ CommandBus.handleCommand RegisterStudent         [CONSUMER]
   └─ Repository.load Student studentId-42
   └─ Repository.save Student studentId-42
   └─ EventBus.publishEvent StudentRegistered       [PRODUCER]
      └─ EventProcessor[studentProjection].process StudentRegistered  [CONSUMER]
         └─ QueryBus.query GetStudentById          [PRODUCER]
            └─ QueryBus.handle GetStudentById     [CONSUMER]
```

### 5. Customise (optional)

```yaml
axon:
  tracing:
    enabled: true                          # master switch (default true)
    components:
      query-updates: false                 # don't trace subscription-query updates
    snapshotter:
      separate-trace: true                 # snapshots get their own trace root
      aggregate-type-in-span-name: true
    attribute-providers:
      metadata: false                      # don't dump metadata into span attributes
```

To use your own `SpanFactory` or add a custom `SpanAttributesProvider`, register them as beans — they replace / extend the defaults:

```java
@Bean
public SpanFactory mySpanFactory() {
    return new MyCustomSpanFactory();      // wins over the OpenTelemetrySpanFactory default
}

@Bean
public SpanAttributesProvider tenantAttributesProvider() {
    return message -> Map.of("axon.tenant.id",
            message.getMetaData().getOrDefault("tenantId", "unknown").toString());
}
```

---

## Route B — Plain Java (no Spring) — ≤ 5 lines of configuration

### 1. Add the dependencies

```xml
<dependency>
    <groupId>io.axoniq.framework</groupId>
    <artifactId>axoniq-tracing-opentelemetry</artifactId>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-sdk</artifactId>
</dependency>
```

### 2. Wire it up

```java
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.TracingConfigurationEnhancer;
import io.axoniq.framework.tracing.opentelemetry.OpenTelemetrySpanFactory;

OpenTelemetry otel = /* your OTel SDK */ OpenTelemetrySdk.builder().build();
SpanFactory spanFactory = new OpenTelemetrySpanFactory(otel);

MessagingConfigurer configurer = MessagingConfigurer.create()
        .componentRegistry(cr -> cr
                .registerComponent(SpanFactory.class, c -> spanFactory)
                .registerEnhancer(new TracingConfigurationEnhancer())
        );

AxonConfiguration config = configurer.build();
config.start();
```

That's the whole thing. Five lines (six if you count the `registerEnhancer`).

### 3. Add custom attributes (optional)

```java
SpanAttributesProvider tenantProvider = message -> Map.of("axon.tenant.id",
        message.getMetaData().getOrDefault("tenantId", "unknown").toString());

MessagingConfigurer.create()
        .componentRegistry(cr -> cr
                .registerComponent(SpanFactory.class, c -> spanFactory)
                .registerComponent(SpanAttributesProvider.class, "tenant", c -> tenantProvider)
                .registerEnhancer(new TracingConfigurationEnhancer())
        );
```

`TracingConfigurationEnhancer.enhance()` discovers every `SpanAttributesProvider` registered in the `ComponentRegistry` and feeds them into the `SpanFactory` after construction.

---

## What gets traced out of the box

| Concern | Span name template | Span kind |
|---|---|---|
| Command dispatch | `CommandBus.dispatchCommand <commandName>` | `PRODUCER` (AF4 binding; `INTERNAL` for the in-process leg once distributed/in-process branching lands) |
| Command handling | `CommandBus.handleCommand <commandName>` | `CONSUMER` |
| Event publication (per event, around context propagation) | `EventBus.publishEvent <eventName>` | `PRODUCER` |
| Event commit (per `publish(...)` call, around UoW prepare/commit/after-commit phases) | `EventBus.commitEvents` | `INTERNAL` |
| Event handling | `EventProcessor[<name>].process <eventName>` | `CONSUMER` |
| Query dispatch | `QueryBus.query <queryName>` | `PRODUCER` |
| Query handling | `QueryBus.handle <queryName>` | `CONSUMER` |
| Query update emit | `QueryUpdateEmitter.emit <updateType>` | `PRODUCER` |
| Entity load | `Repository.load <entityType> <id>` | `INTERNAL` |
| Entity save | `Repository.save <entityType> <id>` | `INTERNAL` |
| Snapshot create | `Snapshotter.create <entityType> <id>` | `INTERNAL` |
| Snapshot read | `Snapshotter.read <entityType> <id>` | `INTERNAL` |
| Annotation handlers (`@EventSourcingHandler` etc.) | `<handler-method-name>` | `INTERNAL` |

Standard attributes attached by the default `SpanAttributesProvider`s:

- `axoniq.message.id`
- `axoniq.message.name`
- `axoniq.message.type` ∈ {`COMMAND`, `EVENT`, `QUERY`}
- `axoniq.message.payloadType`
- `axoniq.metadata.<allowlisted-key>` (each entry of `MetadataSpanAttributesProvider`'s allowlist)
- `axoniq.aggregate.identifier` — **conditional**. Sourced from `ProcessingContext.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)`. Present on traces flowing through a legacy aggregate-based event storage engine (e.g., `AggregateBasedJpaEventStorageEngine`); absent on DCB / entity-based operations (`@EventSourcedEntity(tagKey = ...)` + `@EventTag` model). Trace consumers MUST treat it as optional. (A future `EntityTagSpanAttributesProvider` for the DCB path is a deferred future improvement.)

---

## Disabling tracing

```yaml
axon:
  tracing:
    enabled: false
```

Or programmatically — simply don't register `TracingConfigurationEnhancer` and don't depend on `axoniq-tracing-opentelemetry`. The framework defaults to `NoOpSpanFactory` and produces no spans (FR-005 / SC-008).

---

## Cross-process propagation

Trace context propagates automatically through `MetaData`:

- **Outbound** (`CommandBus.dispatch`, `EventBus.publish`, `QueryBus.query`): `SpanFactory#propagateContext(message)` injects W3C `traceparent` / `tracestate` headers into the message metadata.
- **Inbound** (handler side, e.g., `TracingEventHandlingComponent`): the incoming `traceparent` / `tracestate` is extracted and used as the parent of the new handler span.

This works across the Axon Server connector and any other transport that preserves message metadata. No user configuration is required.

---

## Reference

- Full public-API contract: [contracts/public-api.md](./contracts/public-api.md)
- AF4 → AF5 concern mapping + decoration approach + ProcessingContext binding: [research.md](./research.md)
- Worked sequence diagrams (command, async event, snapshot — incl. cross-thread propagation): [flows.md](./flows.md)
- Why this shape: [spec.md](./spec.md)
