# Implementation Plan: Distributed Tracing Support

**Branch**: `feat/3594-DistributedTracing` | **Date**: 2026-05-26 | **Spec**: [spec.md](./spec.md)

**GitHub Issue**: [AxonIQ/AxonFramework#3594](https://github.com/AxonIQ/AxonFramework/issues/3594)

## Summary

Port Axon Framework 4's distributed-tracing capability (the only working implementation today) into AxoniqFramework, re-shaped around Axon Framework 5 idioms. The new code lives in **five** new flat-sibling Maven modules under a new top-level `tracing/` directory — `axoniq-tracing-api` (SPI + public decorator-authoring helpers, depends only on `axon-messaging`), `axoniq-tracing-messaging`, `axoniq-tracing-modelling`, `axoniq-tracing-eventsourcing` (the per-concern decorators, mirroring AF5's `axon-messaging`/`axon-modelling`/`axon-eventsourcing` artifact split), and `axoniq-tracing-opentelemetry` (the OTel binding) — and additional Spring Boot auto-configuration is added to the existing `dependency-injection/spring/spring-boot-autoconfigure` module (no new spring-tracing module). The split lets a consumer take tracing for exactly the AF5 modules it uses, and lets external modules / repos add their own tracing by depending on `axoniq-tracing-api` + registering a ServiceLoader-discovered `ConfigurationEnhancer` (see spec.md clarification 2026-05-26).

The migration **consolidates** the AF4 per-component span-factory interface family (`CommandBusSpanFactory`, `EventBusSpanFactory`, …, `SagaManagerSpanFactory`) into a single public `SpanFactory`. All per-component span shapes — naming, kind (`CLIENT`/`SERVER`/`PRODUCER`/`CONSUMER`/`INTERNAL`), attribute keys, distributed-vs-in-process branching, and metadata-based W3C context propagation — live inside **private** (`@Internal` or package-private) delegating wrapper classes registered via **per-module `ConfigurationEnhancer`s** (one per per-concern tracing module, each ServiceLoader-discovered) using `DecoratorDefinition`s. Each wrapper follows AF5's `TracingCommandBus` shape: implements the target interface, holds a `delegate` field of the same interface plus a `SpanFactory`, calls through to the delegate, and exposes itself to introspection via `ComponentDescriptor.describeWrapperOf(delegate)`. Where the wrapped operation already participates in a `ProcessingContext`, span open/close rides on the `ProcessingLifecycle` hooks rather than `try/finally` so span scope tracks the framework's processing phases correctly through async / reactive continuations.

**Implementation strategy (revised 2026-05-26 — see spec.md FR-022b)**: After landing core abstractions + OpenTelemetry implementation + autoconfig skeleton up-front, the per-component decorators are built as **six sequential end-to-end vertical slices** (CommandBus → EventSink/EventHandling → QueryBus → QueryUpdateEmitter → Repository/StateManager → SnapshotStore), each landing its decorator + handler-enhancer extension + autoconfig toggle + focused Boot integration test + explicit human-validation checkpoint before the next slice begins. Cross-component end-to-end testing, Jaeger testcontainers verification, the example application, and reference-guide documentation are produced as a batched final phase. This replaces the original plan's single "P3 — all decorators in one phase" with the per-component sequencing the user requested, without removing any work originally scoped.

After the AxoniqFramework migration lands, all dead tracing source in the upstream Axon Framework 5 repository is deleted (a checklist of ~60 files / one full extension module / one stash sub-tree).

## Technical Context

**Language/Version**: Java 21 (sealed classes, records, pattern matching allowed; raw types and wildcard imports forbidden — Constitution §III).

**Primary Dependencies**:
- Upstream `org.axonframework:axon-messaging:5.2.0-SNAPSHOT` (consumed via `axoniq-framework-bom`, pinned in `build/parent/pom.xml`).
- `io.opentelemetry:opentelemetry-api` — added to `build/parent/pom.xml` dependencyManagement (version pinned by `opentelemetry-bom`). The `-api` artifact only; the SDK is a user-side concern (host application supplies the exporter).
- `io.opentelemetry:opentelemetry-sdk` + `io.opentelemetry:opentelemetry-exporter-otlp` — test-scope dependencies on the integration-tests / Jaeger testcontainers run only.
- `org.testcontainers:testcontainers` + `jaegertracing/all-in-one` image — test-scope dependency on the Jaeger integration test only (FR-023a).
- JSpecify (already present at parent level) for `@NullMarked` / `@Nullable`.
- Spring Boot 3.5.x (autoconfig only).

**Storage**: N/A — tracing is in-memory, side-effect-only.

**Testing**: JUnit 5 + AssertJ + Awaitility (Constitution §Testing Standards). Mocks only where behaviour-based verification of a decorator demands it (e.g., `verify(delegate, never())` to prove a disabled component is not wrapped). Two-tier integration testing: (1) per-slice focused integration tests + a cross-component E2E test (`TracingEndToEndIntegrationTest`) use OpenTelemetry SDK with `InMemorySpanExporter` (fast, deterministic, no Docker); (2) a Jaeger testcontainers integration test (`TracingJaegerIntegrationTest`) runs in the `integration-test` Maven profile against `jaegertracing/all-in-one` with OTLP receiver, skipped automatically when Docker is unavailable.

**Target Platform**: JVM 21+. The core module is Spring-free; OpenTelemetry module is Spring-free; only the autoconfig glue depends on Spring Boot.

**Project Type**: Multi-module Maven library — adds five new leaf modules (under `tracing/`) and autoconfig classes to an existing module.

**Performance Goals**: No measurable overhead when tracing is disabled (no-op factory). No microbenchmark target is set; the no-op call sites must be trivially inlinable (single method dispatch returning a shared no-op span). When enabled, the overhead is dominated by the OpenTelemetry SDK itself and is out of scope for this feature.

**Constraints**:
- No `ThreadLocal` in framework code (Constitution §V). Active-span tracking goes through `ProcessingContext` resources keyed by `ResourceKey<SpanScope>`.
- No public `*BusSpanFactory` / `*ManagerSpanFactory` / `*ProcessorSpanFactory` / `*EmitterSpanFactory` / `RepositorySpanFactory` / `SagaManagerSpanFactory` / `SnapshotterSpanFactory` interfaces (FR-016). Deadline and saga / process-manager tracing are permanently out of scope (clarifications 2026-05-26 — neither component exists in AF5).
- **No reference to `DomainEventMessage` or any other AF4-era upstream type removed from AxonFramework 5** (constitution v2.1.0). Legacy data those types carried — primarily the aggregate identifier — is sourced from `ProcessingContext` resources exposed via `org.axonframework.messaging.core.LegacyResources` (`AGGREGATE_IDENTIFIER_KEY`, `AGGREGATE_TYPE_KEY`, `AGGREGATE_SEQUENCE_NUMBER_KEY`). This drives the `SpanAttributesProvider` SPI shape: `Map<String, String> provideForMessage(Message<?> message, @Nullable ProcessingContext context)`.
- Wrappers MUST be delegating decorators that implement the target component interface by composition and call `describeWrapperOf(delegate)` (FR-013).
- Cross-process trace context propagates through message metadata under reserved keys; collision behaviour with user metadata is documented.
- AF5 tracing source is dead code (commented-out, no wiring) — never used as a starting point. Code is read from AF4 and re-implemented for AF5 idioms.
- **Implementation MUST proceed component-by-component as vertical end-to-end slices** in the order given by FR-022b. Each slice ends with an explicit human-validation checkpoint; the next slice does not start until the user has accepted the previous slice's span tree. One feature branch, one rollup PR.

**Scale/Scope**:
- 5 new Maven modules (`tracing/axoniq-tracing-api`, `tracing/axoniq-tracing-messaging`, `tracing/axoniq-tracing-modelling`, `tracing/axoniq-tracing-eventsourcing`, `tracing/axoniq-tracing-opentelemetry`).
- 1 new autoconfig class + 1 new `TracingProperties` class + 1 new entry in `AutoConfiguration.imports` in the existing `axoniq-spring-boot-autoconfigure` module.
- ~7 component-tracing decorators (commands, events sink, event handling component, query bus, query update emitter, repository/state, snapshotter), plus the annotation `HandlerEnhancerDefinition` wrapper. Deadlines and sagas / process-managers are excluded — see clarifications 2026-05-26.
- ~6 built-in `SpanAttributesProvider` implementations (parity with AF4).
- 6 focused per-slice Boot integration tests + 1 cross-component E2E test (`InMemorySpanExporter`) + 1 propagation test + 1 AF4 span-shape parity test + 1 Jaeger testcontainers test.
- 1 reference-guide page populated under `docs/reference-guide/modules/tracing/pages/`.
- 1 cleanup PR (or PR set) against the upstream Axon Framework 5 repository deleting all dead tracing source there.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Mapped against the **Foundation Principles** and **Architecture Rules** in `.specify/memory/constitution.md` (v2.1.0).

| Principle / Rule | Status | Note |
|---|---|---|
| I. Simplicity First | PASS | One `SpanFactory` (vs. 9 in AF4). Decorators only, no new interceptor level. Component-by-component slicing reduces in-flight complexity per task. |
| II. Minimal Impact | PASS | No upstream AxonFramework changes needed beyond consuming existing extension points (`DecoratorDefinition`, `ConfigurationEnhancer`, `ProcessingLifecycle`). No edits to `messaging/`, `connector/`, `dependency-injection/spring/spring-boot-starter`. |
| III. Java 21 Baseline | PASS | Sealed-class option used where helpful (e.g., `Span.Kind`). Records used for trace-context carriers. No wildcard imports. |
| Relationship to AxonFramework Upstream (v2.1.0) | PASS | No reference to `DomainEventMessage` or other AF4-removed types. `AggregateIdentifierSpanAttributesProvider` sources its value from `ProcessingContext.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)`. SPI signature carries `@Nullable ProcessingContext`. |
| IV. Dual Paradigm Support | PASS | Span open/close binds to `ProcessingLifecycle` hooks; works for both imperative and reactive call paths because hooks fire on the right phases regardless of paradigm. |
| V. No ThreadLocals | PASS | **Zero ThreadLocal.** The active span is the OpenTelemetry `Context` stored as a `ProcessingContext` resource; parents resolve from that resource (in-process) or from message metadata (cross-boundary). `Context.current()` / `Context.makeCurrent()` are **never** called — not even at the imperative edge (`Span.start()` records the active context on the `ProcessingContext`, not a thread-local). This is the OpenTelemetry-recommended explicit-`Context` pattern for reactive code (maintainer confirmation + sources in spec.md clarification 2026-05-27). The `io.opentelemetry.context.Context` value object is used as an explicit carrier (that is not a ThreadLocal). |
| VI. Composition over Inheritance | PASS | All decorators implement target interface by composition; AF4's `DistributedCommandBus`-style direct re-implementation is explicitly rejected (FR-013). |
| VII. Declarative over Annotation-Heavy | PASS | The per-module `ConfigurationEnhancer`s are ServiceLoader-discovered (drop the module on the classpath → tracing wires itself), so no explicit registration call is needed; Spring auto-configuration is a thin wrapper supplying the `SpanFactory` bean + properties. |
| Module Dependency Hierarchy | PASS | New `tracing/` modules sit beside `messaging/`, `connector/`, `dependency-injection/`, `testing/`. Tracing depends on `axon-messaging` (upstream) but **not** on other AxoniqFramework modules; it has no reverse dependency from messaging back to tracing. |
| Async-First | PASS | Decorators preserve `CompletableFuture<…>` / `MessageStream<…>` return types from the wrapped methods unchanged. |
| ProcessingContext as Unit of Work | PASS | `SpanScope` is stored via `ResourceKey<SpanScope>`. |
| Interceptor Levels | PASS | Tracing does NOT introduce a new interceptor level. It plugs into the established `HandlerEnhancerDefinition` (for annotation handlers) and `DecoratorDefinition` (for component wiring). |
| Annotation Processing Pipeline | PASS | `TracingHandlerEnhancerDefinition` participates in step 3 of the inspector → definition → enhancer chain, grown incrementally across slices. |
| CompletableFuture Blocking Rule | PASS | No new blocking points are introduced. Test code that has to wait on exported spans uses Awaitility, not raw `join()`. |
| Type Safety (`TypeReference` over raw `Class`) | N/A | Tracing does not pass `Type` to the converter. |

**Conclusion**: Constitution Check passes with no violations. The component-by-component slice restructure introduced by clarifications 2026-05-26 does not change any constitutional gate — it changes only the sequencing of work, not the shape of the produced code. **Complexity Tracking** section is therefore empty.

## Project Structure

### Documentation (this feature)

```text
specs/3594-distributed-tracing/
├── spec.md                                  # Existing — feature specification
├── plan.md                                  # This file
├── research.md                              # Phase 0 — AF4→AF5 concern mapping, decoration design, ProcessingContext lifecycle binding, propagation
├── research-batch-tracing.md                # Phase 0 supplement — batch-tracing design (Option C), AF4 hierarchy, rejected alternatives, subscribing-processor parity
├── af4-span-inventory.md                    # Phase 0 supplement — exhaustive AF4 SpanFactory ↔ AF5 decorator mapping (44 methods across 9 families), implementation reference
├── contracts/
│   └── public-api.md                        # Phase 1 — Public API of axoniq-tracing-api + axoniq-tracing-opentelemetry + autoconfig
├── quickstart.md                            # Phase 1 — Spring Boot + plain Java getting-started
├── flows.md                                 # Phase 1 — Worked sequence diagrams (mermaid) for command / async event / snapshot / streaming-batch flows + "when does SpanAttributesProvider fire?" cheat sheet
└── tasks.md                                 # Phase 2 — /speckit-tasks output (NOT created by this command)
```

Per the user's instruction, **no `data-model.md`** is generated for this feature — tracing has no domain data model.

### Source Code (repository root)

```text
# NEW: top-level tracing/ directory — five flat siblings mirroring AF5's axon-messaging / -modelling / -eventsourcing split.
# See spec.md clarification 2026-05-26 (five-module structure) and FR-017/FR-018.
tracing/
├── axoniq-tracing-api/                      # SPI + public decorator-authoring helpers; depends ONLY on axon-messaging
│   ├── pom.xml                              # artifactId: axoniq-tracing-api, parent: axoniq-framework-parent
│   └── src/
│       ├── main/java/io/axoniq/framework/tracing/
│       │   ├── package-info.java            # @NullMarked
│       │   ├── SpanFactory.java             # PUBLIC — sole public span-creation API
│       │   ├── Span.java                    # PUBLIC — span lifecycle (start/end/error/attribute/run)
│       │   ├── SpanScope.java               # PUBLIC — active-span scope, stored under ResourceKey<SpanScope>
│       │   ├── SpanAttributesProvider.java  # PUBLIC — SPI for contributing attributes
│       │   ├── NoOpSpanFactory.java         # PUBLIC — default when no factory is configured
│       │   ├── MultiSpanFactory.java        # PUBLIC — composes multiple SpanFactories (e.g., OTel + LoggingSpanFactory)
│       │   ├── LoggingSpanFactory.java      # PUBLIC — SLF4J implementation for dev/debug
│       │   ├── ProcessingContextSpanBinding.java  # PUBLIC helper (DFI-001) — binds Span open/close to ProcessingLifecycle hooks
│       │   ├── SpanNames.java               # PUBLIC helper (DFI-001) — span-name conventions shared by built-in + external decorators
│       │   ├── MetadataContextPropagator.java     # PUBLIC SPI (DFI-001) — W3C metadata getter/setter contract (OTel binding in -opentelemetry)
│       │   └── attributes/
│       │       ├── package-info.java
│       │       ├── MessageIdSpanAttributesProvider.java
│       │       ├── MessageNameSpanAttributesProvider.java
│       │       ├── MessageTypeSpanAttributesProvider.java
│       │       ├── PayloadTypeSpanAttributesProvider.java
│       │       ├── MetadataSpanAttributesProvider.java
│       │       └── AggregateIdentifierSpanAttributesProvider.java
│       └── test/java/io/axoniq/framework/tracing/
│           ├── NoOpSpanFactoryTest.java
│           ├── MultiSpanFactoryTest.java
│           ├── attributes/                  # one test per provider
│           └── support/
│               └── TestSpanFactory.java     # recording test double (no Mockito); ports from AF4's TestSpanFactory
│
├── axoniq-tracing-messaging/                # decorators for axon-messaging components; depends on -api + axon-messaging
│   ├── pom.xml                              # artifactId: axoniq-tracing-messaging
│   └── src/
│       ├── main/
│       │   ├── java/io/axoniq/framework/tracing/messaging/
│       │   │   ├── package-info.java        # @NullMarked
│       │   │   ├── MessagingTracingConfigurationEnhancer.java  # @Internal — registers the DecoratorDefinitions below
│       │   │   └── internal/                # @Internal/package-private decorators; populated slice-by-slice
│       │   │       ├── TracingCommandBus.java                  # Slice 1
│       │   │       ├── TracingEventSink.java                   # Slice 2 (dispatch side)
│       │   │       ├── TracingEventHandlingComponent.java      # Slice 2 (handling side)
│       │   │       ├── TracingQueryBus.java                    # Slice 3
│       │   │       ├── TracingQueryUpdateEmitter.java          # Slice 4
│       │   │       └── TracingHandlerEnhancerDefinition.java   # @CommandHandler (S1) / @EventHandler (S2) / @QueryHandler (S3)
│       │   └── resources/META-INF/services/
│       │       └── org.axonframework.common.configuration.ConfigurationEnhancer  # → MessagingTracingConfigurationEnhancer
│       └── test/java/io/axoniq/framework/tracing/messaging/    # one test per decorator (added with its slice)
│
├── axoniq-tracing-modelling/                # decorators for axon-modelling components; depends on -api + axon-modelling
│   ├── pom.xml                              # artifactId: axoniq-tracing-modelling
│   └── src/
│       ├── main/
│       │   ├── java/io/axoniq/framework/tracing/modelling/
│       │   │   ├── package-info.java
│       │   │   ├── ModellingTracingConfigurationEnhancer.java  # @Internal
│       │   │   └── internal/
│       │   │       ├── TracingRepository.java                  # Slice 5
│       │   │       └── TracingStateManager.java                # Slice 5
│       │   └── resources/META-INF/services/
│       │       └── org.axonframework.common.configuration.ConfigurationEnhancer  # → ModellingTracingConfigurationEnhancer
│       └── test/java/io/axoniq/framework/tracing/modelling/
│
├── axoniq-tracing-eventsourcing/            # decorators for axon-eventsourcing components; depends on -api + axon-eventsourcing
│   ├── pom.xml                              # artifactId: axoniq-tracing-eventsourcing
│   └── src/
│       ├── main/
│       │   ├── java/io/axoniq/framework/tracing/eventsourcing/
│       │   │   ├── package-info.java
│       │   │   ├── EventSourcingTracingConfigurationEnhancer.java  # @Internal
│       │   │   └── internal/
│       │   │       ├── TracingSnapshotStore.java               # Slice 6 (decorates AF5 SnapshotStore; no Snapshotter component exists)
│       │   │       ├── TracingEntityLifecycleHandler.java       # Slice 6 — source(...) sourcing span (FR-009 parent for snapshot store span)
│       │   │       └── EventSourcingHandlerTracingEnhancer.java # Slice 6 — extends handler-enhancer coverage to @EventSourcingHandler
│       │   └── resources/META-INF/services/
│       │       └── org.axonframework.common.configuration.ConfigurationEnhancer  # → EventSourcingTracingConfigurationEnhancer
│       └── test/java/io/axoniq/framework/tracing/eventsourcing/
│
└── axoniq-tracing-opentelemetry/            # OTel binding; depends on -api + io.opentelemetry:opentelemetry-api
    ├── pom.xml                              # artifactId: axoniq-tracing-opentelemetry; depends on axoniq-tracing-api + opentelemetry-api
    └── src/
        ├── main/java/io/axoniq/framework/tracing/opentelemetry/
        │   ├── package-info.java            # @NullMarked
        │   ├── OpenTelemetrySpanFactory.java        # PUBLIC — OTel implementation of SpanFactory
        │   ├── OpenTelemetrySpan.java               # @Internal
        │   ├── MetadataContextSetter.java           # @Internal — W3C TextMapSetter writing trace context into Message metadata
        │   └── MetadataContextGetter.java           # @Internal — W3C TextMapGetter reading trace context from Message metadata
        └── test/java/io/axoniq/framework/tracing/opentelemetry/
            ├── OpenTelemetrySpanFactoryTest.java
            ├── OpenTelemetrySpanTest.java
            ├── MetadataContextSetterTest.java
            └── MetadataContextGetterTest.java

# EDITED: existing module — autoconfig only, no new module
dependency-injection/spring/spring-boot-autoconfigure/
└── src/main/
    ├── java/io/axoniq/framework/springboot/autoconfig/
    │   ├── TracingAutoConfiguration.java                # NEW — wires SpanFactory bean + built-in providers (enhancers are ServiceLoader-discovered, not bean-wired)
    │   └── OpenTelemetryTracingAutoConfiguration.java   # NEW — promotes OpenTelemetrySpanFactory over NoOp when OTel is on the classpath
    ├── java/io/axoniq/framework/springboot/
    │   └── TracingProperties.java                       # NEW — @ConfigurationProperties("axon.tracing"); per-slice toggles added incrementally
    └── resources/META-INF/spring/
        └── org.springframework.boot.autoconfigure.AutoConfiguration.imports   # APPEND the two new classes

# EDITED: existing module — pom only, add OpenTelemetry BOM
build/parent/pom.xml
                                            # ADD <opentelemetry.version> property
                                            # ADD io.opentelemetry:opentelemetry-bom import to dependencyManagement
                                            # (the five new modules are registered in the root aggregator pom.xml, not here)

# EDITED: root aggregator
pom.xml                                     # ADD the five <module>tracing/axoniq-tracing-*</module> entries
                                            #   (api, messaging, modelling, eventsourcing, opentelemetry)

# EDITED: existing BOM
axoniq-framework-bom/pom.xml                # ADD all five axoniq-tracing-* artifacts as dependencyManagement entries

# EDITED: docs — module directory already exists at docs/reference-guide/modules/tracing/
# (Produced in P9.7 — the batched final phase — to avoid rewriting the page six times)
docs/reference-guide/modules/tracing/
├── nav.adoc                                # update navigation
└── pages/
    ├── index.adoc                          # NEW — tracing overview, adapted from AF4 docs/old-reference-guide/.../tracing.adoc
    ├── opentelemetry.adoc                  # NEW — OpenTelemetry setup, exporters, propagation
    ├── customisation.adoc                  # NEW — custom SpanFactory, custom SpanAttributesProvider, per-component opt-outs
    └── examples.adoc                       # NEW — Spring Boot + plain Java worked examples

# EDITED: example app (P9.5 — batched final phase; only if examples/ subtree exists at implementation time)
examples/<chosen-example>/                  # add a dependency on axoniq-tracing-opentelemetry + OTel exporter; show a working trace flow

# EDITED: integration tests — single existing pom; per-slice focused tests + batched final E2E / parity / propagation / Jaeger tests
integrationtests/src/test/java/io/axoniq/framework/tracing/
├── slice/                                                     # per-slice focused Boot integration tests (one per slice; grow across P3–P8)
│   ├── SliceCommandBusTracingIntegrationTest.java             # P3
│   ├── SliceEventTracingIntegrationTest.java                  # P4 (covers EventSink + EventHandlingComponent, both PSEP and SEP variants)
│   ├── SliceQueryBusTracingIntegrationTest.java               # P5
│   ├── SliceQueryUpdateEmitterTracingIntegrationTest.java     # P6
│   ├── SliceRepositoryTracingIntegrationTest.java             # P7
│   └── SliceSnapshotterTracingIntegrationTest.java            # P8
├── TracingEndToEndIntegrationTest.java                        # P9.1 — cross-component command → event → query, InMemorySpanExporter
├── AF4SpanShapeParityIntegrationTest.java                     # P9.2 — asserts span name + kind for every row in af4-span-inventory.md §2
├── TracingPropagationIntegrationTest.java                     # P9.3 — remote-bus-style propagation through message metadata (covers SC-009)
└── TracingJaegerIntegrationTest.java                          # P9.4 — Testcontainers + jaegertracing/all-in-one + OTLP exporter + Jaeger query API

# DELETED later, in a separate PR against /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5
# (full deletion checklist captured in research.md and lifted into tasks.md by /speckit-tasks — phase P10)
```

**Structure Decision**: Five new flat-sibling Maven modules under a new top-level `tracing/` directory — `axoniq-tracing-api` (SPI + public decorator-authoring helpers; depends only on `axon-messaging`), `axoniq-tracing-messaging`, `axoniq-tracing-modelling`, `axoniq-tracing-eventsourcing` (per-concern decorators mirroring AF5's `axon-messaging`/`axon-modelling`/`axon-eventsourcing` artifacts), and `axoniq-tracing-opentelemetry` (OTel binding). Follows the per-concern top-level convention already used by `messaging/`, `connector/`, `dependency-injection/`, and `testing/`. Each per-concern decorator module ships its own `@Internal` `ConfigurationEnhancer` discovered via ServiceLoader (matching `axoniq-dead-letter` / `axoniq-distributed-messaging`), so a consumer pulls in tracing only for the AF5 modules it uses and external modules / repos extend tracing by depending on `axoniq-tracing-api`. Spring Boot wiring is added to the **existing** `dependency-injection/spring/spring-boot-autoconfigure` module rather than a new tracing-specific autoconfig module, matching how dead-letter / postgres / axon-server autoconfig are already shipped from the same place. See spec.md clarification 2026-05-26 (five-module structure) for the full rationale and the validated `axoniq-tracing-api → axon-messaging` dependency. **Rejected alternative**: a single `axoniq-tracing` module with `<optional>true</optional>` on `axon-modelling`/`axon-eventsourcing` + runtime class-presence guards. Rejected because AF5's `ComponentRegistry` API is `Class<?>`-typed with no `ifClassPresent` helper, so a single enhancer referencing `Repository.class`/`SnapshotStore.class` would `NoClassDefFoundError` when those artifacts are absent — requiring hand-rolled `Class.forName` guards + class isolation (AF4's `ifClassPresent` dance) with zero API support, plus dead bytecode and silent under-coverage risk. The layered split makes the compile dependency the class-presence guard and ServiceLoader discovery the conditional wiring. Full analysis in the spec clarification.

## Implementation Phases (handed off to `/speckit-tasks`)

The phases below are the structural backbone the task generator will follow. They map to the User Stories in `spec.md` and reflect the **component-by-component vertical-slice** strategy (spec.md FR-022b, clarifications 2026-05-26).

### Mandatory workflow: test-driven development (FR-022a)

**Before writing any production code in any phase below, the implementer MUST invoke the `test-driven-development` skill** and follow its red-green-refactor cycle. This is non-negotiable for this feature (see `spec.md` FR-022a):

1. **Invoke the `test-driven-development` skill** at the start of each task that produces production code.
2. **Translate the AF4 equivalent test first**, if one exists. Locate the matching test under `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/...` (e.g., `DefaultCommandBusSpanFactoryTest`, `DefaultEventBusSpanFactoryTest`, `DefaultEventProcessorSpanFactoryTest`, `DefaultQueryBusSpanFactoryTest`, `DefaultRepositorySpanFactoryTest`, `DefaultSnapshotterSpanFactoryTest`, `TracingHandlerEnhancerDefinitionTest`, `MultiSpanFactoryTest`, etc.) and start by porting it to the AF5 API surface. **Translation is not transcription** — improve the AF4 test where it asserts on implementation details rather than behaviour (CLAUDE.md Test Guidelines forbid implementation-detail testing), drop dead assertions, and modernise to AssertJ + JUnit 5 `@Nested`.
3. **Prefer stub / recording implementations over Mockito mocks** where a stub produces a cleaner test. CLAUDE.md already mandates this ("avoid Mock whenever possible, try to use simplest implementation"); the AF4 codebase has a `TestSpanFactory` recording double that the new modules SHOULD port and extend rather than mocking `SpanFactory` ad-hoc.
4. Write the failing test first; run it; confirm it fails for the expected reason.
5. Write the minimum production code to make it pass.
6. Refactor with the test as a safety net.
7. **Run the `simplify` skill (or an equivalent code-simplifier agent) on the resulting test code** to ensure clarity and consistency before considering the task complete. This is mandatory, not optional — the test is the primary artefact reviewers consult to understand intended behaviour, so its readability matters as much as the production code's.
8. Repeat for the next behaviour.

**No production code without a failing test that justifies it.** This applies to phases P2 through P9. Exemptions: P1 (module scaffolding ships no behaviour) and P10 (deletion-only phase in upstream AxonFramework 5).

**If writing a test is genuinely difficult** — e.g., no AF4 equivalent exists, the natural AF5 assertion would test implementation details rather than behaviour (forbidden by CLAUDE.md Test Guidelines), a recording double doesn't yet exist for a needed collaborator and isn't trivially constructable, or the OpenTelemetry SDK's `InMemorySpanExporter` doesn't expose what you need to assert on — **stop and ask the user**. Do not mock around it, do not skip the test, do not ship untested code. Examples of legitimate questions to surface to the user:
- "AF4 has no test for `<Tracing*>` covering `<behaviour>`; how should I assert it?"
- "What's the recording double for `<X>` in this project, or should I create one (and what should it record)?"
- "The natural assertion couples to `<implementation detail>`; how should I assert this behaviourally?"
- "OpenTelemetry's `InMemorySpanExporter` reports `<X>`; can I assert on that, or is there a different convention?"

Compliance with this workflow is a precondition for marking any P2–P9 task complete in `tasks.md`.

### Mandatory workflow: per-slice human validation checkpoint (FR-022b)

Phases P3–P8 are vertical slices. **The next slice MUST NOT start until the previous slice's human-validation checkpoint has been completed.** Each slice ends with a discrete `tasks.md` task of the form:

> **STOP — Human validation gate for Slice N.** Commit the slice on `feat/3594-DistributedTracing`. Run `./mvnw -Pintegration-test verify -pl integrationtests -Dtest=Slice<N>...IntegrationTest`. Post the resulting span tree (span names, kinds, attributes, parent-child structure) to the user. Wait for explicit go/no-go before starting Slice N+1.

The task-executor agent (or human implementer) MUST respect this gate. No skipping, no batching, no "I'll come back to it" — the next slice is blocked until the user has accepted the previous slice. The same gate applies after the cross-component E2E test in P9 before the Jaeger testcontainers test runs.

### Mandatory workflow: no-ThreadLocal / cross-thread context test (Constitution §V)

Tracing carries parent/child context through the `ProcessingContext` resource (the unit-of-work scoped carrier), **never** a `ThreadLocal` / `Context.current()` (see spec.md clarification 2026-05-27). A passing single-threaded test does **not** prove this — a thread-local implementation would pass it too. Therefore **every slice whose component relies on `ProcessingContext` to nest or propagate spans MUST ship a test that exercises the context across two different threads** and asserts the spans still connect:

- **Pattern.** Create a real `ProcessingContext` with `org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils.aUnitOfWork()` (or run inside `unitOfWork.executeWithResult(ctx -> …)`). Start the parent span on one thread; create + start the child span on a **different** `Thread` (or executor) sharing the **same** `ProcessingContext`; assert the child has the parent's `traceId` and `parentSpanId`. For dispatch→handle (cross-boundary) the carrier is message metadata — run the handler on a separate thread and assert the same.
- **Prove it is not vacuous.** The test must be written so that it would **fail** against a thread-local implementation. Reference proof: `OpenTelemetrySpanFactoryTest$CrossThreadNestingViaRealUnitOfWork` and `SliceCommandBusTracingIntegrationTest#handlerExecutedOnADifferentThreadStillNestsUnderTheDispatchSpan` — both verified to fail when parent resolution is switched to `Context.current()`.
- **Scope.** Required for slices that introduce in-process span nesting through `ProcessingContext` (the handler-enhancer span, the event batch span, repository inner spans, snapshot store/load nesting) and for the dispatch→handle metadata path of each bus. A new `Tracing*` decorator is **not** "done" without it.

### Phase listing

1. **P1 — Foundations**: parent POM updates (add `<opentelemetry.version>` property + OTel BOM import), the five new Maven modules' scaffolding (`pom.xml` with the validated dependency wiring — `-api → axon-messaging`; `-messaging → -api + axon-messaging`; `-modelling → -api + axon-modelling`; `-eventsourcing → -api + axon-eventsourcing`; `-opentelemetry → -api + opentelemetry-api`; `package-info.java` with `@NullMarked`, root aggregator wiring, BOM entries). No production code yet — this lands the empty modules so the build is green before any classes arrive. **TDD-exempt.**

2. **P2 — Core abstractions (`axoniq-tracing-api`) + OpenTelemetry module + autoconfig skeleton**: builds everything the slices will share, in one phase so each slice starts on top of a stable foundation:
    - `axoniq-tracing-api`: `SpanFactory`, `Span`, `SpanScope`, `SpanAttributesProvider` SPI, `NoOpSpanFactory`, `MultiSpanFactory`, `LoggingSpanFactory`, the six built-in `SpanAttributesProvider` implementations under `attributes/` (**Option B** attribute-key convention — OTel-style dotted `axoniq.*` keys with snake_case leaf segments, e.g. `axoniq.message.payload_type`; each provider's key — or, for `MetadataSpanAttributesProvider`, its prefix — is **overridable via constructor** to restore the AF4 key such as `axon_message_name`; see `contracts/public-api.md` §1.7 and `spec.md` clarification 2026-05-27; Option C — full OTel `messaging.*` semantic conventions — is a deferred future improvement), the **public** decorator-authoring helpers (`SpanNames`, `ProcessingContextSpanBinding`, `MetadataContextPropagator` SPI — DFI-001 realized), and the `TestSpanFactory` recording double ported from AF4. No `TracingConfigurationEnhancer` here — the enhancers live in the per-concern decorator modules (P3–P8) so the api module has zero dependency on the decorators.
    - `axoniq-tracing-opentelemetry`: `OpenTelemetrySpanFactory`, `OpenTelemetrySpan`, `MetadataContextSetter`, `MetadataContextGetter`. These don't need per-component shaping — they implement the generic `SpanFactory` API and the `MetadataContextPropagator` W3C TextMap contract from `-api`.
    - `dependency-injection/spring/spring-boot-autoconfigure`: `TracingProperties` skeleton (global `axon.tracing.enabled` only; per-component toggles are added by the corresponding slice), `TracingAutoConfiguration` (registers default `SpanFactory` bean — `OpenTelemetrySpanFactory` if OTel is on the classpath, else `NoOpSpanFactory` — plus the six built-in `SpanAttributesProvider` beans; the per-module `ConfigurationEnhancer`s are ServiceLoader-discovered, not bean-registered here), `OpenTelemetryTracingAutoConfiguration`, the new entries in `AutoConfiguration.imports`. Autoconfig conditions each component group on the presence of the corresponding `axoniq-tracing-*` module.
    - Unit tests for all of the above. The autoconfig has a smoke `@SpringBootTest` confirming the context starts and registers the `SpanFactory` bean (the slices, each shipping their own ServiceLoader-discovered enhancer, will assert their components are actually decorated).
    - Public-API surface test that fails if anyone introduces a public `*BusSpanFactory` / `*ManagerSpanFactory` / `*ProcessorSpanFactory` / `*EmitterSpanFactory` / `RepositorySpanFactory` / `SagaManagerSpanFactory` / `SnapshotterSpanFactory` interface (enforces FR-016 / SC-003).
    - **No component decorators yet.** No per-slice integration tests yet.

3. **P3 — Slice 1: `TracingCommandBus` + `@CommandHandler` enhancement** (Story 1 / 4, FR-006, FR-013):
    - **P3.1**: `TracingCommandBus` decorator + unit tests (`TracingCommandBusTest`). Ports `DefaultCommandBusSpanFactoryTest` from AF4 first, then improves to behaviour-only assertions. Dispatch + handle spans, kinds per AF4's OpenTelemetry binding (`createDispatchSpan` → `PRODUCER`, `createHandlerSpan` → `CONSUMER`, `createInternalSpan` → `INTERNAL`; AF4's in-process dispatch leg used `createInternalSpan` → `INTERNAL`, the distributed leg used `createDispatchSpan` → `PRODUCER`), metadata-based W3C context propagation via `MetadataContextSetter` / `MetadataContextGetter`. Span lifecycle bound to `ProcessingContext` via `ProcessingContextSpanBinding` (FR-013a).
    - **P3.2**: Introduce `TracingHandlerEnhancerDefinition` with `@CommandHandler` coverage + unit tests (`TracingHandlerEnhancerDefinitionTest` with `@Nested CommandHandlerEnhancement`). Ports `TracingHandlerEnhancerDefinitionTest` from AF4 first; only the `@CommandHandler` cases land in this slice — the other annotations are explicitly out of scope here and produce no enhancement until their slice runs. **Eager-name guard (FR-003a)**: the `SpanFactory` takes an eager `String` (not AF4's `Supplier<String>`), so the enhancer MUST decide it will open a span (enabled + handler-type not suppressed) **before** building the reflective span name `getSpanName(target, signature)`, computing the name only on the span-creating branch — never eagerly per invocation. Add a unit test asserting the name builder is **not** invoked when the enhancer is disabled / the handler type is suppressed (e.g. spy/recording the name supplier, or asserting via a handler whose name-build would throw). This guard matters most for `@EventSourcingHandler` (slice 6) on the replay hot path, but the pattern is established here in slice 1. See `af4-span-inventory.md` §0 rows 7/7a and §1.9.
        - **AUTO-WIRING DEFERRED TO SLICE 2 (decision 2026-05-27).** `TracingHandlerEnhancerDefinition` (with `@CommandHandler` coverage) is **implemented and unit-tested** in this slice, but it is **not registered into the live annotation pipeline here**. AF5 has no config-aware handler-enhancer registration API (no AF4-style `Configurer.registerHandlerEnhancerDefinition(Function<Configuration,…>)`); the only seam is registering a `HandlerDefinition` **component**, which is a single global slot shared by command/query/event handling. Because that wiring is global and is first genuinely needed once the enhancer is applied through the real pipeline, it is introduced and validated **once** in slice 2 (see P4.3a) rather than command-only here. Consequently slice 1's integration test asserts the dispatch + handle spans only (not the per-method enhancer span). Full research, approach, and limitations: see the "Handler-enhancer auto-wiring in AF5" appendix below and `spec.md` clarification 2026-05-27.
    - **P3.3**: Add `axon.tracing.commandBus.enabled` toggle to `TracingProperties`. Wire it into `TracingAutoConfiguration` so that the `CommandBus` decoration is skipped when disabled. Unit test the toggle via the Spring `ApplicationContextRunner`.
    - **P3.4**: Register `DecoratorDefinition.forType(CommandBus.class)` in `MessagingTracingConfigurationEnhancer` (in `axoniq-tracing-messaging`; add its `META-INF/services` entry). Update `SpanNames` (in `axoniq-tracing-api`) for the command-bus spans. **(The `HandlerEnhancerDefinition` is NOT wired here — see P3.2 deferral note and P4.3a.)**
    - **P3.5**: Focused Boot integration test (`SliceCommandBusTracingIntegrationTest`) using `@SpringBootTest` + `InMemorySpanExporter` SDK. Asserts (a) a dispatched command produces the expected dispatch span with `PRODUCER` kind, (b) the handler produces the expected child handler span with `CONSUMER` kind, (c) ~~the `@CommandHandler`-annotated method produces the enhancer-added child span~~ **— DEFERRED to slice 2 (P4.3a), since handler-enhancer auto-wiring is global and lands in slice 2; the `@CommandHandler` per-method span is validated then (see P4.6)**, (d) `axon.tracing.commandBus.enabled=false` skips decoration. Span name + kind + attributes asserted against the corresponding rows in `af4-span-inventory.md` §1 (the AF4 → AF5 mapping audit).
    - **P3.6**: **STOP — Human validation gate for Slice 1.** Commit. Run `./mvnw -Pintegration-test verify -pl integrationtests -Dtest=SliceCommandBusTracingIntegrationTest`. Post the asserted span tree to the user. Wait for go/no-go before starting Slice 2.

4. **P4 — Slice 2: `TracingEventSink` + `TracingEventHandlingComponent` + `@EventHandler` enhancement** (Story 1 / 4, FR-007, FR-007a):
    - **P4.1**: `TracingEventSink` decorator + unit tests. Implements the AF4 two-span pattern (per-event publish span synchronous around `propagateContext`, UoW-scoped commit span bound to `ProcessingContext` lifecycle via `runOnPrepareCommit` / `onError` / `whenComplete`). Falls back to `Span.run(...)` when `ProcessingContext == null`. See spec.md clarification entry on `TracingEventSink` and `flows.md` Flow 2.
    - **P4.2**: `TracingEventHandlingComponent` decorator + unit tests. Per-event consumer span (`EventProcessor.process <eventName>`, kind `CONSUMER`). Lazy batch span via `ctx.computeResourceIfAbsent(BATCH_SPAN_KEY, …)` gated on `ctx.getResource(Segment.RESOURCE_KEY).isPresent()`. Batch span bound to UoW lifecycle. See `research-batch-tracing.md` and `flows.md` Flow 4.
    - **P4.3**: Extend `TracingHandlerEnhancerDefinition` to cover `@EventHandler` + unit tests (a new `@Nested EventHandlerEnhancement` block in `TracingHandlerEnhancerDefinitionTest`).
    - **P4.3a**: **Auto-wire `TracingHandlerEnhancerDefinition` into the live annotation pipeline (deferred from slice 1).** Register a `HandlerDefinition` framework component (via the messaging tracing enhancer / Spring autoconfig `ConfigurationEnhancer`) that bundles the configured `SpanFactory`-bearing `TracingHandlerEnhancerDefinition` on top of the AF5 defaults — see the "Handler-enhancer auto-wiring in AF5" appendix for the exact code, the AF4 difference, and the limitations. **This single wiring activates the per-method span for every annotation type the enhancer currently gates on — i.e. BOTH `@CommandHandler` (implemented in slice 1) AND `@EventHandler` (this slice).** Therefore P4.3a MUST verify the `@CommandHandler` per-method span retroactively as well (re-enable the slice-1 integration assertion (c), and/or assert it in `SliceEventTracingIntegrationTest`). The enhancer self-gates per message type, so command and event per-method spans appear only for their respective handlers. Unit-test the registration (component present, composes with — does not replace — the standard `ClasspathHandlerDefinition` definitions/enhancers).
    - **P4.4**: Add `axon.tracing.eventSink.enabled`, `axon.tracing.eventProcessor.enabled`, `axon.tracing.eventProcessor.disableBatchTrace`, `axon.tracing.eventProcessor.distributedInSameTrace`, `axon.tracing.eventProcessor.distributedInSameTraceTimeLimit` (default `PT2M`) toggles to `TracingProperties`. Semantics match AF4's `DefaultEventProcessorSpanFactory` (FR-007a).
    - **P4.5**: Register `DecoratorDefinition.forType(EventSink.class)` and `DecoratorDefinition.forType(EventHandlingComponent.class)` in `MessagingTracingConfigurationEnhancer`. The `EventHandlingComponent` registration uses a `cfg.getOptionalComponent(EventProcessorConfiguration.class).isPresent()` scope guard (covers both `PooledStreamingEventProcessor` and `SubscribingEventProcessor`; FR-007a).
    - **P4.6**: Focused Boot integration test (`SliceEventTracingIntegrationTest`) covering (a) PSEP: per-event spans nested under a single batch root span enclosing the UoW's prepare-commit (token-store write) and after-commit (segment-status update); (b) `SubscribingEventProcessor`: per-event spans inheriting the publisher's trace, **no** batch span (AF4 parity, FR-007a); (c) cross-thread W3C propagation between publication and async handling via `MetadataContextSetter`/`Getter`; (d) all five new toggles behave as documented; (e) **the `@EventHandler` per-method enhancer span appears (P4.3a wiring); and the `@CommandHandler` per-method enhancer span now also appears (retroactively activated by the same P4.3a wiring) — re-enable the slice-1 integration assertion (c) at this point.**
    - **P4.7**: **STOP — Human validation gate for Slice 2.** Commit, run the slice test, post span tree, wait for go/no-go.

5. **P5 — Slice 3: `TracingQueryBus` + `@QueryHandler` enhancement** (Story 1 / 4, FR-008 — query bus portion):
    - **P5.1**: `TracingQueryBus` decorator + unit tests. Dispatch + handle spans for direct query, scatter-gather, and subscription-query initial-result paths. Distributed-vs-in-process branching.
    - **P5.2**: Extend `TracingHandlerEnhancerDefinition` to cover `@QueryHandler` + unit tests.
    - **P5.3**: Add `axon.tracing.queryBus.enabled` toggle.
    - **P5.4**: Register `DecoratorDefinition.forType(QueryBus.class)` in `MessagingTracingConfigurationEnhancer`.
    - **P5.5**: Focused Boot integration test (`SliceQueryBusTracingIntegrationTest`).
    - **P5.6**: **STOP — Human validation gate for Slice 3.**

6. **P6 — Slice 4: `TracingQueryUpdateEmitter`** (Story 1 / 4, FR-008 — query update portion):
    - **P6.1**: `TracingQueryUpdateEmitter` decorator + unit tests. Implements the AF4 two-span pattern for `emit`: a schedule span (when the emission is queued) plus an emit span (when delivery actually happens). Plus single spans for `complete` / `completeExceptionally`.
    - **P6.2**: Add `axon.tracing.queryUpdateEmitter.enabled` toggle.
    - **P6.3**: Register `DecoratorDefinition.forType(QueryUpdateEmitter.class)` in `MessagingTracingConfigurationEnhancer` (QueryUpdateEmitter is an `axon-messaging` component).
    - **P6.4**: Focused Boot integration test (`SliceQueryUpdateEmitterTracingIntegrationTest`) exercising a subscription query end-to-end.
    - **P6.5**: **STOP — Human validation gate for Slice 4.**

7. **P7 — Slice 5: `TracingRepository` + `TracingStateManager`** (Story 1 / 4, FR-009, FR-010 partial):
    - **P7.1**: `TracingRepository` decorator + unit tests. Multi-span pattern from AF4: `Repository.load` outer span containing nested `obtainLock` and `initializeState` spans. Save-side spans.
    - **P7.2**: `TracingStateManager` decorator + unit tests.
    - **P7.3**: End-to-end verification of `AggregateIdentifierSpanAttributesProvider` — exercise the path where `LegacyResources.AGGREGATE_IDENTIFIER_KEY` is populated by a legacy aggregate-based event stream so that `axoniq.aggregate.identifier` appears on spans; verify it is absent on DCB / entity-based paths.
    - **P7.4**: Add `axon.tracing.repository.enabled` toggle.
    - **P7.5**: Register `DecoratorDefinition.forType(Repository.class)` and `DecoratorDefinition.forType(StateManager.class)` in `ModellingTracingConfigurationEnhancer` (in `axoniq-tracing-modelling`; add its `META-INF/services` entry).
    - **P7.6**: Focused Boot integration test (`SliceRepositoryTracingIntegrationTest`).
    - **P7.7**: **STOP — Human validation gate for Slice 5.**

8. **P8 — Slice 6: `TracingSnapshotStore` + `@EventSourcingHandler` enhancement** (Story 1 / 4, FR-010 remainder):
    - **P8.1**: `TracingSnapshotStore implements SnapshotStore` decorator + unit tests. Two spans: `SnapshotStore.store <entityType>` + `SnapshotStore.load <entityType>`, via `createInternalSpan(String)` + local `addAttribute`. **NB**: AF5 has no `Snapshotter` component (it's in `stash/todo`); snapshot creation is an inline `SnapshotPolicy`-gated side-effect of `SnapshottingEntityLifecycleHandler.source(...)`. The store/load spans nest under the FR-009 entity-sourcing span (`EntityLifecycleHandler.source`) via `ProcessingContext` — no separate snapshot outer decoration. `SnapshotStore` is `@Internal` (accepted coupling; no internals modified). See spec.md clarification 2026-05-26 (B3).
    - **P8.2**: Extend `TracingHandlerEnhancerDefinition` to cover `@EventSourcingHandler` + unit tests. This is the last annotation to land — after P8 the enhancer covers all four (`@CommandHandler`, `@EventHandler`, `@QueryHandler`, `@EventSourcingHandler`). **This is the hot-path case for the FR-003a eager-name guard** established in slice 1 (P3.2): `@EventSourcingHandler` fires once per event during entity replay, and is suppressed by default (`showEventSourcingHandlers=false`). Add a unit test asserting that, with `showEventSourcingHandlers=false`, an `@EventSourcingHandler` invocation neither opens a span nor invokes `getSpanName(...)` (the reflective name builder) — proving the guard short-circuits before the expensive name construction. See `af4-span-inventory.md` §0 row 7a and FR-003a.
    - **P8.3**: Add `axon.tracing.snapshotStore.enabled` toggle.
    - **P8.4**: Register `DecoratorDefinition.forType(SnapshotStore.class)` (and the `EntityLifecycleHandler.source` sourcing-span decorator) in `EventSourcingTracingConfigurationEnhancer` (in `axoniq-tracing-eventsourcing`; add its `META-INF/services` entry).
    - **P8.5**: Focused Boot integration test (`SliceSnapshotStoreTracingIntegrationTest`) exercising both snapshot store (write) and snapshot load (read), asserting the store/load spans nest under the entity-sourcing span.
    - **P8.6**: **STOP — Human validation gate for Slice 6.** This is the last per-component gate; after this the implementation moves to the batched final phase.

9. **P9 — Batched finalization**: cross-component verification, realistic backend test, example, and documentation, in this order:
    - **P9.1 — Cross-component end-to-end integration test** (`TracingEndToEndIntegrationTest`, FR-023, SC-004): one `@SpringBootTest` runs a full command → event → query flow with all six slices' decorators active. Uses OpenTelemetry SDK with `InMemorySpanExporter`. Asserts the connected span tree (root command-dispatch span → command-handler span → annotated-command-handler enhancer span → event-publish + commit spans → event-handler spans + batch span where applicable → query-dispatch + query-handler spans → annotated-query-handler enhancer span). Covers SC-003a (AF4 span shape equivalence) by comparing against the expected shape derived from `af4-span-inventory.md` §2.
    - **P9.2 — AF4 span-shape parity test** (`AF4SpanShapeParityIntegrationTest`, supports SC-003a / Story 4 acceptance criterion 3): asserts span name + kind for every row in `af4-span-inventory.md` §2 (the AF4 reference-guide cross-check). Makes Story 4 acceptance criterion 3 ("the consolidation does not regress the observable trace shape") verifiable rather than aspirational.
    - **P9.3 — Cross-process propagation integration test** (`TracingPropagationIntegrationTest`, FR-015, SC-009): exercises remote-bus-style propagation through message metadata. Against the AxonServer connector if available in the integration-tests profile; otherwise an in-process serialise/deserialise route that strips and rebuilds metadata between the two halves.
    - **P9.4 — STOP — Human validation gate before Jaeger.** Commit the three batched-phase integration tests. Run them. Post the asserted span tree from P9.1 to the user. Wait for explicit go/no-go before starting P9.5 (the Jaeger testcontainers test). This is the second-to-last gate.
    - **P9.5 — Jaeger testcontainers integration test** (`TracingJaegerIntegrationTest`, FR-023a): runs the same command → event → query flow against a `jaegertracing/all-in-one` container with the OTLP receiver. Exports spans via the OpenTelemetry OTLP exporter, then queries Jaeger's REST/JSON API to assert the same span tree shape verified by P9.1. Runs in the `integration-test` Maven profile and is skipped automatically when Docker is unavailable (Testcontainers' built-in check). Does **not** replace P9.1 — both tests ship.
    - **P9.6 — Example application** (Story 1, FR-027, SC-005): wire tracing into one example under `examples/` with a working exporter (logging or Jaeger via Testcontainers) so `./mvnw -Pexamples clean verify` exercises a real trace. If no suitable example exists in the repo at implementation time, this step scaffolds a minimal `examples/tracing-springboot/` instead.
    - **P9.7 — Reference-guide documentation** (Story 5, FR-025, FR-026, SC-006): populate `docs/reference-guide/modules/tracing/pages/*.adoc` adapted from AF4's `docs/old-reference-guide/modules/monitoring/pages/tracing.adoc`, updated for AF5 terminology, the consolidated `SpanFactory` API, the `DecoratorDefinition`-based wiring, and the actual `TracingProperties` toggle names landed across P3–P8. Antora builds clean. Docs are produced **last** in P9 so the page reflects the as-shipped span shapes and toggle names without needing to be rewritten six times.

10. **P10 — Cleanup of upstream Axon Framework 5** (Story 6, FR-028, FR-029, SC-007): a separate PR against `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5` deleting all dead tracing source listed in `research.md` (≈60 files plus the `extensions/tracing/` module and the `stash/todo` placeholders), and asserting `./mvnw clean verify` is green afterwards. This phase runs **after** P1–P9 are merged into AxoniqFramework. **TDD-exempt** — deletion-only phase in a different repository.

Each phase's tasks file (produced by `/speckit-tasks`) inherits the constitutional gates above, the TDD workflow (FR-022a), the per-slice human-validation gate (FR-022b), and the FR / SC cross-references from `spec.md`.

## Complexity Tracking

> Filled ONLY if Constitution Check has violations that must be justified.

*No violations. This section is intentionally empty.*

---

**Out-of-band notes** (resolved during planning, captured for completeness):

- The spec mentions `examples/university-java-springboot4` as a candidate to host the demo. As of plan time, the `examples/` subtree in AxoniqFramework is empty (parent pom only). P9.6 keeps the choice flexible: use whichever example exists at implementation time, or scaffold a minimal one.
- AxoniqFramework currently uses Spring Boot 3.5.x (`spring-boot.version=3.5.14` in `build/parent/pom.xml`), not Spring Boot 4. The autoconfig style matches the existing Spring Boot 3 conventions in `dependency-injection/spring/spring-boot-autoconfigure`. If/when the framework moves to Spring Boot 4, only the autoconfig annotations may need a sweep — not the tracing core.
- AF5 already ships a class called `TracingCommandBus` in `messaging/src/main/java/org/axonframework/messaging/commandhandling/tracing/TracingCommandBus.java`. The plan's `TracingCommandBus` lives under `io.axoniq.framework.tracing.internal` (different package + different module). No collision.
- Cross-process propagation (SC-009) is exercised end-to-end against the Axon Server connector if available in the integration-tests profile in P9.3; otherwise the test simulates the two sides in-process by routing a `CommandMessage` through a serialise-deserialise step that strips/rebuilds metadata. This still validates the propagation contract (FR-015) deterministically.
- **Slice rationale**: the chosen order (Command → Event → Query → QueryUpdate → Repository → SnapshotStore) starts with the simplest single-span dispatch+handle case (CommandBus) so the core abstractions get exercised end-to-end with minimum noise from multi-span patterns. EventSink + EventHandling lands second because it's the highest-stress test of the abstractions (two-span publish+commit, cross-thread propagation, batch detection, AF4 subscribing-processor parity) — any gaps in the core abstractions surface here before three more slices are built on top. QueryBus is a near-clone of CommandBus and validates the dispatch+handle pattern at a different message type. QueryUpdateEmitter, Repository, and SnapshotStore are the multi-span / non-Message specialised cases. (Slice 6 decorates AF5's `SnapshotStore`, not a `Snapshotter` — AF5 has no `Snapshotter` component; see spec.md clarification 2026-05-26 B3.)
- **Core abstraction changes during slice work**: if a later slice exposes a gap in `SpanFactory` / `Span` / `SpanScope` / `SpanAttributesProvider` / `ProcessingContextSpanBinding` (e.g., a new method is needed, an existing signature is wrong, a `ResourceKey` shape needs widening), the implementer MUST flag the change to the user before committing it — one-line summary of what's being changed and why, then proceed if the user accepts. Tests for the abstraction change land alongside the production change (no untested API growth).
- **AF5 anchoring types check**: the plan introduces no new dependency on a type outside Constitution §"AF5 Anchoring Types". All decorator interfaces wrapped — `CommandBus`, `EventSink`, `EventHandlingComponent`, `QueryBus`, `QueryUpdateEmitter`, `Repository`, `StateManager`, `SnapshotStore` — are listed there or are direct subtypes of types listed there. **Note**: `SnapshotStore` is `@Internal` in AF5 (not a stable public type); decorating it is an accepted, documented coupling (the only viable snapshot-tracing decoration point, since AF5 exposes no `Snapshotter`) — flagged here so a constitution reviewer sees the deliberate exception. See spec.md clarification 2026-05-26 (B3).

---

## Appendix: Handler-enhancer auto-wiring in AF5 (research, approach, limitations)

*Captured 2026-05-27 while building slice 1. Drives P4.3a. The "per-method handler span" below is the innermost span produced by `TracingHandlerEnhancerDefinition` wrapping an annotated handler `MessageHandlingMember` — e.g. `RoomBookingHandler.handle(BookRoom)` `[INTERNAL]` — and is distinct from the bus-level dispatch/handle spans that `TracingCommandBus` already produces. Verified against `AxonFramework5` (`5.2.0-SNAPSHOT`) and `AxonFramework4` clones.*

### Trace shape with vs. without the per-method span

```
without (slice 1 as shipped):              with the enhancer wired (P4.3a):
CommandBus.dispatchCommand BookRoom         CommandBus.dispatchCommand BookRoom      [PRODUCER]
└─ CommandBus.handleCommand BookRoom        └─ CommandBus.handleCommand BookRoom     [CONSUMER]
                                               └─ RoomBookingHandler.handle(BookRoom) [INTERNAL]  ← enhancer span
```

### AF4 mechanism (what we are porting from)

`HandlerEnhancerDefinition` is an SPI that wraps each annotated handler method. AF4 exposed a **first-class, config-aware** registration on the configurer:

```java
// AF4 — org.axonframework.config.Configurer:492
Configurer registerHandlerEnhancerDefinition(Function<Configuration, HandlerEnhancerDefinition> handlerEnhancerBuilder);
// usage: cfg.registerHandlerEnhancerDefinition(
//            c -> TracingHandlerEnhancerDefinition.builder().spanFactory(c.spanFactory()).build());
```

The builder lambda receives the `Configuration`, so it can pull the configured `SpanFactory`. AF4's `AnnotatedHandlerInspector` applied registered enhancers alongside the classpath (ServiceLoader) ones.

### AF5 mechanism (what we must target)

`HandlerEnhancerDefinition` still exists (`org.axonframework.messaging.core.annotation.HandlerEnhancerDefinition`) and is applied — but **only through a `HandlerDefinition`**. `MultiHandlerDefinition` (a `HandlerDefinition`) holds a `HandlerEnhancerDefinition` and wraps each member with it (`MultiHandlerDefinition.java:34-37`, ordered factory at `:68`). `AnnotatedHandlerInspector.inspectType(...)` has **no** enhancer parameter and no reference to enhancers — enhancers are entirely a property of the `HandlerDefinition`.

The annotated handling components obtain their `HandlerDefinition` from the configuration:

```java
// AF5 — CommandHandlingModule.java:202 (identical shape: QueryHandlingModule:204, EventHandlingComponentsConfigurer:107/130)
c.getOptionalComponent(HandlerDefinition.class)
 .orElse(ClasspathHandlerDefinition.forClass(c.getClass()))
```

The default `ClasspathHandlerDefinition.forClass(...)` builds `MultiHandlerDefinition.ordered(findDelegates(cl))`, whose enhancer is `ClasspathHandlerEnhancerDefinition` — **ServiceLoader-discovered, no-arg constructors only**. A no-arg enhancer cannot receive the configured `SpanFactory`.

### The gap (AF5 vs AF4)

AF5 has **no** `registerHandlerEnhancerDefinition(Function<Configuration,…>)` equivalent on the configurer / component registry. The **only** seam for a config-aware (SpanFactory-bearing) enhancer is to register a whole `HandlerDefinition` **component**:

| | AF4 | AF5 |
|---|---|---|
| Enhancer SPI | `HandlerEnhancerDefinition` | `HandlerEnhancerDefinition` (same name, package `…core.annotation`) |
| Applied via | `AnnotatedHandlerInspector` | `MultiHandlerDefinition` (a `HandlerDefinition`) |
| Config-aware registration API | `Configurer.registerHandlerEnhancerDefinition(Function<Config,…>)` | **none** — only `getOptionalComponent(HandlerDefinition.class)` |
| Default enhancer source | classpath + registered | classpath (ServiceLoader, no-arg) only |
| Per-message-type slot | per-enhancer | one global `HandlerDefinition` shared by command/query/event |

### Approach for P4.3a

Register a `HandlerDefinition` component that reconstructs the AF5 default and adds the tracing enhancer on top:

```java
registry.registerComponent(HandlerDefinition.class, config -> {
    SpanFactory spanFactory = config.getComponent(SpanFactory.class);
    ClassLoader cl = getClass().getClassLoader();           // or the application class loader
    HandlerEnhancerDefinition enhancers = MultiHandlerEnhancerDefinition.ordered(
            ClasspathHandlerEnhancerDefinition.forClassLoader(cl),   // keep standard enhancers
            new TracingHandlerEnhancerDefinition(spanFactory));      // add ours
    return MultiHandlerDefinition.ordered(
            enhancers,
            ClasspathHandlerDefinition.forClassLoader(cl));          // keep standard handler definitions
});
```

(Confirm the exact `MultiHandlerDefinition.ordered(HandlerEnhancerDefinition, HandlerDefinition…)` / `MultiHandlerEnhancerDefinition.ordered(…)` overloads against the AF5 source at implementation time.) **This one registration activates the per-method span for every annotation type the enhancer gates on** — at slice 2 that is **both `@CommandHandler` (slice 1) and `@EventHandler` (slice 2)**; later slices widen the gating to `@QueryHandler` (slice 3) and `@EventSourcingHandler` (slice 6) with no wiring change. The enhancer self-gates per `Message` type (`canHandleMessageType(...)`), so each handler kind gets a span only once its slice has enabled it.

### Limitations / accepted risks

1. **Single global slot.** `HandlerDefinition` is one component shared by command, query and event annotated components. Mitigated by the enhancer self-gating per message type.
2. **Collision with a user-supplied `HandlerDefinition`.** If the application registers its own `HandlerDefinition` component, ours and theirs conflict. The default `ClasspathHandlerDefinition` is created inline (not a registered component), so there is no `getOptionalComponent`-decorate seam for the fallback. Preferred mitigation: register a `DecoratorDefinition.forType(HandlerDefinition.class)` that composes on top of whatever is present **if** the registry applies decorators to `getOptionalComponent` results (verify); otherwise register the component only when absent and document that a custom `HandlerDefinition` opts out of per-method tracing.
3. **Reconstruction fragility.** We must rebuild the default composition so the standard `Method{Command,Event,Query}HandlerDefinition` and the standard enhancers (`MessageHandlerInterceptorDefinition`, `HandlerTimeoutHandlerEnhancerDefinition`, `ReplayAwareMessageHandlerWrapper`) survive. If AF5 changes that composition, our reconstruction can silently drift — prefer decorate-on-top over replace.
4. **No first-class API.** This is heavier than AF4's one-liner. Worth a follow-up upstream request to add an AF5 config-aware handler-enhancer registration hook so this becomes a clean, non-global registration; track alongside the P10 upstream work.

### Why deferred from slice 1 to slice 2

Slice 1's headline (`TracingCommandBus` dispatch + handle spans) is complete and tested without the per-method span. The wiring above is global and shared across all annotation types, so it is introduced and validated **once** in slice 2 — where the enhancer must run through the real annotation pipeline anyway — instead of command-only in slice 1. `TracingHandlerEnhancerDefinition` (`@CommandHandler` coverage) is already implemented + unit-tested in slice 1; only its auto-wiring moved. When P4.3a lands it retroactively activates the `@CommandHandler` per-method span, which slice 2's test re-verifies (re-enabling slice-1 integration assertion (c)).
