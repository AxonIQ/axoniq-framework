# Implementation Plan: Distributed Tracing Support

**Branch**: `feat/3594-DistributedTracing` | **Date**: 2026-05-26 | **Spec**: [spec.md](./spec.md)

**GitHub Issue**: [AxonIQ/AxonFramework#3594](https://github.com/AxonIQ/AxonFramework/issues/3594)

## Summary

Port Axon Framework 4's distributed-tracing capability (the only working implementation today) into AxoniqFramework, re-shaped around Axon Framework 5 idioms. The new code lives in two new flat-sibling Maven modules under a new top-level `tracing/` directory — `tracing/axoniq-tracing-core` and `tracing/axoniq-tracing-opentelemetry` — and an additional Spring Boot auto-configuration class is added to the existing `dependency-injection/spring/spring-boot-autoconfigure` module (no new spring-tracing module).

The migration **consolidates** the AF4 per-component span-factory interface family (`CommandBusSpanFactory`, `EventBusSpanFactory`, …, `SagaManagerSpanFactory`) into a single public `SpanFactory`. All per-component span shapes — naming, kind (`CLIENT`/`SERVER`/`PRODUCER`/`CONSUMER`/`INTERNAL`), attribute keys, distributed-vs-in-process branching, and metadata-based W3C context propagation — live inside **private** (`@Internal` or package-private) delegating wrapper classes registered via a single public `TracingConfigurationEnhancer` using `DecoratorDefinition`s. Each wrapper follows AF5's `TracingCommandBus` shape: implements the target interface, holds a `delegate` field of the same interface plus a `SpanFactory`, calls through to the delegate, and exposes itself to introspection via `ComponentDescriptor.describeWrapperOf(delegate)`. Where the wrapped operation already participates in a `ProcessingContext`, span open/close rides on the `ProcessingLifecycle` hooks rather than `try/finally` so span scope tracks the framework's processing phases correctly through async / reactive continuations.

After the AxoniqFramework migration lands, all dead tracing source in the upstream Axon Framework 5 repository is deleted (a checklist of ~60 files / one full extension module / one stash sub-tree).

## Technical Context

**Language/Version**: Java 21 (sealed classes, records, pattern matching allowed; raw types and wildcard imports forbidden — Constitution §III).

**Primary Dependencies**:
- Upstream `org.axonframework:axon-messaging:5.2.0-SNAPSHOT` (consumed via `axoniq-framework-bom`, pinned in `build/parent/pom.xml`).
- `io.opentelemetry:opentelemetry-api` — added to `build/parent/pom.xml` dependencyManagement (version pinned by `opentelemetry-bom`). The `-api` artifact only; the SDK is a user-side concern (host application supplies the exporter).
- JSpecify (already present at parent level) for `@NullMarked` / `@Nullable`.
- Spring Boot 3.5.x (autoconfig only).

**Storage**: N/A — tracing is in-memory, side-effect-only.

**Testing**: JUnit 5 + AssertJ + Awaitility (Constitution §Testing Standards). Mocks only where behaviour-based verification of a decorator demands it (e.g., `verify(delegate, never())` to prove a disabled component is not wrapped). Integration test uses OpenTelemetry SDK with `InMemorySpanExporter`.

**Target Platform**: JVM 21+. The core module is Spring-free; OpenTelemetry module is Spring-free; only the autoconfig glue depends on Spring Boot.

**Project Type**: Multi-module Maven library — adds two new leaf modules and one autoconfig class to an existing module.

**Performance Goals**: No measurable overhead when tracing is disabled (no-op factory). No microbenchmark target is set; the no-op call sites must be trivially inlinable (single method dispatch returning a shared no-op span). When enabled, the overhead is dominated by the OpenTelemetry SDK itself and is out of scope for this feature.

**Constraints**:
- No `ThreadLocal` in framework code (Constitution §V). Active-span tracking goes through `ProcessingContext` resources keyed by `ResourceKey<SpanScope>`.
- No public `*BusSpanFactory` / `*ManagerSpanFactory` / `*ProcessorSpanFactory` / `*EmitterSpanFactory` / `RepositorySpanFactory` / `SagaManagerSpanFactory` / `SnapshotterSpanFactory` interfaces (FR-016). Deadline tracing is permanently out of scope (clarification 2026-05-26 — `DeadlineManager` does not exist in AF5).
- Wrappers MUST be delegating decorators that implement the target component interface by composition and call `describeWrapperOf(delegate)` (FR-013).
- Cross-process trace context propagates through message metadata under reserved keys; collision behaviour with user metadata is documented.
- AF5 tracing source is dead code (commented-out, no wiring) — never used as a starting point. Code is read from AF4 and re-implemented for AF5 idioms.

**Scale/Scope**:
- 2 new Maven modules (`tracing/axoniq-tracing-core`, `tracing/axoniq-tracing-opentelemetry`).
- 1 new autoconfig class + 1 new `TracingProperties` class + 1 new entry in `AutoConfiguration.imports` in the existing `axoniq-spring-boot-autoconfigure` module.
- ~8 component-tracing decorators (commands, events, query bus, query update emitter, event handling component, repository/state, snapshotter, sagas/process-managers when their AF5 port exists), plus the annotation `HandlerEnhancerDefinition` wrapper. Deadlines are excluded — see clarification 2026-05-26.
- ~6 built-in `SpanAttributesProvider` implementations (parity with AF4).
- 1 reference-guide page populated under `docs/reference-guide/modules/tracing/pages/`.
- 1 cleanup PR (or PR set) against the upstream Axon Framework 5 repository deleting all dead tracing source there.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Mapped against the **Foundation Principles** and **Architecture Rules** in `.specify/memory/constitution.md` (v2.0.0).

| Principle / Rule | Status | Note |
|---|---|---|
| I. Simplicity First | PASS | One `SpanFactory` (vs. 9 in AF4). Decorators only, no new interceptor level. |
| II. Minimal Impact | PASS | No upstream AxonFramework changes needed beyond consuming existing extension points (`DecoratorDefinition`, `ConfigurationEnhancer`, `ProcessingLifecycle`). No edits to `messaging/`, `connector/`, `dependency-injection/spring/spring-boot-starter`. |
| III. Java 21 Baseline | PASS | Sealed-class option used where helpful (e.g., `Span.Kind`). Records used for trace-context carriers. No wildcard imports. |
| IV. Dual Paradigm Support | PASS | Span open/close binds to `ProcessingLifecycle` hooks; works for both imperative and reactive call paths because hooks fire on the right phases regardless of paradigm. |
| V. No ThreadLocals | PASS | Active span lives in `ProcessingContext` under `ResourceKey<SpanScope>`. The OpenTelemetry `Context.makeCurrent()` ThreadLocal is used **only** at the imperative-style edge of `Span.run...` synchronous helpers (explicitly permitted by Constitution §V). |
| VI. Composition over Inheritance | PASS | All decorators implement target interface by composition; AF4's `DistributedCommandBus`-style direct re-implementation is explicitly rejected (FR-013). |
| VII. Declarative over Annotation-Heavy | PASS | Programmatic `MessagingConfigurer.componentRegistry(cr -> cr.registerEnhancer(new TracingConfigurationEnhancer()))` is the primary path; Spring auto-configuration is a thin wrapper. |
| Module Dependency Hierarchy | PASS | New `tracing/` modules sit beside `messaging/`, `connector/`, `dependency-injection/`, `testing/`. Tracing depends on `axon-messaging` (upstream) but **not** on other AxoniqFramework modules; it has no reverse dependency from messaging back to tracing. |
| Async-First | PASS | Decorators preserve `CompletableFuture<…>` / `MessageStream<…>` return types from the wrapped methods unchanged. |
| ProcessingContext as Unit of Work | PASS | `SpanScope` is stored via `ResourceKey<SpanScope>`. |
| Interceptor Levels | PASS | Tracing does NOT introduce a new interceptor level. It plugs into the established `HandlerEnhancerDefinition` (for annotation handlers) and `DecoratorDefinition` (for component wiring). |
| Annotation Processing Pipeline | PASS | `TracingHandlerEnhancerDefinition` participates in step 3 of the inspector → definition → enhancer chain. |
| CompletableFuture Blocking Rule | PASS | No new blocking points are introduced. Test code that has to wait on exported spans uses Awaitility, not raw `join()`. |
| Type Safety (`TypeReference` over raw `Class`) | N/A | Tracing does not pass `Type` to the converter. |

**Conclusion**: Constitution Check passes with no violations. **Complexity Tracking** section is therefore empty.

## Project Structure

### Documentation (this feature)

```text
specs/3594-distributed-tracing/
├── spec.md                                  # Existing — feature specification
├── plan.md                                  # This file
├── research.md                              # Phase 0 — AF4→AF5 concern mapping, decoration design, ProcessingContext lifecycle binding, propagation
├── contracts/
│   └── public-api.md                        # Phase 1 — Public API of axoniq-tracing-core + axoniq-tracing-opentelemetry + autoconfig
├── quickstart.md                            # Phase 1 — Spring Boot + plain Java getting-started
└── tasks.md                                 # Phase 2 — /speckit-tasks output (NOT created by this command)
```

Per the user's instruction, **no `data-model.md`** is generated for this feature — tracing has no domain data model.

### Source Code (repository root)

```text
# NEW: top-level tracing/ directory — siblings: tracing-core + tracing-opentelemetry
tracing/
├── axoniq-tracing-core/
│   ├── pom.xml                              # artifactId: axoniq-tracing-core, parent: axoniq-framework-parent
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
│       │   ├── TracingConfigurationEnhancer.java         # PUBLIC — sole ConfigurationEnhancer; registers all DecoratorDefinitions
│       │   ├── attributes/
│       │   │   ├── package-info.java
│       │   │   ├── MessageIdSpanAttributesProvider.java
│       │   │   ├── MessageNameSpanAttributesProvider.java
│       │   │   ├── MessageTypeSpanAttributesProvider.java
│       │   │   ├── PayloadTypeSpanAttributesProvider.java
│       │   │   ├── MetadataSpanAttributesProvider.java
│       │   │   └── AggregateIdentifierSpanAttributesProvider.java
│       │   └── internal/                    # @Internal — all per-component decorators live here
│       │       ├── package-info.java
│       │       ├── TracingCommandBus.java
│       │       ├── TracingEventSink.java                  # wraps EventSink/EventBus dispatch side
│       │       ├── TracingEventHandlingComponent.java     # wraps the handling side
│       │       ├── TracingQueryBus.java
│       │       ├── TracingQueryUpdateEmitter.java
│       │       ├── TracingRepository.java                 # modelling
│       │       ├── TracingStateManager.java               # modelling
│       │       ├── TracingSnapshotter.java                # event sourcing
│       │       ├── TracingSagaManager.java                # sagas / process-managers — registered only when AF5 port exists
│       │       ├── TracingHandlerEnhancerDefinition.java  # wraps @CommandHandler/@EventHandler/@QueryHandler/@EventSourcingHandler/@SagaEventHandler
│       │       ├── SpanNames.java                         # @Internal constants ("CommandBus.dispatchCommand", "EventBus.publishEvent", …)
│       │       └── ProcessingContextSpanBinding.java      # @Internal — binds Span open/close to ProcessingLifecycle hooks
│       └── test/java/io/axoniq/framework/tracing/
│           ├── NoOpSpanFactoryTest.java
│           ├── MultiSpanFactoryTest.java
│           ├── TracingHandlerEnhancerDefinitionTest.java
│           ├── attributes/                  # one test per provider
│           ├── internal/                    # one test per decorator (TracingCommandBusTest, TracingQueryBusTest, …)
│           └── support/
│               └── TestSpanFactory.java     # recording test double (no Mockito); ports from AF4's TestSpanFactory
│
└── axoniq-tracing-opentelemetry/
    ├── pom.xml                              # artifactId: axoniq-tracing-opentelemetry; depends on axoniq-tracing-core + io.opentelemetry:opentelemetry-api
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
    │   ├── TracingAutoConfiguration.java                # NEW — wires SpanFactory + providers + TracingConfigurationEnhancer
    │   └── OpenTelemetryTracingAutoConfiguration.java   # NEW — promotes OpenTelemetrySpanFactory over NoOp when OTel is on the classpath
    ├── java/io/axoniq/framework/springboot/
    │   └── TracingProperties.java                       # NEW — @ConfigurationProperties("axon.tracing")
    └── resources/META-INF/spring/
        └── org.springframework.boot.autoconfigure.AutoConfiguration.imports   # APPEND the two new classes

# EDITED: existing module — pom only, add the two new modules + OpenTelemetry BOM
build/parent/pom.xml
                                            # ADD <opentelemetry.version> property
                                            # ADD io.opentelemetry:opentelemetry-bom import to dependencyManagement
                                            # (the two new modules are registered in the root aggregator pom.xml, not here)

# EDITED: root aggregator
pom.xml                                     # ADD <module>tracing/axoniq-tracing-core</module> and <module>tracing/axoniq-tracing-opentelemetry</module>

# EDITED: existing BOM
axoniq-framework-bom/pom.xml                # ADD axoniq-tracing-core + axoniq-tracing-opentelemetry as dependencyManagement entries

# EDITED: docs — module directory already exists at docs/reference-guide/modules/tracing/
docs/reference-guide/modules/tracing/
├── nav.adoc                                # update navigation
└── pages/
    ├── index.adoc                          # NEW — tracing overview, adapted from AF4 docs/old-reference-guide/.../tracing.adoc
    ├── opentelemetry.adoc                  # NEW — OpenTelemetry setup, exporters, propagation
    ├── customisation.adoc                  # NEW — custom SpanFactory, custom SpanAttributesProvider, per-component opt-outs
    └── examples.adoc                       # NEW — Spring Boot + plain Java worked examples

# EDITED: example app (deferred-friendly — only if examples/ subtree exists at implementation time)
examples/<chosen-example>/                  # add a dependency on axoniq-tracing-opentelemetry + OTel exporter; show a working trace flow

# EDITED: integration tests — single existing pom, add tests
integrationtests/src/test/java/io/axoniq/framework/tracing/
├── TracingEndToEndIntegrationTest.java     # exercises command → event → query, asserts span tree
└── TracingPropagationIntegrationTest.java  # exercises remote-bus-style propagation through message metadata (covers SC-009 with an in-process simulation if AxonServer connector isn't available in the test env)

# DELETED later, in a separate PR against /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5
# (full deletion checklist captured in research.md and lifted into tasks.md by /speckit-tasks)
```

**Structure Decision**: Two new flat-sibling Maven modules under a new top-level `tracing/` directory (`tracing/axoniq-tracing-core`, `tracing/axoniq-tracing-opentelemetry`), following the existing per-concern top-level convention already used by `messaging/`, `connector/`, `dependency-injection/`, and `testing/`. Spring Boot wiring is added to the **existing** `dependency-injection/spring/spring-boot-autoconfigure` module rather than a new tracing-specific autoconfig module, matching how dead-letter / postgres / axon-server autoconfig are already shipped from the same place.

## Implementation Phases (handed off to `/speckit-tasks`)

The phases below are the structural backbone the task generator will follow. They map 1:1 to the User Stories in `spec.md`.

1. **P1 — Foundations**: parent POM updates, the two new Maven modules' scaffolding (`pom.xml`, `package-info.java` with `@NullMarked`, root aggregator wiring, BOM entries). No production code yet — this lands the empty modules so the build is green before any classes arrive.
2. **P2 — Core abstractions**: `SpanFactory`, `Span`, `SpanScope`, `SpanAttributesProvider`, `NoOpSpanFactory`, `MultiSpanFactory`, `LoggingSpanFactory`, and the six built-in attribute providers under `attributes/`. Unit tests with `TestSpanFactory` ported from AF4.
3. **P3 — Per-component decorators + `TracingConfigurationEnhancer`** (Story 4 consolidation): all `internal/Tracing*.java` decorators, the `SpanNames` constants table, the `ProcessingContextSpanBinding` helper, and `TracingConfigurationEnhancer` registering them all via `DecoratorDefinition`. Per-decorator unit tests, plus a public-API surface test that fails if anyone introduces a `*BusSpanFactory` interface (SC-003).
4. **P4 — OpenTelemetry module**: `OpenTelemetrySpanFactory`, `OpenTelemetrySpan`, `MetadataContextSetter`, `MetadataContextGetter`. Tests cover the text-map round-trip on `MetaData` and the propagation invariants.
5. **P5 — Spring Boot autoconfig**: `TracingProperties`, `TracingAutoConfiguration`, `OpenTelemetryTracingAutoConfiguration`, `AutoConfiguration.imports` entries. Integration-style Boot test (`@SpringBootTest`) that asserts a default Boot context registers tracing decorators and that `axon.tracing.enabled=false` disables them.
6. **P6 — End-to-end + propagation integration tests**: `TracingEndToEndIntegrationTest` (command → event → query through an OpenTelemetry SDK with `InMemorySpanExporter`, asserting the span tree shape) and `TracingPropagationIntegrationTest` (round-trip trace context through message metadata). Equivalence assertion against an AF4 reference-shape (SC-003a).
7. **P7 — Example application** (Story 1): wire tracing into one example under `examples/` with a working exporter (logging or Jaeger via Testcontainers) so `./mvnw -Pexamples clean verify` exercises a real trace. If no suitable example exists in the repo at implementation time, this phase scaffolds a minimal `examples/tracing-springboot/` instead.
8. **P8 — Documentation** (Story 5): populate `docs/reference-guide/modules/tracing/pages/*.adoc` adapted from AF4's `docs/old-reference-guide/modules/monitoring/pages/tracing.adoc`, updated for AF5 terminology, the consolidated `SpanFactory` API, and the `DecoratorDefinition`-based wiring. Antora builds clean.
9. **P9 — Cleanup of upstream Axon Framework 5** (Story 6): a separate PR against `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5` deleting all dead tracing source listed in `research.md` (≈60 files plus the `extensions/tracing/` module and the `stash/todo` placeholders), and asserting `./mvnw clean verify` is green afterwards. This phase runs **after** P1–P8 are merged into AxoniqFramework.

Each phase's tasks file (produced by `/speckit-tasks`) inherits the constitutional gates above and the FR / SC cross-references from `spec.md`.

## Complexity Tracking

> Filled ONLY if Constitution Check has violations that must be justified.

*No violations. This section is intentionally empty.*

---

**Out-of-band notes** (resolved during planning, captured for completeness):

- The spec mentions `examples/university-java-springboot4` as a candidate to host the demo. As of plan time, the `examples/` subtree in AxoniqFramework is empty (parent pom only). P7 keeps the choice flexible: use whichever example exists at implementation time, or scaffold a minimal one.
- AxoniqFramework currently uses Spring Boot 3.5.x (`spring-boot.version=3.5.14` in `build/parent/pom.xml`), not Spring Boot 4. The autoconfig style matches the existing Spring Boot 3 conventions in `dependency-injection/spring/spring-boot-autoconfigure`. If/when the framework moves to Spring Boot 4, only the autoconfig annotations may need a sweep — not the tracing core.
- AF5 already ships a class called `TracingCommandBus` in `messaging/src/main/java/org/axonframework/messaging/commandhandling/tracing/TracingCommandBus.java`. The plan's `TracingCommandBus` lives under `io.axoniq.framework.tracing.internal` (different package + different module). No collision.
- Cross-process propagation (SC-009) is exercised end-to-end against the Axon Server connector if available in the integration-tests profile; otherwise the test simulates the two sides in-process by routing a `CommandMessage` through a serialise-deserialise step that strips/rebuilds metadata. This still validates the propagation contract (FR-015) deterministically.
