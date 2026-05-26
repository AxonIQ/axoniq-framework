---
description: "Task list for Distributed Tracing Support (feature 3594)"
---

# Tasks: Distributed Tracing Support

**Input**: Design documents from `/specs/3594-distributed-tracing/`

**Prerequisites**: plan.md (required), spec.md (required), research.md, contracts/public-api.md, af4-span-inventory.md, flows.md, research-batch-tracing.md

**Tests**: TDD is MANDATORY for this feature (spec.md FR-022a). Every production-code task in P2–P9 is preceded by a failing test, written by invoking the `test-driven-development` skill and porting the AF4 equivalent test first. P1 (scaffolding) and P10 (deletion) are TDD-exempt.

**Per-slice human-validation gate (FR-022b)**: Slices P3–P8 each end with a STOP task. The next slice MUST NOT start until the user has accepted the previous slice's span tree.

**Organization**: Phases map 1:1 to plan.md's P1–P10. Story labels: [US1] = operator-facing tracing (spec Story 1), [US4] = SpanFactory consolidation (spec Story 4), [US5] = docs (spec Story 5), [US6] = upstream cleanup (spec Story 6).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: User story association (US1/US4/US5/US6); omitted for Setup/Foundational/Polish
- File paths are relative to repo root unless absolute.

## Progress (session 2026-05-27)

- **P1 (T001–T010): DONE** — five modules build + install; parent OTel BOM, aggregator, framework BOM wired.
- **P2 (T011–T032): DONE** — api core (Span/SpanScope/SpanFactory/SpanAttributesProvider), NoOp/Multi/Logging factories, 6 attribute providers, SpanNames/ProcessingContextSpanBinding/MetadataContextPropagator helpers, TestSpanFactory, PublicApiSurfaceTest, full OpenTelemetry module (OpenTelemetrySpanFactory + W3C setter/getter) — all tests green. **T033–T037 (autoconfig skeleton): in progress.**
- **P3 (T038–T044): core DONE** — `TracingCommandBus` + `TracingHandlerEnhancerDefinition` implemented and unit-tested; `MessagingTracingConfigurationEnhancer` (ServiceLoader) registers the CommandBus decorator; `SliceCommandBusTracingIntegrationTest` passes against the real OpenTelemetry SDK (dispatch PRODUCER span → child handle CONSUMER span, same trace, message attributes; NoOp = no spans). **T045 = human-validation gate (Slice 1).**
- **Known gap flagged for the gate:** AF5's `AnnotatedHandlerInspector` does not apply `HandlerEnhancerDefinition`s, and config-dependent enhancers (needing a `SpanFactory`) are not cleanly pluggable through the current AF5 annotation pipeline (AF5's own dead tracing code never solved this). So the per-method `@CommandHandler` enhancer span (P3.2) is implemented + unit-tested but NOT auto-wired end-to-end yet — needs a user decision (options: ServiceLoader + config-populated holder; or wait for an AF5 enhancer-injection point). The dispatch + handle spans (the TracingCommandBus headline) work without it.
- **Span-kind note for the gate:** kinds follow AF4's OpenTelemetry binding (dispatch → PRODUCER, handler → CONSUMER, internal → INTERNAL), i.e. the messaging convention — not the CLIENT/SERVER wording in some planning docs. Confirm acceptable.

## Authority note

Where `contracts/public-api.md` and `plan.md` disagree, **plan.md is authoritative** (it reflects the 2026-05-26 five-module clarification). Concretely: there is **no public `TracingConfigurationEnhancer`**; each per-concern decorator module ships its own `@Internal` `ConfigurationEnhancer` discovered via ServiceLoader. The `PublicApiSurfaceTest` enumerates exactly the 7 public types `SpanFactory`, `Span`, `SpanScope`, `SpanAttributesProvider`, `NoOpSpanFactory`, `MultiSpanFactory`, `LoggingSpanFactory` (no enhancer).

---

## Phase 1: Setup — Foundations (plan P1) — TDD-EXEMPT

**Purpose**: Land empty, buildable modules + parent/BOM/aggregator wiring so the build is green before any class arrives.

- [ ] T001 Add `<opentelemetry.version>` property and import `io.opentelemetry:opentelemetry-bom` into `<dependencyManagement>` in `build/parent/pom.xml`.
- [ ] T002 [P] Create `tracing/axoniq-tracing-api/pom.xml` (artifactId `axoniq-tracing-api`, parent `axoniq-framework-parent`; depends on `org.axonframework:axon-messaging`; test deps: axon-messaging test-jar, junit/assertj/awaitility from parent; Automatic-Module-Name `io.axoniq.framework.tracing`).
- [ ] T003 [P] Create `tracing/axoniq-tracing-messaging/pom.xml` (depends on `axoniq-tracing-api` + `axon-messaging`).
- [ ] T004 [P] Create `tracing/axoniq-tracing-modelling/pom.xml` (depends on `axoniq-tracing-api` + `axon-modelling`).
- [ ] T005 [P] Create `tracing/axoniq-tracing-eventsourcing/pom.xml` (depends on `axoniq-tracing-api` + `axon-eventsourcing`).
- [ ] T006 [P] Create `tracing/axoniq-tracing-opentelemetry/pom.xml` (depends on `axoniq-tracing-api` + `io.opentelemetry:opentelemetry-api`; test: opentelemetry-sdk).
- [ ] T007 [P] Add `@NullMarked` `package-info.java` to each new module's root package (`io/axoniq/framework/tracing/`, `.../messaging`, `.../modelling`, `.../eventsourcing`, `.../opentelemetry`, `.../tracing/attributes`).
- [ ] T008 Register the five new modules in root aggregator `pom.xml` (`tracing/axoniq-tracing-api`, `-messaging`, `-modelling`, `-eventsourcing`, `-opentelemetry`).
- [ ] T009 Add all five `axoniq-tracing-*` artifacts as `<dependencyManagement>` entries in `axoniq-framework-bom/pom.xml`.
- [ ] T010 Verify scaffolding builds green: `./mvnw -q -pl tracing/axoniq-tracing-api,tracing/axoniq-tracing-messaging,tracing/axoniq-tracing-modelling,tracing/axoniq-tracing-eventsourcing,tracing/axoniq-tracing-opentelemetry -am install -DskipTests`.

**Checkpoint**: Five empty modules build and install. Foundation wiring complete.

---

## Phase 2: Foundational — Core abstractions + OTel + autoconfig skeleton (plan P2)

**Purpose**: Everything the slices share. BLOCKS all slices. TDD applies (test-first, port AF4 tests).

### axoniq-tracing-api — core types

- [ ] T011 [P] [US4] Port AF4 `Span` → `Span` interface (start/addAttribute/recordException/run/runSupplier/runSupplierAsync) in `tracing/axoniq-tracing-api/src/main/java/io/axoniq/framework/tracing/Span.java`.
- [ ] T012 [P] [US4] Port AF4 `SpanScope` → `SpanScope` (extends AutoCloseable, `span()`) in `.../tracing/SpanScope.java`.
- [ ] T013 [US4] Define `SpanFactory` interface per contracts/public-api.md §1.1 (createDispatchSpan/createHandlerSpan/createLinkedHandlerSpan/createInternalSpan/propagateContext/registerAttributesProvider) in `.../tracing/SpanFactory.java`.
- [ ] T014 [P] [US4] Define `SpanAttributesProvider` SPI (`Map<String,String> provideForMessage(Message<?>, @Nullable ProcessingContext)`) in `.../tracing/SpanAttributesProvider.java`.
- [ ] T015 [US1] TDD: port AF4 `NoOpSpanFactoryTest`, then implement `NoOpSpanFactory` (singleton `INSTANCE`, no-op spans) in `.../tracing/NoOpSpanFactory.java`.
- [ ] T016 [US1] TDD: port AF4 `MultiSpanFactoryTest`, then implement `MultiSpanFactory(List<SpanFactory>)` in `.../tracing/MultiSpanFactory.java`.
- [ ] T017 [US1] TDD: implement `LoggingSpanFactory` (SLF4J) + test in `.../tracing/LoggingSpanFactory.java`.
- [ ] T018 [US1] Port AF4 `TestSpanFactory` recording double (no Mockito) into test sources `tracing/axoniq-tracing-api/src/test/java/io/axoniq/framework/tracing/support/TestSpanFactory.java`; adapt to the new `SpanFactory` signature.

### axoniq-tracing-api — built-in attribute providers (each TDD, one test per provider)

- [ ] T019 [P] [US1] `MessageIdSpanAttributesProvider` (+test) in `.../tracing/attributes/`.
- [ ] T020 [P] [US1] `MessageNameSpanAttributesProvider` (+test).
- [ ] T021 [P] [US1] `MessageTypeSpanAttributesProvider` (+test).
- [ ] T022 [P] [US1] `PayloadTypeSpanAttributesProvider` (+test).
- [ ] T023 [P] [US1] `MetadataSpanAttributesProvider` (allowlist ctor) (+test).
- [ ] T024 [P] [US1] `AggregateIdentifierSpanAttributesProvider` sourcing `LegacyResources.AGGREGATE_IDENTIFIER_KEY`, null-context-safe, no `DomainEventMessage` reference (+test).

### axoniq-tracing-api — public decorator-authoring helpers (DFI-001)

- [ ] T025 [US4] `SpanNames` public constants for command-bus spans (grown per slice) in `.../tracing/SpanNames.java`.
- [ ] T026 [US4] `ProcessingContextSpanBinding` public helper binding Span open/close to `ProcessingLifecycle` hooks; stores `SpanScope` under `ResourceKey<SpanScope>` (+test) in `.../tracing/ProcessingContextSpanBinding.java`.
- [ ] T027 [US4] `MetadataContextPropagator` public SPI (W3C getter/setter contract) in `.../tracing/MetadataContextPropagator.java`.

### axoniq-tracing-api — architectural guard

- [ ] T028 [US4] `PublicApiSurfaceTest` (classpath scan): asserts the exact 7 public types, forbids `*BusSpanFactory`/`*ManagerSpanFactory`/`*ProcessorSpanFactory`/`*EmitterSpanFactory`/`RepositorySpanFactory`/`SagaManagerSpanFactory`/`SnapshotterSpanFactory`/`DeadlineManagerSpanFactory`, and asserts every `…tracing.internal.*` type is package-private or `@Internal`. (Enforces FR-016/SC-003.) in `.../tracing/PublicApiSurfaceTest.java`.

### axoniq-tracing-opentelemetry

- [ ] T029 [US1] TDD: port AF4 OTel span-factory tests; implement `OpenTelemetrySpanFactory` (OpenTelemetry ctor + GlobalOpenTelemetry no-arg) in `tracing/axoniq-tracing-opentelemetry/src/main/java/io/axoniq/framework/tracing/opentelemetry/OpenTelemetrySpanFactory.java`.
- [ ] T030 [US1] `OpenTelemetrySpan` (@Internal) + test.
- [ ] T031 [P] [US1] `MetadataContextSetter` (@Internal W3C TextMapSetter into Message metadata) + test.
- [ ] T032 [P] [US1] `MetadataContextGetter` (@Internal W3C TextMapGetter from Message metadata) + test.

### spring-boot-autoconfigure skeleton

- [ ] T033 [US1] `TracingProperties` skeleton (`axon.tracing.enabled` master switch only; per-component nested options added by their slice) in `dependency-injection/spring/spring-boot-autoconfigure/src/main/java/io/axoniq/framework/springboot/TracingProperties.java`.
- [ ] T034 [US1] `TracingAutoConfiguration` (registers default `SpanFactory` bean — OTel if on classpath else NoOp — plus the six built-in `SpanAttributesProvider` beans + `SpanAttributesProviderRegistrar`; enhancers are ServiceLoader-discovered, not bean-wired) in `.../springboot/autoconfig/TracingAutoConfiguration.java`.
- [ ] T035 [US1] `OpenTelemetryTracingAutoConfiguration` (promotes `OpenTelemetrySpanFactory` when `io.opentelemetry.api.OpenTelemetry` on classpath) in `.../springboot/autoconfig/OpenTelemetryTracingAutoConfiguration.java`.
- [ ] T036 [US1] Append the two new classes to `dependency-injection/spring/spring-boot-autoconfigure/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- [ ] T037 [US1] Smoke `@SpringBootTest` confirming context starts and a `SpanFactory` bean is registered.

**Checkpoint**: Core API, OTel binding, and autoconfig skeleton all build + tests green. Slices can begin.

---

## Phase 3: Slice 1 — TracingCommandBus + @CommandHandler enhancement (plan P3) 🎯 MVP — [US1]/[US4]

**Goal**: A dispatched command produces dispatch + handler spans with correct kinds/attributes/propagation; `@CommandHandler` methods get an enhancer-added child span; togglable via `axon.tracing.commandBus.enabled`.

**Independent Test**: `./mvnw -Pintegration-test verify -pl integrationtests -Dtest=SliceCommandBusTracingIntegrationTest` shows the expected command span tree.

- [ ] T038 [US1] TDD (P3.1): port AF4 `DefaultCommandBusSpanFactoryTest` → `TracingCommandBusTest` (behaviour-only, AssertJ + `@Nested`, recording `TestSpanFactory`), write failing tests for: dispatch span, handler span, CLIENT/SERVER kinds for distributed, INTERNAL for in-process, distributed-vs-in-process branching, W3C metadata propagation via setter/getter, span lifecycle bound to `ProcessingContext`. In `tracing/axoniq-tracing-messaging/src/test/java/io/axoniq/framework/tracing/messaging/TracingCommandBusTest.java`.
- [ ] T039 [US1] (P3.1) Implement `TracingCommandBus implements CommandBus` (delegate field + `SpanFactory`, `describeWrapperOf(delegate)`, lifecycle via `ProcessingContextSpanBinding`) to make T038 pass. In `tracing/axoniq-tracing-messaging/src/main/java/io/axoniq/framework/tracing/messaging/internal/TracingCommandBus.java`.
- [ ] T040 [US4] TDD (P3.2): port AF4 `TracingHandlerEnhancerDefinitionTest`, `@Nested CommandHandlerEnhancement` only; include the **eager-name guard** test (FR-003a): assert `getSpanName(...)` is NOT invoked when disabled/suppressed (recording name builder or throwing name builder). In `.../messaging/TracingHandlerEnhancerDefinitionTest.java`.
- [ ] T041 [US4] (P3.2) Implement `TracingHandlerEnhancerDefinition` covering `@CommandHandler` only, computing the reflective span name only on the span-creating branch (eager-name guard). In `.../messaging/internal/TracingHandlerEnhancerDefinition.java`.
- [ ] T042 [US1] (P3.3) Add `CommandBusOptions` (`axon.tracing.commandBus.enabled`, `distributedInSameTrace`) to `TracingProperties`; wire `commandBus.enabled` into `TracingAutoConfiguration` so CommandBus decoration is skipped when disabled; unit-test the toggle via Spring `ApplicationContextRunner`.
- [ ] T043 [US1] (P3.4) Create `MessagingTracingConfigurationEnhancer` (@Internal) registering `DecoratorDefinition.forType(CommandBus.class)` + the `HandlerEnhancerDefinition`; add `META-INF/services/org.axonframework.common.configuration.ConfigurationEnhancer` entry in `axoniq-tracing-messaging`. Update `SpanNames` for command-bus spans.
- [ ] T044 [US1] (P3.5) Focused Boot integration test `SliceCommandBusTracingIntegrationTest` (`@SpringBootTest` + `InMemorySpanExporter`): asserts (a) dispatch span CLIENT/INTERNAL, (b) child handler span SERVER/INTERNAL, (c) `@CommandHandler` enhancer child span + attributes, (d) `axon.tracing.commandBus.enabled=false` skips decoration. Span name+kind+attributes checked against af4-span-inventory.md §1. In `integrationtests/src/test/java/io/axoniq/framework/tracing/slice/SliceCommandBusTracingIntegrationTest.java`.
- [ ] T045 [US1] (P3.6) **STOP — Human validation gate for Slice 1.** Commit on `feat/3594-DistributedTracing`. Run `./mvnw -Pintegration-test verify -pl integrationtests -Dtest=SliceCommandBusTracingIntegrationTest`. Post the asserted span tree (names, kinds, attributes, parent-child) to the user. Wait for explicit go/no-go before Slice 2.

**Checkpoint**: Slice 1 (TracingCommandBus) tested and working — MVP increment.

---

## Phase 4: Slice 2 — TracingEventSink + TracingEventHandlingComponent + @EventHandler (plan P4) — [US1]

- [ ] T046 [US1] TDD + impl `TracingEventSink` (two-span publish/commit, UoW-bound commit span, `Span.run` fallback when context null). (P4.1)
- [ ] T047 [US1] TDD + impl `TracingEventHandlingComponent` (per-event CONSUMER span; lazy batch span gated on `Segment.RESOURCE_KEY`). (P4.2)
- [ ] T048 [US4] Extend `TracingHandlerEnhancerDefinition` with `@EventHandler` (`@Nested EventHandlerEnhancement`). (P4.3)
- [ ] T049 [US1] Add eventSink/eventProcessor toggles (`disableBatchTrace`, `distributedInSameTrace`, `distributedInSameTraceTimeLimit=PT2M`) to `TracingProperties`. (P4.4)
- [ ] T050 [US1] Register `DecoratorDefinition.forType(EventSink.class)` + `forType(EventHandlingComponent.class)` (scope-guarded on `EventProcessorConfiguration` presence) in `MessagingTracingConfigurationEnhancer`. (P4.5)
- [ ] T051 [US1] Focused Boot integration test `SliceEventTracingIntegrationTest` (PSEP batch root, SEP no-batch parity, cross-thread W3C propagation, toggles). (P4.6)
- [ ] T052 [US1] **STOP — Human validation gate for Slice 2.** (P4.7)

---

## Phase 5: Slice 3 — TracingQueryBus + @QueryHandler (plan P5) — [US1]

- [ ] T053 [US1] TDD + impl `TracingQueryBus` (direct/scatter-gather/subscription-initial dispatch+handle spans). (P5.1)
- [ ] T054 [US4] Extend `TracingHandlerEnhancerDefinition` with `@QueryHandler`. (P5.2)
- [ ] T055 [US1] Add `axon.tracing.queryBus.enabled` toggle. (P5.3)
- [ ] T056 [US1] Register `DecoratorDefinition.forType(QueryBus.class)`. (P5.4)
- [ ] T057 [US1] Focused test `SliceQueryBusTracingIntegrationTest`. (P5.5)
- [ ] T058 [US1] **STOP — Human validation gate for Slice 3.** (P5.6)

---

## Phase 6: Slice 4 — TracingQueryUpdateEmitter (plan P6) — [US1]

- [ ] T059 [US1] TDD + impl `TracingQueryUpdateEmitter` (schedule+emit two-span pattern; complete/completeExceptionally single spans; `createLinkedHandlerSpan` link to originating query). (P6.1)
- [ ] T060 [US1] Add `axon.tracing.queryUpdateEmitter.enabled` toggle. (P6.2)
- [ ] T061 [US1] Register `DecoratorDefinition.forType(QueryUpdateEmitter.class)`. (P6.3)
- [ ] T062 [US1] Focused test `SliceQueryUpdateEmitterTracingIntegrationTest` (subscription query end-to-end). (P6.4)
- [ ] T063 [US1] **STOP — Human validation gate for Slice 4.** (P6.5)

---

## Phase 7: Slice 5 — TracingRepository + TracingStateManager (plan P7) — [US1]

- [ ] T064 [US1] TDD + impl `TracingRepository` (load outer span + nested obtainLock/initializeState; save-side spans). (P7.1)
- [ ] T065 [US1] TDD + impl `TracingStateManager`. (P7.2)
- [ ] T066 [US1] Verify `AggregateIdentifierSpanAttributesProvider` end-to-end (present on legacy aggregate path, absent on DCB/entity path). (P7.3)
- [ ] T067 [US1] Add `axon.tracing.repository.enabled` toggle. (P7.4)
- [ ] T068 [US1] Create `ModellingTracingConfigurationEnhancer` (@Internal) + `META-INF/services` entry; register `Repository.class` + `StateManager.class` decorators in `axoniq-tracing-modelling`. (P7.5)
- [ ] T069 [US1] Focused test `SliceRepositoryTracingIntegrationTest`. (P7.6)
- [ ] T070 [US1] **STOP — Human validation gate for Slice 5.** (P7.7)

---

## Phase 8: Slice 6 — TracingSnapshotStore + @EventSourcingHandler (plan P8) — [US1]

- [ ] T071 [US1] TDD + impl `TracingSnapshotStore implements SnapshotStore` (store/load spans nesting under entity-sourcing span via ProcessingContext). (P8.1)
- [ ] T072 [US1] TDD + impl `TracingEntityLifecycleHandler` source(...) sourcing span (FR-009 parent). (P8.1)
- [ ] T073 [US4] Extend `TracingHandlerEnhancerDefinition` with `@EventSourcingHandler`; hot-path eager-name guard test: with `showEventSourcingHandlers=false`, neither span nor `getSpanName(...)` is invoked. (P8.2)
- [ ] T074 [US1] Add `axon.tracing.snapshotStore.enabled` toggle. (P8.3)
- [ ] T075 [US1] Create `EventSourcingTracingConfigurationEnhancer` (@Internal) + `META-INF/services`; register `SnapshotStore.class` + sourcing-span decorators in `axoniq-tracing-eventsourcing`. (P8.4)
- [ ] T076 [US1] Focused test `SliceSnapshotterTracingIntegrationTest` (store write + load read nest under entity-sourcing span). (P8.5)
- [ ] T077 [US1] **STOP — Human validation gate for Slice 6.** (P8.6)

---

## Phase 9: Batched finalization (plan P9) — [US1]/[US5]

- [ ] T078 [US1] `TracingEndToEndIntegrationTest` — full command→event→query, all six slices active, `InMemorySpanExporter`, connected span tree. (P9.1)
- [ ] T079 [US4] `AF4SpanShapeParityIntegrationTest` — span name+kind for every row in af4-span-inventory.md §2. (P9.2)
- [ ] T080 [US1] `TracingPropagationIntegrationTest` — remote-bus-style metadata propagation (AxonServer connector if available, else in-process serialize/deserialize). (P9.3)
- [ ] T081 [US1] **STOP — Human validation gate before Jaeger.** Commit + run the three tests; post span tree; await go/no-go. (P9.4)
- [ ] T082 [US1] `TracingJaegerIntegrationTest` — Testcontainers `jaegertracing/all-in-one` + OTLP, query Jaeger REST API, `integration-test` profile, auto-skip when Docker absent. (P9.5)
- [ ] T083 [US1] Example app: wire tracing into an `examples/` app with a working exporter (or scaffold `examples/tracing-springboot/`). (P9.6)
- [ ] T084 [US5] Reference-guide docs under `docs/reference-guide/modules/tracing/pages/*.adoc` (index/opentelemetry/customisation/examples), Antora builds clean. (P9.7)

---

## Phase 10: Cleanup of upstream Axon Framework 5 (plan P10) — [US6] — TDD-EXEMPT

- [ ] T085 [US6] In a separate PR against `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5`, delete all dead tracing source (≈60 files + `extensions/tracing/` + `stash/todo` placeholders) per research.md checklist; verify `./mvnw clean verify` green.

---

## Dependencies & Execution Order

- **Phase 1 (Setup)**: no deps.
- **Phase 2 (Foundational)**: depends on Phase 1; BLOCKS all slices.
- **Phase 3 (Slice 1)**: depends on Phase 2.
- **Phases 4–8 (Slices 2–6)**: strictly sequential — each blocked by the previous slice's STOP gate (FR-022b). No parallelization across slices.
- **Phase 9**: depends on Slices 1–6.
- **Phase 10**: depends on Phases 1–9 merged.

### Within a slice

- Test task before its implementation task (TDD red→green).
- Decorator before its `ConfigurationEnhancer` registration.
- Enhancer registration before the focused integration test.

### Parallel opportunities

- P1: T002–T007 (module poms + package-infos) are [P].
- P2: T011/T012/T014 (independent interfaces) and T019–T024 (attribute providers) are [P]; T031/T032 OTel setters/getters are [P].
- No cross-slice parallelism (gates forbid it).

---

## Implementation Strategy

### Current goal (this run): MVP = Slice 1

1. Phase 1: Setup (T001–T010).
2. Phase 2: Foundational (T011–T037).
3. Phase 3: Slice 1 (T038–T044), then STOP gate T045.

Slices 2–6 and finalization run in later sessions, each behind its human-validation gate.
