---
description: "Task list for Distributed Tracing Support (feature 3594)"
---

# Tasks: Distributed Tracing Support

**Input**: Design documents from `/specs/3594-distributed-tracing/`

**Prerequisites**: plan.md (required), spec.md (required), research.md, contracts/public-api.md, af4-span-inventory.md, flows.md, research-batch-tracing.md

**Tests**: TDD is MANDATORY for this feature (spec.md FR-022a). Every production-code task in P2–P9 is preceded by a failing test, written by invoking the `test-driven-development` skill and porting the AF4 equivalent test first. P1 (scaffolding) and P10 (deletion) are TDD-exempt.

**No-ThreadLocal / cross-thread test (MANDATORY — plan.md "Mandatory workflow: no-ThreadLocal / cross-thread context test")**: any task adding a `Tracing*` decorator that nests or propagates spans via `ProcessingContext` MUST include a test that creates the child span on a **different thread** sharing the same `ProcessingContext` (real `UnitOfWorkTestUtils.aUnitOfWork()`) and asserts the spans still connect — written so it would FAIL against a `Context.current()` (thread-local) implementation. The decorator is not "done" without it. Reference: `OpenTelemetrySpanFactoryTest$CrossThreadNestingViaRealUnitOfWork`, `SliceCommandBusTracingIntegrationTest#handlerExecutedOnADifferentThreadStillNestsUnderTheDispatchSpan`.

**Real-Axon-Server cross-process test (MANDATORY for distributed-capable components — plan.md "Mandatory workflow: real-Axon-Server cross-process test")**: every slice whose component can be distributed over Axon Server (command/query buses, query-update emitter, event publication/handling) MUST ship a Testcontainers IT against a real Axon Server asserting the trace continues across the gRPC hop (producing + remote consuming spans share a `traceId`). This is the only check that proves the AF5 connector carries the W3C `traceparent` on message metadata over the wire (AF4 documented it does). Template: `SliceCommandBusTracingAxonServerIT` + `TracingAxonServerTestInfrastructure`. Done for commands (Slice 1); REQUIRED for events (Slice 2, T051a), queries (Slice 3, T057a), query updates (Slice 4, T062a) — written WITH their slice, since the decorators don't exist before then.

**Per-slice human-validation gate (FR-022b)**: Slices P3–P8 each end with a STOP task. The next slice MUST NOT start until the user has accepted the previous slice's span tree.

**Organization**: Phases map 1:1 to plan.md's P1–P10. Story labels: [US1] = operator-facing tracing (spec Story 1), [US4] = SpanFactory consolidation (spec Story 4), [US5] = docs (spec Story 5), [US6] = upstream cleanup (spec Story 6).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: User story association (US1/US4/US5/US6); omitted for Setup/Foundational/Polish
- File paths are relative to repo root unless absolute.

## Progress (session 2026-05-27)

- **P1 (T001–T010): DONE** — five modules build + install; parent OTel BOM, aggregator, framework BOM wired.
- **P2 (T011–T037): DONE** — api core (Span/SpanScope/SpanFactory/SpanAttributesProvider), NoOp/Multi/Logging factories, 6 attribute providers, SpanNames/ProcessingContextSpanBinding/MetadataContextPropagator helpers, TestSpanFactory, PublicApiSurfaceTest, full OpenTelemetry module (OpenTelemetrySpanFactory + W3C setter/getter), and the Spring autoconfig skeleton (TracingProperties + TracingAutoConfiguration + OpenTelemetryTracingAutoConfiguration + imports) — all tests green.
- **P3 (T038–T044): DONE** — `TracingCommandBus` + `TracingHandlerEnhancerDefinition` implemented and unit-tested; `MessagingTracingConfigurationEnhancer` (ServiceLoader) registers the CommandBus decorator; `SliceCommandBusTracingIntegrationTest` passes against the real OpenTelemetry SDK (dispatch PRODUCER span → child handle CONSUMER span, same trace, message attributes; NoOp = no spans). Committed in 3 scoped commits. **T045 = human-validation gate (Slice 1) — accepted 2026-05-27.**
- **Cross-process tracing over real Axon Server (2026-05-27): command bus DONE** — `SliceCommandBusTracingAxonServerIT` proves a command dispatched through a real Axon Server (Testcontainers) produces dispatch + handle spans sharing one trace across the gRPC hop (W3C `traceparent` on metadata; matches AF4 docs). Events/queries/query-updates are REQUIRED to add the analogous IT in their slices (T051a / T057a / T062a) — they have no tracing decorator yet, so the tests land with those slices.
- **No-ThreadLocal redesign (2026-05-27): DONE** — removed all reliance on OpenTelemetry's thread-local `Context.current()` / `makeCurrent()`. Parents now resolve from message metadata (cross-boundary) and from the active OTel `Context` stored as a `ProcessingContext` resource (in-process); `Span.start()` records/restores that active context instead of `makeCurrent()`. API consequences: `createInternalSpan` gained a `@Nullable ProcessingContext` (B); new `createRootSpan(String, @Nullable ProcessingContext)` (C); `propagateContext` moved `SpanFactory` → `Span`; `MetadataContextPropagator.inject(ProcessingContext)`. Validated by an OTel unit test proving in-process nesting purely through the `ProcessingContext` resource (no thread-local). Confirmed as the OTel-maintainer-recommended reactive pattern (sources in spec.md clarification 2026-05-27). Committed separately. The `io.opentelemetry.context.Context` *value object* is retained as an explicit carrier (it is not a ThreadLocal).
- **Attribute keys (Option B, 2026-05-27): DONE** — OTel-style dotted `axoniq.*` keys (snake_case leaves, `payload_type`); each provider's key/prefix overridable via constructor to restore AF4 keys. Option C (OTel `messaging.*` semantic conventions) deferred.
- **Resolved at the gate (2026-05-27):**
  - *Span kinds:* keep AF4's OpenTelemetry binding (dispatch → PRODUCER, handler → CONSUMER, internal → INTERNAL). The `CLIENT`/`SERVER` wording in the planning docs was inaccurate to AF4 and has been corrected (quickstart.md, af4-span-inventory.md §1, plan.md P3). See spec.md clarification 2026-05-27.
  - *Per-method `@CommandHandler` enhancer span:* **deferred to Slice 2 (P4.3a)**, by user decision. `TracingHandlerEnhancerDefinition` (`@CommandHandler`) is implemented + unit-tested in Slice 1 but its **auto-wiring** moves to Slice 2, because AF5 has no config-aware enhancer-registration API — the only seam is a global `HandlerDefinition` component, best introduced once in the `@EventHandler` slice. **That Slice-2 wiring activates the per-method span for `@CommandHandler` AND `@EventHandler`** (the enhancer self-gates per message type); Slice 2 re-verifies the `@CommandHandler` span. Full research/approach/limitations + AF4 differences: plan.md appendix "Handler-enhancer auto-wiring in AF5", spec.md FR-003b + clarification 2026-05-27. Slice-1 integration assertion (c) is intentionally not asserted until P4.3a lands.

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

- [ ] T011 [P] [US4] Port AF4 `Span` → `Span` interface (start/addAttribute/recordException/**propagateContext**/run/runSupplier/runSupplierAsync) in `tracing/axoniq-tracing-api/src/main/java/io/axoniq/framework/tracing/Span.java`. **No ThreadLocal:** `start()` records the span as active on its creation `ProcessingContext`; `propagateContext(Message)` injects the span's *own* context (moved off `SpanFactory`). See spec.md clarification 2026-05-27.
- [ ] T012 [P] [US4] Port AF4 `SpanScope` → `SpanScope` (extends AutoCloseable, `span()`) in `.../tracing/SpanScope.java`.
- [ ] T013 [US4] Define `SpanFactory` interface per contracts/public-api.md §1.1 (createDispatchSpan/createHandlerSpan/createLinkedHandlerSpan/**createInternalSpan(String, @Nullable ProcessingContext)**/**createRootSpan(String, @Nullable ProcessingContext)**/registerAttributesProvider — **no** `propagateContext`, **no** `Context.current()`; parents resolve from message metadata + the active span on the `ProcessingContext`) in `.../tracing/SpanFactory.java`.
- [ ] T014 [P] [US4] Define `SpanAttributesProvider` SPI (`Map<String,String> provideForMessage(Message<?>, @Nullable ProcessingContext)`) in `.../tracing/SpanAttributesProvider.java`.
- [ ] T015 [US1] TDD: port AF4 `NoOpSpanFactoryTest`, then implement `NoOpSpanFactory` (singleton `INSTANCE`, no-op spans) in `.../tracing/NoOpSpanFactory.java`.
- [ ] T016 [US1] TDD: port AF4 `MultiSpanFactoryTest`, then implement `MultiSpanFactory(List<SpanFactory>)` in `.../tracing/MultiSpanFactory.java`.
- [ ] T017 [US1] TDD: implement `LoggingSpanFactory` (SLF4J) + test in `.../tracing/LoggingSpanFactory.java`.
- [ ] T018 [US1] Port AF4 `TestSpanFactory` recording double (no Mockito) into test sources `tracing/axoniq-tracing-api/src/test/java/io/axoniq/framework/tracing/support/TestSpanFactory.java`; adapt to the new `SpanFactory` signature.

### axoniq-tracing-api — built-in attribute providers (each TDD, one test per provider)

> **Attribute-key convention (Option B, decision 2026-05-27):** default keys are OTel-style dotted `axoniq.*` with snake_case leaves (`axoniq.message.id`, `axoniq.message.name`, `axoniq.message.type`, `axoniq.message.payload_type`, `axoniq.metadata.<key>`, `axoniq.aggregate.identifier`). **Every provider takes a constructor that overrides its attribute key** (for `MetadataSpanAttributesProvider`, its prefix), so AF4 keys (`axon_message_name`, …) can be restored. Option C (full OTel `messaging.*` semantic conventions) is a deferred future improvement. See `contracts/public-api.md` §1.7 + `spec.md` clarification 2026-05-27.

- [ ] T019 [P] [US1] `MessageIdSpanAttributesProvider` — default key `axoniq.message.id` + key-override ctor (+test).
- [ ] T020 [P] [US1] `MessageNameSpanAttributesProvider` — default key `axoniq.message.name` + key-override ctor (+test).
- [ ] T021 [P] [US1] `MessageTypeSpanAttributesProvider` — default key `axoniq.message.type` + key-override ctor (+test).
- [ ] T022 [P] [US1] `PayloadTypeSpanAttributesProvider` — default key `axoniq.message.payload_type` (snake_case) + key-override ctor (+test).
- [ ] T023 [P] [US1] `MetadataSpanAttributesProvider` — default prefix `axoniq.metadata.` + prefix-override and `Set<String>` allowlist ctors (+test).
- [ ] T024 [P] [US1] `AggregateIdentifierSpanAttributesProvider` sourcing `LegacyResources.AGGREGATE_IDENTIFIER_KEY`, null-context-safe, no `DomainEventMessage` reference, key-override ctor (+test).

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

**Goal**: A dispatched command produces dispatch + handler spans with correct kinds/attributes/propagation; togglable via `axon.tracing.commandBus.enabled`. (The per-method `@CommandHandler` enhancer span is implemented + unit-tested here but auto-wired in Slice 2 — see P4.3a / spec.md FR-003b.)

**Independent Test**: `./mvnw -Pintegration-test verify -pl integrationtests -Dtest=SliceCommandBusTracingIntegrationTest` shows the expected command span tree.

- [x] T038 [US1] TDD (P3.1): port AF4 `DefaultCommandBusSpanFactoryTest` → `TracingCommandBusTest` (behaviour-only, AssertJ + `@Nested`, recording `TestSpanFactory`): dispatch span (PRODUCER), handler span (CONSUMER), W3C metadata propagation, span lifecycle bound to `ProcessingContext`. In `tracing/axoniq-tracing-messaging/src/test/java/io/axoniq/framework/tracing/messaging/TracingCommandBusTest.java`.
- [x] T039 [US1] (P3.1) Implement `TracingCommandBus implements CommandBus` (delegate field + `SpanFactory`, `describeWrapperOf(delegate)`, dispatch span via `runSupplierAsync`, handler span via `ProcessingContextSpanBinding`). In `tracing/axoniq-tracing-messaging/src/main/java/io/axoniq/framework/tracing/messaging/internal/TracingCommandBus.java`.
- [x] T040 [US4] TDD (P3.2): `TracingHandlerEnhancerDefinitionTest`, `@Nested CommandHandlerEnhancement` + `NonCommandHandlersAreNotEnhanced` (eager-name guard: a non-command member is returned unwrapped and its `Executable`/name machinery is never invoked). In `.../messaging/TracingHandlerEnhancerDefinitionTest.java`.
- [x] T041 [US4] (P3.2) Implement `TracingHandlerEnhancerDefinition` covering `@CommandHandler` only, gating at wrap time (`canHandleMessageType(CommandMessage.class)`) so the reflective name is built only for traced handlers. In `.../messaging/internal/TracingHandlerEnhancerDefinition.java`. **NB: auto-wiring into the pipeline is deferred to Slice 2 / T050a (see spec.md FR-003b).**
- [x] T042 [US1] (P3.3) Add nested `CommandBus` toggle (`axon.tracing.command-bus.enabled`) to `TracingProperties`; wire it via the autoconfig `ConfigurationEnhancer` bean (registers `MessagingTracingSettings`); unit-test via Spring `ApplicationContextRunner` (`TracingAutoConfigurationTest`).
- [x] T043 [US1] (P3.4) Create `MessagingTracingConfigurationEnhancer` (@Internal) registering `DecoratorDefinition.forType(CommandBus.class)` only (reads `SpanFactory` + `MessagingTracingSettings`, skips when NoOp/disabled); add `META-INF/services/org.axonframework.common.configuration.ConfigurationEnhancer` entry. Add `SpanNames` command-bus names. **(`HandlerEnhancerDefinition` NOT registered here — deferred to T050a.)**
- [x] T044 [US1] (P3.5) Integration test `SliceCommandBusTracingIntegrationTest` (real OpenTelemetry SDK + `InMemorySpanExporter`): asserts (a) dispatch span `PRODUCER`, (b) child handler span `CONSUMER` (same trace, parent = dispatch), **(c) DEFERRED to Slice 2 / T050a — `@CommandHandler` per-method enhancer span**, (d) `NoOpSpanFactory` → no spans. Attributes checked against af4-span-inventory.md §1. In `integrationtests/src/test/java/io/axoniq/framework/tracing/slice/SliceCommandBusTracingIntegrationTest.java`.
- [x] T045 [US1] (P3.6) **Human validation gate for Slice 1 — ACCEPTED 2026-05-27.** Committed in 3 scoped commits; span tree posted; kinds confirmed (AF4 PRODUCER/CONSUMER); enhancer auto-wiring deferred to Slice 2.

**Checkpoint**: Slice 1 (TracingCommandBus) tested and working — MVP increment.

---

## Phase 4: Slice 2 — TracingEventSink + TracingEventHandlingComponent + @EventHandler (plan P4) — [US1]

- [ ] T046 [US1] TDD + impl `TracingEventSink` (two-span publish/commit, UoW-bound commit span, `Span.run` fallback when context null). (P4.1)
- [ ] T047 [US1] TDD + impl `TracingEventHandlingComponent` (per-event CONSUMER span; lazy batch span gated on `Segment.RESOURCE_KEY`). (P4.2)
- [ ] T048 [US4] Extend `TracingHandlerEnhancerDefinition` with `@EventHandler` (`@Nested EventHandlerEnhancement`); widen the wrap-time gate to also accept event handlers. (P4.3)
- [ ] T050a [US4] **Auto-wire `TracingHandlerEnhancerDefinition` into the live annotation pipeline (deferred from Slice 1; plan P4.3a, spec FR-003b).** Register a `HandlerDefinition` framework component that bundles a `SpanFactory`-bearing `TracingHandlerEnhancerDefinition` on top of the AF5 defaults — `MultiHandlerDefinition.ordered(MultiHandlerEnhancerDefinition.ordered(ClasspathHandlerEnhancerDefinition.forClassLoader(cl), new TracingHandlerEnhancerDefinition(spanFactory)), ClasspathHandlerDefinition.forClassLoader(cl))` — via the messaging tracing enhancer / autoconfig `ConfigurationEnhancer`. Prefer composing on top of any existing `HandlerDefinition` over replacing it (see plan appendix limitation 2). **This single registration activates the per-method span for `@CommandHandler` (Slice 1) AND `@EventHandler` (Slice 2)** — re-enable the Slice-1 integration assertion (c) and assert both per-method spans here. Unit-test that the component is registered and composes with (does not drop) the standard classpath handler definitions/enhancers. AF4 difference + limitations documented in plan.md appendix "Handler-enhancer auto-wiring in AF5".
- [ ] T049 [US1] Add eventSink/eventProcessor toggles (`disableBatchTrace`, `distributedInSameTrace`, `distributedInSameTraceTimeLimit=PT2M`) to `TracingProperties`. (P4.4)
- [ ] T050 [US1] Register `DecoratorDefinition.forType(EventSink.class)` + `forType(EventHandlingComponent.class)` (scope-guarded on `EventProcessorConfiguration` presence) in `MessagingTracingConfigurationEnhancer`. (P4.5)
- [ ] T051 [US1] Focused Boot integration test `SliceEventTracingIntegrationTest` (PSEP batch root, SEP no-batch parity, cross-thread W3C propagation, toggles); **also assert the `@EventHandler` and (retroactively) `@CommandHandler` per-method enhancer spans now appear via T050a**. (P4.6)
- [ ] T051a [US1] Real-Axon-Server cross-process IT `SliceEventTracingAxonServerIT`: publish an event over the Axon Server connector; assert the consuming handler span shares the publishing trace per `distributedInSameTrace` semantics. Mirrors `SliceCommandBusTracingAxonServerIT`/`TracingAxonServerTestInfrastructure`. (P4.6a)
- [ ] T052 [US1] **STOP — Human validation gate for Slice 2.** (P4.7)

---

## Phase 5: Slice 3 — TracingQueryBus + @QueryHandler (plan P5) — [US1]

- [ ] T053 [US1] TDD + impl `TracingQueryBus` (direct/scatter-gather/subscription-initial dispatch+handle spans). (P5.1)
- [ ] T054 [US4] Extend `TracingHandlerEnhancerDefinition` with `@QueryHandler`. (P5.2)
- [ ] T055 [US1] Add `axon.tracing.queryBus.enabled` toggle. (P5.3)
- [ ] T056 [US1] Register `DecoratorDefinition.forType(QueryBus.class)`. (P5.4)
- [ ] T057 [US1] Focused test `SliceQueryBusTracingIntegrationTest`. (P5.5)
- [ ] T057a [US1] Real-Axon-Server cross-process IT `SliceQueryBusTracingAxonServerIT`: dispatch a query over the Axon Server connector; assert the remote handler span shares the dispatch trace. Mirrors `SliceCommandBusTracingAxonServerIT`. (P5.5a)
- [ ] T058 [US1] **STOP — Human validation gate for Slice 3.** (P5.6)

---

## Phase 6: Slice 4 — TracingQueryUpdateEmitter (plan P6) — [US1]

- [ ] T059 [US1] TDD + impl `TracingQueryUpdateEmitter` (schedule+emit two-span pattern; complete/completeExceptionally single spans; `createLinkedHandlerSpan` link to originating query). (P6.1)
- [ ] T060 [US1] Add `axon.tracing.queryUpdateEmitter.enabled` toggle. (P6.2)
- [ ] T061 [US1] Register `DecoratorDefinition.forType(QueryUpdateEmitter.class)`. (P6.3)
- [ ] T062 [US1] Focused test `SliceQueryUpdateEmitterTracingIntegrationTest` (subscription query end-to-end). (P6.4)
- [ ] T062a [US1] Real-Axon-Server cross-process IT `SliceQueryUpdateEmitterTracingAxonServerIT`: subscription-query updates traversing Axon Server; assert update spans share/link the originating query's trace. Mirrors `SliceCommandBusTracingAxonServerIT`. (P6.4a)
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
