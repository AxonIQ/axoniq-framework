# Feature Specification: Distributed Tracing Support

**GitHub Issue**: [#3594](https://github.com/AxonIQ/AxonFramework/issues/3594)

**Feature Branch**: `feat/3594-DistributedTracing`

**Spec Directory**: `specs/3594-distributed-tracing`

**Created**: 2026-05-26

**Status**: Draft

**Input**: Port distributed tracing support from Axon Framework 4 — which is the only place a working implementation currently exists — into AxoniqFramework, using Axon Framework 5 idioms (`DecoratorDefinition`, `ConfigurationEnhancer`) and avoiding the proliferation of bus/component-specific span factory specializations. The tracing source files in upstream Axon Framework 5 are **not** a working or half-working implementation — they were merely relocated into new package layouts (and, where required, commented out) to keep the AF5 build green; they are dead code and are not a viable starting point. Includes Spring Boot auto-configuration, an OpenTelemetry integration module, reference-guide documentation, and a final clean-up phase removing all of those dead tracing files from the upstream Axon Framework 5 repository.

---

## Clarifications

### Session 2026-05-26

- Q: What is the actual state of tracing in upstream Axon Framework 5? → A: Not half-migrated. Files were moved into new package layouts and code was commented out (or otherwise neutralised) just enough to keep AF5 compiling. The AF5 tracing source is dead code: no wiring, no functional behavior, no working autoconfig. AF4 is the only repository with a working implementation, so the port reads from AF4 — AF5 tracing source is treated as scrap to be deleted in the cleanup phase, not as a baseline to build on.
- Q: How should Story 4 / FR-016 consolidate per-bus and per-component span factories? → A: Option A — single generic `SpanFactory` plus **private** per-component `DecoratorDefinition`s registered by one `TracingConfigurationEnhancer`. Per-component span naming, attributes, span kinds (`CLIENT`/`SERVER`/`PRODUCER`/`CONSUMER`/`INTERNAL`), and cross-process metadata-based context propagation all live inside those internal decorators. No per-bus / per-component span-factory interface is part of the public API. Option B (exposing a small public helper SPI — e.g., `SpanNames`, `OperationKinds`, `MetadataContextPropagator` — so advanced users can author their own decorators with identical span shapes) is acknowledged as a **deferred future improvement**, not in scope for this feature; it can be added later without breaking changes by graduating selected internals to public API.
- Q: What is the module structure in AxoniqFramework for the new tracing code? → A: **Two flat sibling modules under a new top-level `tracing/` directory** (mirroring the existing per-concern top-level convention used by `messaging/`, `connector/`, `dependency-injection/`, `testing/`): `tracing/axoniq-tracing-core` (artifact `axoniq-tracing-core`, group `io.axoniq.framework`) and `tracing/axoniq-tracing-opentelemetry` (artifact `axoniq-tracing-opentelemetry`). The core module holds the public abstractions and the private per-component `DecoratorDefinition`s; the OpenTelemetry module holds `OpenTelemetrySpanFactory` and the metadata text-map propagators. Tracing is **not** placed under `messaging/` — it is a cross-cutting concern that legitimately depends on messaging, modelling, event sourcing, and (when ported) sagas (e.g., `TracingHandlerEnhancerDefinition` wraps any handler including `@EventSourcingHandler`, plus repository load/save, snapshot create/read, saga invocation). Spring Boot auto-configuration classes are added to the **existing** `dependency-injection/spring/spring-boot-autoconfigure` module (artifact `axoniq-spring-boot-autoconfigure`); no separate spring-tracing module is created.
- Q: How are the per-component tracing wrappers implemented — what pattern and what hook strategy? → A: **Delegating decorators**, following AF5's `TracingCommandBus` / `InterceptingCommandBus` / `InterceptingQueryBus` pattern: each tracing wrapper class implements the target component interface (`CommandBus`, `EventBus`, `QueryBus`, `QueryUpdateEmitter`, `EventHandlingComponent`, `Repository`, `Snapshotter`, saga manager, etc.) **by composition** — holding a `delegate` reference plus the generic `SpanFactory`, calling through to the delegate, and declaring itself a wrapper via `ComponentDescriptor.describeWrapperOf(delegate)`. Wrappers are registered through `DecoratorDefinition` so they stack cleanly with other decorators (interception, metrics, distributed transport, security). This **explicitly rejects** the AF4 anti-pattern where classes such as `DistributedCommandBus` directly implemented the bus interface without delegation, making it impossible to compose additional concerns without forking. Where the wrapped operation already participates in a `ProcessingContext` (e.g., the AF5 `CommandBus#dispatch(message, ProcessingContext)` signature), span open/close **SHOULD** ride on `ProcessingContext` lifecycle callbacks rather than try/finally / `runSupplier` blocks — so span scope aligns with the framework's lifecycle phases and survives async boundaries naturally. When no `ProcessingContext` is available at the relevant hook point, falling back to a direct `Span.run...` style is acceptable.
- Q: Should deadline scheduling / firing be traced? → A: **No.** `DeadlineManager` is not part of Axon Framework 5 — it never carried over from AF4, so there is no component to wrap. Tracing deadlines is permanently out of scope for this feature. Specifically: no `TracingDeadlineManager` wrapper, no `ifClassPresent("org.axonframework.deadline.DeadlineManager", …)` guard in `TracingConfigurationEnhancer`, no `DeadlineManagerOptions` in `TracingProperties`, no `deadlines` toggle, no deadline-specific edge cases, and no deadline mention in user-facing scenarios. If a deadlines port ever lands in AxonFramework 5 in the future, tracing for it is a pure addition (new `DecoratorDefinition`) — not a regression of this spec.

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Out-of-the-box tracing with OpenTelemetry in a Spring Boot application (Priority: P1)

A developer building an event-driven application on AxoniqFramework with Spring Boot wants to observe how commands, events, and queries flow through their system. They add a single dependency, configure an OpenTelemetry exporter, start the application, and immediately see correlated spans for command dispatch, event publication and handling, query handling, snapshot creation, saga / process-manager invocations, and aggregate / entity loading — including cross-service propagation when messages cross process boundaries.

**Why this priority**: This is the canonical "happy path" that almost all framework users want. Tracing only provides value when it appears automatically across the full messaging lifecycle without requiring developers to instrument each handler. Without P1, the feature has no business case.

**Independent Test**: Build an example application (`examples/university-java-springboot4` or a dedicated tracing example) that depends on the tracing OpenTelemetry module, exports spans to an in-memory exporter or a local collector (e.g., Jaeger/Zipkin via Testcontainers), exercises a representative command → event → query flow, and asserts that the resulting trace contains a connected hierarchy of spans with the expected names, attributes (message id, message name, payload type, aggregate identifier where applicable), and parent-child relationships including across an asynchronous event-handler boundary.

**Acceptance Scenarios**:

1. **Given** a Spring Boot 4 application that includes the AxoniqFramework tracing OpenTelemetry starter and an OpenTelemetry SDK on the classpath, **When** the application context starts, **Then** the framework auto-registers tracing decorators around the command bus, event bus, query bus, query update emitter, event-handling components, snapshotter, repository / state manager, and saga / process-manager components without the developer writing any tracing code.
2. **Given** a tracing-enabled application, **When** a command is dispatched that produces events that are consumed by an asynchronous event processor which in turn issues a query, **Then** the exported trace contains one root span for the command dispatch with child spans for command handling, event publication, event handling (correctly linked to the publishing span as a parent or producer-consumer link), and query handling — and each span carries attributes that identify the message.
3. **Given** a tracing-enabled application that sends a command to a remote node via the AxonServer connector (or another transport), **When** the remote node handles the command, **Then** the remote handler span is parented to the dispatcher span via trace context propagation through message metadata, producing a single connected trace across processes.
4. **Given** an application without an OpenTelemetry SDK on the classpath, **When** the tracing core module is on the classpath, **Then** the framework uses a no-op tracer by default and behaves identically to an untraced application (no exceptions, no measurable overhead beyond the no-op call sites).

---

### User Story 2 - Programmatic tracing configuration without Spring (Priority: P1)

A developer using AxoniqFramework in a plain-Java (non-Spring) application wants the same tracing capabilities. They register a `SpanFactory` implementation against `MessagingConfigurer` (or whatever configurer is appropriate) and the framework wires tracing across all messaging components automatically, using AF5's `ConfigurationEnhancer` and `DecoratorDefinition` mechanism rather than annotation-driven Spring auto-configuration.

**Why this priority**: AxoniqFramework explicitly supports non-Spring usage (see `examples/university-demo`). The tracing module must not assume Spring. Sharing this priority with Story 1 because the Spring auto-configuration is a thin wrapper on top of the programmatic API — if the programmatic API is missing or awkward, Spring auto-configuration becomes a leaky abstraction.

**Independent Test**: A plain-Java integration test in `integrationtests/` registers an `OpenTelemetrySpanFactory` (backed by an in-memory `InMemorySpanExporter`) through `MessagingConfigurer`, dispatches commands and queries, and asserts the resulting spans match expectations — without any Spring on the classpath.

**Acceptance Scenarios**:

1. **Given** a plain-Java application using `MessagingConfigurer`, **When** the developer registers a `SpanFactory` via the tracing module's `ConfigurationEnhancer` (e.g., `MessagingConfigurer.componentRegistry(...).registerEnhancer(new TracingConfigurationEnhancer())`), **Then** all relevant messaging components are decorated with tracing without further wiring.
2. **Given** a developer wants to provide custom span attributes, **When** they register additional `SpanAttributesProvider` instances through the public configuration API, **Then** spans produced by all message types include those attributes.

---

### User Story 3 - Customisation and extensibility (Priority: P2)

A developer wants to (a) replace the default span naming, (b) add domain-specific attributes (e.g., correlation IDs, tenant identifiers from metadata), (c) selectively disable tracing for noisy components (e.g., a heartbeat command), and (d) provide their own `SpanFactory` implementation backed by a non-OpenTelemetry tracer.

**Why this priority**: Customisation needs are real but not blocking for adoption. Most teams use the defaults; advanced users need a clear extension surface.

**Independent Test**: Tests register a custom `SpanFactory`, a custom `SpanAttributesProvider`, and configure component-level opt-outs through `application.properties` (Spring case) or through programmatic configuration. Verify the resulting spans reflect the customisations.

**Acceptance Scenarios**:

1. **Given** a Spring Boot application with `axon.tracing.enabled=true` and `axon.tracing.<component>.enabled=false` for one component, **When** the framework boots, **Then** the disabled component is not wrapped with a tracing decorator while other components are.
2. **Given** a developer provides their own `SpanFactory` `@Bean`, **When** the framework boots, **Then** the user-provided bean is used instead of the default `OpenTelemetrySpanFactory`.
3. **Given** custom `SpanAttributesProvider` `@Bean`s are registered, **When** any span is created, **Then** those providers contribute attributes to the span.

---

### User Story 4 - Consolidation of per-bus / per-component span factories (Priority: P2)

A framework maintainer wants the new tracing module to avoid the proliferation of bus/component-specific span factory interfaces seen in AF4 / dead-coded in AF5 (`CommandBusSpanFactory`, `EventBusSpanFactory`, `QueryBusSpanFactory`, `EventProcessorSpanFactory`, `QueryUpdateEmitterSpanFactory`, `SnapshotterSpanFactory`, `SagaManagerSpanFactory`, `RepositorySpanFactory`). The new module exposes a single, generic `SpanFactory` as the only public span-creation abstraction. All per-component tracing concerns — span naming (e.g., `"CommandBus.dispatchCommand commandName"`, `"EventBus.publishEvent eventName"`, `"EventProcessor[name].process"`), span kinds (`CLIENT` / `SERVER` / `PRODUCER` / `CONSUMER` / `INTERNAL`), distinguishing distributed vs in-process dispatch, attribute population, and cross-process metadata-based context propagation — live inside **private** `DecoratorDefinition` implementations registered by a single `TracingConfigurationEnhancer`. Adding tracing for a new component is a new internal `DecoratorDefinition` with **zero** public API churn.

**Why this priority**: This is a structural goal of the migration. Not user-visible behavior, but it shapes the design and binds the implementation.

**Independent Test**: Inspect the resulting public API of the new tracing module. It exposes exactly one `SpanFactory` interface, the `Span` / `SpanScope` / `SpanAttributesProvider` types, and the `TracingConfigurationEnhancer`. Inspect that there is **no** public `*BusSpanFactory`, `*ManagerSpanFactory`, `*ProcessorSpanFactory`, or `*EmitterSpanFactory` type. Inspect that per-component logic lives in internal (package-private or `@Internal`-annotated) decorator classes. The same trace coverage as Story 1 is achieved through these private decorators applied via `DecoratorDefinition`.

**Acceptance Scenarios**:

1. **Given** the new tracing module is built, **When** its public API is enumerated, **Then** it contains a single `SpanFactory` abstraction (not duplicated per bus / component) and the per-component span-creation logic lives inside private decorator implementations, not in dedicated factory interfaces.
2. **Given** a new component (e.g., a future scheduler) needs tracing, **When** a maintainer adds support, **Then** the change consists of a new private `DecoratorDefinition` registered by the existing `TracingConfigurationEnhancer`, without adding a new public `*SpanFactory` interface.
3. **Given** an OpenTelemetry export is being inspected for a command → event → query flow, **When** the resulting spans are compared against the AF4 / AF5 reference shape, **Then** span names, kinds, and attributes are equivalent — i.e., the consolidation does not regress the observable trace shape.
4. **Given** each tracing wrapper class (`TracingCommandBus`, `TracingEventBus`, `TracingQueryBus`, etc.) is inspected, **When** its structure is reviewed, **Then** it implements the target component interface by composition (holds a `delegate` of the same interface + a `SpanFactory`), calls through to the delegate on every operation, and reports itself via `ComponentDescriptor.describeWrapperOf(delegate)` — i.e., it never re-implements the target interface from scratch the way AF4's `DistributedCommandBus` did.
5. **Given** tracing is registered alongside interception, metrics, distributed transport, and other component decorators, **When** the application boots, **Then** all wrappers stack into a single delegation chain (verified by walking the `describeWrapperOf` chain or by an integration test that asserts a command flows through tracing, interceptors, and the distributed transport in the expected order without any wrapper having to know about the others).
6. **Given** a wrapped operation receives a `ProcessingContext`, **When** the tracing decorator opens a span, **Then** the span's open/close lifecycle is bound to the `ProcessingContext`'s lifecycle hooks (so async / reactive continuations see the correct active span and the span closes correctly on commit / rollback) rather than to a try/finally block scoped to the synchronous call stack.

#### Deferred future improvement (out of scope for this feature)

- **DFI-001 (deferred)**: Graduate a small subset of internal helpers used by the decorators — for example a `SpanNames` constants/helpers class, an `OperationKinds` enum, and a public `MetadataContextPropagator` SPI — to public API, so advanced users can write their own custom `DecoratorDefinition`s that produce identically-named spans (Option B from the clarification session). This is an additive change and can be delivered later without breaking the API shipped by this feature.

---

### User Story 5 - Documentation in the AxoniqFramework reference guide (Priority: P2)

A developer reads the AxoniqFramework reference guide ("Monitoring" / "Observability" section) and finds an up-to-date page describing how to enable distributed tracing, which spans are produced, which attributes are attached, how to add custom attributes and span factories, how to configure component-level opt-outs, and how trace context propagates across remote message transports.

**Why this priority**: Documentation is required for adoption but is fully independent of the runtime implementation and can be developed in parallel.

**Independent Test**: The Antora-built reference guide includes a "Tracing" page reachable from the navigation, the AsciiDoc builds without warnings, the page reflects the *new* API (no references to removed `*BusSpanFactory` interfaces), and code examples compile against the actual published classes.

**Acceptance Scenarios**:

1. **Given** the documentation source tree, **When** Antora builds the reference guide, **Then** a Tracing page is present under the Monitoring / Observability module with content adapted from the AF4 `docs/old-reference-guide/modules/monitoring/pages/tracing.adoc` source.
2. **Given** the documentation is reviewed against the new tracing API, **When** the reviewer checks each code snippet and class reference, **Then** all references resolve to classes that exist in the new AxoniqFramework tracing module (no broken references to removed AF5 interfaces).

---

### User Story 6 - Removal of tracing artefacts from upstream Axon Framework 5 (Priority: P3)

The tracing source currently sitting in upstream Axon Framework 5 is dead code — files that were moved into new package layouts with the bodies commented out (or otherwise neutralised) just to keep AF5 compiling. It provides no working functionality. After the AxoniqFramework migration is in place and verified, that dead code is deleted from the upstream Axon Framework 5 repository: no tracing classes, no extension module, no `stash/todo` placeholders, and no Spring Boot autoconfig remain. The Axon Framework 5 repository ships zero tracing source — that responsibility lives entirely in AxoniqFramework.

**Why this priority**: Cleanup is sequenced last because it is purely a deletion task and depends on AxoniqFramework holding the working implementation before AF5 loses its (already non-functional) copies. The sequencing is about safety of review, not about preserving functionality (there is no functionality in AF5 to preserve).

**Independent Test**: After the cleanup PR(s) land in Axon Framework 5, a search across that repository for `package org.axonframework.tracing`, the `extensions/tracing` directory, `AxonTracingAutoConfiguration`, and `OpenTelemetryAutoConfiguration` returns no results. AxonFramework 5 builds green with no tracing code, and its tests still pass (none should have depended on tracing, given the source was non-functional).

**Acceptance Scenarios**:

1. **Given** the migration is complete, **When** the upstream Axon Framework 5 repository is searched for tracing source, **Then** no tracing source files, modules, or configuration classes are found in `messaging/`, `extensions/tracing/`, `stash/todo/`, or `spring-boot-autoconfigure/`.
2. **Given** the cleanup is applied, **When** Axon Framework 5 is built, **Then** the build passes without referencing any of the removed tracing classes.

---

### Edge Cases

- **OpenTelemetry SDK absent**: The tracing core must default to a no-op factory; the application must function without an SDK on the classpath. No `ClassNotFoundException` may surface at runtime.
- **Multiple `SpanFactory` beans**: When a user supplies their own `SpanFactory`, it must replace the default. Behavior with multiple user-supplied factories is documented (composition via `MultiSpanFactory` or "last wins" — to be decided during planning).
- **Exception in handler**: When a handler throws, the surrounding span must be marked with error status and the exception recorded; the exception must still propagate to the caller unchanged.
- **Async event handling**: When an event is published synchronously but handled by an asynchronous event processor on another thread, the handling span must be linked to the publishing span (either as a parent via context propagation through metadata, or as a producer-consumer link — to be decided during planning based on OpenTelemetry semantic conventions for messaging).
- **Reactive code paths**: AxoniqFramework supports both imperative and reactive styles. Tracing must not assume a `ThreadLocal`-based active span (which would break in reactive pipelines); the implementation must use AF5's `ProcessingContext` to carry span scope through async boundaries.
- **Cross-process propagation**: When a message is sent through the AxonServer connector (or any other transport) to a remote node, the trace context must be serialised into message metadata on the sending side and rehydrated on the receiving side, producing a single connected trace.
- **Snapshotting on a background thread**: Snapshot creation runs outside the original processing context; its spans must still be attributable to a parent (the originating command or a standalone scheduled root) per documented conventions.
- **Saga / process-manager invocation**: Events that trigger saga / process-manager handlers must produce spans that show the saga handler as a child of the event handling span (with the saga identifier as an attribute).
- **Disabled tracing at runtime**: A user must be able to disable tracing completely (either by not including the module, by using the no-op factory explicitly, or by a configuration flag), and the application must run with no tracing overhead.
- **Metadata-based context propagation collisions**: Trace context is written into message metadata under specific keys; these keys must not collide with user-defined metadata keys, and behavior on collision must be defined.

---

## Requirements *(mandatory)*

### Functional Requirements

#### Core abstractions

- **FR-001**: The framework MUST expose a single public `SpanFactory` abstraction in the new tracing module that supports creating internal, dispatch (producer), and handler (consumer) spans for any `Message` type — no bus-specific or component-specific span factory interfaces are exposed as public API.
- **FR-002**: The framework MUST expose a public `Span` abstraction with operations to start, end, mark as errored (recording an exception), set attributes, and run a `Runnable` / `Callable` inside the span's active scope, supporting both imperative and reactive execution.
- **FR-003**: The framework MUST expose a public `SpanAttributesProvider` SPI so that users can contribute additional attributes to any span created for a message.
- **FR-004**: The framework MUST ship built-in `SpanAttributesProvider` implementations covering at least: message identifier, message name (qualified name), message type, payload type, selected metadata entries, and aggregate identifier (where applicable).
- **FR-005**: The framework MUST default to a no-op `SpanFactory` when no other `SpanFactory` is configured, so that the tracing core module is safe to depend on even without an OpenTelemetry SDK.

#### Coverage of messaging components

- **FR-006**: The framework MUST trace command dispatch and command handling, producing one span on dispatch and a child span on handling (across process boundaries when applicable).
- **FR-007**: The framework MUST trace event publication and event handling, supporting both synchronous and asynchronous (streaming / pooled) event handling components. Cross-thread / cross-process links MUST follow OpenTelemetry messaging semantic conventions.
- **FR-008**: The framework MUST trace query dispatch, query handling, and query update emission (subscription queries).
- **FR-009**: The framework MUST trace aggregate / entity loading and saving through the `Repository` / `StateManager` mechanism.
- **FR-010**: The framework MUST trace event sourcing operations: replaying events for an entity, snapshot creation, and snapshot reading.
- **FR-011**: *(withdrawn — see clarification 2026-05-26)* Deadline tracing is out of scope; `DeadlineManager` does not exist in Axon Framework 5. No tracing decorator, no `TracingProperties` group, and no documentation for deadlines is produced by this feature. The FR number is preserved to keep references in `plan.md` / `research.md` / `tasks.md` stable.
- **FR-012**: The framework MUST trace saga / process-manager invocation when those features are present (the requirement applies once their AF5 ports are available; the tracing module MUST be structured so adding tracing for them later requires only a new `DecoratorDefinition`, not new public API).

#### Design constraints

- **FR-013**: The framework MUST wire tracing into messaging components via a single `TracingConfigurationEnhancer` (implementing AF5's `ConfigurationEnhancer`) that registers `DecoratorDefinition`s for the relevant component types. Each `DecoratorDefinition` MUST produce a **delegating wrapper** following the AF5 decoration pattern: the wrapper class implements the target component interface (e.g., `CommandBus`, `EventBus`, `QueryBus`, `Repository`, `Snapshotter`) by composition, holds a `delegate` field of that same interface plus a `SpanFactory`, calls through to the delegate, and declares itself a wrapper via `ComponentDescriptor.describeWrapperOf(delegate)`. The AF4 anti-pattern of directly implementing a target interface without delegation (as `DistributedCommandBus` did) is explicitly disallowed because it prevents stacking with interception, metrics, distribution, security, and other concerns. No alternative wiring mechanism is used (no annotation processors, no per-bus configurer methods, no static wrapper subclasses).
- **FR-013a**: Where the wrapped operation already participates in a `ProcessingContext` — e.g., dispatch / handle methods that receive a `ProcessingContext` parameter — span open/close lifecycle SHOULD ride on `ProcessingContext` lifecycle callbacks (start / commit / rollback hooks) rather than `try/finally` or `Span.runSupplier(...)` blocks, so span scope aligns with the framework's processing phases and survives async / reactive boundaries naturally. Where no `ProcessingContext` is available at the relevant hook point (e.g., synchronous snapshot creation triggered outside a processing context), a direct `Span.run...` style is acceptable.
- **FR-014**: The framework MUST carry the active span / scope through AF5's `ProcessingContext` (using `ResourceKey<T>`) rather than `ThreadLocal`, so the implementation works correctly in reactive and asynchronous code paths.
- **FR-015**: The framework MUST propagate trace context across message boundaries by serialising it into message metadata on dispatch and rehydrating it on the receiving side, so distributed traces span multiple JVMs / processes. The serialisation/rehydration logic lives inside the per-component `DecoratorDefinition`s (not as a separate public propagator SPI in this feature; see DFI-001 under Story 4 for a future graduation path).
- **FR-016**: The framework MUST NOT introduce any public per-bus or per-component span factory interfaces such as `CommandBusSpanFactory`, `EventBusSpanFactory`, `QueryBusSpanFactory`, `EventProcessorSpanFactory`, `QueryUpdateEmitterSpanFactory`, `SnapshotterSpanFactory`, `SagaManagerSpanFactory`, or `RepositorySpanFactory`. (The forbid-list is defensive; `DeadlineManagerSpanFactory` is also implicitly forbidden because deadlines are out of scope — see clarification 2026-05-26.) All per-component span-creation logic (span names, span kinds, attributes, distributed-vs-in-process branching, propagation) MUST live inside **private** (package-private or `@Internal`-annotated) `DecoratorDefinition` implementations registered by the single `TracingConfigurationEnhancer`. Per-component span shapes (names, kinds, attribute keys) MUST remain equivalent to those produced by AF4 today, so that existing trace consumers/dashboards continue to work after the migration.

#### Module structure

- **FR-017**: The framework MUST add a new core tracing module to AxoniqFramework at `tracing/axoniq-tracing-core` (group `io.axoniq.framework`, artifact `axoniq-tracing-core`, parent `axoniq-framework-parent`) under a new top-level `tracing/` directory. The module contains the public abstractions (`SpanFactory`, `Span`, `SpanScope`, `SpanAttributesProvider`, the built-in attribute providers), the private (package-private or `@Internal`) per-component `DecoratorDefinition`s, and the single public `TracingConfigurationEnhancer`. The module follows the established AxoniqFramework convention of thematic flat-sibling modules under a per-concern top-level directory (`messaging/`, `connector/`, `dependency-injection/`, `testing/`).
- **FR-018**: The framework MUST add an OpenTelemetry integration module at `tracing/axoniq-tracing-opentelemetry` (artifact `axoniq-tracing-opentelemetry`) as a flat sibling of `axoniq-tracing-core`. It provides `OpenTelemetrySpanFactory` and the OpenTelemetry-specific metadata text-map getter/setter for cross-process context propagation. It MUST depend on `axoniq-tracing-core` and on `io.opentelemetry:opentelemetry-api`, and MUST NOT depend on Spring.
- **FR-019**: The framework MUST add Spring Boot auto-configuration for tracing into the **existing** `dependency-injection/spring/spring-boot-autoconfigure` module (artifact `axoniq-spring-boot-autoconfigure`) — alongside the autoconfig already shipped there for `axoniq-dead-letter`, `axoniq-postgresql`, `axon-server-connector`, etc. — so that, when the tracing core and OpenTelemetry modules are on the classpath, the framework registers a default `OpenTelemetrySpanFactory` bean, the built-in `SpanAttributesProvider` beans, the `TracingConfigurationEnhancer`, and a `TracingProperties` `@ConfigurationProperties` class for enabling / disabling tracing per component group. **No** separate `axoniq-tracing-spring-boot-autoconfigure` module is created.

#### Configuration

- **FR-020**: The Spring Boot auto-configuration MUST expose a `@ConfigurationProperties` class (e.g., `TracingProperties`) that supports at minimum a global enable / disable toggle and per-component-group toggles (commands, events, queries, query updates, snapshotting, repository / state, sagas / process-managers).
- **FR-021**: Any user-supplied `SpanFactory` bean MUST replace the default `OpenTelemetrySpanFactory`. Any user-supplied `SpanAttributesProvider` beans MUST be added to the set of providers used by spans.

#### Testing

- **FR-022**: The new modules MUST include unit tests for each public abstraction (no-op factory, multi factory, attribute providers, decorator behavior) following project test conventions (JUnit 5, AssertJ, no Mockito mocks unless justified).
- **FR-023**: The framework MUST include an integration test that exercises the full command → event → query path through a running OpenTelemetry SDK with an in-memory exporter (or equivalent) and asserts the resulting span tree.
- **FR-024**: The OpenTelemetry module MUST include unit tests for the text-map getters / setters and for context propagation through metadata.

#### Documentation

- **FR-025**: The AxoniqFramework reference guide MUST gain a Tracing page under the Monitoring / Observability module, adapted from the AF4 `docs/old-reference-guide/modules/monitoring/pages/tracing.adoc` source, updated to reflect (a) AF5 terminology (no aggregates-as-classes language where it has been replaced, etc.), (b) the new `DecoratorDefinition` / `ConfigurationEnhancer`–based wiring, and (c) the consolidated `SpanFactory` API.
- **FR-026**: The documentation MUST include at least one worked example for Spring Boot and one for plain-Java usage.

#### Examples

- **FR-027**: At least one example application under `examples/` (the most appropriate one — likely `university-java-springboot4`) MUST be updated to demonstrate tracing enabled with OpenTelemetry, including a working exporter configuration (e.g., to a Testcontainers-hosted Jaeger or to logging), so the feature is exercised in `./mvnw -Pexamples clean verify`.

#### Cleanup of upstream Axon Framework 5

- **FR-028**: After the migration is complete and validated, the upstream Axon Framework 5 repository (`/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework5`) MUST be cleaned of all tracing-related source. This is a deletion of dead code (relocated + commented-out files that exist only to keep AF5 compiling), not removal of working functionality. Specifically:
  - all classes under `messaging/src/main/java/org/axonframework/tracing/` and corresponding tests
  - the `extensions/tracing/` module and its `tracing-opentelemetry` sub-module
  - all tracing-related classes in `stash/todo/src/main/java/org/axonframework/tracing/`
  - `AxonTracingAutoConfiguration` and `OpenTelemetryAutoConfiguration` (wherever they currently live in AF5, including any `stash/todo/.../springboot/autoconfig` location)
  - all `*SpanFactory` interfaces and default implementations that currently live in AF5 packages such as `messaging/.../commandhandling/tracing`, `messaging/.../eventhandling/tracing`, `messaging/.../queryhandling/tracing`, and any tracing-related classes in the legacy-saga, legacy-aggregate, and snapshotting `stash/` packages.
- **FR-029**: After cleanup, the upstream Axon Framework 5 build (`./mvnw clean verify`) MUST succeed without referencing any removed tracing class, and a grep for `org.axonframework.tracing` across that repository MUST return zero source matches.

#### Out of scope (explicit non-requirements)

- **FR-NS-001**: This work does NOT migrate the legacy standalone `extension-tracing` repository (https://github.com/AxonFramework/extension-tracing). That extension is abandoned and not being ported.
- **FR-NS-002**: This work does NOT introduce a metrics / monitoring integration (separate effort) and does NOT modify the existing metrics extension archive.

### Key Entities

- **`SpanFactory`**: The single public abstraction for creating spans. Receives the message being processed (or descriptors of a non-message operation such as snapshot creation), the span kind (internal / dispatch / handler), and produces a `Span`.
- **`Span`**: A unit of traced work, with lifecycle (start, end, error) and the ability to host a runnable / callable inside its active scope.
- **`SpanScope`**: The "active span" lifetime, scoped to a `ProcessingContext` resource rather than a `ThreadLocal`.
- **`SpanAttributesProvider`**: Pluggable SPI that contributes attributes to a span based on the message being traced.
- **Tracing `ConfigurationEnhancer`**: AF5-style enhancer that registers `DecoratorDefinition`s wrapping the relevant messaging components with tracing behavior.
- **`OpenTelemetrySpanFactory`**: The OpenTelemetry implementation of `SpanFactory`, located in the OpenTelemetry integration module.
- **Trace context carriers**: Text-map getter / setter implementations that read trace context from / write it to message metadata using OpenTelemetry's propagation API.
- **`TracingProperties`**: Spring Boot `@ConfigurationProperties` controlling enable / disable per component group.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new AxoniqFramework user can enable distributed tracing in a Spring Boot application by adding a single starter dependency and configuring an OpenTelemetry exporter (no Java code changes), and see a connected trace for a command → event → query flow on first run.
- **SC-002**: A non-Spring AxoniqFramework user can enable tracing in fewer than five lines of configuration code, registering a single `ConfigurationEnhancer` against `MessagingConfigurer`.
- **SC-003**: The new tracing module's public API exposes exactly **one** `SpanFactory` interface, with zero public `*BusSpanFactory` / `*ManagerSpanFactory` / `*ProcessorSpanFactory` / `*EmitterSpanFactory` types (verified by an architectural / Javadoc check). All per-component decorator implementations are package-private or `@Internal`.
- **SC-003a**: For the canonical command → event → query flow exercised by SC-004, the exported span names, span kinds, and attribute keys are equivalent to those produced by the AF4 reference implementation (asserted by an explicit equivalence test against a recorded AF4 span snapshot, or against a static expected-shape definition derived from AF4). The consolidation does not regress the observable trace shape.
- **SC-004**: The integration test suite includes at least one end-to-end trace assertion covering command → event → query in a single test, run against an OpenTelemetry SDK with an in-memory exporter, executed by the standard `./mvnw -Pintegration-test verify` workflow.
- **SC-005**: An example application under `examples/` runs with tracing enabled and emits the expected spans during `./mvnw -Pexamples clean verify`.
- **SC-006**: The AxoniqFramework reference guide builds successfully (Antora) and renders a Tracing page; all class names and code snippets in that page resolve to classes that exist in the new module.
- **SC-007**: After the cleanup phase, a recursive grep for `org.axonframework.tracing` and for the directory `extensions/tracing` across the upstream Axon Framework 5 repository returns zero source matches, and Axon Framework 5 builds green with no tracing code present.
- **SC-008**: When the tracing core module is on the classpath but no `SpanFactory` is configured (no OpenTelemetry SDK, no override), the framework runs with the no-op factory and produces no spans, with no `ClassNotFoundException` or runtime warnings about missing tracing infrastructure.
- **SC-009**: Trace context successfully propagates across at least one message-transport boundary in an integration test (AxonServer connector or another supported transport), producing a single connected trace across two JVM-equivalent contexts.

---

## Assumptions

- **Source of truth is AF4.** The working tracing implementation exists only in `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4` (`messaging/src/main/java/org/axonframework/tracing/`, the `tracing-opentelemetry` module, and the two Spring Boot autoconfig classes). The AF5 tracing source — including everything in `messaging/src/main/java/org/axonframework/tracing/`, `messaging/.../{commandhandling,eventhandling,queryhandling}/tracing/`, `extensions/tracing/`, and `stash/todo/.../tracing/` and `stash/todo/.../springboot/autoconfig/` — is **dead code**: it was relocated and commented out to keep AF5 compiling. It has no wiring, no functional behavior, and is not used as a baseline for the port. The port reads behavior from AF4 and re-implements it in AxoniqFramework using AF5 idioms.
- The tracing source files archived under `_archive/` in AxoniqFramework (`_archive/messaging/.../tracing/`, `_archive/extensions/tracing/`, `_archive/stash/todo/.../tracing/`) are equally non-functional snapshots. They MAY be consulted as a convenient mirror of AF4 / AF5 sources, but they are not authoritative and are not "partially working".
- The AF5 messaging-core abstractions already present in AxoniqFramework (`Message`, `ProcessingContext`, `ResourceKey`, `MessagingConfigurer`, `ConfigurationEnhancer`, `DecoratorDefinition`) are available and stable enough to host tracing decorators. They do not need to be modified by this feature; if a gap is found, it is treated as a blocking dependency and surfaced separately.
- The new tracing modules live under a new top-level `tracing/` directory as two flat siblings: `tracing/axoniq-tracing-core` and `tracing/axoniq-tracing-opentelemetry` (see FR-017 / FR-018). This follows the per-concern top-level directory convention already used by `messaging/`, `connector/`, `dependency-injection/`, and `testing/`. Tracing is intentionally **not** placed under `messaging/` because it is a cross-cutting concern that depends on messaging, modelling, event sourcing, and (when ported) sagas alike (event-sourcing handler tracing alone makes the "messaging-only" placement incorrect).
- Spring Boot auto-configuration for tracing lives in the existing `dependency-injection/spring/spring-boot-autoconfigure` module (artifact `axoniq-spring-boot-autoconfigure`), alongside the autoconfig already shipped for `axoniq-dead-letter`, `axoniq-postgresql`, `axon-server-connector`, etc. No separate spring-tracing autoconfig module is created.
- AxoniqFramework targets OpenTelemetry as the primary (and currently only first-party) tracing backend. Other backends are supported by users implementing `SpanFactory` themselves; no other backend ships in-tree.
- Saga and process-manager components are scoped here only to the extent that their AF5 ports exist. If the AF5 port of sagas / process-managers is not yet present in AxoniqFramework at implementation time, tracing for them is deferred to a follow-up but the module structure must allow that follow-up to be a pure addition (new `DecoratorDefinition` in the existing enhancer).
- The cleanup phase in the upstream Axon Framework 5 repository is delivered as a separate PR (or set of PRs) against that repository, sequenced after the AxoniqFramework migration PRs are merged. The two repositories are treated independently for git workflow purposes.
- The legacy standalone `extension-tracing` GitHub repository is abandoned and not part of this migration.
- The AF4 documentation source at `docs/old-reference-guide/modules/monitoring/pages/tracing.adoc` is the canonical content baseline. Recent fixes in the AF4 branch are picked up by reading the latest version of that file at implementation time, not at spec time.
- Span attributes follow OpenTelemetry semantic conventions for messaging where applicable; deviations are documented in the reference guide.
- Performance overhead of tracing when disabled (no-op factory) is treated as "negligible" — no specific microbenchmark target is set in this spec; if a hot-path concern arises during implementation, it is surfaced as a follow-up.
