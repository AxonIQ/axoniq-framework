# Axoniq Framework — Project Constitution

This constitution defines the **project-wide rules** every feature specification in this
repository MUST respect: foundation principles, architecture, code style, testing
standards, and governance. Feature-specific principles belong in
`specs/<feature>/spec.md`, not here.

## Foundation Principles

### I. Simplicity First

Every change MUST be as simple as possible. Find root causes; no temporary fixes; senior
developer standards. Don't add error handling, fallbacks, or validation for scenarios that
cannot happen — trust internal code and framework guarantees. Validate only at system
boundaries (user input, external APIs).

### II. Minimal Impact

Changes MUST only touch what is necessary. Refactors, cleanups, and abstractions that
exceed the scope of the task are forbidden unless explicitly requested. Three similar
lines is better than a premature abstraction.

### III. Java 21 Baseline

All code MUST target Java 21. Modern features (sealed classes, records, pattern matching,
virtual threads where appropriate) MAY be used. Raw types, wildcard imports, and other
legacy idioms MUST NOT be introduced into new code.

### IV. Dual Paradigm Support

The framework MUST support both reactive and imperative programming styles without
enforcing either. Public APIs MUST be expressible from both styles. Async-first return
types (`CompletableFuture<T>`, `MessageStream<M>`) are the default.

### V. No ThreadLocals in Framework Code

Internally, the framework MUST NOT rely on `ThreadLocal` for state propagation. Per-message
state lives on `ProcessingContext`. `ThreadLocal` is permitted only at imperative-style
edges when explicitly bridging to legacy APIs.

### VI. Composition over Inheritance

Component design MUST favor composition. Inheritance is reserved for cases where the
subclass IS-A specialization of the parent. Sealed hierarchies are preferred over open
inheritance for closed sets of variants.

### VII. Declarative over Annotation-Heavy

Configuration and wiring MUST be expressible declaratively. Annotations are supported but
MUST NOT be the only way to configure a component. Every annotation-based registration
MUST have a programmatic equivalent.

### VIII. Vertical-Slice Delivery

Implementation plans MUST prefer **component-by-component vertical slices** over layered
(all-of-X, then all-of-Y) execution. A vertical slice delivers one component end-to-end
through the abstraction layers it depends on — core SPI, decorators, configuration,
autoconfig glue, focused integration test — and ships an observable, validatable behaviour
before the next component starts.

This principle binds the outputs of `/speckit-plan` and `/speckit-tasks`:

- Phases that would otherwise group "all decorators", "all handlers", or "all wiring" MUST
  be split into one phase per component.
- Core abstractions (SPIs, base types, shared helpers) MUST be produced up-front in a
  single foundation phase so each slice starts on stable ground. Modifications to those
  abstractions mid-stream are permitted but MUST be flagged to the user before commit, with
  a one-line summary of what is changing and why.
- Each vertical slice MUST end with an explicit human-validation checkpoint task: commit
  the slice, run the slice's focused integration test, post the observable result (e.g.,
  the produced span tree, the rendered output, the persisted state shape) to the user, and
  wait for explicit go/no-go before the next slice starts.
- Cross-component end-to-end tests, reference documentation, example applications, and
  external-system tests (Testcontainers, real-backend verification, cleanup of upstream
  artefacts) are produced as a single batched final phase after all slices are validated.
  Documentation in particular MUST land last so it reflects the as-shipped API surface and
  configuration knobs without needing to be rewritten per slice.
- One feature branch, one rollup PR by default. Per-slice PRs are an opt-in deviation, not
  the norm.

**Rationale**: vertical slices stress-test the core abstractions on the first slice while
their cost of change is lowest; keep each commit reviewable and reversible; bound the blast
radius when a slice exposes a design flaw (one component to revert, not all of them); and
create natural human-feedback boundaries instead of deferring all observable behaviour to a
final integration phase. Layered execution magnifies the cost of late discovery.

**When to deviate**: only when a change is genuinely orthogonal across components (e.g., a
parent-pom dependency bump touching every module, a checkstyle rule rollout, a one-shot
deletion phase against an upstream repository), or when no per-component slice is
meaningfully smaller than the whole. Deviations MUST be justified explicitly in the spec's
`## Clarifications` section before the plan is finalised.

## Relationship to AxonFramework Upstream

Axoniq Framework builds on top of [AxonFramework](https://github.com/AxonFramework/AxonFramework)
(5.2.0+). AxonFramework provides the core building blocks — `Message`, `MessageType`,
buses, registries, `ProcessingContext`, `EventStore`, conversion infrastructure.

Axoniq Framework features:
- MUST NOT redefine upstream types locally; extend or configure them.
- MUST track upstream changes in the BOM (`axoniq-framework-bom`) when bumping
  AxonFramework versions.
- MAY introduce new components in the active modules (`messaging/`, `connector/`,
  `dependency-injection/`, `testing/`), provided they compose with upstream types.
- MUST NOT introduce duplicate concepts that conflict with upstream APIs.
- **MUST NOT reference AF4-era upstream types that have been removed from
  AxonFramework 5** (e.g., `DomainEventMessage`). Where data those types carried
  is still required for legacy bridging, source it from `ProcessingContext`
  resources exposed via
  `org.axonframework.messaging.core.LegacyResources`
  (`AGGREGATE_IDENTIFIER_KEY`, `AGGREGATE_TYPE_KEY`,
  `AGGREGATE_SEQUENCE_NUMBER_KEY`). New (DCB / entity-based) code paths SHOULD
  use `@EventSourcedEntity(tagKey = …)` + `@EventTag` instead and treat the
  legacy resources as best-effort fallbacks (present only on legacy aggregate-
  based event streams).

The `_archive/` directory contains earlier AF5 drafts that have been moved upstream into
AxonFramework. Do not treat archived classes as the current API.

## Architecture Rules

### Module Dependency Hierarchy

Active modules MUST respect this direction:

```
axoniq-framework-bom         (dependency management)
        │
    messaging                (core messaging components on top of AxonFramework)
        │
    connector                (transport / distributed messaging connectors)
        │
    dependency-injection     (declarative configuration / wiring)
        │
    testing                  (test fixtures and support)
        │
    integrationtests         (cross-module integration tests)
```

Modules MUST NOT depend on a module further down the chain. New dependencies that violate
this direction are rejected.

### Message-Centric Architecture

Every cross-boundary interaction flows through a `Message` with a bus and a registry:

| Message | Bus | Registry | Handler | Component | Annotation |
|---|---|---|---|---|---|
| `CommandMessage` | `CommandBus` | `CommandHandlerRegistry` | `CommandHandler` | `CommandHandlingComponent` | `@CommandHandler` |
| `EventMessage` | `EventBus` | `EventHandlerRegistry` | `EventHandler` | `EventHandlingComponent` | `@EventHandler` |
| `QueryMessage` | `QueryBus` | `QueryHandlerRegistry` | `QueryHandler` | `QueryHandlingComponent` | `@QueryHandler` |

Handlers MUST be registrable both via annotation (`Annotated*HandlingComponent`) and
programmatically (implementing `*Handler` / `*HandlingComponent` directly).

### Async-First

Message handler return types MUST be async (`CompletableFuture<T>`, `MessageStream<M>`).
Synchronous APIs are bridges, not the destination. Blocking on `CompletableFuture` is
governed by the **CompletableFuture Blocking Rule** below.

### ProcessingContext as Unit of Work

Per-message state, transaction boundaries, and lifecycle hooks MUST flow through
`ProcessingContext`. Resources MUST be stored via type-safe `ResourceKey<T>`. No global
state, no `ThreadLocal`. `ProcessingContext` supports branching for sub-contexts.

### Interceptor Levels

Two interceptor levels are supported, and these are the only blessed interception points:
1. **`MessageDispatchInterceptor`** — before dispatch, no `ProcessingContext` yet. Can
   reject or transform the outgoing message.
2. **`MessageHandlerInterceptor`** — around handler invocation, with active
   `ProcessingContext`. Also available as `@MessageHandlerInterceptor` on methods.

New cross-cutting concerns MUST plug in via one of these, or via
`HandlerEnhancerDefinition` for handler-level wrapping.

### Annotation Processing Pipeline

The annotation-based handler discovery chain MUST follow this order:
1. `AnnotatedHandlerInspector` scans class methods for `@MessageHandler` (or
   meta-annotated).
2. `HandlerDefinition` SPI (ServiceLoader-based) produces `MessageHandlingMember`
   instances.
3. `HandlerEnhancerDefinition` wraps members to add behavior (tracing, timeouts, etc.).
4. `Annotated*HandlingComponent` registers them with the bus.

Custom handler logic MUST extend the pipeline via `HandlerDefinition` /
`HandlerEnhancerDefinition`, not by bypassing it.

## AF5 Anchoring Types

All feature designs MUST be validated against these upstream AxonFramework types. New
designs that introduce a dependency on a type not listed here MUST justify it explicitly
in the spec.

| Type | Role |
|---|---|
| `Message` | Base type — payload + `MessageType` + metadata |
| `MessageType` | `QualifiedName` + version (record; version defaults to `0.0.1`) |
| `QualifiedName` | Routing identifier derived from payload type or annotation attribute |
| `CommandMessage` / `EventMessage` / `QueryMessage` | Message variants for the three buses |
| `MessageStream<M>` | Async stream of messages |
| `Converter` | General representation conversion primitive |
| `MessageConverter` | Format/serialization conversion at the message level |
| `Message#withConvertedPayload(...)` | Derive a new `Message` with payload converted |
| `ProcessingContext` | Per-message processing lifecycle and resources |
| `TrackingToken` | Position tracking in event streams |
| `EventStore` / `EventStorageEngine` | High-level / low-level event persistence |

## Code Style

- **Indentation**: 4 spaces. **Line limit**: 120 characters. **Encoding**: UTF-8.
  **Line endings**: LF.
- **IntelliJ code style** MUST be imported from `axon_code_style.xml` at the repo root.
- **No wildcard imports**: `class_count_to_use_import_on_demand = 99` is enforced.
- **Nullability** uses JSpecify (`org.jspecify.annotations`):
    - `@NullMarked` at the package level.
    - `@Nullable` to mark explicitly nullable parameters/returns.
    - Jakarta `@Nonnull` / `@Nullable` are **forbidden** (enforced via `build/checkstyle.xml`).
- Implementations MUST enforce non-null parameters with `Objects.requireNonNull` checks.

## Javadoc Standards

- **Coverage**: document all public methods, constructors, and classes. `@Internal`
  constructors MUST be documented to explain why they are internal.
- **Fragment style for tags**: `@param`, `@return`, `@throws` start lowercase, no trailing
  period. (Class-level and method-level description paragraphs remain normal sentences.)
- **Always use `@link` tags** for class/interface/method references.
- **Mark internal code** with `@Internal` (`org.axonframework.common.annotation.Internal`)
  on classes and methods not part of the public API.
- **Class-level documentation** MUST cover: purpose, architectural role, framework
  integration (who creates it, how users access it), usage example, `@author`, `@since`.
- **Examples** in javadoc MUST use proper framework APIs (`MessagingConfigurer.create()`,
  etc.), not raw instantiation.

## Testing Standards

- **Frameworks**: JUnit 5, AssertJ (preferred over JUnit assertions), Awaitility.
- **No mocks unless justified**: prefer the simplest real implementation, or a recording
  implementation. `Mockito.spy()` is permitted only when behavior-based assertions cannot
  validate the interaction (e.g., verifying a decorator prevents delegate calls, or that
  the delegate receives correct arguments). Ask before introducing a new mock or custom
  implementation; check `messaging/src/test/java/org/axonframework/utils/` first.
- **Test behavior, not implementation details** (e.g., do not assert on implemented
  interfaces; assert on the observable API).
- **Structure**: `// given / // when / // then` with a space between `//` and the section
  name.
- **Sample events** MUST be created via `EventTestUtils`.
- **Grouping**: use JUnit 5 `@Nested` to group logically connected test cases (same
  method, same given section, etc.).
- **Naming**: do not add `@DisplayName`; method names MUST be self-explanatory. Add inline
  comments in given/when/then only when intent is non-obvious.
- **Test class naming**:
    - Unit tests (Surefire): `*Test.java`, `*Tests.java`, `*Test_*.java`, `*Tests_*.java`.
    - Integration tests (Failsafe): `*IntegrationTest.java`, `*IntegrationTests.java`,
      `IT*.java`, `*IT.java`, `*ITCase.java`.

## CompletableFuture Blocking Rule

**Never call `CompletableFuture.join()` or `.get()` without a timeout in framework code.**

A blocking call without a deadline silently turns a transient issue (pool exhaustion, lock
contention, deadlock, network partition) into a permanent thread leak with no diagnostics.

Preferred forms:

| Approach | Timeout | Exception unwrapping | When to use |
|---|---|---|---|
| `future.orTimeout(...).join()` | explicit | no — `CompletionException` | default — standard JDK |
| `FutureUtils.joinAndUnwrap(future)` | 30s default | yes — sneaky-throws cause | bridging async internals behind a sync contract |
| `FutureUtils.joinAndUnwrap(future, duration)` | explicit | yes — sneaky-throws cause | same, with a custom timeout |
| `future.get(timeout, unit)` | explicit | no — checked exceptions | legacy code only |
| `future.join()` / `future.get()` | **none** | — | **never** |

Any new `join()` / `get()` without a timeout MUST be refactored to one of the above. Each
`joinAndUnwrap` call site is a temporal bridge — a candidate for future removal once the
surrounding API becomes async-native.

## Type Safety

When passing a `Type` to APIs that accept it, use `TypeReference` for generic targets
instead of raw `Class`, to preserve generic information and avoid
`@SuppressWarnings("unchecked")`:

```java
private static final TypeReference<Map<String, String>> METADATA_MAP_TYPE_REF =
        new TypeReference<>() {};

Map<String, String> result = converter.convert(data, METADATA_MAP_TYPE_REF.getType());
```

- Define `TypeReference` constants as `private static final` when reused.
- For simple non-generic types, `Class<T>` is fine.

## API Design Principles

1. **Discover API essence** — design for future flexibility over current completeness.
2. **Ease of use** — support bare-bones, declarative, and annotation-based styles.
3. **Threading agnostic** — no assumptions about the caller's threading model.
4. **Dual paradigm support** — both reactive and imperative styles MUST be expressible.
5. **Annotation flexibility** — every annotation-based registration MUST have a
   programmatic equivalent.
6. **Interface Segregation** — interfaces MUST be small and focused. Clients MUST NOT be
   forced to implement methods they do not need. If different shapes are needed, split
   into separate interfaces.
7. **Dependency Inversion** — depend on `Message` and framework abstractions, not on
   concrete event/command/query classes or serialization internals.
8. **Open/Closed** — components MUST be open for extension (composition, registration) and
   closed for modification of established behavior.
9. **Meaningful names** — class and method names MUST describe purpose, not mechanism.

## Documentation Standards

- **Reference guide** at `docs/reference-guide/`, written in AsciiDoc, built with Antora.
  Additional getting-started content at `docs/af5-getting-started/`.
- **Feature docs**: every new feature MUST update the relevant pages in `docs/`. When a
  feature demonstrates a new capability, an example SHOULD be added to `examples/`.
- **Documentation migration guidance** (Axon 4 → 5 terminology, style rules, verification
  workflow) is governed by `docs/CLAUDE.md`.

## Governance

**This constitution supersedes ad-hoc conventions.** Any feature specification under
`specs/<feature>/spec.md` MUST be consistent with the principles, architecture rules,
anchoring types, and standards above. Feature-specific principles (e.g., the upcasting
spec's immutability, determinism, SRP-per-upcaster, chain-over-direct, type-based
versioning rules) belong in the spec file, not here.

**Amendment procedure**: changes to scope, principles, architecture rules, or anchoring
types require updating this file with a version bump and a note in the Sync Impact Report
at the top:
- **MAJOR**: principle removals, scope contraction, or backward-incompatible architecture
  changes.
- **MINOR**: new principles, new architecture rules, new anchoring types.
- **PATCH**: wording clarifications without semantic change.

**Compliance**: spec.md and plan.md MUST reference the principles they rely on. Designs
that contradict a principle without an explicit, justified amendment to this constitution
are rejected.

**Sources**: this constitution distills the project conventions documented in `CLAUDE.md`
(root) and the task-specific rules under `.claude/rules/`. When CLAUDE.md and this file
disagree, this file wins; flag the divergence as a sync issue and update both.

**Version**: 2.2.0 | **Ratified**: 2026-05-18 | **Last Amended**: 2026-05-26

---

## Sync Impact Report — 2.2.0 (2026-05-26)

**Bump type**: MINOR — new Foundation Principle added; no existing principle removed or weakened.

**Change**: Added **Principle VIII — Vertical-Slice Delivery** to the Foundation Principles section. Implementation plans MUST prefer component-by-component vertical slices over layered execution. Each slice ships one component end-to-end (core SPI usage + decorator + configuration + autoconfig glue + focused integration test) and ends with an explicit human-validation checkpoint. Core abstractions land up-front in a foundation phase; cross-component E2E tests, docs, examples, and external-system / Testcontainers tests are batched in a final phase. Documentation in particular lands last so it reflects the as-shipped API. Deviations require justification in the spec's Clarifications section.

**Motivation**: Layered plans (all decorators in one phase, then all autoconfig, then all docs) defer observable behaviour to the final phase, magnifying the cost of late discovery and removing the human's natural feedback opportunities. Vertical slicing stress-tests the core abstractions on slice 1 (when the cost of change is lowest), keeps each commit reviewable, and bounds blast radius when a slice exposes a design flaw. Codifying this as a Foundation Principle makes it the default for `/speckit-plan` and `/speckit-tasks` going forward, rather than something every feature has to argue from scratch.

**Trigger**: feat/3594-DistributedTracing clarification session 2026-05-26. The original plan grouped "all tracing decorators" into a single P3 phase. The user pushed back and asked for component-by-component slicing (CommandBus → EventSink → QueryBus → QueryUpdateEmitter → Repository → Snapshotter), with explicit human-validation gates between slices and docs/Testcontainers/example batched at the end. The plan was restructured into 10 phases (P1 foundations, P2 core abstractions + OTel + autoconfig skeleton, P3–P8 six per-component slices, P9 batched finalization, P10 upstream cleanup) and recorded in FR-022b of `specs/3594-distributed-tracing/spec.md`. Promoting this preference to a constitutional principle prevents the same restructuring discussion on the next feature.

**Modified sections**:
- *Foundation Principles* — added **VIII. Vertical-Slice Delivery**.

**Added sections**: none beyond the new principle (VIII is added in the existing Foundation Principles section).

**Removed sections**: none.

**Renamed principles**: none. Principles I–VII keep their numbering, titles, and content. VIII is appended after VII.

**Templates requiring updates**:
- ✅ `.specify/templates/tasks-template.md` — already structures phases per user story (Phase 3+ = one phase per User Story), which aligns with VIII. No edit needed; the principle reinforces existing template intent and adds the human-validation-gate requirement on top.
- ✅ `.specify/templates/plan-template.md` — generic placeholder template; does not enforce a specific phase strategy. No edit needed; concrete plans inherit VIII via the Constitution Check gate.
- ✅ `.specify/templates/spec-template.md` — does not prescribe an implementation strategy; deviations from VIII are captured in the spec's `## Clarifications` section per the new principle. No edit needed.
- ✅ `.specify/templates/checklist-template.md` — unaffected.
- ✅ `.specify/templates/constitution-template.md` — the source template for this very file; no propagation step needed.

**Impact on existing feature specs**:
- `specs/3594-distributed-tracing/` — already compliant. FR-022b in `spec.md` and the P3–P8 + P9 + P10 structure in `plan.md` are the worked example that motivated this principle. The plan's Constitution Check table will be updated on next plan refresh to list VIII explicitly.
- No other in-flight feature specs exist at amendment time.

**Sync issues observed**: none. The principle codifies what the user requested in the tracing feature; no other rules conflict with it. `CLAUDE.md` already aligns ("Spec-first: Enter plan mode for non-trivial tasks") but does not prescribe slicing — this amendment narrows that to a specific, testable rule.

**Deferred items**: none. No `TODO()` placeholders remain.

---

## Sync Impact Report — 2.1.0 (2026-05-26)

**Bump type**: MINOR — new architecture rule added; no existing rule removed or weakened.

**Change**: Added a new bullet under *Relationship to AxonFramework Upstream* forbidding references to AF4-era upstream types removed from AxonFramework 5 (e.g., `DomainEventMessage`), and pointing new code at `org.axonframework.messaging.core.LegacyResources` resource keys (`AGGREGATE_IDENTIFIER_KEY` / `AGGREGATE_TYPE_KEY` / `AGGREGATE_SEQUENCE_NUMBER_KEY`) on `ProcessingContext` when legacy bridging data is still needed, and at `@EventSourcedEntity` / `@EventTag` for DCB/entity-based code paths.

**Motivation**: AF5 has removed `DomainEventMessage` (and other AF4 message subtypes) entirely from production code. Without an explicit constitutional rule, ports from AF4 risk re-introducing references to those types — they compile only as long as a stash dependency leaks them, and break the moment the upstream shadow disappears. The rule pins the canonical sourcing path for the data those types used to carry.

**Trigger**: feat/3594-DistributedTracing — AF4's `AggregateIdentifierSpanAttributesProvider` casts to `DomainEventMessage`. The port reads from `LegacyResources.AGGREGATE_IDENTIFIER_KEY` instead.

**Impact on other specs**: None at amendment time. Any existing feature spec that references `DomainEventMessage` MUST be flagged at next clarification pass. None do.

**Sync issues observed**: None — `CLAUDE.md` already says "Composition over Inheritance" and "AF5 idioms first"; this amendment makes the implicit rule explicit and verifiable.