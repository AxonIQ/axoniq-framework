---

description: "Task list for feature 137-message-transformation -- 5.2.0 deliverable slice"

---

# Tasks: Event Transformation API (5.2.0 deliverable slice)

**Input**: `/specs/137-message-transformation/` -- spec.md, plan.md, contracts/ (spi-commands-queries.md is 5.3+ reference only).

**Scope**: US1 (MUST -- includes both `from(MessageType)` AND `from(Predicate<MessageType>)` overloads) + US2 rename (SHOULD). Chain-build DEBUG entry (FR-013 first half) lands in foundational T009. Deferred to follow-up (T043): US3 split, US4 drop, US5 multi-hop chaining, US6 conflict detection beyond FR-018, US7 per-transformer hooks. US8 / US9 / snapshots remain 5.3+.

**Tests**: JUnit 5 + AssertJ per in-scope FR; JMH for FR-011 in Polish.

**Demo + docs**: per-story tasks -- T034 + T035 for US1, T039 + T040 for US2 (US7 docs / demo deferred to T044 follow-up). Demo lives in `AxonFramework/examples/university-demo/` (cross-repo PR; note case: `AxonFramework`, not `axon-framework`); docs in `docs/reference-guide/modules/message-transformation/`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no incomplete dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2)
- Paths are absolute under the new module: `messaging/axoniq-message-transformation/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the new Maven module skeleton and source-tree layout.

- [ ] T001 Create new Maven module skeleton at `messaging/axoniq-message-transformation/pom.xml` (parent: `axoniq-framework-parent` via `../../build/parent/pom.xml`; artifactId `axoniq-message-transformation`; dependencies on `org.axonframework:axon-messaging` and `org.axonframework:axon-eventsourcing`; test dependencies on JUnit 5, AssertJ, Awaitility, and `org.axonframework:axon-messaging:test-jar`)
- [ ] T002 Wire the new module into the build: (a) add `<module>messaging/axoniq-message-transformation</module>` to the root `axoniq-framework/pom.xml` `<modules>` block alongside the existing three messaging modules (`axoniq-dead-letter`, `axoniq-distributed-messaging`, `axoniq-event-streaming`); (b) add a `<dependency>` entry for `io.axoniq.framework:axoniq-message-transformation:${project.version}` under the `<!-- Axoniq Framework: Messaging -->` section of `axoniq-framework-bom/pom.xml` (the BOM does NOT auto-import; explicit entry required -- verified at `axoniq-framework-bom/pom.xml:61-76`)
- [ ] T003 [P] Create source directory layout `messaging/axoniq-message-transformation/src/main/java/io/axoniq/framework/messaging/transformation/{,events/,events/configuration/}` and `src/test/java/...`
- [ ] T004 [P] Create `package-info.java` files for `io.axoniq.framework.messaging.transformation`, `...transformation.events`, and `...transformation.events.configuration` annotated with `@org.jspecify.annotations.NullMarked` (per CLAUDE.md nullability rule)

**Checkpoint**: `./mvnw -pl messaging/axoniq-message-transformation -am compile` succeeds on an empty module.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: SPI base types and chain skeleton. Slim by design -- only what 5.2.0 MUST +
SHOULD scope needs. Deferred-story scaffolding (per-transformer `.when` / `.onApplied` hooks,
multi-step cycle detection across predicate edges, `Builder.maxIterations`, etc.) is
intentionally absent and lands additively later (see T043).

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T005 [P] Create `MessageTransformer<M extends Message>` base SPI in `messaging/axoniq-message-transformation/src/main/java/io/axoniq/framework/messaging/transformation/MessageTransformer.java` per contracts/spi-base.md (single method `MessageStream<? extends M> transform(M message, @Nullable ProcessingContext context)`, annotated with BOTH `@FunctionalInterface` (matches AF5 convention -- verified at `EventHandler.java:31`, `CommandHandler.java:30`, `QueryHandler.java:29`, all three carry it) AND `@NullMarked`, fragment-style Javadoc per CLAUDE.md). Generic over `Message` from day one (Forward-compat invariant #1).
- [ ] T006 [P] Create `EventTransformer extends MessageTransformer<EventMessage>` specialization in `.../transformation/events/EventTransformer.java` per contracts/spi-events.md
- [ ] T007 [P] Create `ChainConfigurationException` (RuntimeException) in `.../transformation/ChainConfigurationException.java` per contracts/spi-base.md (used by FR-004 lock-after-build and FR-018 output-identity-mismatch in US1; cycle / duplicate / self-loop detection lands later in the deferred US6 follow-up)
- [ ] T008 Create `EventTransformerChain` class in `.../transformation/events/EventTransformerChain.java`: `public static Builder builder()`, `public MessageStream<? extends EventMessage> transform(MessageStream<EventMessage> stream)` returning the input stream untransformed for now (US1 implementation fills the body). Internal storage: `QualifiedName`-keyed map for concrete-`from` transformers + a flat list for predicate-`from` transformers (both used in 5.2.0). Unknown `MessageType`s pass through (FR-005 + Forward-compat invariant #6 snapshot pass-through). The chain is typed to `EventMessage` (not generic) -- command and query chains are separate sibling classes in 5.3+ (`CommandTransformerChain`, `QueryTransformerChain`), not subtypes of a shared base.
- [ ] T009 Create `EventTransformerChain.Builder` shell with `register(EventTransformer)` and `build()` signatures per contracts/spi-events.md. Empty bodies in foundational; routing + locking logic land in T025 / T029. NO chain-wide hook methods (per design); deferred-story conflict-detection call sites stay as no-op placeholders (Forward-compat invariant #8).

**Checkpoint**: SPI skeleton compiles. No transformations applied yet. US1 + US2 can now proceed.

---

## Phase 3: User Story 1 -- Structural Payload Transformation (Priority: P1) 🎯 MVP

**Goal**: A developer can register a single 1:1 transformation (`from(MessageType).to(MessageType).transform(Class<T>, BiFunction<T, ProcessingContext, U>)`) and have it applied at read time on every `EventStore` read path (entity load, DCB read, tracking processor).

**Independent Test**: With a stored `CourseCreated@1.0.0` event and a v1->v2 transformation registered, three reads (entity load, DCB `source()`, tracking processor `open()`) all observe `CourseCreated@2.0.0` with the transformed payload. A `SystemHeartbeat@1.0.0` in the same stream passes through unchanged.

### Tests for User Story 1 (write FIRST, ensure they FAIL before implementation)

- [ ] T010 [P] [US1] Contract test for `EventTransformer` (verifies `MessageStream<EventMessage> -> MessageStream<EventMessage>` shape; non-matching pass-through; metadata + envelope identity preservation) in `messaging/axoniq-message-transformation/src/test/java/io/axoniq/framework/messaging/transformation/events/EventTransformerContractTest.java`
- [ ] T011 [P] [US1] FR-001 acceptance test for `EventTransformation.from(...).to(...).transform(...)` covering US1 scenarios 1 + 2 (single transformation observed by multiple handlers) in `.../transformation/events/EventTransformationFr001Test.java`
- [ ] T012 [P] [US1] FR-005 pass-through test: events whose `MessageType` matches no registered transformation pass through unchanged with no `Converter` invocation (US1 scenario 3) in `.../transformation/events/EventTransformerChainFr005Test.java`
- [ ] T013 [P] [US1] FR-006 concurrency + nullable-context test: invoke a registered transformation from N threads × M iterations (e.g. `N=8, M=10_000` per SC-008 once plan finalises numbers) using `Awaitility` for sync; assert all output payloads are byte-identical (US1 scenario 6). Include cases where `ProcessingContext` is `null` (tracking-processor read path) and a non-null context (entity load path) -- both MUST produce identical output. In `.../transformation/events/EventTransformerChainFr006ConcurrencyTest.java`.
- [ ] T014 [P] [US1] FR-009 declarative-target-type test: covers BOTH overloads. (a) `transform(Class<T>, ...)`: registered transformation declares `JsonNode` as input type; framework invokes `MessageConverter.convertPayload(message, JsonNode.class)` to obtain the typed payload before invoking the mapper. (b) `transform(TypeReference<T>, ...)`: registered transformation declares a generic input type via `new TypeReference<Map<String, Object>>() {}`; the lambda parameter type `Map<String, Object>` is inferred (no manual cast); framework invokes `MessageConverter.convertPayload(message, typeReference.getType())` and the mapper receives a parameterised `Map`. Both in `.../transformation/events/EventTransformationFr009Test.java`
- [ ] T015 [P] [US1] FR-010 envelope-preservation test: after transformation, the output `EventMessage`'s tracking token, sequence number, entity identifier, and entity type equal the input's; metadata flows forward (US1 scenario 1) in `.../transformation/events/EventEnvelopeFr010Test.java`
- [ ] T016 [P] [US1] FR-011 lazy-deserialization test: with a chain registered and a stream of non-matching events, assert `Converter` is never invoked AND lookup is constant-time (no per-event allocation; verify using `axon-framework`'s recording converter test util; JMH numbers land in T041) in `.../transformation/events/EventTransformerChainFr011LazyTest.java`
- [ ] T017 [P] [US1] FR-012 read-context test: same stored `CourseCreated@1.0.0` event read through (a) `EventStore.transaction(...).source(...)` for entity load, (b) the same `source(...)` for DCB read, (c) `EventStore.open(...)` for tracking processor reads -- all produce the identical `CourseCreated@2.0.0` payload (US1 scenario 4) in `.../transformation/events/TransformingEventStoreFr012Test.java`
- [ ] T018 [P] [US1] FR-016 legacy-unversioned-events test: events stored without an explicit version (no `@Revision`-equivalent) match transformations registered for version `0.0.1` (US1 scenario 5) in `.../transformation/events/EventTransformationFr016Test.java`
- [ ] T019 [P] [US1] FR-017 unit-testability test: an `EventTransformer` returned by `EventTransformation.from(...).to(...).transform(...)` can be invoked directly from a plain JUnit test with only a `Converter` (no event store, no processor, no `ProcessingContext`) in `.../transformation/events/EventTransformationFr017Test.java`
- [ ] T020 [P] [US1] FR-018 output-identity-check test (resolver-permitting). Three cases in `.../transformation/events/EventTransformerChainFr018Test.java`: (a) **POJO-with-@Event mismatch** -- mapper returns a typed POJO whose resolved `MessageType` differs from the declared `to`: assert `ChainConfigurationException` (or runtime equivalent) is raised with declared-to + actual-output + stream-position context (US6 scenario 7). (b) **POJO-with-@Event match** -- mapper returns a typed POJO whose resolved `MessageType` equals the declared `to`: no exception. (c) **Untyped (JsonNode / Map) output** -- mapper returns a `JsonNode` (or `Map<String, Object>`); `MessageTypeResolver.resolve(Class<?>)` returns `Optional.empty()`; assert the chain SKIPS the check (no exception thrown even if the runtime payload-type doesn't carry an identity annotation). This is the only feasible behaviour per FR-018's resolver-permitting clause.
- [ ] T021 [P] [US1] FR-021 data-protection-ordering test: assert `TransformingEventStore.DECORATION_ORDER` is numerically less than `InterceptingEventStore.DECORATION_ORDER` (use upstream constant or reflection) so the chain runs before any handler-side interceptor in `.../transformation/events/TransformingEventStoreFr021OrderTest.java`
- [ ] T022 [P] [US1] FR-004 lock-after-build test: registration attempts after `EventTransformerChain.Builder.build()` throw `ChainConfigurationException("chain is locked")` in `.../transformation/events/EventTransformerChainFr004LockTest.java`

### Implementation for User Story 1

- [ ] T023 [P] [US1] Implement `EventTransformation.from(MessageType)` AND `EventTransformation.from(Predicate<MessageType>)` in `.../transformation/events/EventTransformation.java` per contracts/public-api.md. Both overloads return the same `SingleEventTransformationBuilder`. Method names `from`/`to`/`transform`/`rename`/`split`/`drop` reserved (Forward-compat invariant #4); deferred ones (`split`, `drop`) simply absent in 5.2.0.
- [ ] T024 [P] [US1] Implement `SingleEventTransformationBuilder.to(MessageType)` and BOTH `transform(...)` overloads on `SingleEventTransformationWithTargetBuilder`: `transform(Class<T> inputType, BiFunction<T, ProcessingContext, U> payloadMapper)` for non-generic input types AND `transform(org.axonframework.common.TypeReference<T> inputType, BiFunction<T, ProcessingContext, U> payloadMapper)` for generic input types like `Map<String, Object>` or `List<Foo>` (per `.claude/rules/type-safety.md` and mirroring `Configuration.getComponent(TypeReference)` at `Configuration.java:109-136`; using `TypeReference<T>` binds `T` at compile time so the lambda parameter type is inferred -- no manual cast). Both overloads return an `EventTransformer` that captures `from`, `to`, and the input type (stored as `java.lang.reflect.Type` internally: the `Class<T>` overload stores the class directly; the `TypeReference<T>` overload stores the result of `inputType.getType()`). The internal `Type` is what `MessageConverter.convertPayload(message, Type)` accepts at chain-application time (T027).
- [ ] T025 [US1] Implement `EventTransformerChain.Builder.register(EventTransformer)` (FR-004 lifecycle): rejects null, rejects registration after `build()` lock. Routes the entry by `from` flavour -- concrete `MessageType` -> `QualifiedName`-keyed map (by `from.qualifiedName()`); `Predicate<MessageType>` -> flat predicate list. The `from` identity / predicate is accessible via a package-private metadata accessor on the transformer.
- [ ] T026 [US1] Implement `EventTransformerChain.transform(MessageStream<EventMessage>)` non-matching path (FR-005, FR-011): O(1) `QualifiedName`-keyed lookup for concrete-`from` transformers + scan the predicate list (O(P) where P = predicate-`from` count). No match anywhere -> input passes through unchanged with no `Converter` invocation, no per-event allocation. `SnapshotEventMessage` entries on the entity-load stream (prepended by `SnapshotCapableEventStorageEngine.java:82` when `SourcingStrategy.Snapshot` is active) pass through unchanged because their `MessageType` matches no registered event transformer (Forward-compat invariant #6, entity-load path only -- tracking-processor reads via `open(...)` carry no snapshots).
- [ ] T027 [US1] Implement `EventTransformerChain.transform(MessageStream<EventMessage>)` fixed-point iteration path (FR-007, FR-001, FR-009): per input element, walk concrete-`from` candidates (from the `QualifiedName`-keyed list) AND predicate-`from` candidates (from the flat predicate list) in overall registration order; **track the last match** (do not short-circuit on first). For the matched transformer, obtain the typed input payload by calling `MessageConverter.convertPayload(message, inputType)` (verified at `messaging/core/conversion/MessageConverter.java`; the `Type`-taking overload covers both the `Class<T>` and generic-`Type` paths from T024), invoke the mapper with `(payload, ProcessingContext)`, wrap the result preserving envelope (FR-010), update `MessageType` to the transformer's `to`. Restart the lookup from the top with the new message; terminate when nothing matches. Include a defensive safety bound on per-message iteration to guard against pathological misconfiguration; throw `ChainConfigurationException` if exceeded.
- [ ] T028 [US1] Implement `EventTransformerChain` FR-018 output identity check (resolver-permitting): after a 1:1 mapper returns, call `messageTypeResolver.resolve(mapperOutput.getClass())` (verified at `messaging/core/MessageTypeResolver.java:33-92`). If the returned `Optional<MessageType>` is **empty** (typical for untyped representations: `JsonNode`, `Map<String, Object>`, raw bytes -- their classes carry no `@Event` / `@Message` annotation), SKIP the check (this is the only feasible behaviour; the framework trusts the mapper). If the `Optional` is **present** and differs from the declared `to`, throw `ChainConfigurationException` including declared-to, actual-output-class, and stream-position. Trivially satisfied for rename in US2 (framework sets identity itself).
- [ ] T029 [US1] Implement `EventTransformerChain.Builder.build()`: flip the locked flag, wrap the internal map + list in immutable views, emit the chain-build DEBUG entry (FR-013), return the chain. FR-008 multi-step cycle detection on concrete-edge graph is deferred to follow-up; duplicate / self-loop checks land here.
- [ ] T030 [US1] Implement `TransformingEventStore` decorator in `.../transformation/events/TransformingEventStore.java` per contracts/spi-events.md: implements `EventStore`, constructor takes `(EventStore delegate, EventTransformerChain chain, MessageConverter converter)` -- the converter is supplied by `EventTransformationConfigurationEnhancer` at decorator-registration time (T032), the user never threads it through the builder. The chain is a passive registry; this decorator owns the `MessageConverter` reference and calls `converter.convertPayload(message, transformer.inputType())` before invoking each matched transformer's mapper. `open(StreamingCondition, @Nullable ProcessingContext)` pipes the inner stream through the chain-with-converter -- **note the deliberate deviation** from the `InterceptingEventStore` precedent (`InterceptingEventStore.java:153-155` delegates `open(...)` straight through; we MUST wrap to honour FR-012 on tracking-processor reads), no per-call caching because the context is nullable; `transaction(ProcessingContext)` returns a `TransformingEventStoreTransaction` cached per `ProcessingContext` via `Context.ResourceKey.withLabel("transformingEventStoreTransaction")` (mirrors `StorageEngineBackedEventStore.java:75` / `InterceptingEventStore.java:78-79`); `publish`, `firstToken`, `latestToken`, `tokenAt`, `subscribe`, `describeTo` delegate unchanged; set `DECORATION_ORDER = Integer.MIN_VALUE + 1000` -- outer (later) than `InterceptingEventStore` (`MIN_VALUE + 50`, verified) with deliberate ~950 headroom so users / framework can slot other decorators in between (FR-021 ordering). Higher order = outer per `DefaultComponentRegistry.java:299-303`.
- [ ] T031 [US1] Implement `TransformingEventStoreTransaction` (package-private) in `.../transformation/events/TransformingEventStoreTransaction.java`: implements `EventStoreTransaction`; constructor takes `(EventStoreTransaction delegate, EventTransformerChain chain, MessageConverter converter)` -- the same trio of dependencies as `TransformingEventStore` (T030); the chain is passive and the converter is needed to call `convertPayload(message, inputType)` when a transformer matches. `source(SourcingCondition)` pipes the delegate's stream through `chain.transform(...)` (with converter access in the chain-walk implementation); `appendEvent`, `onAppend`, `overrideAppendCondition`, `appendPosition` delegate unchanged (chain at READ only, FR-021).
- [ ] T032 [US1] Implement `EventTransformationConfigurationEnhancer` in `.../transformation/events/configuration/EventTransformationConfigurationEnhancer.java` per the body in contracts/spi-events.md: in `enhance(ComponentRegistry registry)`, call `registry.registerDecorator(EventStore.class, TransformingEventStore.DECORATION_ORDER, (config, name, delegate) -> {...})` where the decorator lambda receives a `Configuration`, resolves both `config.getComponent(EventTransformerChain.class)` and `config.getComponent(MessageConverter.class)`, returns a no-op (just `delegate`) when no chain is registered, otherwise constructs `new TransformingEventStore(delegate, chain, converter)`. Pattern mirrors `EventSourcingConfigurationDefaults.java:96-108` (the canonical AF5 way to inject a `MessageConverter` into a decorator).
- [ ] T033 [US1] Register the enhancer for ServiceLoader discovery: create `messaging/axoniq-message-transformation/src/main/resources/META-INF/services/org.axonframework.configuration.ConfigurationEnhancer` containing one line, the fully-qualified name of `EventTransformationConfigurationEnhancer`

### Documentation for User Story 1

- [ ] T034 [US1] Create the reference-guide module for this feature at `docs/reference-guide/modules/message-transformation/` (mirror the structure of existing modules like `dead-letter-queue/`, `distributed-messaging/`): add `antora.yml`, `pages/index.adoc` (module overview), `pages/structural-transformation.adoc` (US1 walkthrough -- `CourseCreated v1 -> v2` with a typed `JsonNode` mapper, registering the chain via `EventSourcingConfigurer`, FR-009 / FR-010 / FR-012 / FR-018 / FR-021 notes), `pages/predicate-matching.adoc` (the `from(Predicate<MessageType>)` overload + a "**Watch out: overlap**" worked example showing last-match-in-registration-order semantics per FR-005 -- "a later registration overrides an earlier one whose `from` also matches"); add the module to `docs/reference-guide/antora.yml` navigation. AsciiDoc style per `docs/CLAUDE.md`.

### Demo example for User Story 1

- [ ] T035 [US1] Add the US1 scenario to `AxonFramework/examples/university-demo/` (cross-repo, opens a PR on the `AxonFramework` repo -- case-sensitive directory name): introduce a `CourseCreatedV1` -> `CourseCreatedV2` evolution in the demo's faculty events, register a 1:1 `EventTransformation.from(...).to(...).transform(JsonNode.class, ...)` against `io.axoniq.framework:axoniq-message-transformation` (declared in the demo's `pom.xml`), seed an `events.jsonl`/in-memory stream with a v1 event so a JUnit test asserts the projection observes v2 -- ties to spec SC-003

**Checkpoint**: User Story 1 is fully functional, documented, demoed, and testable independently. A developer can register a single 1:1 transformation and have it observed across every read path. This is the MVP.

---

## Phase 4: User Story 2 -- Rename (Priority: P2)

**Goal**: A pure rename (`EventTransformation.rename(from, to)`) is FR-001 with no payload mapper -- a one-call convenience factory over the existing `from()`/`to()` flow.

**Independent Test**: A stored `CourseOpened@1.0.0` event with a registered `rename(CourseOpened@1.0.0, CourseCreated@1.0.0)` reaches a handler registered for `CourseCreated@1.0.0` with the original payload unchanged.

### Tests for User Story 2

- [ ] T036 [P] [US2] FR-002 rename test covering US2 scenarios 1 + 2 (renamed event reaches new-name handler; not delivered to old-name handler) in `messaging/axoniq-message-transformation/src/test/java/io/axoniq/framework/messaging/transformation/events/EventTransformationFr002RenameTest.java`
- [ ] T037 [P] [US2] FR-002 version-only-bump test (US2 scenario 3) in `.../transformation/events/EventTransformationFr002VersionBumpTest.java`

### Implementation for User Story 2

- [ ] T038 [US2] Add `public static EventTransformer rename(MessageType from, MessageType to)` to `EventTransformation` in `.../transformation/events/EventTransformation.java`; internally equivalent to `from(from).to(to)` with an identity payload mapper, but the rename factory sets the output identity itself, trivially satisfying FR-018 with no check needed (per contracts/public-api.md Javadoc)

### Documentation for User Story 2

- [ ] T039 [P] [US2] Extend the reference-guide module with `docs/reference-guide/modules/message-transformation/pages/rename.adoc`: walkthrough of `EventTransformation.rename(from, to)` covering the three US2 scenarios (cross-name rename, version-only bump, handler subscribes to new identity); update the module's navigation in `pages/index.adoc`

### Demo example for User Story 2

- [ ] T040 [P] [US2] Add the US2 rename scenario to `AxonFramework/examples/university-demo/` (cross-repo PR; case-sensitive directory name): introduce a `CourseOpened@1.0.0 -> CourseCreated@1.0.0` rename alongside the US1 structural example; JUnit test asserts a stored `CourseOpened` event reaches a handler registered for `CourseCreated` with the payload unchanged

**Checkpoint**: US1 + US2 work independently, are documented in the reference guide, and demonstrated in the university-demo.

---

## Phase 5: Polish & Cross-Cutting

**Purpose**: Performance verification, Javadoc completeness, and follow-up tracking for deferred work. Per-story documentation and demo examples already shipped in their respective phases.

- [ ] T041 [P] JMH benchmark suite in `messaging/axoniq-message-transformation/src/test/java/io/axoniq/framework/messaging/transformation/jmh/EventTransformerChainJmh.java` (or a sibling `jmh` source-set per `axon-framework` convention) covering both paths:
  - **Non-matching path (FR-011)**: chain length `{1, 10, 50, 100}` × 1M events, `-prof gc`. Pass: per-event latency variance `< 10%` across chain lengths; `gc.alloc.rate.norm` constant in chain length.
  - **Matching path**: `{1, 5, 20}` transformers × `{1, 3, 5}` hops per message, `-prof gc`. Records baseline cost of the fixed-point iteration so future regressions surface.
- [ ] T042 [P] Javadoc completeness pass on the 5.2.0 public API surface: `EventTransformation` (only the methods actually shipped: `from(MessageType)`, `from(Predicate<MessageType>)`, `to`, `transform(Class<T>, BiFunction<T, ProcessingContext, U>)`, `transform(TypeReference<T>, BiFunction<T, ProcessingContext, U>)`, `rename`), `EventTransformerChain` (+ `Builder` with only `register` and `build`), `EventTransformer`, `ChainConfigurationException`. Follow CLAUDE.md "Javadoc Guidelines" (fragment-style `@param`/`@return`/`@throws`, `@link` references, `@since 5.2.0`, framework integration notes)
- [ ] T043 [P] Mark `TransformingEventStore`, `TransformingEventStoreTransaction`, `EventTransformationConfigurationEnhancer` with `@org.axonframework.common.annotation.Internal` (per contracts/spi-events.md) and document why in their Javadoc
- [ ] T044 Open a follow-up issue in `axoniq-framework` for the deferred work: US3 split, US4 drop, US5 chaining (multi-hop), US6 conflict detection beyond FR-018 (cycle detection across predicate edges), US7 per-transformer hooks (`.when` / `.onApplied` on the base SPI), and `Builder.maxIterations(int)` to make the chain's defensive runtime safety bound user-tunable (default stays implementation-defined, override for unusually deep chains). FRs: FR-003, FR-007 (multi-hop), FR-008 (predicate-edge cycles), FR-013 (per-transformer half), FR-014, FR-015. Also open a separate `axon-common` follow-up for the `SemverPredicate` helper (FR-020 convenience). Each story MUST include its own docs page + demo extension. Forward-compat invariants #3, #4, #5, #6, #8, #9 keep all of this as pure additive change.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependencies; can start immediately.
- **Foundational (Phase 2)**: depends on Setup. BLOCKS US1, US2.
- **US1 (Phase 3)**: blocked only by Foundational. MUST scope -- the issue's defining increment. Docs (T034) + demo (T035) depend on US1 implementation (T023-T033) being functional.
- **US2 (Phase 4)**: blocked by Foundational. Implementation (T038) is a thin add-on to US1's factory; in practice deliver after US1. Docs (T039) + demo (T040) extend US1's reference-guide page + the same university-demo module.
- **Polish (Phase 5)**: T041-T043 depend on US1 + US2 being complete; T044 (follow-up issue) is independent paperwork.

### Within Each User Story

- Tests are written FIRST and MUST fail before implementation tasks start (per CLAUDE.md TDD preference).
- Within a story phase, [P] tasks may run in parallel; non-[P] tasks are sequential and typically depend on the immediately-preceding [P] block.
- Docs and demo tasks come AFTER the story's tests + implementation are green so the artefacts describe behaviour that actually works.

### Parallel Opportunities

- **Phase 1**: T003 + T004 in parallel after T001 + T002.
- **Phase 2**: T005, T006, T007 in parallel; T008 + T009 sequential after.
- **Phase 3 (US1)**: all 13 test tasks (T010-T022) in parallel; T023 + T024 in parallel; the rest of the chain wiring (T025-T033) is sequential; T034 (docs) + T035 (demo) in parallel after T033.
- **Phase 4 (US2)**: T036 + T037 in parallel; T038 sequential after; T039 (docs) + T040 (demo) in parallel after T038.
- **Phase 5 (Polish)**: T041, T042, T043, T044 all parallel.

---

## Implementation Strategy

### MVP (US1 only)

1. Complete Phase 1: Setup.
2. Complete Phase 2: Foundational.
3. Complete Phase 3: US1.
4. **STOP and VALIDATE**: prove US1's independent test scenarios (entity load + DCB + tracking processor produce same v2 payload from stored v1 event).
5. Ship as a 5.2.0 preview. The issue's MUST scope is now closed.

### Incremental delivery

- Add Phase 4 (US2 rename) right after US1 -- cheap rename win.
- Phase 6 polish + follow-up issue creation closes out 5.2.0.
- All remaining deferred user stories (US3-US6) land in subsequent issues per Forward-compat invariants -- pure additive, no rewrites.

---

## Notes

- [P] = different files, no incomplete dependencies. Never [P] within the same file.
- Tests written first; verify they FAIL before implementing the corresponding US tasks.
- Method names on `EventTransformation` (`from`, `to`, `transform`, `rename`, plus the reserved-but-absent `split`, `drop`) are reserved from day one (Forward-compat invariant #4) even though `split` and `drop` are deferred. Never ship a different shape that would have to be renamed.
- `EventTransformerChain.Builder` 5.2.0 surface: `register(...)`, `build()` (no converter argument -- the chain is a passive registry; the `EventTransformationConfigurationEnhancer` resolves the `MessageConverter` from `Configuration` and passes it to `TransformingEventStore`). `EventTransformation` factory: `from(MessageType)`, `from(Predicate<MessageType>)`, `to(...)`, `transform(Class<T>, BiFunction<T, ProcessingContext, U>)` AND `transform(org.axonframework.common.TypeReference<T>, BiFunction<T, ProcessingContext, U>)` (the latter for generic input types like `Map<String, Object>` per `.claude/rules/type-safety.md`, mirroring `Configuration.getComponent(TypeReference)`), `rename(...)`. Deferred: `split(...)`, `drop(...)`, per-transformer `.when` / `.onApplied`. No chain-wide hooks; no generic framework-owned name like `Observability`. Per-transformer hooks, when they land, follow the AF4 `SingleEntryUpcaster.canUpcast` / `doUpcast` precedent.
- The `commandhandling/` + `queryhandling/` sub-packages are deliberately absent here (5.3+ command/query work; "CQRS" is intentionally not in any name). The demo for the 5.2.0 slice lives in `AxonFramework/examples/university-demo/` (note case: `AxonFramework`, not `axon-framework`) and is delivered per-story via T035 (US1) + T040 (US2); no separate cross-repo demo follow-up task exists. Deferred stories US3-US6 + the US7 per-transformer-hooks half carry their own per-story demo + docs requirement via T044.
- Commit after each task or after a logical group; the `after_tasks` git extension hook offers to commit after this file is generated.
