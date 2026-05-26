---

description: "Task list for feature 137-message-transformation -- 5.2.0 deliverable slice"

---

# Tasks: Event Transformation API (5.2.0 deliverable slice)

**Input**: `/specs/137-message-transformation/` -- spec.md, plan.md, contracts/ (spi-commands-queries.md is 5.3+ reference only).

**Scope**: US1 (MUST) + US2 rename (SHOULD) + US7 chain hooks (SHOULD). US3 split, US4 drop, US5 chaining + VersionComparator, US6 conflict detection beyond FR-018 are deferred to a follow-up issue (T055); US8 / US9 / snapshots remain 5.3+.

**Tests**: JUnit 5 + AssertJ per in-scope FR; JMH for FR-011 in Polish.

**Demo + docs**: per-story tasks -- T035 + T036 for US1, T040 + T041 for US2, T050 + T051 for US7. Demo lives in `axon-framework/examples/university-demo/` (cross-repo PR); docs in `docs/reference-guide/modules/message-transformation/`.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no incomplete dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2)
- Paths are absolute under the new module: `messaging/axoniq-message-transformation/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the new Maven module skeleton and source-tree layout.

- [ ] T001 Create new Maven module skeleton at `messaging/axoniq-message-transformation/pom.xml` (parent: `axoniq-framework-parent` via `../../build/parent/pom.xml`; artifactId `axoniq-message-transformation`; dependencies on `org.axonframework:axon-messaging` and `org.axonframework:axon-eventsourcing`; test dependencies on JUnit 5, AssertJ, Awaitility, and `org.axonframework:axon-messaging:test-jar`)
- [ ] T002 Add `<module>messaging/axoniq-message-transformation</module>` to the root `pom.xml` `<modules>` block alongside the existing three messaging modules
- [ ] T003 [P] Create source directory layout `messaging/axoniq-message-transformation/src/main/java/io/axoniq/framework/messaging/transformation/{,events/,events/configuration/}` and `src/test/java/...`
- [ ] T004 [P] Create `package-info.java` files for `io.axoniq.framework.messaging.transformation`, `...transformation.events`, and `...transformation.events.configuration` annotated with `@org.jspecify.annotations.NullMarked` (per CLAUDE.md nullability rule)

**Checkpoint**: `./mvnw -pl messaging/axoniq-message-transformation -am compile` succeeds on an empty module.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: SPI base types and chain skeleton that US1, US2, and US7 depend on. Slim by
design -- only what the 5.2.0 MUST + rename SHOULD + hooks SHOULD scope needs. Deferred-story
scaffolding (`VersionComparator`, `SemverComparator`, conflict detection beyond FR-018) is
intentionally absent and will land additively later (see T055).

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T005 [P] Create `MessageTransformer<M extends Message>` base SPI in `messaging/axoniq-message-transformation/src/main/java/io/axoniq/framework/messaging/transformation/MessageTransformer.java` per contracts/spi-base.md (single method `MessageStream<M> transform(MessageStream<M> stream)`, `@NullMarked`, fragment-style Javadoc per CLAUDE.md). Generic over `Message` from day one (Forward-compat invariant #1)
- [ ] T006 [P] Create `EventTransformer extends MessageTransformer<EventMessage>` specialization in `.../transformation/events/EventTransformer.java` per contracts/spi-events.md
- [ ] T007 [P] Create `ChainConfigurationException` (RuntimeException) in `.../transformation/ChainConfigurationException.java` per contracts/spi-base.md (used by FR-004 lock-after-build and FR-018 output-identity-mismatch in US1; cycle / duplicate / self-loop detection lands later in the deferred US6 follow-up)
- [ ] T008 Create `MessageTransformerChain` class in `.../transformation/MessageTransformerChain.java`: `public static Builder builder()`, `public <M extends Message> MessageStream<M> transform(MessageStream<M> stream)` returning the input stream untransformed for now (US1 implementation fills the body), internal sub-chain map keyed by `QualifiedName` so the routing structure exists from day one (Forward-compat invariant #3). Chain MUST treat unknown `MessageType`s as pass-through from day one (FR-005 + Forward-compat invariant #6 snapshot pass-through)
- [ ] T009 Add default methods to `EventTransformer` per contracts/spi-events.md: `default EventTransformer when(Predicate<EventMessage>)` (wraps with a pre-apply skip) and `default EventTransformer onApplied(BiConsumer<EventMessage, MessageStream<? extends EventMessage>>)` (wraps with a post-apply observer). Both return a new wrapping `EventTransformer`; defaults (when no hook attached) MUST NOT allocate per event.
- [ ] T010 Create `MessageTransformerChain.Builder` with `register(MessageTransformer<?>)` and `build()` only. `build()` returns the immutable chain and flips a `locked` flag; late `register(...)` throws `ChainConfigurationException("chain is locked")` (FR-004). No `versionOrder(...)` yet (US5). Conflict-detection call sites stay as no-op placeholders (Forward-compat invariant #8). NO chain-wide hook methods -- hooks are per-transformer on `EventTransformer`.

**Checkpoint**: SPI skeleton compiles. No transformations applied yet. US1 + US2 + US7 can now proceed.

---

## Phase 3: User Story 1 -- Structural Payload Transformation (Priority: P1) 🎯 MVP

**Goal**: A developer can register a single 1:1 transformation (`from(MessageType).to(MessageType).transform(Class<T>, Function<T, U>)`) and have it applied at read time on every `EventStore` read path (entity load, DCB read, tracking processor).

**Independent Test**: With a stored `CourseCreated@1.0.0` event and a v1->v2 transformation registered, three reads (entity load, DCB `source()`, tracking processor `open()`) all observe `CourseCreated@2.0.0` with the transformed payload. A `SystemHeartbeat@1.0.0` in the same stream passes through unchanged.

### Tests for User Story 1 (write FIRST, ensure they FAIL before implementation)

- [ ] T011 [P] [US1] Contract test for `EventTransformer` (verifies `MessageStream<EventMessage> -> MessageStream<EventMessage>` shape; non-matching pass-through; metadata + envelope identity preservation) in `messaging/axoniq-message-transformation/src/test/java/io/axoniq/framework/messaging/transformation/events/EventTransformerContractTest.java`
- [ ] T012 [P] [US1] FR-001 acceptance test for `EventTransformation.from(...).to(...).transform(...)` covering US1 scenarios 1 + 2 (single transformation observed by multiple handlers) in `.../transformation/events/EventTransformationFr001Test.java`
- [ ] T013 [P] [US1] FR-005 pass-through test: events whose `MessageType` matches no registered transformation pass through unchanged with no `Converter` invocation (US1 scenario 3) in `.../transformation/MessageTransformerChainFr005Test.java`
- [ ] T014 [P] [US1] FR-006 concurrency test: invoke a registered transformation from N threads × M iterations (e.g. `N=8, M=10_000` per SC-008 once plan finalises numbers) using `Awaitility` for sync; assert all output payloads are byte-identical (US1 scenario 6) in `.../transformation/MessageTransformerChainFr006ConcurrencyTest.java`
- [ ] T015 [P] [US1] FR-009 declarative-target-type test: registered transformation declares `JsonNode` as input type; framework converts stored bytes to `JsonNode` via the registered `Converter` before invoking the mapper in `.../transformation/events/EventTransformationFr009Test.java`
- [ ] T016 [P] [US1] FR-010 envelope-preservation test: after transformation, the output `EventMessage`'s tracking token, sequence number, entity identifier, and entity type equal the input's; metadata flows forward (US1 scenario 1) in `.../transformation/events/EventEnvelopeFr010Test.java`
- [ ] T017 [P] [US1] FR-011 lazy-deserialization test: with a chain registered and a stream of non-matching events, assert `Converter` is never invoked AND lookup is constant-time (no per-event allocation; verify using `axon-framework`'s recording converter test util; JMH numbers land in T052) in `.../transformation/MessageTransformerChainFr011LazyTest.java`
- [ ] T018 [P] [US1] FR-012 read-context test: same stored `CourseCreated@1.0.0` event read through (a) `EventStore.transaction(...).source(...)` for entity load, (b) the same `source(...)` for DCB read, (c) `EventStore.open(...)` for tracking processor reads -- all produce the identical `CourseCreated@2.0.0` payload (US1 scenario 4) in `.../transformation/events/TransformingEventStoreFr012Test.java`
- [ ] T019 [P] [US1] FR-016 legacy-unversioned-events test: events stored without an explicit version (no `@Revision`-equivalent) match transformations registered for version `0.0.1` (US1 scenario 5) in `.../transformation/events/EventTransformationFr016Test.java`
- [ ] T020 [P] [US1] FR-017 unit-testability test: an `EventTransformer` returned by `EventTransformation.from(...).to(...).transform(...)` can be invoked directly from a plain JUnit test with only a `Converter` (no event store, no processor, no `ProcessingContext`) in `.../transformation/events/EventTransformationFr017Test.java`
- [ ] T021 [P] [US1] FR-018 output-identity-check test: a 1:1 transformation whose mapper returns a payload whose resolved `MessageType` differs from the declared `to` raises `ChainConfigurationException` (or runtime equivalent) with declared-to + actual-output + stream-position context (US6 scenario 7) in `.../transformation/MessageTransformerChainFr018Test.java`
- [ ] T022 [P] [US1] FR-021 data-protection-ordering test: assert `TransformingEventStore.DECORATION_ORDER` is numerically less than `InterceptingEventStore.DECORATION_ORDER` (use upstream constant or reflection) so the chain runs before any handler-side interceptor in `.../transformation/events/TransformingEventStoreFr021OrderTest.java`
- [ ] T023 [P] [US1] FR-004 lock-after-build test: registration attempts after `MessageTransformerChain.Builder.build()` throw `ChainConfigurationException("chain is locked")` in `.../transformation/MessageTransformerChainFr004LockTest.java`

### Implementation for User Story 1

- [ ] T024 [P] [US1] Implement `EventTransformation` factory class in `.../transformation/events/EventTransformation.java` with `public static SingleEventTransformationBuilder from(MessageType from)` per contracts/public-api.md. **Only the 1:1 path** in this phase. Method names `from`/`to`/`transform`/`rename`/`split`/`drop` are reserved on this factory per Forward-compat invariant #4 -- so even though `split` / `drop` are not implemented here, the factory MUST NOT use a different name that would have to be renamed later (just don't declare them yet; absence is additive)
- [ ] T025 [P] [US1] Implement `SingleEventTransformationBuilder.to(MessageType to)` and `SingleEventTransformationWithTargetBuilder.transform(Class<T> inputType, Function<T, U> payloadMapper)` in the same file; `transform(...)` returns an `EventTransformer` that wraps the mapper, capturing `from`, `to`, and `inputType`
- [ ] T026 [US1] Implement `MessageTransformerChain.Builder.register(MessageTransformer<?>)` (FR-004 lifecycle): rejects null, rejects registration after `build()` lock with `ChainConfigurationException("chain is locked")`, stores the entry into the per-`QualifiedName` sub-chain map keyed by `from.qualifiedName()` (uses the transformer's `from` identity, accessible via a package-private metadata accessor on the transformer)
- [ ] T027 [US1] Implement `MessageTransformerChain.transform(MessageStream<M>)` non-matching path (FR-005, FR-011): O(1) lookup by incoming `MessageType.qualifiedName()` in the sub-chain map; if no sub-chain matches, return the input stream unchanged with no `Converter` invocation, no allocation per event. This also makes snapshots automatically pass-through (Forward-compat invariant #6)
- [ ] T028 [US1] Implement `MessageTransformerChain.transform(MessageStream<M>)` matching 1:1 path (FR-001, FR-009): when the sub-chain matches and a transformer's `(from.qualifiedName(), from.version())` equals the incoming event's, the framework (a) converts the payload to the transformer's declared `inputType` via the registered `Converter`, (b) invokes the mapper, (c) wraps the result as a new `EventMessage` preserving envelope (FR-010), (d) updates the message's `MessageType` to the transformer's `to`. Single 1:1 entry per sub-chain in 5.2.0 -- multi-hop chaining lands additively with US5
- [ ] T029 [US1] Implement `MessageTransformerChain` FR-018 output identity check: after a 1:1 mapper returns, resolve the runtime payload's `MessageType` via the registered `MessageTypeResolver`; if it differs from the declared `to`, throw a `ChainConfigurationException` including declared-to, actual-output, and stream-position. Trivially satisfied for rename in US2 (framework sets identity itself)
- [ ] T030 [US1] Implement `MessageTransformerChain.Builder.build()`: flip the locked flag, wrap the sub-chain map in immutable views, return the chain. No FR-008 conflict detection yet (deferred)
- [ ] T031 [US1] Implement `TransformingEventStore` decorator in `.../transformation/events/TransformingEventStore.java` per contracts/spi-events.md: implements `EventStore`, holds a delegate + the chain; `open(StreamingCondition, ProcessingContext)` pipes through `chain.transform(...)`; `transaction(ProcessingContext)` returns a `TransformingEventStoreTransaction` cached per `ProcessingContext` via `Context.ResourceKey` (mirror `InterceptingEventStore`); `publish`, `firstToken`, `latestToken`, `tokenAt`, `subscribe`, `describeTo` delegate unchanged; set `DECORATION_ORDER = Integer.MIN_VALUE + 1000` -- outer (later) than `InterceptingEventStore` (`MIN_VALUE + 50`) with deliberate ~950 headroom so users / framework can slot other decorators in between (FR-021 ordering)
- [ ] T032 [US1] Implement `TransformingEventStoreTransaction` (package-private) in `.../transformation/events/TransformingEventStoreTransaction.java`: implements `EventStoreTransaction`, `source(SourcingCondition)` pipes through `chain.transform(...)`; `appendEvent`, `onAppend`, `overrideAppendCondition`, `appendPosition` delegate unchanged (chain at READ only, FR-021)
- [ ] T033 [US1] Implement `EventTransformationConfigurationEnhancer` in `.../transformation/events/configuration/EventTransformationConfigurationEnhancer.java`: looks up the user-registered `MessageTransformerChain.class` from the `ComponentRegistry`, registers `TransformingEventStore` as a decorator on `EventStore.class` with `TransformingEventStore.DECORATION_ORDER`
- [ ] T034 [US1] Register the enhancer for ServiceLoader discovery: create `messaging/axoniq-message-transformation/src/main/resources/META-INF/services/org.axonframework.configuration.ConfigurationEnhancer` containing one line, the fully-qualified name of `EventTransformationConfigurationEnhancer`

### Documentation for User Story 1

- [ ] T035 [US1] Create the reference-guide module for this feature at `docs/reference-guide/modules/message-transformation/` (mirror the structure of existing modules like `dead-letter-queue/`, `distributed-messaging/`): add `antora.yml`, `pages/index.adoc` (module overview), `pages/structural-transformation.adoc` (US1 walkthrough -- `CourseCreated v1 -> v2` with a typed `JsonNode` mapper, registering the chain via `EventSourcingConfigurer`, FR-009 / FR-010 / FR-012 / FR-018 / FR-021 notes); add the module to `docs/reference-guide/antora.yml` navigation. AsciiDoc style per `docs/CLAUDE.md`

### Demo example for User Story 1

- [ ] T036 [US1] Add the US1 scenario to `axon-framework/examples/university-demo/` (cross-repo, opens a PR on the `axon-framework` repo): introduce a `CourseCreatedV1` -> `CourseCreatedV2` evolution in the demo's faculty events, register a 1:1 `EventTransformation.from(...).to(...).transform(JsonNode.class, ...)` against `io.axoniq.framework:axoniq-message-transformation` (declared in the demo's `pom.xml`), seed an `events.jsonl`/in-memory stream with a v1 event so a JUnit test asserts the projection observes v2 -- ties to spec SC-003

**Checkpoint**: User Story 1 is fully functional, documented, demoed, and testable independently. A developer can register a single 1:1 transformation and have it observed across every read path. This is the MVP.

---

## Phase 4: User Story 2 -- Rename (Priority: P2)

**Goal**: A pure rename (`EventTransformation.rename(from, to)`) is FR-001 with no payload mapper -- a one-call convenience factory over the existing `from()`/`to()` flow.

**Independent Test**: A stored `CourseOpened@1.0.0` event with a registered `rename(CourseOpened@1.0.0, CourseCreated@1.0.0)` reaches a handler registered for `CourseCreated@1.0.0` with the original payload unchanged.

### Tests for User Story 2

- [ ] T037 [P] [US2] FR-002 rename test covering US2 scenarios 1 + 2 (renamed event reaches new-name handler; not delivered to old-name handler) in `messaging/axoniq-message-transformation/src/test/java/io/axoniq/framework/messaging/transformation/events/EventTransformationFr002RenameTest.java`
- [ ] T038 [P] [US2] FR-002 version-only-bump test (US2 scenario 3) in `.../transformation/events/EventTransformationFr002VersionBumpTest.java`

### Implementation for User Story 2

- [ ] T039 [US2] Add `public static EventTransformer rename(MessageType from, MessageType to)` to `EventTransformation` in `.../transformation/events/EventTransformation.java`; internally equivalent to `from(from).to(to)` with an identity payload mapper, but the rename factory sets the output identity itself, trivially satisfying FR-018 with no check needed (per contracts/public-api.md Javadoc)

### Documentation for User Story 2

- [ ] T040 [P] [US2] Extend the reference-guide module with `docs/reference-guide/modules/message-transformation/pages/rename.adoc`: walkthrough of `EventTransformation.rename(from, to)` covering the three US2 scenarios (cross-name rename, version-only bump, handler subscribes to new identity); update the module's navigation in `pages/index.adoc`

### Demo example for User Story 2

- [ ] T041 [P] [US2] Add the US2 rename scenario to `axon-framework/examples/university-demo/` (cross-repo PR): introduce a `CourseOpened@1.0.0 -> CourseCreated@1.0.0` rename alongside the US1 structural example; JUnit test asserts a stored `CourseOpened` event reaches a handler registered for `CourseCreated` with the payload unchanged

**Checkpoint**: US1 + US2 work independently, are documented in the reference guide, and demonstrated in the university-demo.

---

## Phase 5: User Story 7 -- Per-Transformer Hooks + Chain-Build DEBUG (Priority: P2)

**Goal**: Each `EventTransformer` carries optional `.when(Predicate)` (skip if false) and `.onApplied(BiConsumer)` (post-apply observer) attached at registration. Framework emits one DEBUG entry at chain build. Default (neither hook attached on a transformer) is zero-allocation. Matches AF4 `SingleEntryUpcaster.canUpcast` / `doUpcast` precedent.

**Independent Test**: with a registered `CourseCreated v1->v2` carrying `.when(p)` + `.onApplied(o)`: (a) `p` returning `false` skips the transformer for that input; (b) `o` is invoked with `(input, output)` for every applied transformation; (c) `.build()` emits one DEBUG line; (d) a transformer without `.when` / `.onApplied` attached produces zero per-event allocation on a 1M-event matching run.

### Tests for User Story 7 (write FIRST, ensure they FAIL before implementation)

- [ ] T042 [P] [US7] FR-013 chain-build DEBUG test: `.build()` emits one DEBUG with transformation count + each `from` / `to`. SLF4J test capture. In `.../transformation/MessageTransformerChainFr013BuildDebugTest.java`
- [ ] T043 [P] [US7] FR-013 `.when()` skip test: transformer registered with a predicate returning `false` is skipped; input passes through unchanged. In `.../transformation/events/EventTransformerFr013WhenTest.java`
- [ ] T044 [P] [US7] FR-013 `.onApplied()` observer test: observer receives `(input, output)` for every applied transformation; recorder count equals expected matches. In `.../transformation/events/EventTransformerFr013OnAppliedTest.java`
- [ ] T045 [P] [US7] FR-013 hooks-don't-fire-on-non-matching test: with `.when` + `.onApplied` both attached as counting observers, a non-matching stream leaves `recorder.count == 0`. In `.../transformation/events/EventTransformerFr013NonMatchingTest.java`
- [ ] T046 [P] [US7] FR-013 default-no-allocation test: a transformer without `.when` / `.onApplied` attached keeps `gc.alloc.rate.norm` constant on a 1M-event run. In `.../transformation/events/EventTransformerFr013DefaultsAllocationTest.java`

### Implementation for User Story 7

- [ ] T047 [US7] Implement `EventTransformer.when(Predicate<EventMessage>)` default method: returns a new `EventTransformer` that, on each input, calls the predicate; if `false`, emits the input unchanged; if `true`, delegates to this transformer. The wrapper class lives package-private in `.../transformation/events/`.
- [ ] T048 [US7] Implement `EventTransformer.onApplied(BiConsumer<EventMessage, MessageStream<? extends EventMessage>>)` default method: returns a new `EventTransformer` that delegates to this one, then calls the observer with `(input, output)`. Wrapper class lives package-private in `.../transformation/events/`.
- [ ] T049 [US7] Emit chain-build DEBUG in `MessageTransformerChain.Builder.build()`: SLF4J `DEBUG` with transformation count + each `from` (+ `to` for 1:1). Field set fixed per FR-013.

### Documentation for User Story 7

- [ ] T050 [P] [US7] Add `docs/reference-guide/modules/message-transformation/pages/hooks.adoc`: `.when()` feature-flag skip pattern + `.onApplied()` SLF4J / Micrometer patterns + chain-build DEBUG. Update `pages/index.adoc` navigation.

### Demo example for User Story 7

- [ ] T051 [P] [US7] Add a `.onApplied()` SLF4J logger to one transformation in `axon-framework/examples/university-demo/` (cross-repo PR) -- demonstrates per-transformer per-event TRACE.

**Checkpoint**: US1 + US2 + US7 work independently; the 5.2.0 deliverable slice is complete.

---

## Phase 6: Polish & Cross-Cutting

**Purpose**: Performance verification, Javadoc completeness, and follow-up tracking for deferred work. Per-story documentation and demo examples already shipped in their respective phases.

- [ ] T052 [P] JMH benchmark suite for FR-011 in `messaging/axoniq-message-transformation/src/test/java/io/axoniq/framework/messaging/transformation/jmh/MessageTransformerChainJmh.java` (or a sibling `jmh` source-set per `axon-framework` convention): chain length `{1, 10, 50, 100}` × 1M events, with `-prof gc`. Pass criteria from plan.md "Performance Goals": per-event latency variance `< 10%` across chain lengths; `gc.alloc.rate.norm` constant in chain length on the non-matching path
- [ ] T053 [P] Javadoc completeness pass on the 5.2.0 public API surface: `EventTransformation` (only the methods actually shipped: `from`, `to`, `transform`, `rename`), `MessageTransformerChain` (+ `Builder` with only `register` and `build`), `EventTransformer`, `ChainConfigurationException`. Follow CLAUDE.md "Javadoc Guidelines" (fragment-style `@param`/`@return`/`@throws`, `@link` references, `@since 5.2.0`, framework integration notes)
- [ ] T054 [P] Mark `TransformingEventStore`, `TransformingEventStoreTransaction`, `EventTransformationConfigurationEnhancer` with `@org.axonframework.common.annotation.Internal` (per contracts/spi-events.md) and document why in their Javadoc
- [ ] T055 Open a follow-up issue in `axoniq-framework` for the deferred user stories (US3 split, US4 drop, US5 chaining + `VersionComparator` / `SemverComparator`, US6 conflict detection beyond FR-018, US7 observability) -- their FRs: FR-003, FR-007, FR-008, FR-013, FR-014, FR-015, FR-020. The follow-up issue MUST scope each story to include its own docs page under `docs/reference-guide/modules/message-transformation/` AND its own demo extension under `axon-framework/examples/university-demo/`, matching the per-story doc + demo pattern established here. Reference this spec + plan.md scope tiers; note Forward-compat invariants #3, #4, #5, #6, #8, #9 keep all of these as pure additive changes

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: no dependencies; can start immediately.
- **Foundational (Phase 2)**: depends on Setup. BLOCKS US1, US2, US7.
- **US1 (Phase 3)**: blocked only by Foundational. MUST scope -- the issue's defining increment. Docs (T035) + demo (T036) depend on US1 implementation (T024-T034) being functional.
- **US2 (Phase 4)**: blocked by Foundational. Implementation (T039) is a thin add-on to US1's factory; in practice deliver after US1. Docs (T040) + demo (T041) extend US1's reference-guide page + the same university-demo module.
- **US7 (Phase 5)**: blocked by Foundational AND by US1 implementation (T026-T028 wire the matching path the hooks plug into). Independent of US2. Adds the chain-build DEBUG entry + per-transformer `.when` / `.onApplied` default methods on `EventTransformer`. Docs (T050) + demo (T051) extend the reference-guide module + the demo.
- **Polish (Phase 6)**: T052-T054 depend on US1 + US2 + US7 being complete; T055 (follow-up issue) is independent paperwork.

### Within Each User Story

- Tests are written FIRST and MUST fail before implementation tasks start (per CLAUDE.md TDD preference).
- Within a story phase, [P] tasks may run in parallel; non-[P] tasks are sequential and typically depend on the immediately-preceding [P] block.
- Docs and demo tasks come AFTER the story's tests + implementation are green so the artefacts describe behaviour that actually works.

### Parallel Opportunities

- **Phase 1**: T003 + T004 in parallel after T001 + T002.
- **Phase 2**: T005, T006, T007 in parallel; T008 + T009 + T010 sequential after.
- **Phase 3 (US1)**: all 13 test tasks (T011-T023) in parallel; T024 + T025 in parallel; the rest of the chain wiring (T026-T034) is sequential; T035 (docs) + T036 (demo) in parallel after T034.
- **Phase 4 (US2)**: T037 + T038 in parallel; T039 sequential after; T040 (docs) + T041 (demo) in parallel after T039.
- **Phase 5 (US7)**: all 5 test tasks (T042-T046) in parallel; T047, T048, T049 sequential implementation; T050 (docs) + T051 (demo) in parallel after T049.
- **Phase 6**: T052, T053, T054, T055 all parallel.

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
- Add Phase 5 (US7 chain hooks + chain-build DEBUG) -- enables user-defined per-event logging / metrics, completes the 5.2.0 deliverable slice.
- Phase 6 polish + follow-up issue creation closes out 5.2.0.
- All remaining deferred user stories (US3-US6) land in subsequent issues per Forward-compat invariants -- pure additive, no rewrites.

---

## Notes

- [P] = different files, no incomplete dependencies. Never [P] within the same file.
- Tests written first; verify they FAIL before implementing the corresponding US tasks.
- Method names on `EventTransformation` (`from`, `to`, `transform`, `rename`, plus the reserved-but-absent `split`, `drop`) are reserved from day one (Forward-compat invariant #4) even though `split` and `drop` are deferred. Never ship a different shape that would have to be renamed.
- `MessageTransformerChain.Builder` 5.2.0 surface: `register(...)`, `build()`. `versionOrder(...)` lands with US5. Per-transformer hooks (`.when` / `.onApplied`) live on `EventTransformer` itself -- matches AF4 `SingleEntryUpcaster.canUpcast` / `doUpcast` precedent, no chain-wide hooks, no generic framework-owned name like `Observability`.
- The `commandhandling/` + `queryhandling/` sub-packages are deliberately absent here (5.3+ command/query work; "CQRS" is intentionally not in any name). The demo for the 5.2.0 slice lives in `axon-framework/examples/university-demo/` and is delivered per-story via T036 (US1) + T041 (US2) + T051 (US7); no separate cross-repo demo follow-up task exists. Future deferred stories US3-US6 carry their own per-story demo + docs requirement via T055.
- Commit after each task or after a logical group; the `after_tasks` git extension hook offers to commit after this file is generated.
