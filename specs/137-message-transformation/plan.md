# Implementation Plan: Event Transformation API

**Branch**: `enhancement/137/implementation-message-transformator` | **Date**: 2026-05-21 | **Spec**: [spec.md](spec.md)

**Repo**: this feature now lives in `axoniq-framework` (issue moves there). `axon-framework` stays untouched except for one small additive change (see "Required axon-framework additions").

## Summary

Add an event transformation chain to AxonIQ Framework 5.2.0 that runs as a decorator around the `EventStore` and rewrites a message's identity (`MessageType`) and/or payload before routing. The chain is a single shared object the developer registers once at startup; the framework wires it as a thin decorator. No converter-side decoration; matching events are deserialized eagerly. FR-011 still protects the non-matching path.

The events decorator targets the publicly-facing `EventStore` interface, NOT the underlying `EventStorageEngine`. `EventStorageEngine` is `@Internal` and is an implementation detail of `StorageEngineBackedEventStore`; tying transformation to it would restrict the feature to that one EventStore family. Decorating `EventStore` keeps transformation available to any future `EventStore` implementation that does not route through an `EventStorageEngine`.

The internal SPI is stream-in / stream-out (`MessageStream<M> -> MessageStream<M>`), exposed for events as `EventTransformer extends MessageTransformer<EventMessage>`. User-facing factory `EventTransformation` (`from(...).to(...).transform(...)`, `rename(...)`, `split(...).transform(...)`, `drop(...)`) produces transformer instances; users rarely touch the SPI directly. The SPI is intentionally generic over `Message` so commands and queries can join the design in 5.3+ without an SPI break.

### Delivery scope

**5.2.0 minimum (issue AxonIQ/axoniq-framework#137)** -- delivers what is necessary for a user to configure a 1:1 event transformer:

- **MUST**: US1 (1:1 structural transform), FR-001, FR-004 (programmatic registration + lifecycle), the supporting envelope/payload-access/legacy-version invariants (FR-005, FR-010, FR-011, FR-012, FR-016, FR-017, FR-018, FR-021).
- **SHOULD if it fits**: US2 (rename), FR-002; US7 per-transformer hooks (`.when(...)` + `.onApplied(...)` on the base `MessageTransformer<M>`, so events / commands / queries all inherit them + chain-build DEBUG entry, FR-013).
- **NICE-TO-HAVE for 5.2.0, else 5.3.0**: US3 (split), US4 (drop), US5 (chaining), US6 (conflict / runtime-failure feedback); FRs FR-003, FR-007, FR-008, FR-014, FR-015, FR-020. (US7 ships in SHOULD; an optional reference logging hook may follow.)

**5.3+ candidates** (explicit, not part of this issue):

- **Command transformation** (US8) and **query transformation** (US9) -- design-supported by the generic SPI; decoration point is open (see Integration points / 5.3+ note below).
- **Snapshot transformation** -- architecturally compatible (chain decorates `EventStore`, snapshots flow through the same stream), only user-facing API / docs / fixtures deferred.

## Technical Context

**Language/Version**: Java 21 (sealed types, records, pattern matching).

**Primary Dependencies**: `axon-framework` core modules (`common`, `messaging`, `modelling`, `eventsourcing`). JUnit 5, AssertJ, Awaitility for tests; JMH for FR-011 chain-cost benchmarks.

**Target Repo**: `axoniq-framework`. One NEW module ships in 5.2.0: `messaging/axoniq-message-transformation/`. Depends on `axon-framework`'s `messaging` + `eventsourcing` modules. Internal package layout follows the `axoniq-distributed-messaging` precedent (`commandhandling/` + `queryhandling/` in one module): a shared `transformation/` base + an `events/` sub-package for 5.2.0. A `cqrs/` sub-package is filled in for 5.3+ when command and query transformation are delivered. Because the handler-registration decoration target lives in `axon-framework` (`CommandBus` / `QueryBus` are upstream types), no additional axoniq dependency is needed in 5.3+ either -- the module dependency set stays constant.

**Integration point (5.2.0)** -- one decorator on `EventStore`, registered through `ComponentRegistry`:

- **Events**: the chain decorates the publicly-facing `EventStore` interface (NOT any storage-engine implementation type), covering both read paths (`transaction(...).source(...)` for entity loads and DCB reads, and `open(...)` for tracking-processor reads). Storage-engine filtering and consistency-marker bookkeeping run BEFORE the chain receives the stream (FR-012). The wrapping transaction is cached per `ProcessingContext`. Precedent: `InterceptingEventStore` in `axon-framework` uses the same structural pattern. **Decoration ordering**: outer (later) than `InterceptingEventStore` (which lives at `Integer.MIN_VALUE + 50`). We use `Integer.MIN_VALUE + 1000`, leaving a deliberate gap of ~950 so users (or the framework, later) can slot other decorators in between. Concrete decorator types live in [contracts/spi-events.md](contracts/spi-events.md).

**Integration points (5.3+ candidates, NOT in 5.2.0)**:

- **Commands and queries**: the chain decorates `CommandBus` and `QueryBus` directly -- NOT `CommandBusConnector` / `QueryBusConnector`. Decorating the bus matters: a user can drop in transformation without taking a dependency on a distributed-messaging module, and tests can exercise transformation without going over the wire. The chain fires on every incoming command/query that reaches a handler, regardless of whether it arrived locally or from a remote node, by wrapping each registered handler at `subscribe(...)` time. The module's dependency footprint does not grow when the `commandhandling/` and `queryhandling/` sub-packages fill in -- `CommandBus` and `QueryBus` are upstream `axon-framework` types. **Decoration ordering**: outer (later) than `InterceptingCommandBus` / `InterceptingQueryBus` (both at `Integer.MIN_VALUE + 100`); we use `Integer.MIN_VALUE + 1000` for the same headroom rationale as on the event side. Concrete decorator types are sketched in [contracts/spi-commands-queries.md](contracts/spi-commands-queries.md).

**Testing**: JUnit 5 + AssertJ; Awaitility for async; JMH for FR-011 thresholds.

**Target Platform**: JVM (Java 21+). Library code.

**Project Type**: AxonIQ Framework feature module (`axoniq-framework/messaging/axoniq-message-transformation/`) + a new demo Maven sub-module under `axon-framework/examples/` that depends on `io.axoniq.framework:axoniq-message-transformation` to demonstrate the full feature surface.

**Performance Goals**:

- O(1) per-event lookup on the non-matching path with no per-event allocations (FR-011). Per-`QualifiedName` sub-chain map (FR-007) gives this directly.
- JMH benchmark dimensions: chain length in `{1, 10, 50, 100}` x event count `1M`, with `-prof gc`. Pass: per-event latency variance < 10% across chain lengths; `gc.alloc.rate.norm` constant in chain length.

**Constraints**:

- Programmatic registration only; chain locked once event processing begins (FR-004).
- No `IntermediateEventRepresentation`-equivalent (Constitution II). Transformer operates on `MessageStream<M>` over typed `Message` subtypes.
- Transformer is a decorator-around-`EventStore` for events (5.2.0); for commands and queries (5.3+) a decorator-around-`CommandBus` / `QueryBus` (NOT around the connectors). Never a decorator around `MessageConverter`.
- ASCII-only source files; LF line endings; JSpecify `@NullMarked` package-level (per CLAUDE.md).

**Scale/Scope**: Hundreds of registered transformations per chain feasible (typically <50 per QualifiedName). Millions of events per replay must not allocate per-event on the non-matching path.

## Constitution Check

Validated against `.specify/memory/constitution.md` v2.0.0 (project-wide Foundation Principles + AF5 Anchoring Types + API Design Principles) AND the feature-specific design principles in [spec.md](spec.md) "Addendum: Design Principles".

| Source | Principle | Spec alignment | Status |
|---|---|---|---|
| Constitution v2.0.0 I | Simplicity First | Single SPI shape (`MessageStream<M> -> MessageStream<M>`); no `IntermediateEventRepresentation` analog; no per-event allocations on the non-matching path (FR-011) | PASS |
| Constitution v2.0.0 II | Minimal Impact | One new module (`messaging/axoniq-message-transformation/`); one small additive change to `axon-framework` (`MessageStream.flatMap`); no refactors beyond scope | PASS |
| Constitution v2.0.0 III | Java 21 Baseline | Sealed types, records, pattern matching (Tech Context) | PASS |
| Constitution v2.0.0 IV | Dual Paradigm Support | Async-first `MessageStream<M>`; transformation functions are pure and callable from imperative or reactive composition | PASS |
| Constitution v2.0.0 V | No ThreadLocals | Per-context bookkeeping on `ProcessingContext` (`Context.ResourceKey` cache for the wrapped `EventStoreTransaction`, mirroring `InterceptingEventStore`) | PASS |
| Constitution v2.0.0 VI | Composition over Inheritance | Decorator-around-`EventStore` for events via `ComponentRegistry.registerDecorator(...)`; for commands and queries (5.3+) decorator on `CommandBus` / `QueryBus` (NOT on the connectors) wrapping at `subscribe(...)` time. In both cases wrapping types delegate to inner targets. | PASS |
| Constitution v2.0.0 VII | Declarative over Annotation-Heavy | Programmatic builder API (`EventTransformation.rename(...)`, `from(...).to(...).transform(...)`, `split(...).transform(...)`, `drop(...)`); annotation-based registration deferred (FR-004); chain is wired through a `ConfigurationEnhancer` | PASS |
| Constitution v2.0.0 Upstream | Relationship to AxonFramework Upstream | No upstream type redefined; we depend on upstream `EventStore`, `MessageStream`, `Message`, `MessageType`, `Converter`; one small additive request to upstream (`MessageStream.flatMap`) tracked separately | PASS |
| Constitution v2.0.0 AF5 Anchoring Types | Public surface stays on anchor list | Public surface uses: `Message`, `MessageType`, `MessageConverter`, `EventMessage`, `CommandMessage`, `QueryMessage`, `MessageStream`, `EventStore`, `ProcessingContext`, `TrackingToken`. We do NOT depend on `@Internal` `EventStorageEngine`. | PASS |
| Constitution v2.0.0 API VI | Interface Segregation | Per-type specializations (`EventTransformer`, `CommandTransformer`, `QueryTransformer`) so clients only see the message variant they need | PASS |
| Constitution v2.0.0 API VII | Dependency Inversion | Transformations operate on `Message` / `MessageStream`, not concrete payload classes or serialization internals | PASS |
| spec.md Addendum | Simpler than AF4 -- no `IntermediateEventRepresentation` | SPI operates on `MessageStream<M extends Message>` directly; no IER-equivalent introduced | PASS |
| spec.md Addendum | Single Responsibility per Transformer (Uncle Bob -- SRP) | One transformation = one `from`/`to` (1:1) or one `from` (1:N/1:0); composition via the chain, not bundled transforms | PASS |
| spec.md Addendum | Prefer Chain over Direct (Gregory Young) | US5 acceptance scenarios explicitly verify v1 -> v2 -> v3 chained, not a direct v1 -> v3 transform | PASS |
| spec.md Addendum | ES Versioning Decision Guide | Part A (converter handles natively) and Part B (transformer needed) decision tree implement the guide as a runnable contract | PASS |
| spec.md Addendum scope | Append-only event store; chain at READ only | Storage engine is append-only; chain runs at READ on `EventStore.transaction(...).source(...)` and `EventStore.open(...)` (FR-012, FR-021); stored events never mutated | PASS |

**Gates**: Phase 0 entry passes.

## Project Structure

### Documentation

```text
specs/137-message-transformation/
|-- spec.md                  # /speckit-specify + /speckit-clarify
|-- spec-review-summary.md   # high-level review summary
|-- plan.md                  # this file
|-- _archive/
|     |-- discussion-points.md     # pre-meeting two-phase exploration
|     `-- plan-c-proposal.md       # pre-meeting single-pass proposal (won)
|-- contracts/               # may be re-created post-plan; see follow-on
`-- tasks.md                 # generated by /speckit-tasks
```

### Source code (in `axoniq-framework`)

5.2.0 ships **one new module** -- `axoniq-message-transformation` -- following the existing axoniq-framework convention (one module per feature; sub-features as internal packages, as `axoniq-distributed-messaging` does with `commandhandling/` + `queryhandling/`). In 5.2.0 only the shared `transformation/` base and the `events/` sub-package contain code; the `commandhandling/` and `queryhandling/` sub-packages are reserved and filled in in 5.3+. The module's dependency footprint does NOT change between 5.2.0 and 5.3+ (`CommandBus` / `QueryBus` are upstream `axon-framework` types).

```text
axoniq-framework/messaging/axoniq-message-transformation/    (NEW module, 5.2.0)
|-- depends on: axon-framework (messaging + eventsourcing modules) -- in 5.2.0 AND 5.3+
|              (the command/query sub-packages decorate CommandBus/QueryBus from axon-framework,
|              so no axoniq-distributed-messaging dependency is needed)
|-- src/main/java/io/axoniq/framework/messaging/transformation/
|     |-- MessageTransformer.java               # generic SPI base: MessageTransformer<M extends Message>;
|     |                                         # default .when(Predicate<M>) / .onApplied(BiConsumer<M, ...>) (FR-013)
|     |-- MessageTransformerChain.java          # per-QualifiedName sub-chains (FR-007), .build() locks (FR-004)
|     |-- ChainConfigurationException.java      # thrown by Builder on FR-008 conflicts + FR-018 + FR-004 lock
|     |-- VersionComparator.java                # optional (FR-020) -- deferred, lands with US5
|     |-- SemverComparator.java                 # builder convenience -- deferred, lands with US5
|     |
|     |-- events/                               # 5.2.0
|     |     |-- EventTransformer.java           # specialization: extends MessageTransformer<EventMessage>;
|     |     |                                   # covariant .when / .onApplied overrides for fluent typing
|     |     |
|     |     |-- EventTransformation.java        # factory: from(...).to(...).transform(...) (FR-001, MUST),
|     |     |                                   # rename(...) (FR-002, SHOULD), split(...).transform(...)
|     |     |                                   # and drop(...) (FR-003, nice-to-have)
|     |     |
|     |     |-- TransformedEvent.java           # value type: output of 1:N split mappers
|     |     |
|     |     |-- TransformingEventStore.java     # decorator on EventStore (events read path);
|     |     |                                   # DECORATION_ORDER = Integer.MIN_VALUE + 1000
|     |     |-- TransformingEventStoreTransaction.java
|     |     |                                   # inner wrapping transaction (source() applies the chain;
|     |     |                                   # other 4 methods delegate unchanged)
|     |     |
|     |     `-- configuration/
|     |           `-- EventTransformationConfigurationEnhancer.java
|     |                                         # registers the chain + wires the EventStore decorator
|     |
|     |-- commandhandling/                      # 5.3+ (sub-package reserved; empty in 5.2.0)
|     |     |-- CommandTransformer.java         # extends MessageTransformer<CommandMessage>
|     |     |-- CommandTransformation.java
|     |     |-- TransformingCommandBus.java     # decorator on CommandBus (NOT on CommandBusConnector);
|     |     |                                   # DECORATION_ORDER = Integer.MIN_VALUE + 1000
|     |     `-- configuration/
|     |           `-- CommandTransformationConfigurationEnhancer.java
|     |
|     `-- queryhandling/                        # 5.3+ (sub-package reserved; empty in 5.2.0)
|           |-- QueryTransformer.java           # extends MessageTransformer<QueryMessage>
|           |-- QueryTransformation.java
|           |-- TransformingQueryBus.java       # decorator on QueryBus (NOT on QueryBusConnector);
|           |                                   # DECORATION_ORDER = Integer.MIN_VALUE + 1000
|           `-- configuration/
|                 `-- QueryTransformationConfigurationEnhancer.java
|
`-- src/test/java/...                           # FR-001 1:1 (MUST), FR-002 rename (SHOULD), FR-008 conflict tests
                                                # (nice-to-have), FR-020 comparator (nice-to-have),
                                                # FR-007 sub-chain routing (nice-to-have), ...

```

### Demo module (in `axon-framework` examples)

The demo is a new Maven sub-module under `axon-framework/examples/`, sitting next to the existing `university-demo`, `university-java`, `university-java-springboot-3`, etc. modules. It depends on `io.axoniq.framework:axoniq-message-transformation` (the new module from `axoniq-framework`) and demonstrates the full feature surface as it ships.

```text
axon-framework/examples/
|-- pom.xml                                     # add the new module to <modules>
|-- university-demo/
|-- university-java/
|-- university-java-springboot-3/
|-- university-java-springboot-4/
|-- university-demo-kotlin/
`-- university-message-transformation/          # (NEW sub-module, 5.2.0)
      |-- pom.xml                               # parent: axon-framework-examples; depends on
      |                                         # io.axoniq.framework:axoniq-message-transformation
      `-- src/{main,test}/java/.../transformation/
            |-- CourseCreatedV1V2.java          # US1 -- MUST
            |-- CourseOpenedRenamed.java        # US2 -- SHOULD
            |-- StudentEnrolledSplit.java       # US3 -- nice-to-have
            |-- SystemHeartbeatDropped.java     # US4 -- nice-to-have
            `-- CourseCreatedChain.java         # US5 -- nice-to-have
```

The existing examples-parent pom already imports `axoniq-framework-bom`, so version management of the new axoniq-message-transformation dependency is already handled. Four of five current example modules already depend on `io.axoniq.framework` artifacts (`axon-server-connector`), so this is no new pattern -- just a new sibling.

**Structure Decision**: One module in `axoniq-framework/messaging/` -- `axoniq-message-transformation` -- following the existing axoniq-framework convention (cf. `axoniq-dead-letter`, `axoniq-event-streaming`, and especially `axoniq-distributed-messaging` which holds `commandhandling/` + `queryhandling/` together in one module). The shared SPI base sits in the top-level `transformation/` package; `events/`, `commandhandling/`, and `queryhandling/` sub-packages carry the message-type specializations. The module's dependency footprint (`axon-framework` `messaging` + `eventsourcing`) is identical in 5.2.0 and 5.3+: command/query decoration targets the upstream `CommandBus` / `QueryBus` (not `CommandBusConnector` / `QueryBusConnector`), so no `axoniq-distributed-messaging` dependency is needed when the command/query sub-packages fill in. This also means a user can adopt transformation without pulling in distributed messaging, and tests can exercise it without going over the wire.

## SPI shape

The internal SPI is stream-in / stream-out and reuses AF5's `MessageStream`. One generic base type (`MessageTransformer<M extends Message>`) with per-message-type specializations (`EventTransformer`, `CommandTransformer`, `QueryTransformer`). Command and query specializations expose a single-entry convenience overload because they always carry one message. Users almost never touch the SPI directly -- they use the public factories. Full interface declarations, contract clauses, and integration types live in [contracts/spi-base.md](contracts/spi-base.md), [contracts/spi-events.md](contracts/spi-events.md), and [contracts/spi-commands-queries.md](contracts/spi-commands-queries.md); the user-facing factory API lives in [contracts/public-api.md](contracts/public-api.md).

### Chain composition (architectural)

There is exactly **one** `MessageTransformerChain` object per application. The user builds it once at startup; the chain is locked and immutable thereafter (FR-004). The chain routes incoming messages by message type to per-`QualifiedName` sub-chains; the user sees one chain, one builder, one registration call style across events (5.2.0) and -- when delivered -- commands and queries (5.3+). The `EventTransformationConfigurationEnhancer` (5.2.0), joined by per-sub-package `CommandTransformationConfigurationEnhancer` + `QueryTransformationConfigurationEnhancer` (5.3+), registers the chain in the component registry and wires it into the decoration points described under "Integration point" above. Each decoration point invokes the chain with a typed `MessageStream<M>` for its message subtype; routing inside the chain is per-`QualifiedName` (FR-007).

## Required axon-framework additions

5.2.0 MUST scope (US1, 1:1 only) is implementable purely in `axoniq-framework` via the existing decorator SPI -- no `axon-framework` change required.

**Conditional axon-framework addition**: only needed if the 1:N split nice-to-have (US3, FR-003) lands in 5.2.0.

- **`MessageStream.flatMap(Function<? super M, ? extends MessageStream<R>>)`**: needed for chain composition where 1:N split outputs re-enter their own sub-chain per FR-007. Backward-compatible default method or new operator on `MessageStream`. Note: the return type loses the `Single<M>` / `Empty<M>` refinement (always `MessageStream<R>`) because the function may produce a multi-element stream.

**Fallback if `MessageStream.flatMap` is not available upstream in time**: implement an internal helper in `axoniq-message-transformation` that combines `mapMessage` + a recursive sub-chain entry call to achieve the same 1:N re-entry behaviour, without touching the public `MessageStream` API. Slightly uglier internal code but no upstream coordination required. Final decision deferred to the moment US3 is picked up; if the upstream PR has merged by then, use it; otherwise use the fallback.

Snapshot delivery (5.3+ candidate) may want one further small addition in a future release (a hook in `SnapshottingEntityLifecycleHandler` for snapshot-payload transformation), but that is out of scope here.

## Scope and deferred work

Scope decided with Steven (2026-05-21). The plan covers issue AxonIQ/axoniq-framework#137 (ported from AxonIQ/AxonFramework#3597). The spec describes the full design vision (US1-US9 plus deferred Part C items); this plan documents what of that vision ships in 5.2.0 and what is explicitly held to 5.3+.

### 5.2.0 -- MUST (blocks the issue closing)

- **US1 (1:1 structural payload transform)**, **FR-001** -- user can declare `from`/`to` identity and a payload mapper.
- **FR-004** -- programmatic registration, startup-only, chain locks at `.build()`.
- Supporting invariants without which US1 is unsafe: **FR-005** (exact matching, pass-through for non-matches), **FR-009 declarative target type** (`transform(Class<T>, Function<T, U>)`: framework converts stored bytes to the declared input type via the registered `Converter` before invocation -- the only way the user can write a typed payload mapper for US1), **FR-010** (envelope preservation), **FR-011** (lazy deserialization on non-matching path), **FR-012** (same result across entity load / DCB read / tracking processor), **FR-016** (unversioned legacy events default to `0.0.1`), **FR-017** (unit-testable), **FR-018** (output identity check), **FR-021** (data-protection ordering -- transformer runs before any handler-side interceptor).

### 5.2.0 -- SHOULD (deliver if it fits in the window)

- **US2 (rename)**, **FR-002** -- pure rename without a payload mapper. Thin add-on to the existing factory.
- **US7 (observability)**, **FR-013** -- `.when(Predicate<M>)` (skip-if-false) and `.onApplied(BiConsumer<M, MessageStream<? extends M>>)` (post-apply observer) as default methods on the base `MessageTransformer<M>`, attached at registration time. Events, commands, and queries inherit them via covariant overrides. Plus the chain-build DEBUG entry. Matches AF4 `SingleEntryUpcaster.canUpcast` / `doUpcast` precedent.

### 5.2.0 -- MAY (nice-to-have for 5.2.0, else slip to 5.3.0)

- **US3 (split)** + **US4 (drop)**, **FR-003**. Pulls in `MessageStream.flatMap` (or the in-module fallback -- see Required axon-framework additions).
- **US5 (chaining across versions)**, **FR-007** chain composition + sub-chain routing.
- **US6 (misconfiguration + runtime feedback)**, **FR-008** conflict detection.
- **FR-014** position-advances-past-drops (only relevant if US4 lands), **FR-015** exception propagation, **FR-020** optional `VersionComparator`.

### 5.3+ -- explicitly deferred

- **US8 command transformation**, **US9 query transformation**, **FR-019**. Land in the `commandhandling/` + `queryhandling/` sub-packages of `axoniq-message-transformation`. Because the decoration target (`CommandBus` / `QueryBus`) lives in `axon-framework`, no additional axoniq dependency is added when this work lands. Design constraint: chain MUST fire on every incoming command/query reaching a handler -- local or remote -- by wrapping each registered handler at `subscribe(...)` time on the bus itself (not on any connector). See "Integration points 5.3+" above.
- **Snapshot payload transformation**. Architecturally compatible with the 5.2.0 chain (snapshots flow through the same `EventStore.transaction().source(...)` stream merged in by `SnapshotCapableEventStorageEngine`), but the user-facing API / docs / fixtures (and a `Snapshot.payloadAs(Class<?>)` ergonomic accessor) are deferred. Either decorate `SnapshottingEntityLifecycleHandler`'s converter call site or introduce a `SnapshotPayloadTransformer` SPI hook.
- **Annotation-based registration**. Programmatic only for now (FR-004); annotations may return if added through an explicit `EventTransformationChain` registry bean (see Part C "Annotation-Based Transformation Registration" in spec for the forward-direction note).
- **Sender-side transformation** (new-to-old at the sender). Out of scope per Part C of spec.
- **FR-009 converter-access entry point**. Spec FR-009 describes two entry points for typed payload access. 5.2.0 ships only the declarative target type variant (`transform(Class<T>, Function<T, U>)`). The converter-access variant -- where the transformation receives a `Converter` and converts inline (useful when a single transformation needs multiple representations of the same payload) -- is deferred. Adding it later is purely additive: a new overload on the existing factory.

## Forward-compatibility invariants

The 5.2.0 deliverable is a thin slice of the full design. Everything held to SHOULD / MAY / 5.3+ MUST land as a **pure additive change**, not a breaking rewrite. The 5.2.0 implementation therefore COMMITS to these architectural invariants from day one:

1. **Generic SPI over `Message`**. `MessageTransformer<M extends Message>` is the base; `EventTransformer` is the only 5.2.0 specialization. _Protects: US8/US9/FR-019 (commands and queries join without an SPI break)._
2. **Chain models 0..N outputs**. Internal data structures + decorator return shape MUST treat each transformation as producing a `MessageStream` of zero or more outputs from day one. _Protects: US3/US4/FR-003/FR-014 (split and drop without rewriting the chain)._
3. **Per-`QualifiedName` sub-chain routing**. Transformations grouped by source `QualifiedName`; non-matching lookup is O(1) with no per-event allocation. _Protects: US5/FR-007/FR-011 (chained version hops + lazy non-matching path)._
4. **Factory method names reserved**. `from(...).to(...)`, `rename(...)`, `split(...)`, `drop(...)` are the only public factory shapes; deferred methods are either absent or stubbed -- never ship a shape that would have to be renamed. _Protects: US2/US3/US4 (rename, split, drop API stability)._
5. **Single `MessageTransformerChain` object**. Builder returns one immutable chain holding all message types; future annotation discovery MUST produce the same object. _Protects: deferred annotation registration + 5.3+ command/query rollout._
6. **Snapshot pass-through is automatic**. Snapshots flow inline through the same event stream; FR-005 pass-through on unknown `MessageType`s covers them, so adding a snapshot API later needs no wiring change. _Protects: deferred snapshot transformation._
7. **No annotation-discovery hooks**. The 5.2.0 surface ships no `@Transform`-style annotation, keeping the future annotation mechanism's design space unconstrained. _Protects: Part C annotation-based registration._
8. **Conflict-detection call sites reserved**. The builder validates at both registration time (per-entry checks) and lock time (cross-entry checks); 5.2.0 may register no checks but the call sites MUST exist. _Protects: US6/FR-008/FR-020 (duplicate, self-loop, cycle, version-order detection)._
9. **Per-transformer hooks shipped from 5.2.0**. Default methods on the base `MessageTransformer<M>` SPI: `.when(Predicate<M>)` (skip if `false`) and `.onApplied(BiConsumer<M, MessageStream<? extends M>>)` (post-apply observer). Apply uniformly to events, commands, and queries via covariant overrides on each specialization. Matches AF4 `SingleEntryUpcaster.canUpcast` / `doUpcast`. Defaults: "always" / no-op, zero per-event allocation. No chain-wide hooks on the Builder; no generic framework-owned name like `Observability`. _Delivers: US7/FR-013._

## Complexity Tracking

| Drift / decision | Why needed | Simpler alternative rejected because |
|---|---|---|
| One conditional additive change to `axon-framework` (`MessageStream.flatMap`) -- only triggered if 1:N split (FR-003, MAY) lands in 5.2.0 | Chain composition where 1:N split outputs re-enter their own per-`QualifiedName` sub-chain (FR-007) requires `flatMap` on `MessageStream` | Manually composing via existing `mapMessage` + a recursive sub-chain entry is possible but uglier; flatMap is a standard stream operator the API was missing. Fallback path (internal helper) documented in "Required axon-framework additions". |
| SPI base is generic over `Message` even though only `EventTransformer` ships in 5.2.0 | Forward-compatibility invariant #1 -- avoids an SPI break when commands/queries arrive in 5.3+ | Shipping an event-specific SPI now (`EventTransformer` as the root) would force a refactor of every user-extended transformer when commands land. |
| Single module hosts both events (5.2.0) and the deferred cqrs/ sub-package (5.3+), matching `axoniq-distributed-messaging` precedent | The handler-registration decoration for commands/queries targets `CommandBus` and `QueryBus` -- both upstream `axon-framework` types -- so the module dependency set does not change between 5.2.0 and 5.3+. No `axoniq-distributed-messaging` dependency is needed. | A separate `-spi` / `-events` / `-cqrs` module split was considered for a different reason (isolating the event ingress from the connector decoration originally proposed for commands/queries). Once the design moved to handler-registration on the bus itself, that dep concern evaporated and the single-module convention applies straightforwardly. |
