# Implementation Plan: Event Transformation API

**Branch**: `enhancement/137/implementation-message-transformator` | **Date**: 2026-05-21 | **Spec**: [spec.md](spec.md)

**Repo**: this feature now lives in `axoniq-framework` (issue moves there). `axon-framework` stays untouched except for one small additive change (see "Required axon-framework additions").

## Summary

Add an event transformation chain to AxonIQ Framework 5.2.0 that runs as a decorator around the `EventStore` and rewrites a message's identity (`MessageType`) and/or payload before routing. The chain is a single shared object the developer registers once at startup; the framework wires it as a thin decorator. No converter-side decoration; matching events are deserialized eagerly. FR-011 still protects the non-matching path.

The events decorator targets the publicly-facing `EventStore` interface, NOT the underlying `EventStorageEngine`. `EventStorageEngine` is `@Internal` and is an implementation detail of `StorageEngineBackedEventStore`; tying transformation to it would restrict the feature to that one EventStore family. Decorating `EventStore` keeps transformation available to any future `EventStore` implementation that does not route through an `EventStorageEngine`.

The internal SPI is stream-in / stream-out (`MessageStream<M> -> MessageStream<M>`), exposed for events as `EventTransformer extends MessageTransformer<EventMessage<?>>`. User-facing factory `EventTransformation` (`rename(...)`, `from(...).to(...)`, `split(...)`) produces transformer instances; users rarely touch the SPI directly. The SPI is intentionally generic over `Message<?>` so commands and queries can join the design in 5.3+ without an SPI break.

### Delivery scope

**5.2.0 minimum (issue AxonIQ/axoniq-framework#137)** -- delivers what is necessary for a user to configure a 1:1 event transformer:

- **MUST**: US1 (1:1 structural transform), FR-001, FR-004 (programmatic registration + lifecycle), the supporting envelope/payload-access/legacy-version invariants (FR-005, FR-010, FR-011, FR-012, FR-016, FR-017, FR-018, FR-021).
- **SHOULD if it fits**: US2 (rename), FR-002.
- **NICE-TO-HAVE for 5.2.0, else 5.3.0**: US3 (split), US4 (drop), US5 (chaining), US6 (conflict / runtime-failure feedback), US7 (startup observability), and their FRs (FR-003, FR-007, FR-008, FR-013, FR-014, FR-015, FR-020).

**5.3+ candidates** (explicit, not part of this issue):

- **Command transformation** (US8) and **query transformation** (US9) -- design-supported by the generic SPI; decoration point is open (see Integration points / 5.3+ note below).
- **Snapshot transformation** -- architecturally compatible (chain decorates `EventStore`, snapshots flow through the same stream), only user-facing API / docs / fixtures deferred.

## Technical Context

**Language/Version**: Java 21 (sealed types, records, pattern matching).

**Primary Dependencies**: `axon-framework` core modules (`common`, `messaging`, `modelling`, `eventsourcing`). JUnit 5, AssertJ, Awaitility for tests; JMH for FR-011 chain-cost benchmarks.

**Target Repo**: `axoniq-framework`. One NEW module ships in 5.2.0: `messaging/axoniq-message-transformation/`. Depends on `axon-framework`'s `messaging` + `eventsourcing` modules. Internal package layout follows the `axoniq-distributed-messaging` precedent (`commandhandling/` + `queryhandling/` in one module): a shared `transformation/` base + an `events/` sub-package for 5.2.0. A `cqrs/` sub-package is filled in for 5.3+ when command and query transformation are delivered. Because the handler-registration decoration target lives in `axon-framework` (`CommandBus` / `QueryBus` are upstream types), no additional axoniq dependency is needed in 5.3+ either -- the module dependency set stays constant.

**Integration point (5.2.0)** -- single decorator, registered via `ComponentRegistry.registerDecorator(EventStore.class, TransformingEventStore.DECORATION_ORDER, ...)`:

- **Events**: `EventStore` (publicly-facing interface; covers every `EventStore` implementation regardless of backend: local JPA / JDBC / in-memory via `StorageEngineBackedEventStore`, the Axon Server connector engine, the Postgres engine, and any future non-engine-backed implementation). The decorator wraps both `EventStore.open(StreamingCondition, ProcessingContext)` (tracking-processor reads) and `EventStore.transaction(ProcessingContext)` -- the latter returns a wrapping `EventStoreTransaction` whose `source(SourcingCondition, ...)` applies the chain (entity loads and DCB reads). The wrapping transaction delegates the four other `EventStoreTransaction` methods (`appendEvent`, `onAppend`, `overrideAppendCondition`, `appendPosition`) unchanged. Precedent: `InterceptingEventStore` in `axon-framework` ([eventsourcing/.../InterceptingEventStore.java](../../../AxonFramework/eventsourcing/src/main/java/org/axonframework/eventsourcing/eventstore/InterceptingEventStore.java)) follows the identical structural pattern (decorator on `EventStore` + wrapping `EventStoreTransaction` cached per `ProcessingContext`). `SourcingCondition` and `StreamingCondition` filtering plus `ConsistencyMarker` / `TerminalEventMessage` bookkeeping run in the underlying engine BEFORE the chain receives the stream (FR-012). Decorator ordering: outer (later) than `InterceptingEventStore` so the chain sits closest to the consumer; suggested `Integer.MIN_VALUE + 100`.

**Integration points (5.3+ candidates, NOT in 5.2.0)** -- documented now so the SPI shape doesn't preclude them:

- **Commands and queries**: the design constraint is that transformation MUST fire for **every** incoming command/query that reaches a handler, regardless of whether it arrived locally or via a `CommandBusConnector` / `QueryBusConnector`. Decorating the connector (as initially explored) only catches over-the-wire messages -- local dispatch via `localSegment` in `DistributedCommandBus` and any pure-local `SimpleCommandBus` bypass the connector entirely. The decoration point therefore needs to be at the handler-registration level (`CommandBus.subscribe(QualifiedName, CommandHandler)` / `QueryBus.subscribe(QualifiedName, QueryHandler)`), wrapping each registered handler so the chain runs uniformly on all incoming paths. Concrete design (which bus method to decorate, whether to wrap at registry-level or per-handler, the order vs. `InterceptingCommandBus` if any) is deferred to the 5.3+ command/query transformation issue.

**Testing**: JUnit 5 + AssertJ; Awaitility for async; JMH for FR-011 thresholds.

**Target Platform**: JVM (Java 21+). Library code.

**Project Type**: AxonIQ Framework feature module (`axoniq-framework/messaging/axoniq-message-transformation/`) + a new demo Maven sub-module under `axon-framework/examples/` that depends on `io.axoniq.framework:axoniq-message-transformation` to demonstrate the full feature surface.

**Performance Goals**:

- O(1) per-event lookup on the non-matching path with no per-event allocations (FR-011). Per-`QualifiedName` sub-chain map (FR-007) gives this directly.
- JMH benchmark dimensions: chain length in `{1, 10, 50, 100}` x event count `1M`, with `-prof gc`. Pass: per-event latency variance < 10% across chain lengths; `gc.alloc.rate.norm` constant in chain length.

**Constraints**:

- Programmatic registration only; chain locked once event processing begins (FR-004).
- No `IntermediateEventRepresentation`-equivalent (Constitution II). Transformer operates on `MessageStream<M>` over typed `Message` subtypes.
- Transformer is a decorator-around-`EventStore` for events (5.2.0); for commands and queries (5.3+) the chain wraps at handler-registration level on `CommandBus` / `QueryBus`. Never a decorator around `MessageConverter`.
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
| Constitution v2.0.0 VI | Composition over Inheritance | Decorator-around-`EventStore` for events via `ComponentRegistry.registerDecorator(...)`; for commands and queries (5.3+) handler-registration-level wrapping via `CommandBus.subscribe(...)` / `QueryBus.subscribe(...)`. In both cases wrapping types delegate to inner targets. | PASS |
| Constitution v2.0.0 VII | Declarative over Annotation-Heavy | Programmatic builder API (`EventTransformation.rename(...)`, `from(...).to(...)`, `split(...)`); annotation-based registration deferred (FR-004); chain is wired through a `ConfigurationEnhancer` | PASS |
| Constitution v2.0.0 Upstream | Relationship to AxonFramework Upstream | No upstream type redefined; we depend on upstream `EventStore`, `MessageStream`, `Message`, `MessageType`, `Converter`; one small additive request to upstream (`MessageStream.flatMap`) tracked separately | PASS |
| Constitution v2.0.0 AF5 Anchoring Types | Public surface stays on anchor list | Public surface uses: `Message`, `MessageType`, `MessageConverter`, `EventMessage`, `CommandMessage`, `QueryMessage`, `MessageStream`, `EventStore`, `ProcessingContext`, `TrackingToken`. We do NOT depend on `@Internal` `EventStorageEngine`. | PASS |
| Constitution v2.0.0 API VI | Interface Segregation | Per-type specializations (`EventTransformer`, `CommandTransformer`, `QueryTransformer`) so clients only see the message variant they need | PASS |
| Constitution v2.0.0 API VII | Dependency Inversion | Transformations operate on `Message` / `MessageStream`, not concrete payload classes or serialization internals | PASS |
| spec.md Addendum | Simpler than AF4 -- no `IntermediateEventRepresentation` | SPI operates on `MessageStream<M extends Message<?>>` directly; no IER-equivalent introduced | PASS |
| spec.md Addendum | Single Responsibility per Transformer (Uncle Bob -- SRP) | One transformation = one `from`/`to` (1:1) or one source identity (1:N/1:0); composition via the chain, not bundled transforms | PASS |
| spec.md Addendum | Prefer Chain over Direct (Gregory Young) | US5 acceptance scenarios explicitly verify v1 -> v2 -> v3 chained, not a direct v1 -> v3 transform | PASS |
| spec.md Addendum | ES Versioning Decision Guide | Part A (converter handles natively) and Part B (transformer needed) decision tree implement the guide as a runnable contract | PASS |
| spec.md Addendum scope | Append-only event store; chain at READ only | Storage engine is append-only; chain runs at READ on `EventStore.transaction(...).source(...)` and `EventStore.open(...)` (FR-012, FR-021); stored events never mutated | PASS |

**Gates**: Phase 0 entry passes.

## Project Structure

### Documentation

```text
specs/001-upcasting-api/
|-- spec.md                  # /speckit-specify + /speckit-clarify
|-- spec-review-summary.md   # high-level review summary
|-- plan.md                  # this file
|-- _archive/
|     |-- discussion-points.md     # pre-meeting two-phase exploration
|     `-- plan-c-proposal.md       # pre-meeting single-pass proposal (won)
|-- contracts/               # may be re-created post-plan; see follow-on
`-- tasks.md                 # generated by /speckit-tasks
```

(Spec directory name retains the historical `001-upcasting-api` slug; content is fully transformation-based. A directory rename is out of scope here.)

### Source code (in `axoniq-framework`)

5.2.0 ships **one new module** -- `axoniq-message-transformation` -- following the existing axoniq-framework convention (one module per feature; sub-features as internal packages, as `axoniq-distributed-messaging` does with `commandhandling/` + `queryhandling/`). In 5.2.0 only the shared `transformation/` base and the `events/` sub-package contain code; the `cqrs/` sub-package is reserved and filled in in 5.3+. The module's dependency footprint does NOT change between 5.2.0 and 5.3+ (`CommandBus` / `QueryBus` are upstream `axon-framework` types).

```text
axoniq-framework/messaging/axoniq-message-transformation/    (NEW module, 5.2.0)
|-- depends on: axon-framework (messaging + eventsourcing modules) -- in 5.2.0 AND 5.3+
|              (the cqrs/ sub-package decorates CommandBus/QueryBus from axon-framework,
|              so no axoniq-distributed-messaging dependency is needed)
|-- src/main/java/io/axoniq/framework/messaging/transformation/
|     |-- MessageTransformer.java               # generic SPI base: MessageTransformer<M extends Message<?>>
|     |-- MessageTransformerChain.java          # per-QualifiedName sub-chains (FR-007), .build() locks (FR-004)
|     |-- VersionComparator.java                # optional (FR-020) -- nice-to-have
|     |-- SemverComparator.java                 # builder convenience -- nice-to-have
|     |
|     |-- events/                               # 5.2.0
|     |     |-- EventTransformer.java           # specialization: extends MessageTransformer<EventMessage<?>>
|     |     |
|     |     |-- EventTransformation.java        # factory: rename(...) (FR-002, SHOULD), from(...).to(...)
|     |     |                                   # (FR-001, MUST), split(...) (FR-003, nice-to-have)
|     |     |
|     |     |-- TransformingEventStore.java     # decorator on EventStore (events read path);
|     |     |                                   # DECORATION_ORDER = Integer.MIN_VALUE + 100
|     |     |-- TransformingEventStoreTransaction.java
|     |     |                                   # inner wrapping transaction (source() applies the chain;
|     |     |                                   # other 4 methods delegate unchanged)
|     |     |
|     |     `-- configuration/
|     |           `-- EventTransformationConfigurationEnhancer.java
|     |                                         # registers the chain + wires the EventStore decorator
|     |
|     `-- cqrs/                                 # 5.3+ (sub-package reserved; empty in 5.2.0)
|           |-- CommandTransformer.java         # extends MessageTransformer<CommandMessage<?>>
|           |-- QueryTransformer.java           # extends MessageTransformer<QueryMessage<?>>
|           |-- CommandTransformation.java
|           |-- QueryTransformation.java
|           |-- TransformingCommandBus.java     # handler-registration-level decorator (NOT connector-only,
|           |-- TransformingQueryBus.java       # so all incoming commands/queries are transformed)
|           `-- configuration/
|                 `-- CqrsTransformationConfigurationEnhancer.java
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

**Structure Decision**: One module in `axoniq-framework/messaging/` -- `axoniq-message-transformation` -- following the existing axoniq-framework convention (cf. `axoniq-dead-letter`, `axoniq-event-streaming`, and especially `axoniq-distributed-messaging` which holds `commandhandling/` + `queryhandling/` together in one module). The shared SPI base sits in the top-level `transformation/` package; `events/` and `cqrs/` sub-packages carry the message-type specializations. The module's dependency footprint (`axon-framework` `messaging` + `eventsourcing`) is identical in 5.2.0 and 5.3+: because command/query decoration happens at handler-registration level on the upstream `CommandBus` / `QueryBus` types (not on `CommandBusConnector` / `QueryBusConnector`), no `axoniq-distributed-messaging` dependency is needed when the `cqrs/` sub-package fills in.

## SPI shape

The internal SPI is stream-in / stream-out and reuses AF5's `MessageStream`:

```java
public interface MessageTransformer<M extends Message<?>> {
    MessageStream<M> transform(MessageStream<M> stream);
}
```

Per-type specializations expose convenience overloads for single-entry streams (commands and queries are single-intent):

```java
public interface CommandTransformer extends MessageTransformer<CommandMessage<?>> {
    MessageStream.Single<CommandMessage<?>> transform(MessageStream.Single<CommandMessage<?>> stream);

    @Override
    default MessageStream<CommandMessage<?>> transform(MessageStream<CommandMessage<?>> stream) {
        return transform(stream.first());
    }
}
```

`EventTransformer` and `QueryTransformer` follow the same shape.

Users almost never touch this SPI. They use the factories (`EventTransformation.rename(...)`, `EventTransformation.from(...).to(...).transform(...)`, `EventTransformation.split(...)`, `CommandTransformation.rename(...)`, etc.) which produce typed `MessageTransformer` instances internally.

### Chain usage (user-facing)

There is exactly **one** `MessageTransformerChain` object per application. The user builds it once with all their transformations (events, and in 5.3+ commands and queries) and the framework wires it into the three decoration points behind the scenes:

```java
MessageTransformerChain chain = MessageTransformerChain.builder()
    .register(EventTransformation.from(MT.of("com.example.X", "1.0.0"))
                                 .to(MT.of("com.example.X", "2.0.0"))
                                 .transform(payload -> ...))
    .register(EventTransformation.rename(MT.of("com.example.Y", "1.0.0"),
                                          MT.of("com.example.Z", "1.0.0")))
    // 5.3+:
    .register(CommandTransformation.rename(...))
    .register(QueryTransformation.from(...).to(...).transform(...))
    .build();
```

The `ConfigurationEnhancer` registers this single chain instance and wires:
- `TransformingEventStore` decorator around `EventStore` (5.2.0)
- handler-registration-level decoration on `CommandBus` / `QueryBus` (5.3+)

Each decoration point invokes the chain with a typed `MessageStream<M>` (M is the relevant message subtype at that ingress); the chain internally routes to the sub-chain for that message type. The user-facing model stays simple: register everything once, the framework dispatches.

## Required axon-framework additions

5.2.0 MUST scope (US1, 1:1 only) is implementable purely in `axoniq-framework` via the existing decorator SPI -- no `axon-framework` change required.

**Conditional axon-framework addition**: only needed if the 1:N split nice-to-have (US3, FR-003) lands in 5.2.0.

- **`MessageStream.flatMap(Function<? super M, ? extends MessageStream<R>>)`**: needed for chain composition where 1:N split outputs re-enter their own sub-chain per FR-007. Backward-compatible default method or new operator on `MessageStream`. Note: the return type loses the `Single<M>` / `Empty<M>` refinement (always `MessageStream<R>`) because the function may produce a multi-element stream.

**Fallback if `MessageStream.flatMap` is not available upstream in time**: implement an internal helper in `axoniq-message-transformation` that combines `mapMessage` + a recursive sub-chain entry call to achieve the same 1:N re-entry behaviour, without touching the public `MessageStream` API. Slightly uglier internal code but no upstream coordination required. Final decision deferred to the moment US3 is picked up; if the upstream PR has merged by then, use it; otherwise use the fallback.

Snapshot delivery (5.3+ candidate) may want one further small addition in a future release (a hook in `SnapshottingEntityLifecycleHandler` for snapshot-payload transformation), but that is out of scope here.

## Scope and deferred work

Scope decided with Steven (2026-05-21). The plan covers issue AxonIQ/axoniq-framework#137 (ported from AxonIQ/AxonFramework#3597). The spec describes the full design vision (US1-US9 plus deferred Part C items); this plan documents what of that vision ships in 5.2.0 and what is explicitly held to 5.3+.

### 5.2.0 -- MUST (blocks the issue closing)

- **US1 (1:1 structural payload transform)**, **FR-001** -- user can declare `from`/`to` identity and a payload rule.
- **FR-004** -- programmatic registration, startup-only, chain locks at `.build()`.
- Supporting invariants without which US1 is unsafe: **FR-005** (exact matching, pass-through for non-matches), **FR-010** (envelope preservation), **FR-011** (lazy deserialization on non-matching path), **FR-012** (same result across entity load / DCB read / tracking processor), **FR-016** (unversioned legacy events default to `0.0.1`), **FR-017** (unit-testable), **FR-018** (output identity check), **FR-021** (data-protection ordering -- transformer runs before any handler-side interceptor).

### 5.2.0 -- SHOULD (deliver if it fits in the window)

- **US2 (rename)**, **FR-002** -- pure rename without a payload rule. Small additive surface on the same factory.

### 5.2.0 -- MAY (nice-to-have for 5.2.0, else slip to 5.3.0)

- **US3 (split)** + **US4 (drop)**, **FR-003**. Pulls in `MessageStream.flatMap` (or the in-module fallback -- see Required axon-framework additions).
- **US5 (chaining across versions)**, **FR-007** chain composition + sub-chain routing.
- **US6 (misconfiguration + runtime feedback)**, **FR-008** conflict detection.
- **US7 (startup + per-event observability)**, **FR-013** observability.
- **FR-014** position-advances-past-drops (only relevant if US4 lands), **FR-015** exception propagation, **FR-020** optional `VersionComparator`.

### 5.3+ -- explicitly deferred

- **US8 command transformation**, **US9 query transformation**, **FR-019**. Lives in the `cqrs/` sub-package of `axoniq-message-transformation`. Because the decoration target (`CommandBus` / `QueryBus`) lives in `axon-framework`, no additional axoniq dependency is added when this work lands. Design constraint: chain MUST fire on every incoming command/query reaching a handler -- including local-dispatched ones that bypass `CommandBusConnector` / `QueryBusConnector`. The decoration point therefore needs to be at handler-registration level (see "Integration points 5.3+" above).
- **Snapshot payload transformation**. Architecturally compatible with the 5.2.0 chain (snapshots flow through the same `EventStore.transaction().source(...)` stream merged in by `SnapshotCapableEventStorageEngine`), but the user-facing API / docs / fixtures (and a `Snapshot.payloadAs(Class<?>)` ergonomic accessor) are deferred. Either decorate `SnapshottingEntityLifecycleHandler`'s converter call site or introduce a `SnapshotPayloadTransformer` SPI hook.
- **Annotation-based registration**. Programmatic only for now (FR-004); annotations may return if added through an explicit `EventTransformationChain` registry bean (see Part C "Annotation-Based Transformation Registration" in spec for the forward-direction note).
- **Sender-side transformation** (new-to-old at the sender). Out of scope per Part C of spec.

## Forward-compatibility invariants

The 5.2.0 deliverable is a thin slice of the full design described in the spec. Everything held to SHOULD / MAY / 5.3+ MUST land as a **pure additive change**, not a breaking rewrite. The 5.2.0 implementation therefore COMMITS to these architectural invariants from day one:

1. **SPI generic over `Message<?>` from day one**. `MessageTransformer<M extends Message<?>>` is the base type, even though `EventTransformer` is the only specialization shipped in 5.2.0. `CommandTransformer` and `QueryTransformer` (5.3+) extend the same base without forcing a refactor. Protects US8 / US9 / FR-019.

2. **Chain models 0..N outputs per transformation**. Even though 5.2.0 only ships 1:1, the internal data structures and decorator return shape MUST treat "one transformation produces a `MessageStream` of zero or more outputs" -- never "exactly one output". Implementing it as 1:1-only would force a chain rewrite when FR-003 (split / drop) lands. The decorator wraps as `MessageStream<EventMessage> -> MessageStream<EventMessage>`, not `EventMessage -> EventMessage`. Protects US3 / US4 / FR-003 / FR-014.

3. **Per-`QualifiedName` sub-chain routing structure present from day one**. Even if 5.2.0 has at most one transformation per name, the chain MUST already group transformations by `from.qualifiedName()` so US5 (chaining v1 -> v2 -> v3) lands without reorganising the chain. The non-matching lookup-and-skip path is O(1) with no per-event allocation from the start. Protects US5 / FR-007 / FR-011.

4. **Factory method names reserved**. `EventTransformation.from(...).to(...)`, `EventTransformation.rename(...)`, `EventTransformation.split(...)`, `EventTransformation.drop(...)` are reserved on the factory API. Even if `split(...)` and `drop(...)` are not implemented in 5.2.0, the methods either (a) exist as `throw new UnsupportedOperationException("FR-003 deferred")` stubs or (b) are simply absent so adding them later is additive. **Never** ship a different shape that would have to be renamed. Protects US3 / US4 / US2.

5. **Registration goes through a single `MessageTransformerChain` object**. Builder + `.build()` returns an immutable chain that holds transformations for events (5.2.0) and -- when delivered -- commands and queries (5.3+). Internally the chain routes by message type to per-`QualifiedName` sub-chains; the user sees one chain, one builder, one registration call style. Holds the door open for the FR-019 / Part C annotation-based registration alternative: any future annotation discovery must produce the same chain object the programmatic API produces. Protects deferred annotation work and the 5.3+ command/query rollout.

6. **Snapshot pass-through is automatic**. Because the decorator wraps `EventStore` and `SnapshotCapableEventStorageEngine` merges snapshots into the source stream below it, snapshot entries reach the chain inline. The chain MUST treat unknown `MessageType`s (including snapshot types) as pass-through per FR-005. Adding a snapshot transformation in 5.3+ then needs no wiring change. Protects deferred snapshot transformation.

7. **No annotation-discovery hooks anywhere in the 5.2.0 surface**. Keeps the future annotation mechanism's design space unconstrained (spec Part C). A premature `@Transform` or similar annotation would collide with whichever scheme is eventually chosen.

8. **Conflict-detection extension points reserved at `register(...)` and `.build()`**. Even if 5.2.0 ships without conflict detection (FR-008 is MAY), the chain builder MUST validate at the right phases (`register(...)` for per-entry checks, `.build()` for cross-entry checks). 5.2.0 may register no checks, but the call sites MUST exist so adding duplicate-`from`, self-loop, multi-step-cycle, and version-order checks is a pure addition. Protects US6 / FR-008 / FR-020.

9. **Observability hook points reserved**. Even if 5.2.0 emits no per-event TRACE log, the chain MUST have a single place (a decorator-around-chain or a callback list) where US7 / FR-013 observability can plug in without per-event allocation when disabled. Protects FR-013 disable mechanism.

## Complexity Tracking

| Drift / decision | Why needed | Simpler alternative rejected because |
|---|---|---|
| One conditional additive change to `axon-framework` (`MessageStream.flatMap`) -- only triggered if 1:N split (FR-003, MAY) lands in 5.2.0 | Chain composition where 1:N split outputs re-enter their own per-`QualifiedName` sub-chain (FR-007) requires `flatMap` on `MessageStream` | Manually composing via existing `mapMessage` + a recursive sub-chain entry is possible but uglier; flatMap is a standard stream operator the API was missing. Fallback path (internal helper) documented in "Required axon-framework additions". |
| SPI base is generic over `Message<?>` even though only `EventTransformer` ships in 5.2.0 | Forward-compatibility invariant #1 -- avoids an SPI break when commands/queries arrive in 5.3+ | Shipping an event-specific SPI now (`EventTransformer` as the root) would force a refactor of every user-extended transformer when commands land. |
| Single module hosts both events (5.2.0) and the deferred cqrs/ sub-package (5.3+), matching `axoniq-distributed-messaging` precedent | The handler-registration decoration for commands/queries targets `CommandBus` and `QueryBus` -- both upstream `axon-framework` types -- so the module dependency set does not change between 5.2.0 and 5.3+. No `axoniq-distributed-messaging` dependency is needed. | A separate `-spi` / `-events` / `-cqrs` module split was considered for a different reason (isolating the event ingress from the connector decoration originally proposed for commands/queries). Once the design moved to handler-registration on the bus itself, that dep concern evaporated and the single-module convention applies straightforwardly. |
