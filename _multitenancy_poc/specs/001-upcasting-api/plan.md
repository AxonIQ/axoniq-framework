# Implementation Plan: Event Upcasting API

**Branch**: `enhancement/137/implementation-message-transformator` | **Date**: 2026-05-21 | **Spec**: [spec.md](spec.md)

**Repo**: this feature now lives in `axoniq-framework` (issue moves there). `axon-framework` stays untouched except for one small additive change (see "Required axon-framework additions").

## Summary

Add a transformer chain to AxonIQ Framework 5.2.0 that runs at three ingress sites (the `EventStore` for reads, the command bus connector for incoming commands, the query bus connector for incoming queries) and rewrites a message's identity (`MessageType`) and/or payload before routing. The chain is a single shared object the developer registers once; the framework wires three thin decorators around it. No converter-side decoration; matching events are deserialized eagerly. FR-011 still protects the non-matching path.

The events decorator targets the publicly-facing `EventStore` interface, NOT the underlying `EventStorageEngine`. `EventStorageEngine` is `@Internal` and is an implementation detail of `StorageEngineBackedEventStore`; tying upcasting to it would restrict the feature to that one EventStore family. Decorating `EventStore` keeps upcasting available to any future `EventStore` implementation that does not route through an `EventStorageEngine`.

The internal SPI mirrors AF4's stream-in / stream-out shape (`MessageStream<M> -> MessageStream<M>`), exposed via typed specializations (`EventTransformer`, `CommandTransformer`, `QueryTransformer`). User-facing factories (`EventTransformation`, `CommandTransformation`, `QueryTransformation`) produce these instances; users rarely touch the SPI directly.

5.2.0 delivers events, commands and queries together (US1-US9) under issue AxonIQ/axoniq-framework#137. The same chain, registered once, is wired by a single `ConfigurationEnhancer` into all three ingress decorators. Snapshot transformations remain deferred (Part C of spec); the architecture stays compatible.

## Technical Context

**Language/Version**: Java 21 (sealed types, records, pattern matching).

**Primary Dependencies**: `axon-framework` core modules (`common`, `messaging`, `modelling`, `eventsourcing`). JUnit 5, AssertJ, Awaitility for tests; JMH for FR-011 chain-cost benchmarks.

**Target Repo**: `axoniq-framework`. New module: `messaging/axoniq-message-transformation/`. Depends on `axon-framework`'s messaging + eventsourcing modules; also on `axoniq-distributed-messaging` for the `CommandBusConnector` / `QueryBusConnector` interfaces.

**Integration points** (decorator-around-component, registered via `ComponentRegistry.registerDecorator(...)`):

- **Events**: `EventStore` (publicly-facing interface; covers every `EventStore` implementation regardless of backend: local JPA / JDBC / in-memory via `StorageEngineBackedEventStore`, the Axon Server connector engine, the Postgres engine, and any future non-engine-backed implementation). The decorator wraps both `EventStore.open(StreamingCondition, ProcessingContext)` (tracking-processor reads) and `EventStore.transaction(ProcessingContext)` -- the latter returns a wrapping `EventStoreTransaction` whose `source(SourcingCondition, ...)` applies the chain (entity loads and DCB reads). Precedent: `InterceptingEventStore` in `axon-framework` follows the identical pattern (decorator on `EventStore` + wrapping `EventStoreTransaction` cached per `ProcessingContext`). `SourcingCondition` and `StreamingCondition` filtering plus `ConsistencyMarker` / `TerminalEventMessage` bookkeeping run in the underlying engine BEFORE the chain receives the stream (FR-012).
- **Commands**: `CommandBusConnector` in `messaging/axoniq-distributed-messaging`. The decorator extends `DelegatingCommandBusConnector` (abstract decorator base) and overrides `onIncomingCommand(Handler)`, wrapping the registered `Handler` so the chain runs over the incoming `CommandMessage` before the delegate's `Handler.handle(CommandMessage, ResultCallback)` is invoked. The outbound `dispatch(...)` path is left untouched (downcasting is out-of-scope, see Part C of spec). Precedent: `PayloadConvertingCommandBusConnector` is an existing concrete subclass of `DelegatingCommandBusConnector` shipped in production, the same pattern.
- **Queries**: `QueryBusConnector` in `messaging/axoniq-distributed-messaging`. Same pattern as commands: extend `DelegatingQueryBusConnector`, wrap the registered handler at the incoming-query callback, leave outbound dispatch untouched. Precedent: `PayloadConvertingQueryBusConnector` (same shape as the command equivalent).

**Testing**: JUnit 5 + AssertJ; Awaitility for async; JMH for FR-011 thresholds.

**Target Platform**: JVM (Java 21+). Library code.

**Project Type**: AxonIQ Framework feature module + decorator integrations in the Axon Server connector module + demo in `axoniq-framework/examples/`.

**Performance Goals**:

- O(1) per-event lookup on the non-matching path with no per-event allocations (FR-011). Per-`QualifiedName` sub-chain map (FR-007) gives this directly.
- JMH benchmark dimensions: chain length in `{1, 10, 50, 100}` x event count `1M`, with `-prof gc`. Pass: per-event latency variance < 10% across chain lengths; `gc.alloc.rate.norm` constant in chain length.

**Constraints**:

- Programmatic registration only; chain locked once event processing begins (FR-004).
- No `IntermediateEventRepresentation`-equivalent (Constitution II). Transformer operates on `MessageStream<M>` over typed `Message` subtypes.
- Transformer is a decorator-around-`EventStore` / decorator-around-bus-connector (NOT a decorator around `MessageConverter`).
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
| Constitution v2.0.0 VI | Composition over Inheritance | Decorator-around-`EventStore` / decorator-around-bus-connector via `ComponentRegistry.registerDecorator(...)`; wrapping types delegate to inner targets | PASS |
| Constitution v2.0.0 VII | Declarative over Annotation-Heavy | Programmatic builder API (`EventTransformation.rename(...)`, `from(...).to(...)`, `split(...)`); annotation-based registration deferred (FR-004); chain is wired through a `ConfigurationEnhancer` | PASS |
| Constitution v2.0.0 Upstream | Relationship to AxonFramework Upstream | No upstream type redefined; we depend on upstream `EventStore`, `MessageStream`, `Message`, `MessageType`, `Converter`; one small additive request to upstream (`MessageStream.flatMap`) tracked separately | PASS |
| Constitution v2.0.0 AF5 Anchoring Types | Public surface stays on anchor list | Public surface uses: `Message`, `MessageType`, `MessageConverter`, `EventMessage`, `CommandMessage`, `QueryMessage`, `MessageStream`, `EventStore`, `ProcessingContext`, `TrackingToken`. We do NOT depend on `@Internal` `EventStorageEngine`. | PASS |
| Constitution v2.0.0 API VI | Interface Segregation | Per-type specializations (`EventTransformer`, `CommandTransformer`, `QueryTransformer`) so clients only see the message variant they need | PASS |
| Constitution v2.0.0 API VII | Dependency Inversion | Transformations operate on `Message` / `MessageStream`, not concrete payload classes or serialization internals | PASS |
| spec.md Addendum | Simpler than AF4 -- no `IntermediateEventRepresentation` | SPI operates on `MessageStream<M extends Message<?>>` directly; no IER-equivalent introduced | PASS |
| spec.md Addendum | Single Responsibility per Upcaster (Uncle Bob -- SRP) | One transformation = one `from`/`to` (1:1) or one source identity (1:N/1:0); composition via the chain, not bundled transforms | PASS |
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

### Source code (in `axoniq-framework`)

```text
axoniq-framework/messaging/axoniq-message-transformation/   (NEW module)
|-- src/main/java/io/axoniq/framework/messaging/transformation/
|     |-- MessageTransformer.java               # SPI base, generic over Message<?>
|     |-- EventTransformer.java                 # specialization, extends MessageTransformer<EventMessage<?>>
|     |-- CommandTransformer.java               # specialization, extends MessageTransformer<CommandMessage<?>>
|     |-- QueryTransformer.java                 # specialization, extends MessageTransformer<QueryMessage<?>>
|     |
|     |-- MessageTransformerChain.java          # per-QualifiedName sub-chains (FR-007), .build() locks (FR-004)
|     |-- VersionComparator.java                # optional (FR-020)
|     |-- SemverComparator.java                 # builder convenience
|     |
|     |-- EventTransformation.java              # factory: rename(...), from(...).to(...), split(...)
|     |-- CommandTransformation.java            # factory: rename(...), from(...).to(...)
|     |-- QueryTransformation.java              # factory: rename(...), from(...).to(...)
|     |
|     |-- TransformingEventStore.java            # decorator on EventStore (events read path)
|     |-- TransformingEventStoreTransaction.java # inner wrapping transaction (source() applies the chain)
|     |-- TransformingCommandBusConnector.java   # extends DelegatingCommandBusConnector; wraps incoming Handler
|     |-- TransformingQueryBusConnector.java     # extends DelegatingQueryBusConnector; wraps incoming Handler
|     |
|     `-- configuration/
|           `-- MessageTransformationConfigurationEnhancer.java
|                                               # registers the chain + wires all three decorators in one pass
|
`-- src/test/java/...                           # FR-008 conflict tests, FR-020 comparator, FR-007 sub-chain routing, ...

axoniq-framework/examples/                      # university-demo + analogous examples
`-- src/{main,test}/java/.../upcasting/
      |-- CourseCreatedV1V2.java                # US1
      |-- CourseOpenedRenamed.java              # US2
      |-- StudentEnrolledSplit.java             # US3
      |-- SystemHeartbeatDropped.java           # US4
      |-- CourseCreatedChain.java               # US5
      |-- EnrollStudentCommandUpcaster.java     # US8
      `-- FindCoursesByFacultyQueryUpcaster.java # US9
```

**Structure Decision**: Single module in `axoniq-framework`. Per Steven's meeting confirmation, all upcasting code lives there; pure Axon Framework users do not get upcasting. The new module sits next to `axoniq-distributed-messaging` and `axoniq-dead-letter` in the messaging family.

## SPI shape

The internal SPI mirrors AF4's `Upcaster<T>` (stream-in, stream-out) and reuses AF5's `MessageStream`:

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

## Required axon-framework additions

Plan C as a whole is implementable purely in `axoniq-framework` via existing decorator SPI. One small additive change to `axon-framework` is needed:

- **`MessageStream.flatMap(Function<? super M, ? extends MessageStream<R>>)`**: needed for the chain implementation (1:N splits re-enter sub-chains per FR-007). Backward-compatible default method or new operator.

Snapshot delivery (deferred) may want one further small addition in a future release (a hook in `SnapshottingEntityLifecycleHandler` for snapshot-payload upcasting), but that is out of scope here.

## Scope and deferred work

This plan delivers the full set of in-scope user stories (US1-US9) under issue AxonIQ/axoniq-framework#137 (ported from AxonIQ/AxonFramework#3597). Events, commands and queries ship together; the SPI is uniform and the `ConfigurationEnhancer` wires all three decorators in one pass.

Deferred (separate issues, not part of this plan):

- **Snapshot payload upcasting** (5.3+ candidate). Either decorate `SnapshottingEntityLifecycleHandler`'s converter call site or introduce a `SnapshotPayloadUpcaster` SPI hook. Small `axon-framework` change required. Tracked separately.
- **Annotation-based registration**. Programmatic only for now (FR-004); annotations may return if added through an explicit `EventTransformationChain` registry bean (see Part C "Annotation-Based Transformation Registration" in spec for the forward-direction note).
- **Downcasting** (sender-side new-to-old). Out of scope, see Part C of spec.

## Complexity Tracking

| Drift / decision | Why needed | Simpler alternative rejected because |
|---|---|---|
| Plan depends on axoniq-only integration types (`CommandBusConnector`, `QueryBusConnector`) | The transformer decorates these; we MUST call into them. They live in `axoniq-framework/messaging/axoniq-distributed-messaging`, not on the upstream anchor list. | Wrapping them in a new abstraction would reintroduce the AF4 `IntermediateEventRepresentation` pattern. Events do NOT introduce drift -- they decorate the upstream `EventStore` which IS on the anchor list. |
| One small additive change to `axon-framework` (`MessageStream.flatMap`) | Chain composition via per-QualifiedName sub-chains (FR-007) requires flatMap on `MessageStream` | Manually composing via existing `mapMessage` + reentrant calls is possible but uglier; flatMap is a standard stream operator the API was missing |
