# AF4 SpanFactory Inventory — Exhaustive Coverage Audit

**Feature**: 3594 — Distributed Tracing Support

**Status**: Audited 2026-05-26. Load this file **only when implementing or reviewing tracing decorators** (e.g., `TracingCommandBus`, `TracingEventSink`, `TracingQueryBus`, `TracingQueryUpdateEmitter`, `TracingRepository`, `TracingSnapshotter`, `TracingEventHandlingComponent`, `TracingHandlerEnhancerDefinition`). Skip otherwise.

This file enumerates every AF4 `*SpanFactory` interface, every method on it, every production-source caller, and the corresponding AF5 decorator + span-name mapping. It is the **verification artifact** for Story 4 acceptance criterion 3 ("span names, kinds, and attributes are equivalent — the consolidation does not regress the observable trace shape").

The audit covered AF4 source under `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4` (`messaging/`, `modelling/`, `eventsourcing/`, `axon-server-connector/`, all `src/main/java/**` — excluding tests, `target/`, and the legacy `extension-tracing` repository). Deadlines and Sagas are intentionally out of scope (clarifications 2026-05-26 — neither component exists in AF5).

---

## 1. Inventory by AF4 SpanFactory family

### 1.1 `SpanFactory` — core generic interface

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createRootTrace(operationName)` | `AbstractSnapshotter.java:102` | Snapshot scheduling root trace | `TracingSnapshotter` → `SpanFactory.createInternalSpan("Snapshotter.create <entityType>")` | Covered (FR-010) |
| `createHandlerSpan(opName, msg, isChildTrace, linkedParents…)` | `TracingHandlerEnhancerDefinition:83`, `DefaultQueryBusSpanFactory:60/66/87/93/95` | Annotation handler invocation | `TracingHandlerEnhancerDefinition` + message-context via `ProcessingContext` | Covered (FR-003) |
| `createDispatchSpan(opName, msg, linkedSiblings…)` | `DefaultCommandBusSpanFactory:48`, `DefaultEventBusSpanFactory:39`, `DefaultQueryBusSpanFactory:50/58/72/85` | Command / event / query dispatch entry | `TracingCommandBus`, `TracingEventSink`, `TracingQueryBus` → `SpanFactory.createDispatchSpan(...)` | Covered (FR-006/007/008) |
| `createInternalSpan(opName)` | `TracingHandlerEnhancerDefinition`, `DefaultQueryBusSpanFactory`, `DefaultRepositorySpanFactory:60/66/72` | Internal phase (commit, lock, replay, scatter-gather handler index) | Multiple decorators via `SpanFactory.createInternalSpan(String)` + local `Span.addAttribute(...)` | Covered (FR-009/010, FR-013a) |
| `createLinkedHandlerSpan(opName, msg, linkedParents…)` | `DefaultQueryBusSpanFactory:93/95` | Distributed query handler (link back, not same-trace) | `TracingQueryBus` → `SpanFactory.createHandlerSpan(...)` with W3C extract from metadata | Covered (FR-008, FR-015) |
| `propagateContext(msg)` | `SimpleCommandBus:164`, `AbstractEventBus:122`, `SimpleQueryUpdateEmitter:153/158`, `DefaultCommandBusSpanFactory:55`, `DefaultEventBusSpanFactory:56`, `DefaultQueryBusSpanFactory:105`, `DefaultQueryUpdateEmitterSpanFactory:62` | Inject W3C trace context into message metadata | All `Tracing*` decorators before delegating dispatch | Covered (FR-015) |

### 1.2 `CommandBusSpanFactory`

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createDispatchCommandSpan(cmd, distributed)` | `SimpleCommandBus:124`, `AxonServerCommandBus` (dispatch leg) | Command dispatch (in-process vs distributed) | `TracingCommandBus.dispatch()` → `"CommandBus.dispatchCommand <name>"` (kind `CLIENT` if distributed, `INTERNAL` otherwise) | Covered (FR-006) |
| `createHandleCommandSpan(cmd, distributed)` | `SimpleCommandBus:191`, `AxonServerCommandBus` (handler leg) | Command handling | `TracingCommandBus.handle()` or `TracingHandlerEnhancerDefinition` → `"CommandBus.handleCommand <name>"` (kind `SERVER`) | Covered (FR-006) |
| `propagateContext(cmd)` | `SimpleCommandBus:164`, `DefaultCommandBusSpanFactory:55` | Inject context | `TracingCommandBus` | Covered (FR-015) |

### 1.3 `EventBusSpanFactory`

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createPublishEventSpan(event)` | `AbstractEventBus.java:121` | Per-event publish, synchronous, around `propagateContext` | `TracingEventSink` → `"EventBus.publishEvent <eventName>"` (kind `PRODUCER`), per event | Covered (FR-007) |
| `createCommitEventsSpan()` | `AbstractEventBus.java:144/160` | UoW-scoped commit span (prepareCommit → commit → afterCommit → cleanup), with no-UoW fallback at line 144 | `TracingEventSink` → `"EventBus.commitEvents"` bound to `ProcessingContext` via `ProcessingContextSpanBinding` | Covered (FR-007, clarification 2026-05-26) |
| `propagateContext(event)` | `AbstractEventBus:122`, `DefaultEventBusSpanFactory:56` | Inject context per event | `TracingEventSink.publish()` | Covered (FR-015) |

### 1.4 `EventProcessorSpanFactory`

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createBatchSpan(streaming, messages)` | `AbstractEventProcessor.java:165` | Batch root (NoOp when `!streaming` — i.e., subscribing) | `TracingEventHandlingComponent` lazy-opens via `ctx.computeResourceIfAbsent(BATCH_SPAN_KEY, …)`, guarded by `ctx.getResource(Segment.RESOURCE_KEY) != null` → `"StreamingEventProcessor.batch"` | Covered (FR-007a) |
| `createProcessEventSpan(streaming, event)` | `AbstractEventProcessor.java:169` (nested inside `createBatchSpan`) | Per-event handler invocation | `TracingEventHandlingComponent.handle(event, ctx)` → `"EventProcessor.process <eventName>"` (kind `CONSUMER`) | Covered (FR-007a) |

See [`research-batch-tracing.md`](./research-batch-tracing.md) for the full batch-tracing analysis (AF4 vs Option C coverage matrix, rejected alternatives, subscribing-processor AF4-parity rationale).

### 1.5 `QueryBusSpanFactory`

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createQuerySpan(query, distributed)` | `SimpleQueryBus:174`, `AxonServerQueryBus` | Regular query dispatch / handling (in-process vs distributed) | `TracingQueryBus.query()` → `"QueryBus.query <name>"` (kind `CLIENT`/`INTERNAL`) on dispatch, `"QueryBus.handle <name>"` (kind `SERVER`) on handling | Covered (FR-008) |
| `createSubscriptionQuerySpan(query, distributed)` | Default factory + handler enhancer | Subscription query dispatch (initial result) | `TracingQueryBus.subscriptionQuery()` → same shape as `createQuerySpan` for initial result | Covered (FR-008) |
| `createSubscriptionQueryProcessUpdateSpan(update, query)` | `DefaultQueryBusSpanFactory:66` | Handler-side processing of incoming subscription-query update | `TracingQueryBus` or `TracingQueryUpdateEmitter` consumer path → `SpanFactory.createInternalSpan(...)` | Covered (FR-008) |
| `createScatterGatherSpan(query, distributed)` | `SimpleQueryBus:409` | Scatter-gather query dispatch wrapper | `TracingQueryBus.scatterGather()` → `"QueryBus.scatterGather <name>"` | Covered (FR-008) |
| `createScatterGatherHandlerSpan(query, handlerIndex)` | `SimpleQueryBus:413` | Per-handler index inside scatter-gather | `TracingQueryBus` → `SpanFactory.createInternalSpan("QueryBus.scatterGather <name> [handler-<index>]")` with `axoniq.handler.index` attribute | Covered (FR-008) |
| `createStreamingQuerySpan(query, distributed)` | `SimpleQueryBus:234` | Streaming query dispatch | `TracingQueryBus.streamingQuery()` → `"QueryBus.streamingQuery <name>"` | Covered (FR-008) |
| `createQueryProcessingSpan(query)` | `DefaultQueryBusSpanFactory:93` (handler-side of distributed routing) | Distributed query processing on handler side | `TracingQueryBus` handler-side branch → `"QueryProcessingTask <name>"` (matches AF4 ref guide line 649) | Covered (FR-008) |
| `createResponseProcessingSpan(query)` | `DefaultQueryBusSpanFactory:100` (dispatcher-side of distributed routing) | Distributed query response processing on dispatcher side | `TracingQueryBus` response branch → `"AxonServerQueryBus.ResponseProcessingTask <name>"` (matches AF4 ref guide line 651) | Covered (FR-008) |
| `propagateContext(query)` | `DefaultQueryBusSpanFactory:105` | Inject context | `TracingQueryBus` | Covered (FR-015) |

### 1.6 `QueryUpdateEmitterSpanFactory`

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createUpdateScheduleEmitSpan(update)` | `SimpleQueryUpdateEmitter.java:154` | Outer span — synchronous schedule (before UoW after-commit) | `TracingQueryUpdateEmitter.emit()` → `"QueryUpdateEmitter.emit <updateType>"` (kind `PRODUCER`), opened synchronously | Covered (FR-008) — **two-span pattern, see note below** |
| `createUpdateEmitSpan(update)` | `SimpleQueryUpdateEmitter.java:156` | Inner span — actual emission scheduled via `runOnAfterCommitOrNow(...)` (fires at UoW after-commit, or immediately if no UoW) | `TracingQueryUpdateEmitter.emit()` → `"QueryUpdateEmitter.emit <updateType> <queryName>"` (or equivalent per-consumer name), opened inside the after-commit hook | Covered (FR-008) — **two-span pattern, see note below** |
| `propagateContext(update)` | `SimpleQueryUpdateEmitter:153/158`, `DefaultQueryUpdateEmitterSpanFactory:62` | Inject context (twice — once before scheduling, once before doEmit) | `TracingQueryUpdateEmitter` two propagation points | Covered (FR-015) |

> **Two-span pattern note.** `SimpleQueryUpdateEmitter.emit` at AF4 lines 153-159 opens an OUTER `createUpdateScheduleEmitSpan` span that wraps an INNER `createUpdateEmitSpan` span; the inner span's body is registered via `runOnAfterCommitOrNow(...)` so it executes after the current UoW commits (or immediately, when no UoW is active). This is structurally the same as the `AbstractEventBus.publish` two-span pattern that FR-007 already documents (per-event `createPublishEventSpan` + UoW-scoped `createCommitEventsSpan`). `TracingQueryUpdateEmitter` MUST reproduce the same shape:
> 1. Open the outer span synchronously around `propagateContext(...)`.
> 2. Bind the inner span to `runOnAfterCommit(...)` on the surrounding `ProcessingContext` via `ProcessingContextSpanBinding` (mirrors how `TracingEventSink` binds its commit span). When `ProcessingContext` is `null`, fall back to running the inner span synchronously around the emission body.

### 1.7 `RepositorySpanFactory`

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createLoadSpan(aggregateId)` | `AbstractRepository.java:144` | Entity load (outer span — event-stream fetch + hydration) | `TracingRepository.load()` → `"Repository.load <entityType> <id>"` (kind `INTERNAL`) with `axoniq.aggregate.identifier` attribute attached locally | Covered (FR-009) |
| `createObtainLockSpan(aggregateId)` | `LockingRepository.java:96/134/150` (three call sites) | Lock acquisition (sub-phase of load) | `TracingRepository` or a separate `TracingLockingRepository` if AF5 still has a locking repository → `"LockingRepository.obtainLock <id>"`. **Implementation note**: AF5's repository model has shifted to entity / DCB; verify at implementation time whether `LockingRepository` still exists. If yes, decorate it as well; if no, this span is unreachable and can be dropped (no AF4 caller surfaces if the class is gone). | Covered (FR-009) with implementation-time check |
| `createInitializeStateSpan(aggregateType, aggregateId)` | `EventSourcingRepository.java:137` | Replay / hydration of aggregate from events (sub-phase of load) | `TracingRepository.load()` inner span → `"Repository.initializeState <entityType> <id>"` opened during the hydration step | Covered (FR-009, FR-010) |

> **Three-span pattern note.** AF4's `AbstractRepository.load` produces one outer `createLoadSpan` enclosing two inner spans (`createObtainLockSpan` + `createInitializeStateSpan`). The AF5 `TracingRepository` MUST reproduce this nesting where the AF5 repository model still has both phases (locking + replay). For DCB / entity-based loads where there is no lock, only the outer load span + (optionally) the initialize-state inner span are produced — no warning, no error. Cross-reference [`research.md`](./research.md) §3.2 for aggregate-identifier sourcing.

### 1.8 `SnapshotterSpanFactory`

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createScheduleSnapshotSpan(aggregateType, aggregateId)` | `AbstractSnapshotter.java:102` (outer) | Outer span — `scheduleSnapshot(...)` entry, submits work to executor | `TracingSnapshotter.scheduleSnapshot()` → `"Snapshotter.scheduleSnapshot <entityType>"` (kind `INTERNAL`) with `axoniq.entity.type` + `axoniq.aggregate.identifier` attributes | Covered (FR-010) |
| `createCreateSnapshotSpan(aggregateType, aggregateId)` | `AbstractSnapshotter.java:104` (inner, inside the runnable submitted to the executor) | Inner span — actual snapshot creation running on the executor thread | `TracingSnapshotter` → `"Snapshotter.createSnapshot <entityType> <id>"` opened on the executor thread via `Span.runSupplierAsync(...)` at the imperative edge (no surrounding `ProcessingContext`) | Covered (FR-010) |

> **Two-span pattern note.** AF4 produces a synchronous outer span (the user-visible "schedule" event) plus an asynchronous inner span (the actual snapshot creation, possibly running much later when the executor picks the task up). `TracingSnapshotter` MUST reproduce this nesting with the AF4 `axon.tracing.snapshotter.separateTrace` toggle preserved as `axon.tracing.snapshotter.separateTrace` on `TracingProperties.snapshotter` — when `true`, the inner span becomes a **root** trace linked back to the outer span via OTel link, matching AF4 reference guide line 615.

### 1.9 `TracingHandlerEnhancerDefinition` (annotation-handler wrapper)

| AF4 wrapping target | AF4 source | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `@CommandHandler` | `TracingHandlerEnhancerDefinition.java` | Per-method invocation | `TracingHandlerEnhancerDefinition` in `axoniq-tracing-core/internal/` | Covered (FR-003) |
| `@EventHandler` | same | Per-method invocation | same | Covered (FR-003) |
| `@EventSourcingHandler` | same, gated by `axon.tracing.showEventSourcingHandlers=false` default | Aggregate state-evolution handler invocation | same, gated by `TracingProperties.showEventSourcingHandlers` | Covered (FR-003, FR-020) |
| `@QueryHandler` | same | Per-method invocation | same | Covered (FR-003) |
| `@SagaEventHandler` | same | — | **OUT OF SCOPE** (sagas removed in AF5) | Excluded |
| `@DeadlineHandler` | same | — | **OUT OF SCOPE** (deadlines removed in AF5) | Excluded |

---

## 2. AF4 reference guide vs inventory cross-check

Every span name in the AF4 reference guide (`AxonFramework4/docs/old-reference-guide/modules/monitoring/pages/tracing.adoc`, "Traced components" section, lines 497-744) has been verified to map to one of the inventoried callers above. Specifically:

| AF4 ref guide span (`tracing.adoc`) | Inventory row |
|---|---|
| `${CommandBusClass}.dispatch(${cmd})` (line 520, 533) | §1.2 `createDispatchCommandSpan` |
| `${CommandBusClass}.handle(${cmd})` (line 523, 534) | §1.2 `createHandleCommandSpan` |
| `AxonServerCommandBus.dispatch/handle(${cmd})` (line 520-521) | §1.2 with `distributed=true` |
| `${RepositoryClass}.load ${id}` (line 524, 535) | §1.7 `createLoadSpan` |
| `LockingRepository.obtainLock` (line 525, 536) | §1.7 `createObtainLockSpan` |
| `${EventBusClass}.publish(${event})` (line 552) | §1.3 `createPublishEventSpan` |
| `${EventBusClass}.commit` (line 553) | §1.3 `createCommitEventsSpan` |
| `${ProcessorType}[${name}](${event})` (line 567) — root | §1.4 `createBatchSpan` |
| `${ProcessorType}[${name}].process(${event})` (line 568) | §1.4 `createProcessEventSpan` |
| `${ProcessorType}[${name}].batch` (line 569) | §1.4 `createBatchSpan` (StreamingEventProcessor only) |
| `SimpleQueryBus.query/scatterGather/streamingQuery(${q})` (lines 650, 659, 676, 684, 701, 709) | §1.5 various |
| `AxonServerQueryBus.query/streamingQuery/scatterGather(${q})` (lines 648, 674, 699) | §1.5 with `distributed=true` |
| `QueryProcessingTask(${q})` (lines 649, 675, 700) | §1.5 `createQueryProcessingSpan` |
| `AxonServerQueryBus.ResponseProcessingTask(${q})` (line 651) | §1.5 `createResponseProcessingSpan` |
| `SimpleQueryUpdateEmitter.emit(${payload})` (line 726) | §1.6 `createUpdateScheduleEmitSpan` |
| `SimpleQueryUpdateEmitter.emit ${q} (${payload})` (line 727) | §1.6 `createUpdateEmitSpan` |
| `${SnapshotterClass}.createSnapshot(${type})` (line 611) — outer | §1.8 `createScheduleSnapshotSpan` |
| `${SnapshotterClass}.createSnapshot(${type}, ${id})` (line 612) — inner | §1.8 `createCreateSnapshotSpan` |
| `${ContainingClass}.${method}(${args})` (line 737-741) — per-handler invocations | §1.9 `TracingHandlerEnhancerDefinition` |

---

## 3. Coverage summary

- **9 AF4 SpanFactory families** (1 generic + 8 per-component) — all mapped except deadlines and sagas (intentionally out of scope).
- **44 factory methods** across the 9 families — all mapped to a decorator + named span, with multi-span patterns (Snapshotter schedule+create, Repository load+lock+initializeState, EventBus publish+commit, QueryUpdateEmitter schedule+emit) explicitly enumerated.
- **Story 4 acceptance criterion 3** (equivalent span names, kinds, attributes — no observable trace-shape regression): verified at the row level by this audit. Acceptance criterion 3 is met IF and ONLY IF the implementation produces span names + kinds + attributes consistent with the AF5-mapping column above.

### Implementation gates

For each `Tracing*` decorator in `axoniq-tracing-core/src/main/java/io/axoniq/framework/tracing/internal/`, the implementer MUST cross-check that the spans it emits match the rows under the corresponding section above. A defensive integration test (`AF4SpanShapeParityIntegrationTest`) SHOULD assert the user-visible span name + kind for each AF4 reference-guide span listed in §2.

### Single confirmed gap

**Deadlines** (§"DeadlineManagerSpanFactory" — 6 production callers across `SimpleDeadlineManager`, `QuartzDeadlineManager`, `DbSchedulerDeadlineManager`, `JobRunrDeadlineManager`, `DeadlineJob`, distributed bridge) and **Sagas** (`@SagaEventHandler` annotation handler) are **intentionally not mapped**, because `DeadlineManager` and the AF4 saga / process-manager components do not exist in Axon Framework 5. Both are explicit out-of-scope clarifications (2026-05-26). When/if these components are ported to AF5 in the future, tracing support is added as new private `DecoratorDefinition`s registered with the existing `TracingConfigurationEnhancer` — a pure addition, no API churn.

---

## 4. Related files

- Spec including all clarifications: [`spec.md`](./spec.md)
- Concern-mapping table (one-row-per-concern summary): [`research.md`](./research.md) §1
- Batch-tracing deep-dive (AF4 vs Option C, subscribing-parity rationale, rejected alternatives): [`research-batch-tracing.md`](./research-batch-tracing.md)
- Worked sequence diagrams: [`flows.md`](./flows.md)
- Public API contract: [`contracts/public-api.md`](./contracts/public-api.md)
- Quickstart: [`quickstart.md`](./quickstart.md)
- Implementation plan: [`plan.md`](./plan.md)
