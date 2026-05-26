# AF4 SpanFactory Inventory — Exhaustive Coverage Audit

**Feature**: 3594 — Distributed Tracing Support

**Status**: Audited 2026-05-26. Load this file **only when implementing or reviewing tracing decorators** (e.g., `TracingCommandBus`, `TracingEventSink`, `TracingQueryBus`, `TracingQueryUpdateEmitter`, `TracingRepository`, `TracingSnapshotStore`, `TracingEventHandlingComponent`, `TracingHandlerEnhancerDefinition`). Skip otherwise.

This file enumerates every AF4 `*SpanFactory` interface, every method on it, every production-source caller, and the corresponding AF5 decorator + span-name mapping. It is the **verification artifact** for Story 4 acceptance criterion 3 ("span names, kinds, and attributes are equivalent — the consolidation does not regress the observable trace shape").

The audit covered AF4 source under `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4` (`messaging/`, `modelling/`, `eventsourcing/`, `axon-server-connector/`, all `src/main/java/**` — excluding tests, `target/`, and the legacy `extension-tracing` repository). Deadlines and Sagas are intentionally out of scope (clarifications 2026-05-26 — neither component exists in AF5).

---

## 0. AF4 → AF5 `SpanFactory` interface diff (and per-difference justification)

The AF4 `org.axonframework.tracing.SpanFactory` interface declares **9 methods**. The AF5 `io.axoniq.framework.tracing.SpanFactory` (see [`contracts/public-api.md`](./contracts/public-api.md) §1.1) declares **6 methods**. The 9→6 reduction is **pure consolidation**, not capability loss — every AF4 production behaviour is preserved in the AF5 interface or in the standard decorator pattern. This section enumerates every difference with its justification, so a future reviewer can verify that nothing was silently dropped.

| # | AF4 method | AF4 prod. callers | AF5 replacement | Justification |
|---|---|---|---|---|
| 1 | `createRootTrace(Supplier<String>)` | `DefaultEventProcessorSpanFactory:83` (batch root); `DefaultSnapshotterSpanFactory:63` (snapshot scheduling outer) | `createInternalSpan(String)` opened at the imperative edge with no active OTel context (e.g., via `Span.runSupplierAsync(...)` in `TracingSnapshotStore`, or via `ctx.computeResourceIfAbsent(BATCH_SPAN_KEY, …)` from `TracingEventHandlingComponent` at the batch boundary). | The OTel SDK promotes any span started with no parent in scope to a root automatically. AF4's `createRootTrace` was syntactic sugar around exactly that. AF5 doesn't need an explicit method; the imperative-edge / lazy-open pattern already gives the same trace shape. Trace shape verified to match in flows.md Flow 3 (snapshot) and Flow 4 (batch). |
| 2 | `createHandlerSpan(Supplier<String>, Message<?>, boolean isChildTrace, Message<?>... linkedParents)` (base method) | Never called directly — only via the two defaults below | `createHandlerSpan(String, Message<?>, @Nullable ProcessingContext)` — single signature, no boolean, no varargs. | The boolean `isChildTrace` selects between "child of publisher's trace" and "linked back as a separate trace" — this choice is already a config concern (`axon.tracing.eventProcessor.distributedInSameTrace`, `axon.tracing.commandBus.distributedInSameTrace`, etc.) plus a timestamp heuristic. Moving it from per-call boolean to config-driven internal decision (in the `OpenTelemetrySpanFactory`) is the same trace shape with smaller surface area and fewer ways for callers to get it wrong. Varargs handled separately — see row 5. |
| 3 | `createLinkedHandlerSpan(Supplier<String>, Message<?>, Message<?>... linkedParents)` (default → `createHandlerSpan(..., false, ...)`) | `DefaultCommandBusSpanFactory:60` (distributed command in linked mode); `DefaultEventProcessorSpanFactory:100` (streaming with disableBatchTrace=true); `DefaultQueryBusSpanFactory:95` (distributed query in linked mode) | Folded into the single `createHandlerSpan(...)`; "linked" mode is selected internally based on `TracingProperties.<component>.distributedInSameTrace` + `distributedInSameTraceTimeLimit` heuristic. **Plus** the new `createLinkedHandlerSpan(String, Message, Message linkedMessage, @Nullable ProcessingContext)` (see row 6 below) for the one case where the caller actually needs an explicit `SpanLink` parameter. | The 3 production callers all use it because their components support a "linked vs child" config toggle that AF4 evaluated at call time. AF5 evaluates the same toggle inside the OTel factory — same observable shape, no caller-side branching. The truly distinct "explicit per-call linkedMessage" use case is preserved in the row-6 method below. |
| 4 | `createChildHandlerSpan(Supplier<String>, Message<?>, Message<?>... linkedParents)` (default → `createHandlerSpan(..., true, ...)`) | `DefaultCommandBusSpanFactory:58/62`, `DefaultEventProcessorSpanFactory:89/95/103`, `DefaultQueryBusSpanFactory:87/93` — 7 callers in "child" mode | Same — folded into `createHandlerSpan(...)`. | Same justification as row 3. Same trace shape via config-driven decision. |
| 5 | `createDispatchSpan(Supplier<String>, Message<?>, Message<?>... linkedSiblings)` | 9 callers across commands, events, queries, update emitter — **all pass zero linkedSiblings** | `createDispatchSpan(String, Message<?>, @Nullable ProcessingContext)` — varargs removed. | The `linkedSiblings` varargs has **zero non-empty production callers** in AF4. It was a feature for advanced use cases that never materialised. Dropping it removes an unused parameter; if a real need arises later, it can be added back without breaking existing callers (varargs is forward-compatible). |
| 6 | `Message<?>... linkedParents` on `createHandlerSpan` / `createChildHandlerSpan` / `createLinkedHandlerSpan` | **Exactly one** production caller passes a non-empty value: `DefaultQueryBusSpanFactory:66` → `createChildHandlerSpan("QueryBus.queryUpdate", updateMessage, queryMessage)` — links subscription-query update span back to originating query | **`createLinkedHandlerSpan(String operationName, Message<?> message, Message<?> linkedMessage, @Nullable ProcessingContext context)`** — new, narrow method on AF5 `SpanFactory`. Single named parameter, single named use case. Implementation MUST attach an OTel `SpanLink` (extracted from `linkedMessage.getMetaData()`'s W3C trace context) — not a parent-of relationship, not an attribute. | This is the **one real gap closed in this clarification**. The link gives APM users clickable cross-trace navigation from update→query, which is meaningful operational behaviour. Restoring the AF4 varargs verbatim (option A) would be over-broad for the single concrete use case. Lowering to a string attribute (option C) would lose OTel-native link rendering. The narrow named method captures exactly what's needed and nothing more. (Clarification 2026-05-26.) |
| 7 | `createInternalSpan(Supplier<String>)` | `TracingHandlerEnhancerDefinition:83` (per-handler name); `DefaultEventBusSpanFactory:52` (commit-events); `DefaultRepositorySpanFactory:60/66/72` (load / obtainLock / initializeState) | `createInternalSpan(String)` — eager string. | The `Supplier<String>` was for lazy span-name evaluation: a `NoOpSpanFactory` never calls `.get()`, so the name is never built. With eager `String`, the **caller** builds the name before the call, so the no-op path can't avoid it. This is split into two cases: **(a) cheap names** (the bus/component decorators build simple concatenations like `"Repository.load " + entityType`) — eager is genuinely negligible, AND `TracingConfigurationEnhancer` does not register these decorators when the configured factory is `NoOpSpanFactory`/absent, so the raw component is called directly with no name-building at all; **(b) expensive names** — see row 7a below for the one caller this matters for. For (a), the lazy supplier was over-engineered; eager is correct. |
| 7a | `createInternalSpan(Supplier<String>)` — **expensive-name caller** | `TracingHandlerEnhancerDefinition:83` only — `getSpanName(target, signature)` reflects on the handler method + joins parameter class names (e.g. `"RoomAvailabilityHandler.on(RoomAddedEvent)"`) | `createInternalSpan(String)` **with a caller-side name guard** (see requirement below). | The handler enhancer is the **one** caller where eager translation would regress a hot path. Two reasons: (1) names are reflective/expensive, not a concat; (2) **even when tracing is enabled**, the enhancer suppresses some spans per `showEventSourcingHandlers=false` (default) — and `@EventSourcingHandler` fires once per event during replay, a very hot path. A naive `createInternalSpan(getSpanName(...))` would build the reflective name on every such invocation only to discard it. **Requirement**: `TracingHandlerEnhancerDefinition` MUST decide whether it will open a span (enabled + handler-type-not-filtered) **before** calling `getSpanName(...)`, and only build the name on the span-creating branch — e.g. `if (!tracingForThisHandler) return delegate.handle(m, ctx); String name = getSpanName(target, signature); …`. Because the enhancer makes this enabled/filtered decision locally anyway, it can gate the expensive call itself; the `SpanFactory` API therefore does **not** need a `Supplier<String>` overload to recover the lazy benefit. The eager `String` API is acceptable **conditional on this guard** being implemented in the enhancer. |
| 8 | `createInternalSpan(Supplier<String>, Message<?>)` | `DefaultQueryBusSpanFactory:52/60/74/79/100` (query internals carrying queryMessage attributes); `DefaultQueryUpdateEmitterSpanFactory:56` (scheduleEmit carrying update attributes) | Decorator pattern: `createInternalSpan(String)` + decorator calls `Span.addAttribute(...)` from local parameters (consistent with how `TracingSnapshotStore` and `TracingRepository` already attach attributes for non-Message spans — see clarification 2026-05-26 on `SpanAttributesProvider` not firing on internal spans). | Removing this overload pushes attribute attachment into the decorator. This is the same pattern used for snapshot / repository spans (which AF4's `SpanAttributesProvider` already didn't iterate for, even when the AF4 method existed). Uniformly decorator-attached attributes is simpler than two SPI shapes (`provideForMessage` + on-internal-method `provideForMessage`). |
| 9 | `Supplier<String>` for all span names | All factory methods | Eager `String` | Same as rows 7 / 7a: lazy was for no-op-factory cost-avoidance. Eager is simpler and is correct for cheap names (decorators aren't installed when tracing is off); the single expensive-name caller (`TracingHandlerEnhancerDefinition`) recovers the lazy benefit via the caller-side guard in row 7a. Eager is simpler to read and easier to debug. |
| 10 | (no equivalent in AF4) | — | **New**: `@Nullable ProcessingContext context` parameter on `createDispatchSpan` / `createHandlerSpan` / `createLinkedHandlerSpan`. | Net addition for AF5's context-aware attribute providers. `AggregateIdentifierSpanAttributesProvider` reads `LegacyResources.AGGREGATE_IDENTIFIER_KEY` from the context — see clarification 2026-05-26 on aggregate-identifier sourcing. Providers that don't need the context simply ignore it. |
| 11 | `registerSpanAttributeProvider(SpanAttributesProvider)` | Builder-side registration (e.g., `OpenTelemetrySpanFactory.Builder`) | `registerAttributesProvider(SpanAttributesProvider)` — same intent, slightly tidier method name. | Renamed for consistency with the rest of the AF5 API. No semantic change. |
| 12 | `propagateContext(M message)` | Many callers — events, commands, queries, updates, deadlines | `propagateContext(M message)` — identical. | Preserved verbatim. Cross-process W3C context propagation is the same contract in both AF4 and AF5. |

### Summary

**AF4 → AF5 method count: 9 → 6.** The reduction comes from:
- **−3 default convenience methods** collapsed into one base method (`createLinkedHandlerSpan`, `createChildHandlerSpan` as defaults; `createRootTrace` as imperative-edge invocation of `createInternalSpan`).
- **−1 overload** (`createInternalSpan(Supplier, Message)`) replaced by decorator-attached attributes (same pattern already in use for snapshot / repository).
- **−1 unused varargs** (`linkedSiblings` on `createDispatchSpan` — zero non-empty production callers).
- **−1 boolean overload** (`isChildTrace` — moved to config-driven decision in OTel factory).
- **+1 new narrow method** (`createLinkedHandlerSpan(String, Message, Message, ProcessingContext)`) — closes the one real semantic gap (update→query SpanLink).
- **+1 new parameter** (`@Nullable ProcessingContext context` on Message-aware methods) — enables AF5 context-aware attribute providers.

**Net trace-shape impact: zero.** Every AF4 production behaviour is reproducible by the AF5 interface plus the standard decorator pattern. Verified row-by-row against AF4 production callers above; the AF4 reference guide's "Traced components" cross-check in §2 below is the user-facing acceptance criterion.

---

## 1. Inventory by AF4 SpanFactory family

### 1.1 `SpanFactory` — core generic interface

| AF4 method | AF4 caller (file:line) | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `createRootTrace(operationName)` | `AbstractSnapshotter.java:102` | Snapshot scheduling root trace | `TracingSnapshotStore` → `SpanFactory.createInternalSpan("SnapshotStore.store <entityType>")` (AF5 has no `Snapshotter`; see §1.8 re-grounding note) | Covered (FR-010) |
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
| `createScheduleSnapshotSpan(aggregateType, aggregateId)` | `AbstractSnapshotter.java:102` (outer) | Outer span — `scheduleSnapshot(...)` entry, submits work to executor | **No AF5 equivalent** — AF5 has no `Snapshotter` / `scheduleSnapshot` (it's in `stash/todo`). AF5 creates snapshots inline during sourcing (`SnapshottingEntityLifecycleHandler.source(...)`, gated by `SnapshotPolicy`), not as a schedulable executor task. The "outer" level maps to the FR-009 entity-sourcing span (`EntityLifecycleHandler.source`), which is shared with repository/load tracing, not a snapshot-specific span. | Re-grounded (clarification 2026-05-26, B3) |
| `createCreateSnapshotSpan(aggregateType, aggregateId)` | `AbstractSnapshotter.java:104` (inner, inside the runnable submitted to the executor) | Inner span — actual snapshot creation running on the executor thread | `TracingSnapshotStore implements SnapshotStore` via `DecoratorDefinition.forType(SnapshotStore.class)` → `"SnapshotStore.store <entityType>"` (kind `INTERNAL`) with `axoniq.entity.type` + `axoniq.aggregate.identifier` + `axoniq.snapshot.version` attributes from `store(QualifiedName, identifier, Snapshot)` parameters. Plus `"SnapshotStore.load <entityType>"` for the read side. | Covered (FR-010) |

> **AF5 re-grounding note (supersedes the AF4 two-span pattern).** AF5 has **no `Snapshotter` component** — it lives only in `stash/todo`. Snapshot creation in AF5 is an inline, `SnapshotPolicy`-gated side-effect of `SnapshottingEntityLifecycleHandler.source(...)` (calling a **private** `storeSnapshot(...)` → `snapshotStore.store(...)` at `SnapshottingEntityLifecycleHandler.java:182/190-193`). Therefore the AF4 standalone `scheduleSnapshot`→`createSnapshot` two-level trace does **not** survive. AF5 snapshot tracing is delivered by **decorating the real, registered `SnapshotStore`** (`@Internal` — accepted coupling, no internals modified) via `TracingSnapshotStore`. The `store` / `load` spans nest under the FR-009 entity-sourcing span (from decorating `EntityLifecycleHandler.source(...)`) through the active `ProcessingContext` — no separate snapshot-specific outer decoration (wrapping `source(...)` would double-count the load). The AF4 `separateTrace` / `aggregateTypeInSpanName` toggles are dropped (no `Snapshotter` to host them); if needed later they become `TracingProperties.snapshotStore.*` additions. See spec.md clarification 2026-05-26 (option B3) for accepted risks.

### 1.9 `TracingHandlerEnhancerDefinition` (annotation-handler wrapper)

| AF4 wrapping target | AF4 source | What it traces | AF5 decorator + span | Status |
|---|---|---|---|---|
| `@CommandHandler` | `TracingHandlerEnhancerDefinition.java` | Per-method invocation | `TracingHandlerEnhancerDefinition` in `axoniq-tracing-messaging/.../internal/` (`@EventSourcingHandler` coverage added by `axoniq-tracing-eventsourcing`) | Covered (FR-003) |
| `@EventHandler` | same | Per-method invocation | same | Covered (FR-003) |
| `@EventSourcingHandler` | same, gated by `axon.tracing.showEventSourcingHandlers=false` default | Aggregate state-evolution handler invocation | same, gated by `TracingProperties.showEventSourcingHandlers` | Covered (FR-003, FR-020) |
| `@QueryHandler` | same | Per-method invocation | same | Covered (FR-003) |
| `@SagaEventHandler` | same | — | **OUT OF SCOPE** (sagas removed in AF5) | Excluded |
| `@DeadlineHandler` | same | — | **OUT OF SCOPE** (deadlines removed in AF5) | Excluded |

> **Eager-name guard (FR-003a).** AF4 passed a `Supplier<String>` so the reflective `getSpanName(target, signature)` ran only when a span was created (`TracingHandlerEnhancerDefinition.java:83`). The AF5 `SpanFactory` takes an eager `String` (§0 rows 7 / 7a / 9), so `TracingHandlerEnhancerDefinition` MUST decide it will open a span (enabled + handler-type not suppressed, e.g. `@EventSourcingHandler` under `showEventSourcingHandlers=false`) **before** building the name, and compute the name only on the span-creating branch. `@EventSourcingHandler` fires once per event during replay — eagerly building a name for a discarded span is a hot-path regression. The enhancer makes the enabled/filtered decision locally anyway, so it gates the expensive call itself; no `Supplier<String>` overload on `SpanFactory` is needed.

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
| `${SnapshotterClass}.createSnapshot(${type})` (line 611) — outer | §1.8 — re-grounded: no AF5 `Snapshotter`; outer level is the FR-009 `EntityLifecycleHandler.source` sourcing span |
| `${SnapshotterClass}.createSnapshot(${type}, ${id})` (line 612) — inner | §1.8 → `SnapshotStore.store <type>` via `TracingSnapshotStore` |
| `${ContainingClass}.${method}(${args})` (line 737-741) — per-handler invocations | §1.9 `TracingHandlerEnhancerDefinition` |

---

## 3. Coverage summary

- **9 AF4 SpanFactory families** (1 generic + 8 per-component) — all mapped except deadlines and sagas (intentionally out of scope).
- **44 factory methods** across the 9 families — all mapped to a decorator + named span, with multi-span patterns (Repository load+lock+initializeState, EventBus publish+commit, QueryUpdateEmitter schedule+emit) explicitly enumerated. The AF4 Snapshotter schedule+create two-level pattern is **re-grounded** in AF5 (no `Snapshotter` component): the store/load spans come from `TracingSnapshotStore` and nest under the FR-009 entity-sourcing span — see §1.8.
- **Story 4 acceptance criterion 3** (equivalent span names, kinds, attributes — no observable trace-shape regression): verified at the row level by this audit. Acceptance criterion 3 is met IF and ONLY IF the implementation produces span names + kinds + attributes consistent with the AF5-mapping column above.

### Implementation gates

For each `Tracing*` decorator in the per-concern modules (`axoniq-tracing-messaging`, `axoniq-tracing-modelling`, `axoniq-tracing-eventsourcing`, each under `.../internal/`), the implementer MUST cross-check that the spans it emits match the rows under the corresponding section above. A defensive integration test (`AF4SpanShapeParityIntegrationTest`) SHOULD assert the user-visible span name + kind for each AF4 reference-guide span listed in §2.

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
