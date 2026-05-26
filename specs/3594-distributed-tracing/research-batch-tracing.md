# Batch-Tracing Research — AF4 → AF5 Mapping for Streaming Event Processors

**Feature**: 3594 — Distributed Tracing Support

**Status**: Decided 2026-05-26. Chosen approach: **Option C** (lazy-open batch span on shared `ProcessingContext`). Documented for future maintainers and LLM context windows; **load only when working on batch-span behavior**, otherwise skip.

This document preserves the design conversation behind FR-007a so the rationale is recoverable from the repo alone, without re-deriving it from the AF4 and AF5 sources.

---

## TL;DR

- AF5's `EventProcessor` interface does **not** expose batch processing — that ruled out a `TracingEventProcessor` decorator.
- We need AF4-parity nested spans (`StreamingEventProcessor.batch` containing `EventProcessor.process` per event) with no upstream change to AxonFramework 5.
- The decorator on `EventHandlingComponent` opens the batch span **lazily on the first per-event `handle()` call** via `ctx.computeResourceIfAbsent(BATCH_SPAN_KEY, …)`, guarded by `ctx.getResource(Segment.RESOURCE_KEY).isPresent()`.
- The batch span binds to the batch UoW's `whenComplete`, so it encloses **all** event handlers + `runOnPrepareCommit` (token-store write) + `runOnAfterCommit` (status update). AF4 batch-span coverage parity confirmed.
- The cleaner alternative (B+upstream — a `BatchInterceptor` API on `PooledStreamingEventProcessorConfiguration`) was rejected because it requires an upstream PR and release. Kept as a deferred future improvement.
- The decorator covers **both** `PooledStreamingEventProcessor` and `SubscribingEventProcessor` via a single `DecoratorDefinition.forType(EventHandlingComponent.class)` registration scoped to the common `EventProcessorConfiguration` base. `SubscribingEventProcessor` deliberately produces **no batch root span** — AF4 parity, not a regression — because subscribing handlers run synchronously inside the publisher's UoW where a separate batch span would be a redundant wrapper.

---

## AF4 vs AF5 event-processor hierarchy — and why one decorator covers both

### AF4 hierarchy

```
EventProcessor (interface)
  └── AbstractEventProcessor (abstract)
        ├── SubscribingEventProcessor
        ├── TrackingEventProcessor                 (removed in AF5)
        └── PooledStreamingEventProcessor
```

The batch-span code lived in `AbstractEventProcessor.processInUnitOfWork` (line 165 — `spanFactory.createBatchSpan(this instanceof StreamingEventProcessor, eventMessages).runCallable(...)`), so all three subclasses inherited the same wiring. The `streaming` boolean was derived from `this instanceof StreamingEventProcessor`, and `DefaultEventProcessorSpanFactory.createBatchSpan` returned `NoOpSpan.INSTANCE` when `!streaming` — so `SubscribingEventProcessor` ran the call but received a NoOp span back.

### AF5 hierarchy

```
EventProcessor (interface)
  ├── SubscribingEventProcessor
  └── PooledStreamingEventProcessor
```

No abstract base. The two processor types are independent siblings. **However**, their configuration classes share a common base:

```
EventProcessorConfiguration (concrete, in messaging/.../eventhandling/configuration/)
  ├── SubscribingEventProcessorConfiguration
  └── PooledStreamingEventProcessorConfiguration
```

Both processor types ultimately route to `eventHandlingComponents.handle(entries, ctx)` (the `ProcessorEventHandlingComponents.handle(...)` entry at `messaging/.../eventhandling/processing/ProcessorEventHandlingComponents.java:110-124`). So a single `DecoratorDefinition.forType(EventHandlingComponent.class)` registration catches both — provided its scope guard accepts both configuration types.

### Registration scope decision (Clarification 2026-05-26 Q: "How is TracingEventHandlingComponent scoped …?")

Chosen: **guard on the common `EventProcessorConfiguration` base**.

```java
registry.registerDecorator(
    DecoratorDefinition
        .forType(EventHandlingComponent.class)
        .with((cfg, name, delegate) -> {
            if (cfg.getOptionalComponent(EventProcessorConfiguration.class).isEmpty()) {
                return delegate;  // not inside any event-processor module — skip
            }
            return new TracingEventHandlingComponent(delegate,
                                                     cfg.getComponent(SpanFactory.class),
                                                     cfg.getComponent(TracingProperties.class).eventProcessor());
        })
);
```

This is broader than the DLQ enhancer's PSEP-only scope. Justification: tracing semantics legitimately apply to every event-handling component, not just pooled-streaming. The DLQ's PSEP-only scope reflects DLQ semantics (segment-aware replay) and is appropriate for that feature; tracing has different semantics and a different correct scope.

Rejected scope alternatives:
- **No guard at all**: decorator would wrap any `EventHandlingComponent`, including test stubs and ad-hoc registrations outside any processor.
- **Two specific guards (PSEP OR Subscribing)**: requires updating the enhancer whenever a new processor type ships. The common base avoids that.
- **PSEP only**: AF4 regression — subscribing processors would lose per-event spans entirely.

### Why `SubscribingEventProcessor` gets no batch root span — and why that's AF4 parity, not a regression

The user instinct that "AF4 shared the abstract class, so all types got the batch span" is half-right: the **code** was shared, the **span** was not. AF4's `DefaultEventProcessorSpanFactory.createBatchSpan` returned a NoOp for non-streaming processors:

```java
// AxonFramework4/.../DefaultEventProcessorSpanFactory.java:81-86  (verbatim)
public Span createBatchSpan(boolean streaming, List<? extends EventMessage> eventMessages) {
    if (distributedInSameTrace || disableBatchTrace || !streaming) {
        return NoOpSpanFactory.NoOpSpan.INSTANCE;   // ← here for SubscribingEventProcessor
    }
    return spanFactory.createRootTrace(() -> "StreamingEventProcessor.batch");
}
```

The AF4 reference guide documents this explicitly at `AxonFramework4/docs/old-reference-guide/modules/monitoring/pages/tracing.adoc:193-195`:

> | Subscribing event processor | Child of the publishing span (synchronous invocation) | None (handler runs inside the publishing unit of work) |

AF4's design rationale (preserved by AF5's mapping):

1. **Subscribing processors run synchronously inside the publisher's UoW.** The handler call stack is `publisher.dispatchCommand → @CommandHandler → eventSink.publish(events) → subscribingProcessor.process(events, publisherCtx) → handler`. There is no separate batch boundary — the publisher's UoW *is* the batch.
2. **The publisher already has a span covering its whole UoW**, plus the AF4 `commit-events` span (and AF5's `EventBus.commitEvents` span via `TracingEventSink`) covering the per-event publication. Adding a `SubscribingEventProcessor.batch` span on top of that would be a redundant wrapper around an already-spanned boundary — every event handler would get a great-grandparent span that adds nothing the publisher's span doesn't already provide.
3. **AF4 trace dashboards rely on this shape.** Story 4's acceptance criterion 3 explicitly requires "span names, kinds, and attributes are equivalent — i.e., the consolidation does not regress the observable trace shape." Introducing a new batch span for subscribing is a regression *in the other direction*.

How AF5's `TracingEventHandlingComponent` reproduces this with zero special-casing:
- `SubscribingEventProcessor.process(events, context)` does not write `Segment.RESOURCE_KEY` to its UoW. Only `WorkPackage.processBatch` (`PooledStreamingEventProcessor`'s code path) writes that key.
- `TracingEventHandlingComponent.openBatchSpan(ctx)` checks `ctx.getResource(Segment.RESOURCE_KEY) != null`. For subscribing, the resource is absent, the check fails, the method returns `NoOpSpan.INSTANCE`. No batch root is opened. **Identical to AF4 behavior.**
- The per-event span (`EventProcessor.process <eventName>`) still opens. Its parent is whatever is current on the surrounding `ProcessingContext`:
  - When `SubscribingEventProcessor.process(events, context)` reuses the caller's `ProcessingContext` (`AxonFramework5/.../SubscribingEventProcessor.java:145-146`) — typically the publisher's UoW — the per-event span becomes a child of the publisher's trace. Identical to AF4.
  - When `context == null` and a fresh UoW is created (line 148), the per-event span starts a new trace (or, if a W3C parent is present in `event.getMetaData()`, parents under that). Identical to AF4.

### Subscribing-processor coverage matrix

| Processor | Batch root span | Per-event spans | Parent of per-event spans |
|---|---|---|---|
| `PooledStreamingEventProcessor` | **`StreamingEventProcessor.batch`** (lazy-opened via `Segment.RESOURCE_KEY` guard, bound to batch UoW `whenComplete`) | `EventProcessor.process <eventName>` | The batch span |
| `SubscribingEventProcessor` (publisher-supplied ctx) | None (NoOp — AF4 parity) | `EventProcessor.process <eventName>` | The publisher's surrounding span (via reused `ProcessingContext`) |
| `SubscribingEventProcessor` (`context == null`, fresh UoW) | None (NoOp — AF4 parity) | `EventProcessor.process <eventName>` | New root trace (or W3C parent from `event.getMetaData()` if present) |
| Future `EventProcessor` impls | Yes if they write `Segment.RESOURCE_KEY` to the batch UoW | Yes — inherits from common-base scope guard | Their batch span if present, else the surrounding span |

### When would deviating from AF4 here make sense?

Deferred future improvement (DFI candidate) — not in scope for this feature:

A `axon.tracing.eventProcessor.subscribingBatchTrace` property (default `false`, matching AF4) could be added later. Enabling it would require writing a second sentinel key (e.g., a `EventProcessorConfiguration.PROCESSOR_NAME_KEY`) from both processor types and broadening `openBatchSpan`'s detection logic. Both pieces are pure additions to FR-007a — no breaking changes — if a future user case justifies the deviation.

---

## AF4 reference behavior — what's inside the batch span

### Call sequence in `WorkPackage.processEvents()`

`AxonFramework4/messaging/src/main/java/org/axonframework/eventhandling/pooled/WorkPackage.java:297-336`:

```java
List<TrackedEventMessage<?>> eventBatch = new ArrayList<>();
while (...) { /* drain queue */ eventBatch.add(...); }            // ❶ batch built

UnitOfWork<...> unitOfWork = new BatchingUnitOfWork<>(eventBatch); // ❷ UoW created
unitOfWork.attachTransaction(transactionManager);
unitOfWork.resources().put(segmentIdResourceKey, segment.getSegmentId());
unitOfWork.resources().put(lastTokenResourceKey, lastConsumedToken);
unitOfWork.onPrepareCommit(u -> storeToken(lastConsumedToken));    // ❸ token-store write registered
unitOfWork.afterCommit(u -> {
    segmentStatusUpdater.accept(status -> status.advancedTo(...)); // ❹ status update
    batchProcessedCallback.run();
});

batchProcessor.processBatch(eventBatch, unitOfWork, ...);
// → AbstractEventProcessor.processInUnitOfWork
```

`AxonFramework4/messaging/src/main/java/org/axonframework/eventhandling/AbstractEventProcessor.java:165-188`:

```java
spanFactory.createBatchSpan(this instanceof StreamingEventProcessor, eventMessages).runCallable(() -> {
    ResultMessage<?> resultMessage = unitOfWork.executeWithResult(() -> {       // ← batch span wraps THIS
        EventMessage<?> message = unitOfWork.getMessage();
        ...
        return spanFactory.createProcessEventSpan(this instanceof StreamingEventProcessor, message)
                          .runCallable(() -> new DefaultInterceptorChain<>(...).proceed());
    }, rollbackConfiguration);
    ...
});
```

The batch span wraps `unitOfWork.executeWithResult(...)`. That call:
1. Fires registered `onPrepareCommit` callbacks → ❸ `storeToken(...)` (token-store write).
2. Commits the UoW.
3. Fires registered `afterCommit` callbacks → ❹ status update.

### What the AF4 batch span covers / doesn't cover

| Phase | Inside AF4 batch span? |
|---|---|
| ❶ Drain queue into `eventBatch` | No — runs before any UoW or span exists |
| ❷ UoW construction + resource attach | No — runs before `processBatch` is called |
| Per-event handler invocations (inside `unitOfWork.executeWithResult`) | **Yes** |
| `onPrepareCommit` → token-store write | **Yes** (inside `executeWithResult`) |
| `afterCommit` → segment status update | **Yes** (inside `executeWithResult`) |
| `extendClaim` in the no-events branch (line 333) | No — separate path with no batch span at all |

### AF4 `EventProcessorSpanFactory` flags

`AxonFramework5/messaging/src/main/java/org/axonframework/messaging/eventhandling/tracing/DefaultEventProcessorSpanFactory.java:81-106` (still present in AF5's dead-code tree; identical to AF4):

| Property | Default | Effect |
|---|---|---|
| `disableBatchTrace` | `false` | When `true`, no batch span; each event is a root trace |
| `distributedInSameTrace` | `false` | When `true`, per-event spans parent to the publisher's trace if published within `distributedInSameTraceTimeLimit` |
| `distributedInSameTraceTimeLimit` | `PT2M` | Window for `distributedInSameTrace` |

And the boolean hack: `createBatchSpan(boolean streaming, ...)` returns `NoOpSpan` when `!streaming` — i.e., `SubscribingEventProcessor` never gets a batch root trace.

---

## AF5 hook points — full enumeration

Discovered by reading `PooledStreamingEventProcessor`, `WorkPackage`, `PooledStreamingEventProcessorConfiguration`, and the existing `DeadLetterQueueConfigurationEnhancer` integration.

| # | Hook point | Granularity | Notes |
|---|---|---|---|
| 1 | `DecoratorDefinition.forType(EventHandlingComponent.class)` | per-event call | DLQ uses this pattern (`DeadLetterQueueConfigurationEnhancer.java:137-144`) |
| 2 | `DecoratorDefinition.forType(PooledStreamingEventProcessorConfiguration.class)` | config-time | DLQ uses this for `SegmentChangeListener` (line 105-130). Today does **not** expose any batch-listener API |
| 3 | `PooledStreamingEventProcessorConfiguration.interceptors(MessageHandlerInterceptor)` | per-event | `MessageHandlerInterceptor<? super EventMessage>` is per-event, not per-batch |
| 4 | `schedulingProcessingContextProvider` | per-event scheduling | Provides the *scheduling* ctx (segmenting/sequence-id), not the *processing* ctx |
| 5 | `DecoratorDefinition.forType(EventProcessor.class)` | processor lifecycle | The `EventProcessor` interface only exposes `start`/`stop`/`shutdown` — batch processing is internal to `WorkPackage.processBatch` |
| 6 | `UnitOfWorkFactory` decoration | per-UoW | `WorkPackage.processBatch` creates one UoW per batch via `unitOfWorkFactory.create()` (line 377), but the factory is shared with non-batch callers |

### Critical finding — `EventProcessor` interface is too narrow

```java
// org.axonframework.messaging.eventhandling.processing.EventProcessor
public interface EventProcessor {
    void start();
    void shutdown();
    boolean isRunning();
    // ...
    // NO method that exposes batch processing
}
```

`PooledStreamingEventProcessor.processWithErrorHandling(entries, context)` is a private lambda. Wrapping `EventProcessor` only gives control-plane hooks — not a data-plane batch hook. This rules out a `TracingEventProcessor` decorator class entirely.

### AF5 `WorkPackage.processBatch` lifecycle

`AxonFramework5/messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/pooled/WorkPackage.java:377-394`:

```java
var unitOfWork = unitOfWorkFactory.create();
unitOfWork.runOnPreInvocation(ctx -> {
    ctx.putResource(Segment.RESOURCE_KEY, segment);                  // ← only writer of this key in AF5
    ctx.putResource(TrackingToken.BATCH_END_RESOURCE_KEY, lastConsumedToken);
});
unitOfWork.onInvocation(ctx -> batchProcessor.process(eventBatch, ctx).asCompletableFuture());
unitOfWork.runOnPrepareCommit(ctx -> storeToken(lastConsumedToken, ctx));
unitOfWork.runOnAfterCommit(ctx -> {
    segmentStatusUpdater.accept(status -> status.advancedTo(lastConsumedToken));
    if (batchProcessedCallback != null) {
        batchProcessedCallback.run();
    }
});
FutureUtils.joinAndUnwrap(unitOfWork.execute());
```

Verified by `grep -rn "putResource(Segment\|Segment\.RESOURCE_KEY" AxonFramework5 --include='*.java'`: line 379 is the only writer in production code.

---

## Chosen approach — Option C (lazy-open on `ProcessingContext`)

### Sketch

```java
// in axoniq-tracing-core (package-private)
final class TracingEventHandlingComponent implements EventHandlingComponent {

    private static final ResourceKey<Span> BATCH_SPAN_KEY =
            ResourceKey.withLabel("axoniq.tracing.batch-span");

    private final EventHandlingComponent delegate;
    private final SpanFactory spanFactory;
    private final TracingProperties.EventProcessor config;

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event, ProcessingContext ctx) {
        // (1) Lazy-open the batch span — first event in this ctx wins
        ctx.computeResourceIfAbsent(BATCH_SPAN_KEY, () -> openBatchSpan(ctx));

        // (2) Open per-event span as a child of whatever is currently on this ctx
        Span eventSpan = spanFactory.createHandlerSpan(
                "EventProcessor.process", event, ctx);
        ProcessingContextSpanBinding.bindPerEvent(ctx, eventSpan);

        return delegate.handle(event, ctx);
    }

    private Span openBatchSpan(ProcessingContext ctx) {
        boolean isStreamingBatch = ctx.getResource(Segment.RESOURCE_KEY) != null;
        if (!isStreamingBatch || config.isDisableBatchTrace()) {
            return NoOpSpan.INSTANCE;  // matches AF4: no batch root for !streaming or disableBatchTrace
        }
        Span batch = spanFactory.createInternalSpan("StreamingEventProcessor.batch");
        ProcessingContextSpanBinding.bindBatch(ctx, batch);  // start now, close on whenComplete
        return batch;
    }
}
```

Wiring (mirrors DLQ pattern exactly — `DeadLetterQueueConfigurationEnhancer.java:137-144`):

```java
// in TracingConfigurationEnhancer
registry.registerDecorator(
    DecoratorDefinition
        .forType(EventHandlingComponent.class)
        .with((cfg, name, delegate) -> {
            if (!cfg.getOptionalComponent(PooledStreamingEventProcessorConfiguration.class).isPresent()) {
                return delegate;  // only inside streaming-processor scope
            }
            return new TracingEventHandlingComponent(delegate,
                                                     cfg.getComponent(SpanFactory.class),
                                                     cfg.getComponent(TracingProperties.class).eventProcessor());
        })
);
```

### Why the lazy-open works through `ResourceOverridingProcessingContext`

`ProcessorEventHandlingComponents.handle(entries, context)` creates per-event sub-contexts on line 116-118:

```java
ProcessingContext perEventContext = TrackingToken.fromContext(entry)
        .map(token -> context.withResource(TrackingToken.RESOURCE_KEY, token))
        .orElse(context);
```

`context.withResource(key, value)` returns a `ResourceOverridingProcessingContext` — a transparent shadow. Verified by reading `AxonFramework5/messaging/src/main/java/org/axonframework/messaging/core/unitofwork/ResourceOverridingProcessingContext.java`:

- `getResource(key)` (line 190-193): for any key other than the override, **delegates to parent**.
- `computeResourceIfAbsent(key, supplier)` (line 221-228): for any key other than the override, **delegates to parent**.
- Every lifecycle method (`runOnPreInvocation`, `whenComplete`, …, lines 82-175): **delegates to parent**.

Implications:
1. `perEventContext.getResource(Segment.RESOURCE_KEY)` falls through to the batch ctx — returns the segment.
2. `perEventContext.computeResourceIfAbsent(BATCH_SPAN_KEY, …)` falls through — the batch span is stored on the **batch ctx**, not the transient per-event view. Every event sees the same parent, hits the same `computeResourceIfAbsent`, only the first writes.
3. `perEventContext.whenComplete(closeBatchSpan)` registers on the batch ctx — the batch span closes when the **batch UoW** completes.

### AF4 vs Option C batch-span coverage

| Phase | AF4 batch span | AF5 Option C batch span | AF5-only step |
|---|---|---|---|
| Drain queue → eventBatch | outside | outside | — |
| UoW construction | outside | outside | — |
| `runOnPreInvocation` resource-attach | n/a | **outside** (opens at first `handle()`) | AF5 has this phase; covers a microsecond `ctx.putResource(...)` only |
| Event handler invocations | **inside** | **inside** | — |
| Token-store write (`prepareCommit`) | **inside** | **inside** | — |
| Segment status update (`afterCommit`) | **inside** | **inside** | — |
| `extendClaim` in no-events branch | n/a (separate path) | n/a (separate path) | — |

**Verdict**: AF4-parity. The only AF4 thing Option C technically misses is the AF5-specific `runOnPreInvocation` resource-attach, which has no AF4 analog and isn't user-meaningful trace content.

### Configuration parity

`TracingProperties.eventProcessor`:

```java
public class EventProcessorOptions {
    private boolean disableBatchTrace = false;
    private boolean distributedInSameTrace = false;
    private Duration distributedInSameTraceTimeLimit = Duration.ofMinutes(2);
}
```

Maps 1:1 to AF4's `DefaultEventProcessorSpanFactory.Builder`. Semantics:
- `disableBatchTrace=true` → `openBatchSpan` returns `NoOpSpan`; per-event spans become roots (or linked-back to publisher if propagation header is present).
- `distributedInSameTrace=true` → in `TracingEventHandlingComponent.handle`, before opening the per-event span, check `event.timestamp().isAfter(Instant.now().minus(timeLimit))`. If yes, skip the batch-span parent and let the per-event span pick up the W3C parent extracted from `event.getMetaData()`.
- The default (both `false`) gives the traditional shape: one batch root span containing per-event children.

---

## Rejected alternatives

### Option A — `TracingEventProcessor` decorator

**Idea**: `DecoratorDefinition.forType(EventProcessor.class)` wraps the processor.

**Rejected because**: the AF5 `EventProcessor` interface only exposes `start`/`stop`/`shutdown`. The data-plane (`processWithErrorHandling`) is a private lambda on `PooledStreamingEventProcessor`. Wrapping `EventProcessor` gives control-plane hooks only — no batch interception is possible.

### Option B+upstream — `BatchInterceptor` API on PSEP config

**Idea**: Add an upstream public API in AxonFramework 5:

```java
// in PooledStreamingEventProcessorConfiguration
public PooledStreamingEventProcessorConfiguration registerBatchInterceptor(BatchInterceptor interceptor) {
    this.batchInterceptors.add(interceptor);
    return this;
}

@FunctionalInterface
public interface BatchInterceptor {
    void onBatch(List<? extends EventMessage> events, ProcessingContext ctx);
}
```

And invoke in `WorkPackage.processBatch`, around line 383:
```java
unitOfWork.runOnPreInvocation(ctx -> {
    ctx.putResource(Segment.RESOURCE_KEY, segment);
    ctx.putResource(TrackingToken.BATCH_END_RESOURCE_KEY, lastConsumedToken);
    for (var bi : batchInterceptors) {
        bi.onBatch(eventBatch.stream().map(Entry::message).toList(), ctx);
    }
});
```

Tracing wiring:
```java
registry.registerDecorator(
    DecoratorDefinition
        .forType(PooledStreamingEventProcessorConfiguration.class)
        .<PooledStreamingEventProcessorConfiguration>with((cfg, name, delegate) -> {
            SpanFactory sf = cfg.getComponent(SpanFactory.class);
            delegate.registerBatchInterceptor((events, ctx) -> {
                Span batch = sf.createInternalSpan("StreamingEventProcessor.batch");
                ProcessingContextSpanBinding.bindBatch(ctx, batch);
                ctx.putResource(BATCH_SPAN_KEY, batch);
            });
            return delegate;
        })
);
```

**Pros**
- Explicit, named extension point — readers see "batch interceptor" and understand intent immediately.
- Batch span opens precisely at the UoW pre-invocation phase, before any per-event handling — matches AF4 timing exactly (no "first-event-wins" implicit contract).
- Other modules (metrics, audit) can register their own batch interceptors without reinventing the `ResourceKey` dance.
- No implicit "Segment.RESOURCE_KEY presence" contract; the batch boundary is an explicit upstream concept.
- Naturally supports streaming-only by living on `PooledStreamingEventProcessorConfiguration` — no `isStreaming` branch needed.

**Cons (and why rejected)**
- **Requires an upstream AxonFramework 5 PR + release before this AxoniqFramework feature can ship.** Violates the plan's "no upstream changes" constraint (`plan.md` Constitution Check row "II. Minimal Impact": "No upstream AxonFramework changes needed").
- Adds public API to AxonFramework 5 — every future maintainer has to weigh it.
- If the upstream PR is rejected or delayed, we fall back to Option C anyway. The coordination cost outweighs the architectural cleanness.

**Deferred future improvement (DFI candidate)**: when an upstream PR is acceptable (e.g., during a future AxonFramework 5 minor release coordinated with AxoniqFramework), graduate Option C's implicit `Segment.RESOURCE_KEY` contract into an explicit `BatchInterceptor` SPI. Once the SPI exists, `TracingEventHandlingComponent`'s lazy-open code path can be replaced by an interceptor registration; the user-visible span shape stays identical.

### Option D — `UnitOfWorkFactory` decoration

**Idea**: Decorate the `UnitOfWorkFactory` resolved from the PSEP config. Each `unitOfWorkFactory.create()` produces one batch UoW; open span on create, close on commit.

**Rejected because**: the same `UnitOfWorkFactory` is used by many non-batch callers (commands, queries, deadlines if ever ported). Scoping the decoration cleanly to "only when called from `WorkPackage.processBatch`" requires either:
- A separate, dedicated factory per processor — not how AF5 wires it today (the factory is a shared component).
- Stack inspection or thread-local sentinels — directly forbidden by Constitution §V (no `ThreadLocal`s).

### Option F — no batch span, accept AF4 regression

**Idea**: AF5 has one `ProcessingContext` per batch and per-event spans share its `traceId`. Trace UIs can group them; drop the explicit batch root span.

**Rejected because**: AF4 trace dashboards explicitly rely on the `StreamingEventProcessor.batch` root span for grouping. Dropping it is a user-visible regression. The Story 4 acceptance criterion (`spec.md` line 96) explicitly requires: "span names, kinds, and attributes are equivalent — i.e., the consolidation does not regress the observable trace shape."

---

## Implicit contracts to preserve

Two pieces of load-bearing AF5 behavior that Option C depends on. Both are stable as of AxonFramework 5.2.0-SNAPSHOT but should be documented as contracts so future refactors don't silently break tracing:

1. **`Segment.RESOURCE_KEY` is set by streaming processors on the batch UoW.** Today only `WorkPackage.processBatch` writes it. Any future `StreamingEventProcessor` implementation MUST set this key (or its successor) on the batch-level `ProcessingContext` for the tracing decorator to recognize the batch boundary.

2. **`ResourceOverridingProcessingContext` falls through to the delegate for non-overridden keys.** Verified at `ResourceOverridingProcessingContext.java:185-228`. If this ever changes to an isolation model, the per-event sub-context returned by `context.withResource(TrackingToken.RESOURCE_KEY, …)` would no longer see the batch ctx's `Segment.RESOURCE_KEY` or `BATCH_SPAN_KEY`, breaking the lazy-open mechanism.

A defensive integration test (`TracingBatchSpanIntegrationTest`) should assert:
- One `StreamingEventProcessor.batch` span exists per batch UoW when `disableBatchTrace=false`.
- All `EventProcessor.process` per-event spans in that batch are children of the batch span.
- The batch span's duration spans the UoW from first `handle()` through `whenComplete` (so token-store write is enclosed).
- `disableBatchTrace=true` produces no batch span.
- `SubscribingEventProcessor` produces no batch span.

---

## References

- Spec clarification: [spec.md](./spec.md) — the corresponding Q in Clarifications session 2026-05-26.
- Requirement: [spec.md](./spec.md) — FR-007a.
- Worked sequence diagram: [flows.md](./flows.md) — Flow 4 (streaming event processor batch).
- AF4 implementation: `AxonFramework4/messaging/src/main/java/org/axonframework/eventhandling/AbstractEventProcessor.java:165-188` and `eventhandling/pooled/WorkPackage.java:297-336`.
- AF5 implementation (consumer side): `AxonFramework5/messaging/src/main/java/org/axonframework/messaging/eventhandling/processing/streaming/pooled/WorkPackage.java:377-394` and `processing/ProcessorEventHandlingComponents.java:110-151`.
- AF5 dead-code reference for flag semantics: `AxonFramework5/messaging/src/main/java/org/axonframework/messaging/eventhandling/tracing/DefaultEventProcessorSpanFactory.java:81-106`.
- DLQ integration pattern (template for tracing): `messaging/axoniq-dead-letter/src/main/java/io/axoniq/framework/messaging/eventhandling/deadletter/DeadLetterQueueConfigurationEnhancer.java`.
- Resource-context fall-through semantics: `AxonFramework5/messaging/src/main/java/org/axonframework/messaging/core/unitofwork/ResourceOverridingProcessingContext.java:185-228`.
- AF4 user-facing tracing reference: `AxonFramework4/docs/old-reference-guide/modules/monitoring/pages/tracing.adoc` — see the §"Streaming event processors" table for the AF4 span names and the `disableBatchTrace` / `distributedInSameTrace` property reference.
