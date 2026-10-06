# ADR 004: Trace `DeadlineManager`s with a delegating decorator instead of `DeadlineManagerSpanFactory` (issue [#5111](https://github.com/AxonIQ/AxonFramework/issues/5111))

Date: 2026-10-02
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#5003](https://github.com/AxonIQ/AxonFramework/issues/5003) (deadline core), [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005) (scheduler backends), [#5004](https://github.com/AxonIQ/AxonFramework/issues/5004) (aggregate deadline to command), [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) (migration tooling), [ADR 003](adr-003-deadline-manager-current-scope.md) (current `Scope`)

## Context

Axon Framework 4.9 added tracing to the deadline managers through a per-component span factory:
`DeadlineManagerSpanFactory`, its `DefaultDeadlineManagerSpanFactory` implementation, and a `spanFactory(...)`
builder parameter on each of the four backends (simple, Quartz, JobRunr, db-scheduler). Each backend created the
spans itself:

- schedule and the three cancel operations, wrapped around the call deferred by `runOnPrepareCommitOrNow(...)`;
- a disconnected handler span around the firing unit of work;
- propagation of the trace into the stored deadline, in the Quartz backend only.

Axon Framework 5 redesigned tracing. A single `SpanFactory` component is registered, and per-component tracing is
done by delegating decorators (`TracingCommandBus`, `TracingQueryBus`, `TracingEventSink`, ...) installed by a
`ConfigurationEnhancer`. Its Javadoc states that there is intentionally no per-component span factory interface:
span names, kinds, attributes and propagation are implementation details of those decorators. Tracing is disabled
by not registering a `SpanFactory`, so there is no no-op factory to default to. A span's parent comes from the
tracing context propagated in the message's metadata, or from the active span recorded on the
`ProcessingContext`; anything else is an optional, provider-specific fallback.

The deadline core types were moved from the stash into `axoniq-legacy`. The Axon Framework 4 span types for deadline
managers were left in the stash, and the scheduler backends in the stash still use them.
The question this ADR settles is how the legacy deadline managers are traced on Axon Framework 5.

## Options considered

**Option A: keep `DeadlineManagerSpanFactory` and the backend span code as in Axon Framework 4.** It works, but:

- every span is created without a `ProcessingContext`, so a schedule span only nests under the Saga handler's span
  when the tracing provider happens to apply its ambient fallback;
- the firing span is started before the firing unit of work exists and is not recorded on it, so the Saga handler
  span, or the command dispatched for an aggregate deadline, does not find it as a parent;
- only Quartz propagates the trace into the stored deadline, so the other backends lose the link between scheduling
  and firing;
- every backend needs a no-op default, which Axon Framework 5 no longer provides, and the registered `SpanFactory`
  has to be passed into each backend builder by hand;
- it reintroduces the per-component span factory Axon Framework 5 removed, as public API, with the span code
  repeated in four backends.

Fixing the first two needs a `ProcessingContext` on the `DeadlineManagerSpanFactory` methods, so the Axon Framework 4
interface would not stay unchanged anyway. Its remaining advantage is that Axon Framework 4 configuration code
calling `spanFactory(...)` on a backend builder keeps compiling. That code has to be rewritten for Axon Framework 5's
configuration regardless.

**Option B: a delegating `TracingDeadlineManager` decorator (chosen).** It follows the Axon Framework 5 pattern and
keeps tracing out of the backends.

## Decision

Option B.

- `DeadlineManagerSpanFactory`, `DefaultDeadlineManagerSpanFactory` and `DeadlineSpans` are not ported. The backends
  have no span factory builder parameter and contain no tracing code.
- `TracingDeadlineManager` wraps any `DeadlineManager` and obtains its spans from the registered `SpanFactory`:
  - `schedule(...)`: a dispatch span. A payload that is not a `Message` is wrapped into one, and the delegate receives
    it with the span's tracing context propagated into its metadata. Backends use a given `Message` as the donor of
    the deadline's payload, metadata and identifier, so every backend stores the tracing context, not only Quartz.
    The returned schedule id and the scope description are added as attributes.
  - `cancelSchedule(...)`, `cancelAll(...)`, `cancelAllWithinScope(...)`: internal spans.
  - The span's parent is the active span on the `ProcessingContext` of the current `ContextAwareScope` (the Saga
    handler's span), if there is one.
- When a call is deferred (a `ContextAwareScope` is current), the span is started when the call is made and ended
  when the context completes; a failure of the deferred call is recorded on it. The span is deliberately not recorded
  as the context's active span (`Span.coverLifecycle(...)`), as that would make it the parent of everything the
  Saga handler does afterwards. A call that runs immediately is covered by a branch-scoped span around the call.
- The firing span comes from a `MessageHandlerInterceptor<DeadlineMessage>` that the decorator registers on its
  delegate when that is an `AbstractDeadlineManager`. Every backend runs its handler interceptors around the whole
  firing path inside a unit of work created for that deadline, for every scope descriptor. The interceptor creates a
  disconnected handler span (a new trace linked to the scheduling trace, as in Axon Framework 4) and records it as the
  active span of that unit of work. The Saga handler span, or the command dispatched for an aggregate deadline, then
  nests under it.
- A `ConfigurationEnhancer` in `axoniq-legacy` registers the decorator for `DeadlineManager` components at
  `TracingConfigurationOrder.TRACING_DECORATOR_ORDER`, and only when a `SpanFactory` is registered, like
  `MessagingTracingConfigurationEnhancer` does for the buses.
- The span attribute keys `axon.deadlineId` and `axon.deadlineScope` are kept from Axon Framework 4.

## Consequences

- Deadline traces connect: scheduling nests under the Saga handler that scheduled, the firing trace links back to the
  scheduling trace for all backends, and whatever the deadline triggers nests under the firing span.
- The backends need no tracing code, no no-op span factory default, and no tracing tests of their own.
- Breaking for Axon Framework 4 configuration code, documented in the migration guide and handled by a
  [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) recipe that removes the call:
  - `spanFactory(DeadlineManagerSpanFactory)` no longer exists on the backend builders;
  - custom `DeadlineManagerSpanFactory` implementations, used to rename deadline spans, have no replacement;
  - span names change from `DeadlineManager.scheduleDeadline(<name>)` and friends to the Axon Framework 5 style
    (`DeadlineManager.schedule <name>`).
- Only `DeadlineManager`s registered in the configuration are traced automatically. A manager built and used by hand
  is traced by wrapping it in a `TracingDeadlineManager` explicitly.
- The configured `DeadlineManager` component is the decorator, not the backend class. Code that casts it to
  `AbstractDeadlineManager`, for example to register interceptors, has to register them on the backend instance
  before handing it to the configuration.
- A handler interceptor registered on the backend before the decorator wraps it runs outside the firing span. Keeping
  the firing span outermost needs `AbstractDeadlineManager` to let the decorator register its interceptor first.
- Inside a Saga handler the schedule span ends when the context completes, not when the deferred backend call
  returns, so it can be longer than in Axon Framework 4.
- Tracing is on whenever a `SpanFactory` is registered. `MessagingTracingSettings` belongs to Axon Framework 5 and has
  no deadline toggle.
