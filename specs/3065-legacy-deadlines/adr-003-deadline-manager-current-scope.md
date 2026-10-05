# ADR 003: `DeadlineManager` finds its scope and `ProcessingContext` through the current `Scope` (issue [#5003](https://github.com/AxonIQ/AxonFramework/issues/5003))

Date: 2026-09-30
Status: proposed
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#3728](https://github.com/AxonIQ/AxonFramework/issues/3728) (saga port, merged), [#5001](https://github.com/AxonIQ/AxonFramework/issues/5001) (scope-routing types, including `Scope`), [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005) (scheduler backends), [#5047](https://github.com/AxonIQ/AxonFramework/issues/5047) (saga delivery), [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) (migration tooling)

## Context

A call to an Axon Framework 4 `DeadlineManager` relied on two pieces of ambient state:

- **The current `Scope`.** The overloads without a `ScopeDescriptor` used `Scope.describeCurrentScope()`, which
  describes the Saga (or aggregate) whose handler is running, and threw `IllegalStateException` when none was.
- **The current unit of work.** `AbstractDeadlineManager.runOnPrepareCommitOrNow(...)` deferred the actual call
  to the prepare-commit phase of `CurrentUnitOfWork`, so a call made in a unit of work that later rolls back
  never reaches the backing store.

Both were available however the `DeadlineManager` was reached: as a handler parameter, as a field, or through
a collaborator service the Saga delegates to. The public `DeadlineManager` API must stay source compatible,
so neither can become an explicit parameter.

Axon Framework 5 passes the `ProcessingContext` explicitly and has no ambient unit of work. The merged saga
port already replaced the static `SagaLifecycle` with a `ProcessingContext` resource. The question this ADR
settles is where a `DeadlineManager` call gets the Saga's scope and `ProcessingContext` from.

## Options considered

**Option A: keep `Scope` and let it carry the `ProcessingContext` (chosen).** `Scope` is ported unchanged.
`AnnotatedSaga` starts a per-invocation scope around the synchronous handler call, exactly where Axon
Framework 4 used `executeWithResult`. That scope is an `@Internal` `ContextAwareScope`, which also exposes the
invocation's `ProcessingContext`. `DeadlineManager` and `AbstractDeadlineManager` keep their Axon Framework 4
shape; only `runOnPrepareCommitOrNow` changes where it looks for the unit of work.

**Option B: add `ProcessingContext`-accepting overloads to `DeadlineManager`.** Rejected: it grows the API of
a class meant to be ported unchanged, and the existing overloads would still have no scope and nothing to
defer to, so every call site would have to change anyway.

**Option C: a context-bound `DeadlineManager`, resolved as a handler parameter.** This was the first
implementation of this issue ([AxonIQ/AxonFramework#5088](https://github.com/AxonIQ/AxonFramework/pull/5088), closed
unmerged). Following the `SagaLifecycle` precedent, a `DeadlineManager` parameter
resolved to a wrapper bound to the invocation's `ProcessingContext`, and scope-less calls fell back to
`NoScopeDescriptor`. Rejected after review, because it changed behaviour without Axon Framework 5 forcing it:

- a `DeadlineManager` reached any other way (field, collaborator, custom Saga factory, the test fixture's
  `StubDeadlineManager`) had no scope and no deferral: a scope-less `schedule(...)` stored a deadline no
  `ScopeAware` can resolve, so it was silently never delivered, where Axon Framework 4 threw;
- the parameter resolver outranked the configuration resolver but had nothing registering what it looked
  up, so a `DeadlineManager` parameter failed where Axon Framework 4 injected the configured manager;
- `AbstractDeadlineManager` got a new extension contract (`doSchedule` and friends), so every Axon Framework 4
  backend and custom subclass had to be restructured instead of ported;
- dispatch interceptors ran when the call was made rather than inside the deferred call, and the deferral
  registered for `PREPARE_COMMIT`, which a `ProcessingContext` rejects while it is already running that phase.

## Decision

Option A.

- `Scope` lives in `axoniq-legacy` as `org.axonframework.messaging.Scope`, unchanged.
- `AnnotatedSaga.handle(...)` makes a per-invocation `ContextAwareScope` the current scope for the duration of
  the synchronous handler invocation. It describes the Saga as Axon Framework 4 did: the simple class name of
  the Saga instance plus the Saga identifier, which is what `AbstractSagaManager.canResolve(...)` compares
  against. `ScopeDescriptorParameterResolverFactory` gets its Axon Framework 4 body back.
- `DeadlineManager`'s scope-less overloads use `Scope.describeCurrentScope()` again and throw outside a scope.
- `AbstractDeadlineManager.runOnPrepareCommitOrNow(Runnable)` defers when a `ContextAwareScope` is current, and
  runs the call immediately otherwise. Deferred calls of one manager and context are kept in the order they were
  made and run by a single action in `RUN_DEADLINE_CALLS` (`PREPARE_COMMIT + 7_500`). A call that arrives once
  that phase has started fails with an `IllegalStateException` (see below).
- A `DeadlineManager` handler parameter resolves through the configuration (or Spring) like any other
  component, as in Axon Framework 4. No dedicated resolver exists.
- `AbstractDeadlineManager.processDispatchInterceptors(...)` blocks with a timeout through
  `FutureUtils.joinAndUnwrap(...)` and surfaces a failing interceptor's exception. An interceptor that ends the
  chain without a message fails the call with an `IllegalStateException`, instead of scheduling `null` as Axon
  Framework 4 did.
- All `schedule(...)` overloads are `@Deprecated`, because scheduling new deadlines is supported only to keep
  Axon Framework 4 code running while migrating. Cancelling stays fully supported.

### Why a `ThreadLocal` is acceptable here

Axon Framework 5 avoids `ThreadLocal`s internally and allows them only at the edges, for imperative style.
This one is such an edge: it lives in `axoniq-legacy` only, is set and cleared in a single `try`/`finally` at the
hand-off to imperative user code, and is read only by legacy code. The merged saga port already fails a Saga
handler that does not complete on the thread that invoked it, so the scope is never observed from a thread it
was not set on. No Axon Framework 5 core module depends on `axoniq-legacy`, so only legacy code and applications
that opted into the module can reach `Scope` at all.

### Why `RUN_DEADLINE_CALLS` sits where it does

A `ProcessingContext` rejects a registration for the phase it is already running, and a subscribing event
processor fed by the `SimpleEventBus` invokes a Saga from within `PREPARE_COMMIT`. The deferred calls therefore
run in the gap above it, like the Saga write (`AnnotatedSagaRepository.WRITE_SAGA`, `PREPARE_COMMIT + 5_000`).
They run after the Saga write, because Axon Framework 4 registered the Saga write for prepare-commit when the
Saga was loaded, before its handler made any deadline call. One action per queue, instead of one per call,
keeps the calls in the order they were made: actions registered for the same phase may run concurrently.

### Calls that arrive once `RUN_DEADLINE_CALLS` has started

Nothing runs a call deferred to a context whose `RUN_DEADLINE_CALLS` phase has already started. Such a call fails
with an `IllegalStateException` instead of being dropped. The message is the same whether earlier calls were
deferred to that context or not:

- If they were, their action takes the calls and marks them as run in one step. Deferring a call does the same
  check and add in one step, under the same lock. A call is therefore either taken along or rejected. It is never
  accepted after the calls were taken, which could otherwise happen when a context with a multithreaded work
  scheduler invokes a Saga from another action of the same phase.
- If none were, the context rejects the registration of the action itself. `AbstractDeadlineManager` rethrows that
  rejection with the same message, keeping the original as its cause.

Running such a call immediately was considered and rejected, because neither Axon Framework 4 nor Axon Framework 5
lets a Saga get that far:

- In Axon Framework 4, an event processor invoked a Saga in its own unit of work, nested in the publisher's unit of
  work and still to run its own prepare-commit. A Saga could not be reached after that point either:
  `AbstractEventBus.publish(...)` refused events once the current or the root unit of work was past prepare-commit.
- In Axon Framework 5, `SimpleEventBus` refuses events once a context has delivered its events in `PREPARE_COMMIT`,
  and once it has committed.

A Saga can only be reached this late by invoking it directly with a context that has passed `PREPARE_COMMIT`,
bypassing the event bus. Running the call immediately would make the port more lenient than either version, and
would schedule outside the ordering and rollback guarantees the deferral exists for.

## Consequences

- An Axon Framework 4 Saga schedules and cancels deadlines unchanged, whether it declares a `DeadlineManager`
  parameter or delegates to a collaborator holding one. No migration step or OpenRewrite recipe is needed for
  this.
- The [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005) backends can be ported close to literally:
  they keep calling `runOnPrepareCommitOrNow(...)` and `processDispatchInterceptors(...)` as in Axon Framework 4.
- Divergences Axon Framework 5 forces, to be documented in the Axon Framework repository's
  `axon-5/api-changes/02-processing-context.md#scope`:
  - deferral needs a Saga invocation: a call made outside a Saga handler runs immediately, even while some
    other `ProcessingContext` is running, because there is no ambient unit of work to find it through;
  - deferred calls run in `RUN_DEADLINE_CALLS` instead of in `PREPARE_COMMIT` itself;
  - rollback is stricter. Axon Framework 4 ran deferred calls in the prepare-commit of the event processor's nested
    unit of work, so a later rollback of the publisher's unit of work did not undo them. Axon Framework 5 invokes
    the Saga within the publisher's context, so its deferred calls are skipped when that context rolls back before
    `RUN_DEADLINE_CALLS`. A failure in a later phase, such as `COMMIT`, still does not undo calls that already ran.
- A Saga handler that hands work to another thread cannot schedule deadlines from there within the Saga's
  scope. Axon Framework 4 had the same limit, and the saga port already rejects such handlers.
