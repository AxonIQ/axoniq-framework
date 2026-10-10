# ADR 011: A `DeadlineManager` handler parameter is bound to the handler's `ProcessingContext` (issue [#5006](https://github.com/AxonIQ/AxonFramework/issues/5006))

Date: 2026-10-10
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#5003](https://github.com/AxonIQ/AxonFramework/issues/5003) (deadline core), [ADR 003](adr-003-deadline-manager-current-scope.md) (current `Scope`), [ADR 006](adr-006-deadline-manager-builders-on-af5.md) (context of the dispatch interceptors), [ADR 007](adr-007-aggregate-deadline-delivery-through-scope-aware.md) (aggregate deadline delivery), [ADR 009](adr-009-aggregate-deadline-entity-id-via-metadata.md) (entity id of a translated deadline)

## Context

ADR 003 defers a `DeadlineManager` call when a `ContextAwareScope` is current, which is the case while a Saga handler
runs, and runs every other call immediately. Its consequences name the price: "deferral needs a Saga invocation".

ADR 007 and ADR 009 made aggregate deadlines work on Axon Framework 5 entities, so a migrated aggregate now schedules
its deadlines from an entity `@CommandHandler`. Such a handler runs in the `ProcessingContext` of its command, but in no
scope: Axon Framework 5 starts none around it. Two differences from Axon Framework 4 followed, pinned by the
`OnAnEntity.SchedulingFromTheEntity` tests of `AbstractDeadlineManagerTestSuite`:

- `AbstractDeadlineManager.runOnPrepareCommitOrNow(...)` found no context and scheduled at once. Axon Framework 4
  deferred the call to `CurrentUnitOfWork.get().onPrepareCommit(...)`, so a command that failed after scheduling never
  stored its deadline.
- The deadline's dispatch interceptors got a `null` context, where Axon Framework 4 ran them within the command's unit
  of work.

Two constraints from earlier decisions shape the fix:

- The public `DeadlineManager` API and the backends stay in their Axon Framework 4 shape (ADR 003, ADR 006). A
  backend implements `schedule(...)` without a context and calls `runOnPrepareCommitOrNow(...)` from inside it.
- Axon Framework 5 has no ambient unit of work, and `axoniq-legacy` must not reintroduce one.

So whatever tells `runOnPrepareCommitOrNow(...)` about the command's context has to travel with the synchronous call
into the backend, without being a parameter of it.

## Options considered

**Option A: a `DeadlineManager` parameter resolves to a context-bound view (chosen).** A `ParameterResolverFactory`
resolves a parameter declared as `DeadlineManager` to a view of the configured `AbstractDeadlineManager`, bound to the
`ProcessingContext` the handler runs in. This is how Axon Framework 5 binds `CommandDispatcher` and `EventAppender`
parameters (`CommandDispatcher.forContext(context)`). The view delegates each call to the same method of the manager.
While the call runs, it sets a `ThreadLocal` that is private to that manager instance, and clears it again in a
`finally` block. `runOnPrepareCommitOrNow(...)` uses that context only when no `ContextAwareScope` provides one.

**Option B: the view pushes a `ContextAwareScope` instead of a private `ThreadLocal`.** This reuses the mechanism
`runOnPrepareCommitOrNow(...)` already reads, and adds no field. Rejected: a scope also answers
`Scope.describeCurrentScope()`. The scope-less overloads would then describe the view's scope instead of throwing, and
`Scope.getCurrentScope()` would return it to anything else that asks during the call. To stay correct, the view's
`describeScope()` would have to fake "no scope". That leaks the binding into a public mechanism it has nothing to do
with.

**Option C: a `HandlerEnhancerDefinition` starts a `ContextAwareScope` around every handler.** It would also defer calls
made by collaborators the handler delegates to, so it would match Axon Framework 4 more closely. Rejected: a scope that
carries the `ProcessingContext` around every handler of the application is an ambient unit of work under another name.
It also makes `Scope.getCurrentScope()` succeed in every handler. And it cannot describe an aggregate scope, as the
enhancer knows neither the entity type nor its identifier.

**Option D: the view defers the whole delegated call.** The view registers `manager.schedule(...)` on the context's
deferral phase instead of calling it, so no thread-confined state is needed. Rejected: `schedule(...)` has to return
the schedule identifier, which the backend creates only inside the call. Also, the deferred call would run outside any
scope or binding, so the interceptors would still get `null`.

**Option E: context-accepting overloads or a context-aware backend contract.** ADR 003 rejected both (its options B and
C). They grow the public API or restructure every backend, and they still give nothing to a call made through the
existing overloads.

**Option F: a `ScopedValue` instead of a `ThreadLocal`.** It fits a binding that lasts for one call, but it is a
preview API in Java 21, the framework's baseline.

### How option A answers ADR 003's objections to its option C

ADR 003 rejected an earlier context-bound `DeadlineManager` parameter. This one differs on each point:

- *Scope-less calls stored deadlines no `ScopeAware` can resolve.* The view does not touch `Scope`. The scope-less
  overloads still ask for the current scope and still throw outside a Saga, as in Axon Framework 4.
- *The resolver outranked the configuration but registered nothing it looked up.* The resolver looks up the configured
  `DeadlineManager`. It returns no resolver when there is none, when the configured one is not an
  `AbstractDeadlineManager` (such as the test fixture's `StubDeadlineManager`), or when the parameter is declared as an
  implementation type. In each of those cases, the configuration resolver injects the configured component as before.
- *Backends had to be restructured.* No backend changes. The view calls the backend's own methods, including the ones
  it overrides from the interface's defaults.
- *Dispatch interceptors ran too early, and the deferral used `PREPARE_COMMIT`.* Calls deferred through the view take
  the same path as a Saga's: one action per context in `RUN_DEADLINE_CALLS`, with the interceptors running inside the
  deferred call and receiving the context it was deferred to (ADR 006).

## Decision

Option A.

- `AbstractDeadlineManager` gains a package-private `forContext(ProcessingContext)`, returning the view.
  `runOnPrepareCommitOrNow(...)` takes the context from the current `ContextAwareScope` first, then from the view's
  binding, and runs the call immediately when neither provides one. A Saga's scope therefore keeps deciding where a
  Saga's calls go, even when the Saga handler declares a `DeadlineManager` parameter too.
- The package-private `DeadlineManagerParameterResolverFactory` resolves a parameter declared as `DeadlineManager`.
  `DeadlineManagerParameterResolverFactoryConfigurationEnhancer` registers it with the configuration's
  `ParameterResolverFactory`, as Axon Framework 5's `CommandDispatcherParameterResolverFactoryConfigurationEnhancer`
  does. The enhancer is discovered through the `ServiceLoader`. An application can disable it through
  `ComponentRegistry.disableEnhancer(...)` to get the configured manager itself.
- The binding is a `ThreadLocal` field of each manager instance. It is set and cleared around a single synchronous
  delegated call, it restores an enclosing binding afterwards, and nothing outside `AbstractDeadlineManager` reads it.
  It carries no state across handlers or user code, which is what distinguishes it from Axon Framework 4's
  `CurrentUnitOfWork`. It is acceptable for the same reason as ADR 003's `Scope`: it lives at the edge where AF4-shaped
  legacy code meets the explicit `ProcessingContext`, and only in `axoniq-legacy`.

Axon Framework 5 forces the deviation from Axon Framework 4's mechanism in two ways. It has no ambient unit of work,
and the backends' `schedule(...)` has no context parameter. Together, these leave a call-scoped binding as the only
channel into `runOnPrepareCommitOrNow(...)` that changes neither the public API nor the backends.

ADR 003 remains accepted. This decision narrows its consequence "deferral needs a Saga invocation" to "deferral needs
a Saga invocation or a `DeadlineManager` handler parameter".

## Consequences

- A failing entity command schedules and cancels nothing, and a succeeding one schedules its deadlines during its
  commit, after `PREPARE_COMMIT` and before `COMMIT`. The dispatch interceptors receive the command's context. This is
  pinned by `aFailingCommandSchedulesNoDeadline`, `aSucceedingCommandSchedulesItsDeadlineWhenItCommits` and
  `theDispatchInterceptorsGetTheContextOfTheCommand`, for all five backends.
- The same applies to any handler that runs in a `ProcessingContext` and declares a `DeadlineManager` parameter, for
  example a plain `@CommandHandler` or `@EventHandler` component.
- Remaining deviations from Axon Framework 4, each forced by the absence of an ambient unit of work:
  - a `DeadlineManager` reached any other way than as a parameter declared as `DeadlineManager` is not bound. Examples
    are a field of a collaborator the handler delegates to, a parameter declared as an implementation type, and a
    manager the configuration wraps in a decorator that does not extend `AbstractDeadlineManager`. Outside a Saga
    handler, such a call still runs immediately. This is pinned by
    `aCollaboratorHoldingTheManagerSchedulesEvenWhenTheCommandFails` and by
    `aCallOnTheManagerItselfStillRunsImmediately`;
  - the view does not make an entity command handler a scope, so the scope-less overloads still throw there. A migrated
    aggregate passes its `AggregateScopeDescriptor` explicitly. In Axon Framework 4, the aggregate was the current
    scope.
- Correlation data is not attached automatically. Axon Framework 4 merged the current unit of work's correlation data
  into every new `GenericMessage`. In Axon Framework 5, a deadline gets it only through a dispatch interceptor that
  reads it from the context it now receives, such as `CorrelationDataInterceptor`. Wiring that up, together with
  handing each deferred call its own context when several branches of one context defer calls, is a separate
  decision.
- The migration guide's `axon-5/api-changes/02-processing-context.md#scope` entry for ADR 003 needs the narrowed
  condition above.
