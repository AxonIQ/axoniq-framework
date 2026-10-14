# ADR 006: Deadline manager builders take a `UnitOfWorkFactory` and a `Converter` (issue [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005))

Date: 2026-10-05
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#5003](https://github.com/AxonIQ/AxonFramework/issues/5003) (deadline core), [#5004](https://github.com/AxonIQ/AxonFramework/issues/5004) (aggregate deadline to command), [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) (migration tooling), [#5111](https://github.com/AxonIQ/AxonFramework/issues/5111) (tracing decorator), [#564](https://github.com/AxonIQ/axoniq-framework/issues/564) (Saga deadline delivery), [ADR 001](adr-001-aggregate-deadline-command-translation.md) (aggregate deadlines as commands), [ADR 003](adr-003-deadline-manager-current-scope.md) (current `Scope`), [ADR 004](adr-004-deadline-manager-tracing-decorator.md) (tracing), [ADR 005](adr-005-deadline-stored-job-format.md) (stored job format)

## Context

Users construct the four deadline managers through a builder, in their application or Spring configuration, or let
Axon Framework 4 build them (the `DefaultConfigurer`'s `SimpleDeadlineManager`, Spring Boot's JobRunr and db-scheduler
auto-configuration). In Axon Framework 4.13 those builders take:

| Manager | Builder parameters |
|---|---|
| `SimpleDeadlineManager` | `scopeAwareProvider`, `scheduledExecutorService`, `transactionManager`, `spanFactory` |
| `QuartzDeadlineManager` | `scheduler`, `scopeAwareProvider`, `transactionManager`, `serializer`, `spanFactory`, `refireImmediatelyPolicy` |
| `JobRunrDeadlineManager` | `jobScheduler`, `scopeAwareProvider`, `serializer`, `transactionManager`, `spanFactory` |
| `DbSchedulerDeadlineManager` | `scheduler`, `scopeAwareProvider`, `serializer`, `transactionManager`, `spanFactory`, `useBinaryPojo`, `startScheduler`, `stopScheduler` |

`transactionManager` is optional everywhere and defaults to a `NoTransactionManager`. `serializer` is a hard
requirement for JobRunr and db-scheduler. Quartz defaults it to an `XStreamSerializer`.

Three of these parameters have no Axon Framework 5 counterpart to pass through:

- **`transactionManager`**: an Axon Framework 4 backend started a `DefaultUnitOfWork` for each fired deadline and
  attached the transaction manager to it. That unit of work does not exist in Axon Framework 5. A unit of work now
  comes from a `UnitOfWorkFactory`. The configuration registers a default one, a `TransactionalUnitOfWorkFactory`
  around a `SimpleUnitOfWorkFactory` that is backed by the configuration's `ApplicationContext`
  (`MessagingConfigurationDefaults`). The `ProcessingContext` of such a unit of work resolves components, and the
  firing path needs that: a Saga `@DeadlineHandler` with a `CommandDispatcher` parameter gets it from
  `CommandDispatcher.forContext(...)`, which looks up `context.component(CommandGateway.class)`.
- **`serializer`**: Axon Framework 5 has no `Serializer`. It converts with a `Converter`, and dropped XStream support.
- **`spanFactory`**: removed by ADR 004; tracing comes from the decorator of
  [#5111](https://github.com/AxonIQ/AxonFramework/issues/5111).

The stash versions of the backends also carry a `messageNameResolver(...)` builder method. It was added during Axon
Framework 5 development and never released, so no Axon Framework 4 code calls it.
`AbstractDeadlineManager` resolves the type of a bare payload with a fixed `ClassBasedMessageTypeResolver`. The type
has no effect, as storage and delivery use only the payload class.

## Options considered

For the unit of work:

**Option A: keep `transactionManager(...)` and build the factory inside the backend.** The backend would wrap the
given transaction manager in a `TransactionalUnitOfWorkFactory` around a `SimpleUnitOfWorkFactory`. Axon Framework 4
builder calls keep compiling. But the backend has no `ApplicationContext` of the application, only
`EmptyApplicationContext`. So:

- `context.component(...)` fails during firing, as soon as a Saga deadline handler resolves a parameter from it,
  such as a `CommandDispatcher`;
- the deadline's unit of work ignores what the application configured for every other unit of work, such as a
  customized or timeout-decorated `UnitOfWorkFactory`.

These failures surface when a deadline fires, possibly long after startup.

**Option B: Option A as the default, plus an optional `unitOfWorkFactory(...)` that overrides it.** Existing calls
keep compiling, and a correct setup is possible. It offers two ways to configure the same thing. The one that
existing code ends up with is Option A's, with the same late failure.

**Option C: a required `unitOfWorkFactory(...)`, and no `transactionManager(...)` (chosen).** The application passes
the factory it configured, so a fired deadline runs in the same kind of unit of work as everything else. A missing
factory fails at build time.

For conversion:

**Option D: an optional `converter(...)` that defaults to a `JacksonConverter`.** This would replace Quartz's
`XStreamSerializer` default with a different format without the application choosing it. Jobs written by the old
default are not readable through it anyway.

**Option E: a required `converter(...)` for the three persistent backends (chosen).** This matches how the legacy saga
stores (`JpaSagaStore`, `JdbcSagaStore`) replaced their `serializer(...)`. Which converter reads the stored jobs stays
an explicit choice of the application. `SimpleDeadlineManager` keeps its deadlines in memory and needs none.

## Decision

Options C and E.

- Every backend builder takes a required `unitOfWorkFactory(UnitOfWorkFactory)`. `transactionManager(...)` is removed.
  Transactions come from the factory, typically the configured `TransactionalUnitOfWorkFactory`.
- The Quartz, JobRunr and db-scheduler builders take a required `converter(Converter)` in place of
  `serializer(Serializer)`. Axon Framework 4 configured three serializers (general, message, event), and Axon
  Framework 5 keeps those levels as the `GeneralConverter`, `MessageConverter` and `EventConverter`, all of them
  `Converter`s. The builder takes any of them. The application passes the counterpart of the serializer its Axon
  Framework 4 manager used, so stored jobs keep reading (ADR 005). For JobRunr and db-scheduler managers that Axon
  Framework 4's Spring Boot auto-configuration built, that is the `EventConverter`. A single converter per manager
  mirrors Axon Framework 4, where one serializer handled payload, metadata and scope. The builder deliberately takes
  `Converter`, not `EventConverter`, as the legacy saga store builders do: Axon Framework 4's builder took any
  `Serializer`, and the backends only call `Converter.convert(...)`. `axoniq-legacy` ports Axon Framework 4's Spring
  Boot auto-configuration of the JobRunr and db-scheduler managers, and it wires the `EventConverter`, as Axon
  Framework 4's wired the `eventSerializer`, together with the configured `UnitOfWorkFactory`.
- `messageNameResolver(...)` is not ported. A fired deadline reaches its handler by deadline name and payload class
  (`ScopeAware.send(...)` → `AnnotatedSaga.handle(...)`), never by message type, so a configurable message name would
  have no effect on delivery. It would also not fit the stored job layout, whose payload type field holds the class
  name the payload is deserialized into (ADR 005):
  - stored in an extra field, it would add to the Axon Framework 4 layout something that nothing reads when the
    deadline fires;
  - stored in place of the class name, it would have to be resolved back to a class, or converted by name on the
    Saga path, and Axon Framework 4 nodes could no longer read those jobs.

  A deadline's message type is neither stored nor used for delivery, so a resolver would change nothing. Making it
  configurable later is an additive change.
- `spanFactory(...)` is removed (ADR 004).
- All other parameters keep their Axon Framework 4 names, types and defaults: `scopeAwareProvider`,
  `scheduledExecutorService`, `scheduler`, `jobScheduler`, `refireImmediatelyPolicy`, `useBinaryPojo`,
  `startScheduler` and `stopScheduler`.
- `validate()` checks each required parameter with `assertNonNull(...)`, as in Axon Framework 4 and the legacy saga
  stores, and fails with an `AxonConfigurationException` naming the first missing one.

The unit-of-work factory is runtime wiring, not stored data. Quartz hands it to `DeadlineJob` through the scheduler
context, as it did with the transaction manager, so stored jobs are unaffected.

The factory covers firing only. Scheduling happens before any firing unit of work exists: the dispatch interceptors
run inside the call passed to `runOnPrepareCommitOrNow(...)`, before the job is stored, and
`AbstractDeadlineManager.processDispatchInterceptors(...)` passed them `null`. With this decision, they get the
context that exists:

- a call deferred inside a Saga invocation passes the Saga's `ProcessingContext`, the one it was deferred to, to the
  dispatch interceptors;
- a call that runs immediately, outside a Saga invocation, passes `null`, as there is no context, like Axon Framework
  4 outside a unit of work.

`AbstractDeadlineManager` replaces its two Axon Framework 4 methods for this:
`runOnPrepareCommitOrNow(Consumer<ProcessingContext>)`, which hands the call the context it was deferred to, or `null`
when it runs immediately, replaces `runOnPrepareCommitOrNow(Runnable)`, and
`processDispatchInterceptors(DeadlineMessage, ProcessingContext)`, which passes that context to the interceptor chain,
replaces `processDispatchInterceptors(DeadlineMessage)`. The deferred call cannot look the context up itself, because
the Saga's scope is no longer current once `RUN_DEADLINE_CALLS` runs.

The Axon Framework 4 variants are removed, not kept for custom subclasses. Kept, they would hand the interceptors
`null` even inside a Saga invocation, the context this decision gives them. No subclass outside the four backends is
known, and adapting one is mechanical (see Consequences).

Two alternatives were rejected. Documenting `null` as the contract hides a context that exists. A unit of work from
the factory around an immediate schedule call would give the interceptors a context in both cases, but it would wrap
storing the job in a unit of work, and with a `TransactionalUnitOfWorkFactory` in a transaction, that Axon Framework
4 did not have.

## Consequences

- Every Axon Framework 4 builder call of a deadline manager needs changes: it adds `unitOfWorkFactory(...)`, swaps
  `serializer(...)` for `converter(...)`, and drops `transactionManager(...)` and `spanFactory(...)`. The migration
  guide documents this, with an example that takes the factory from the configuration
  (`configuration.getComponent(UnitOfWorkFactory.class)`) or from Spring.
- [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) can automate only part of it:
  - removing `spanFactory(...)` and `transactionManager(...)` calls;
  - replacing a known serializer such as `JacksonSerializer.defaultSerializer()` with its converter.

  Neither the factory nor an arbitrary serializer's converter can be inferred at the call site, so the recipe leaves a
  marker for those.
- A custom `AbstractDeadlineManager` subclass written against Axon Framework 4 no longer compiles. It takes the
  context in its deferred calls (`context -> ...` instead of `() -> ...`) and passes it to
  `processDispatchInterceptors(message, context)`.
- An application that relied on Quartz's `XStreamSerializer` default has to pick a converter, and cannot read the jobs
  that default wrote (ADR 005).
- A deadline manager built without a configuration, as in a plain unit test, passes a factory built by hand, for
  example a `SimpleUnitOfWorkFactory` over `EmptyApplicationContext`. It then has the same limits as Option A: no
  component lookups during firing.
- The fired deadline runs inside whatever the configured factory provides: transactions, timeouts, and a
  `ProcessingContext` that resolves components. Aggregate deadlines dispatched as commands
  ([#5004](https://github.com/AxonIQ/AxonFramework/issues/5004)) can rely on the same context (see the amendment
  below).

## Amendment to ADR 001, ADR 003 and ADR 004

This decision evolves what ADR 001, ADR 003 and ADR 004 say about the `ProcessingContext` of a deadline, when it is
scheduled and when it fires. All three remain accepted otherwise.

- ADR 001 dispatches the command for a fired aggregate deadline with no active `ProcessingContext` (`null`), because
  the trigger comes from a scheduler thread. With this decision every backend fires a deadline, for every scope
  descriptor, inside a unit of work from the configured `UnitOfWorkFactory`. The command is dispatched with that
  unit of work's `ProcessingContext` instead of `null`. It then runs within the transaction the factory provides,
  and the firing span of ADR 004 becomes the parent of the command's dispatch. ADR 001's decision to translate the
  deadline into a command is unchanged.
- ADR 004 lets the firing span nest the dispatched command under it, through the unit of work that each backend
  creates for a deadline. This decision fixes where that unit of work comes from: the configured
  `UnitOfWorkFactory`, not a factory the backend builds itself. ADR 004's tracing decision is unchanged.
- ADR 003 lets the backends call `runOnPrepareCommitOrNow(...)` and `processDispatchInterceptors(...)` as in Axon
  Framework 4. They now call the methods above, which hand the deferring Saga's `ProcessingContext` to the
  dispatch interceptors, and the Axon Framework 4 variants are removed. ADR 003's deferral decision is unchanged.
