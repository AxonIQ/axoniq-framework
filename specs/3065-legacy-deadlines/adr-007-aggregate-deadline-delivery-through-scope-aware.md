# ADR 007: Deliver aggregate deadlines through `ScopeAware`, with the command translator as the target (issues [#5004](https://github.com/AxonIQ/AxonFramework/issues/5004), [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005))

Date: 2026-10-06
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [#564](https://github.com/AxonIQ/axoniq-framework/issues/564) (Saga deadline delivery), [#565](https://github.com/AxonIQ/axoniq-framework/issues/565) (Saga auto-discovery into the provider), [ADR 001](adr-001-aggregate-deadline-command-translation.md) (aggregate deadlines as commands), [ADR 002](adr-002-saga-deadline-scope-aware-delivery.md) (Saga deadline delivery), [ADR 005](adr-005-deadline-stored-job-format.md) (stored job format), [ADR 006](adr-006-deadline-manager-builders-on-af5.md) (builders)

## Context

ADR 001 decides that a fired deadline with an `AggregateScopeDescriptor` is translated into a command and dispatched
through the `CommandGateway`. It reads the descriptor only to recognize the aggregate target, "not to resolve or
invoke anything through `ScopeAware`". ADR 002 keeps Axon Framework 4's model for Sagas: the backend asks its
`ScopeAwareProvider` for the `ScopeAware` components and sends the deadline to each one that `canResolve(...)` the
descriptor.

#5005 therefore plans a branch in every backend: an `AggregateScopeDescriptor` goes to the #5004 translator, anything
else to the `ScopeAwareProvider`. Every backend in the stash fires through the same code, which the branch would
replace:

```java
scopeAwareProvider.provideScopeAwareStream(deadlineScope)
                  .filter(scopeAwareComponent -> scopeAwareComponent.canResolve(deadlineScope))
                  .forEach(scopeAwareComponent -> scopeAwareComponent.send(deadlineMessage, context, deadlineScope));
```

(`SimpleDeadlineManager`, Quartz's `DeadlineJob`, `JobRunrDeadlineManager`, `DbSchedulerDeadlineManager`; a failure
of `send(...)` is rethrown as an `ExecutionException`.)

In Axon Framework 4, an aggregate was reached through that same path: each aggregate `Repository` was a `ScopeAware`
for its own aggregate type. `ScopeAware.send(Message, ProcessingContext, ScopeDescriptor)` also already receives the
`ProcessingContext` of the unit of work in which the deadline fires, which ADR 006 has the command dispatched with.

## Options considered

**Option A: a branch on the descriptor type in every backend (as planned).** The backends call the translator for an
`AggregateScopeDescriptor` and the `ScopeAwareProvider` otherwise. The translator stays an internal component. Every
backend diverges from its Axon Framework 4 firing code, and four copies of the branch need their own tests. Each
backend builder also needs the translator, or a `CommandGateway` to build one, as an additional parameter.

**Option B: the translator is a `ScopeAware` (chosen).** It resolves every `AggregateScopeDescriptor`, and its
`send(...)` dispatches the command. It is one more element of the provider that ADR 002 builds from an explicit
list. The backends keep their Axon Framework 4 firing code, and their builders keep their parameters.

For where the translator gets its `CommandGateway`:

**Option C: from the `ProcessingContext` passed to `send(...)`** (`context.component(CommandGateway.class)`). The
translator needs no constructor argument, but a missing gateway is detected only when an aggregate deadline fires,
possibly long after startup, and only a unit of work that resolves components can provide one (ADR 006).

**Option D: a constructor argument (chosen).** A missing gateway fails when the provider is built.

## Decision

Options B and D.

- The #5004 translator, `AggregateDeadlineCommandTranslator` in `org.axonframework.deadline`, implements `ScopeAware`
  and takes a `CommandGateway` in its constructor:
  - `canResolve(...)` is true for every `AggregateScopeDescriptor`, whatever its aggregate type, and false otherwise;
  - `send(...)` dispatches the deadline's payload and metadata as a command with
    `CommandGateway.send(payload, metadata, context)`, passing the context of the firing unit of work (ADR 006), and
    waits for the result with `FutureUtils.joinAndUnwrap(...)`, as `AbstractSagaManager.send(...)` does. A failed
    command is rethrown, so the backend applies its Axon Framework 4 failure handling (ADR 005);
  - an `UnknownDeadlinePayload`, the payload of a deadline whose stored type name could not be resolved (ADR 005), is
    not dispatched. `send(...)` logs a warning naming the deadline, the stored type name and the aggregate scope, and
    returns.
- The application adds the translator to the explicit list of ADR 002, next to its Saga managers:
  `new LegacyScopeAwareProvider(new AggregateDeadlineCommandTranslator(commandGateway), sagaManager1, ...)`. The
  provider rejects a list with more than one translator, because each would dispatch the same command.
- The four backends port their Axon Framework 4 firing code unchanged. They do not distinguish descriptor types, and
  their builders take no translator or `CommandGateway`.

This keeps ADR 001's decision: an aggregate deadline becomes a command, with no `Repository` and no `EntityMetamodel`
involvement. Only its statement that nothing is resolved or invoked through `ScopeAware` changes: `ScopeAware` is the
interface through which the backend reaches the translator, not a way to load the aggregate. ADR 002 is unchanged; its
provider gains one more kind of element.

## Consequences

- An application with aggregate deadlines has to add the translator to its provider. Without it, nothing resolves an
  `AggregateScopeDescriptor`, and the backend completes the job without delivering it, as Axon Framework 4 did for an
  aggregate type without a repository. The migration guide shows the provider with the translator, and
  [#5048](https://github.com/AxonIQ/AxonFramework/issues/5048) cannot infer the `CommandGateway` at the call site any
  more than the Saga managers.
- The command is dispatched and awaited inside the deadline's unit of work. `joinAndUnwrap(...)` bounds the wait (30
  seconds by default). A command that takes longer fails the deadline, which the backend then handles as any failure.
- Spring Boot auto-configuration builds the provider from the translator and the Saga managers. The aggregate side
  needs only the `CommandGateway` bean. The Saga side still depends on Saga auto-discovery
  ([#565](https://github.com/AxonIQ/axoniq-framework/issues/565)).
- Tests split along the interface: the translator is tested as a `ScopeAware` against a recording `CommandGateway`
  and a real `@CommandHandler` (#5004); each backend's firing path is tested once against a stub `ScopeAware` (#5005).
  No backend test needs an aggregate case of its own.
- The issue bodies of [#5004](https://github.com/AxonIQ/AxonFramework/issues/5004) (no `ScopeAware` lookup,
  `null` context), [#5005](https://github.com/AxonIQ/AxonFramework/issues/5005) (the descriptor branch) and
  [#564](https://github.com/AxonIQ/axoniq-framework/issues/564) (the provider's elements) are updated to match.
