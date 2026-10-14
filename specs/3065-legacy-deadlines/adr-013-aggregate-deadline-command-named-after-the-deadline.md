# ADR 013: Name an aggregate deadline's command after the deadline, falling back to the payload type

Date: 2026-10-14
Status: accepted
Related: [#3065](https://github.com/AxonIQ/AxonFramework/issues/3065) (parent), [ADR 001](adr-001-aggregate-deadline-command-translation.md) (aggregate deadlines as commands), [ADR 007](adr-007-aggregate-deadline-delivery-through-scope-aware.md) (delivery through `ScopeAware`), [ADR 009](adr-009-aggregate-deadline-entity-id-via-metadata.md) (entity identifier via metadata), [#5140](https://github.com/AxonIQ/AxonFramework/pull/5140) (OpenRewrite recipe)

## Context

ADR 007 has the `AggregateDeadlineCommandTranslator` dispatch a fired aggregate deadline's payload with
`CommandGateway.send(payload, metadata, context)`. The command is therefore named after the payload's type. Only a
deadline without a payload is named after the deadline, because a `null` payload has no type.

Axon Framework 4 told aggregate deadlines apart by their name: `@DeadlineHandler(deadlineName = ...)`. Their payload
types need not be unique. Migrating a real application showed an aggregate with two deadlines that both carry a
`UUID`. Both became the same `java.util.UUID` command, so the entity cannot have a handler for each, and one
`@CommandHandler void handle(UUID payload)` would receive both. The #5140 recipe migrates such handlers to that shape.

5.4.0 shipped with the payload-type naming, so applications may already have migrated handlers named after the
payload type, and AF4 deadlines may still be waiting to fire against them.

## Options considered

**Option A: always name the command after the deadline.** Simple, and fixes the collision. A handler migrated for
5.4.0 no longer matches: its deadlines fail with a `NoHandlerForCommandException`, and the deadline manager retries
them until the handler is renamed. A patch release would break working applications to fix ones that cannot work.

**Option B: a setting choosing the naming.** For example `payload-type` by default in 5.4.x and `deadline-name` from
5.5.0. Deterministic, without a second dispatch. Applications with the collision must find the setting first, and
the choice applies to the whole application, so handlers cannot be migrated one at a time.

**Option C: name the command after the deadline, and fall back to the payload type when no handler exists for the
deadline name (chosen).** Handlers migrated for 5.4.0 keep working unchanged. New migrations, the recipe and the docs
use the deadline name. An application with the collision has no working payload-type setup to keep, so it loses
nothing.

## Decision

Option C.

- `send(...)` dispatches a `GenericCommandMessage` with `new MessageType(deadlineName)`, the deadline's payload and its
  metadata (plus the scope's identifier, ADR 009). A deadline without a payload keeps carrying its name as payload.
- When that dispatch fails with a `NoHandlerForCommandException`, a deadline with a payload is dispatched again with
  `CommandGateway.send(payload, metadata, context)` and a warning that names the deadline and the payload type, and asks
  to rename the handler to `@CommandHandler(commandName = "<deadlineName>")` is logged.
- A payload-less deadline, and any other failure, is rethrown without a fallback, so the deadline manager handles it
  as before.
- The migration guide and the #5140 recipe migrate a `@DeadlineHandler(deadlineName = "x")` with a payload to
  `@CommandHandler(commandName = "x")`.

ADR 001 and ADR 007 remain accepted: a deadline still becomes a command, delivered through `ScopeAware`. This ADR only
changes the name the command is dispatched under.

## Consequences

- Deadlines that share a payload type reach separate handlers.
- A deadline whose handler is still named after its payload type is dispatched twice. The first dispatch fails fast
  with no handler, on a local bus and on both distributed buses: the Axon Server connector maps `AXONIQ-4000`, and the
  Spring Cloud connector maps its no-handler error, to `NoHandlerForCommandException`.
- During a rolling deploy in which no node has registered the handler yet, both dispatches fail, and the deadline
  manager retries the deadline as any other failure. Nothing is lost.
- A `NoHandlerForCommandException` that a deadline-name handler throws itself, for example from a command it
  dispatches, also triggers the fallback. The fallback then usually finds no handler either and its failure is
  rethrown, so the deadline still fails, but the warning is misleading in that case.
- The fallback and its warning can be removed in a later minor release, once applications have renamed their
  handlers.
