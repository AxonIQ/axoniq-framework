# ADR 004: Scheduling an event takes effect immediately, as in Axon Framework 4 (issue [#701](https://github.com/AxonIQ/axoniq-framework/issues/701))

Date: 2026-10-23
Status: accepted
Related: [#3065 ADR 003](../3065-legacy-deadlines/adr-003-deadline-manager-current-scope.md) (deadline calls deferred within a Saga), [#3065 ADR 011](../3065-legacy-deadlines/adr-011-deadline-manager-parameter-bound-to-the-handler-context.md) (deadline manager parameter bound to the handler context)

## Context

The deadline managers defer their calls made within a unit of work to its commit (#3065 ADR 003 and ADR 011), as Axon
Framework 4's `AbstractDeadlineManager` did, so a rolled-back handler leaves no deadline behind. Axon Framework 4's
event schedulers never did that: no scheduler looked at `CurrentUnitOfWork` or registered `onCommit` callbacks, and
none added dispatch interceptors or correlation data. An event scheduled from a handler whose unit of work rolled back
stayed scheduled.

## Options considered

**Option A: defer scheduling to the commit of the handler's `ProcessingContext`, as for deadlines.** Changes
behaviour applications built on, and needs the handler's context, which an `EventScheduler` reached outside a handler
parameter does not have.

**Option B: keep Axon Framework 4's behaviour (chosen).**

## Decision

Option B. `schedule`, `reschedule` and `cancelSchedule` act when called. The schedulers add no dispatch interceptors,
correlation data or tracing of their own. When an event fires, it is published in a fresh unit of work (ADR 002)
without correlation metadata, as Axon Framework 4's fresh `DefaultUnitOfWork` had no message to correlate with.

## Consequences

- An event scheduled from a handler that rolls back stays scheduled, as in Axon Framework 4. The `EventScheduler`
  Javadoc, the migration path and the reference documentation state it.
- The event schedulers stay usable outside a handler, with no context to bind to.
