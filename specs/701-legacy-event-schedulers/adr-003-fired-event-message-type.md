# ADR 003: A fired event gets its message type from the configured `MessageTypeResolver` (issue [#701](https://github.com/AxonIQ/axoniq-framework/issues/701))

Date: 2026-10-23
Status: accepted
Related: [#3065 ADR 005](../3065-legacy-deadlines/adr-005-deadline-stored-job-format.md) (deadline stored job format), [#3065 ADR 006](../3065-legacy-deadlines/adr-006-deadline-manager-builders-on-af5.md) (deadline manager builders), [ADR 001](adr-001-event-scheduler-packages-and-stored-format.md) (stored format), [ADR 002](adr-002-event-scheduler-builders-on-af5.md) (builders)

## Context

Axon Framework 5 messages carry a `MessageType`, and event handlers subscribe by its `QualifiedName`. Axon Framework 4
had no message type, and its stored jobs (ADR 001) hold only the payload's class name and revision.

For deadlines, #3065 ADR 005 derives the type from the payload class, and #3065 ADR 006 does not port a
`messageNameResolver`, because a fired deadline is delivered straight to its Saga by deadline name and payload class.
A fired event is different: it is published and stored, and handlers find it by its type. The configuration's default
`MessageTypeResolver` is an `AnnotationMessageTypeResolver`, which takes the name and version of an `@Event` annotation
on the payload class and falls back to the class name; an application may also register a resolver of its own. Handlers
of an event class with an `@Event` name, or of a type a custom resolver gives, would not be invoked if the fired event
got a type derived from the class name instead.

The payload class named in a stored job may also be missing from an Axon Framework 5 node, for example after a rename.
For deadlines, #3065 ADR 005 delivers an `UnknownDeadlinePayload`, so the Saga can handle it.

### Firing path of an event Axon Framework 4 scheduled

Where the type is resolved and where an unknown class fails follows from the path a stored event takes when it fires
on an Axon Framework 5 node, with Quartz, JobRunr or db-scheduler:

1. **Scheduled on Axon Framework 4.** The Axon Framework 4 serializer wrote the payload and metadata into the
   backend's store, next to the payload's class name and revision, in the layout of ADR 001: a db-scheduler
   `AxonScheduledEvent` task with `DbSchedulerBinaryEventData` or `DbSchedulerHumanReadableEventData`, a Quartz
   `FireEventJob` with its `JobDataMap` keys, or a JobRunr call of `JobRunrEventScheduler.publish(...)` with `String`
   arguments. The Saga holds the returned token.
2. **Picked up by an Axon Framework 5 node.** Any node sharing the store picks the job up when it is due:
   db-scheduler runs the `AxonScheduledEvent` task the node registered, Quartz instantiates `FireEventJob` by its
   stored class name with the scheduler context holding the node's components, and JobRunr invokes `publish(...)` on
   the `JobRunrEventScheduler` its `JobActivator` provides.
3. **Payload and metadata are read.** The stored class name is loaded as a class. Only with an `XStreamConverter`
   is it first resolved through that converter's XStream mapper, as Axon Framework 4's `XStreamSerializer` may have
   stored an alias instead of the class name (#3065 ADR 012); Axon Framework 4's Jackson serializers always stored the
   class name. The configured `Converter` converts the payload into that class. Metadata is
   read untyped and its values become `String`s (ADR 001). The revision is read but not used, as in Axon Framework
   4: its serializers picked the class by type name only, and the schedulers ran no upcasters, the only users of a
   revision. Quartz's stored identifier and timestamp are discarded, as in Axon Framework 4.
4. **The event message is built.** The firing node's `MessageTypeResolver` resolves the `MessageType` from the
   payload, as the job stores no type. The message is new: a new identifier and the firing time as timestamp, with the
   stored payload and metadata.
5. **The event is published** on the configured `EventSink`, in a unit of work from the configured
   `UnitOfWorkFactory` (ADR 002), so it is stored in the event store and passes the dispatch interceptors configured
   on it.
   On success the backend completes the job; a failure reaches the backend, which retries.
6. **The event is handled.** Subscribing processors receive it from the live publication; streaming processors read
   it from the event store, where event transformation (`TransformingEventStore`) applies on reading. The Saga's
   token is spent; cancelling it is a no-op.

The payload class is thus needed in step 3, before the event exists in the event store, while event transformation
only acts in step 6. The Axon Server scheduler differs: steps 2 to 5 run on Axon Server, which appends the stored
event to the context's event store when it is due, with the payload type and revision Axon Framework 4 serialized it
with (ADR 005). No Axon Framework 5 code takes part in firing, and the event is read like any other stored Axon
Framework 4 event.

## Options considered

**Option A: derive the type from the payload class only, as for deadlines.** Diverges from what the `EventGateway`
publishes (using the resolver) for the same payload when the application configured another resolver.

**Option B: resolve it like the `EventGateway` does (chosen).** A scheduled `EventMessage` keeps its type; a plain
payload gets the type from the configured `MessageTypeResolver`.

For an unknown payload class:

**Option C: publish a placeholder payload.** It would be stored in the event store, where it stays for good, and no
handler is subscribed to it.

**Option D: fail the firing and let the backend retry (chosen).**

## Decision

Options B and D.

- Each scheduler that fires locally takes a `messageTypeResolver(MessageTypeResolver)`, defaulting to an
  `AnnotationMessageTypeResolver` as the configuration does, so `@Event` names apply to a scheduler built by hand;
  Spring Boot and the configuration wire the configuration's resolver. The in-memory
  `SimpleEventScheduler` keeps the type of a scheduled `EventMessage`; the persistent backends store no type, so they
  resolve it from the payload when the event fires, on the firing node.
- A stored payload class that cannot be loaded fails the firing, and the backend's own failure handling retries:
  Quartz's refire policy, JobRunr's `RetryFilter`, db-scheduler's retry every five minutes. Nothing unknown is
  published. There is no `UnknownDeadlinePayload` counterpart.
- The test fixture's `StubEventScheduler` resolves a scheduled payload through the configuration's resolver as well.

## Consequences

- A fired event reaches the same handlers as the same event published through the `EventGateway`.
- With Quartz, JobRunr or db-scheduler, an application that renames or removes a scheduled payload class keeps the old
  class until its scheduled events have fired; otherwise the events keep being retried. Event transformation does not
  help here, as it only applies to events read from the event store. During a rolling upgrade, payload classes of
  scheduled events stay unchanged on both versions: Axon Framework 4 nodes share the store and pick up jobs as well,
  so an event an Axon Framework 5 node schedules or reschedules with a new or renamed class fails on every Axon
  Framework 4 node that picks it up. Only once no Axon Framework 4 node shares the store can an application cancel
  its scheduled events and schedule them again with the new class. The migration path documents this.
- With Axon Server, a fired event is an ordinary stored event, so event transformation covers it, and the old class
  can go once a transformation for it is in place.
- The resolver is runtime wiring, not stored data, so changing it later changes the type of events still to fire.
