# ADR 005: The Axon Server event scheduler is ported, to cancel and reschedule Axon Framework 4 schedules (issue [#701](https://github.com/AxonIQ/axoniq-framework/issues/701))

Date: 2026-10-23
Status: accepted
Related: [ADR 001](adr-001-event-scheduler-packages-and-stored-format.md) (stored format), [ADR 002](adr-002-event-scheduler-builders-on-af5.md) (builders)

## Context

With Axon Server, Axon Framework 4's Spring Boot auto-configuration made the `AxonServerEventScheduler` the
application's `EventScheduler`. It schedules through Axon Server's gRPC `EventScheduler` service (`ScheduleEvent`,
`RescheduleEvent`, `CancelScheduledEvent`): the event, with its payload serialized by the event serializer, is stored
on the server, which appends it to the context's event store when it is due. The client returns a
`SimpleScheduleToken` holding the server's token id, and Sagas keep that token.

Firing happens on the server, so Axon Framework 4 schedules keep firing without any client. Cancelling and
rescheduling them, however, needs a client: one `CancelScheduledEvent` call with the token id, which the server applies
through Raft, and which is a no-op for an event that already fired. The token is not bound to the client that
scheduled it. The server-side scheduler only serves aggregate-based contexts; DCB contexts have their own.

## Options considered

**Option A: do not port it, firing needs no client.** Sagas could no longer cancel or reschedule the events they
scheduled on Axon Framework 4.

**Option B: port it into `axoniq-legacy` in its Axon Framework 4 package, on this repository's Axon Server connector
(chosen).**

## Decision

Option B.

- `AxonServerEventScheduler` keeps its package `org.axonframework.axonserver.connector.event.axon`, which no Axon
  Framework 5 jar uses, and returns `SimpleScheduleToken`s (ADR 001).
- `axoniq-legacy` takes an optional dependency on `io.axoniq.framework:axon-server-connector`, and uses its Axon
  Framework 5 connection management and metadata conversion.
- The builder takes a required `converter(Converter)` in place of `eventSerializer(...)`, without the XStream default
  (ADR 002), plus `requestTimeout` and `defaultContext`. It takes no `unitOfWorkFactory` and no `eventSink`.
- `schedule` and `reschedule` stay usable but deprecated like on every scheduler; `reschedule` with a `Duration` stays
  one atomic `RescheduleEvent` call. `cancelSchedule` is not deprecated.
- Spring Boot registers it as the `EventScheduler` when Axon Server is the event store, winning over JobRunr and
  db-scheduler, as in Axon Framework 4. It starts and stops dispatching in the `OUTBOUND_EVENT_CONNECTORS` phase.
  It keeps the interface's no-op `shutdown()`, so the lifecycle enhancer's call in `INBOUND_EVENT_CONNECTORS` leaves
  it dispatching until the event processors, which stop in the same phase, have stopped; dispatching stops later, in
  `OUTBOUND_EVENT_CONNECTORS`.
- DCB contexts are not supported; scheduling events on them is a separate feature.

## Consequences

- Sagas migrated from Axon Framework 4 cancel and reschedule their Axon Server schedules from Axon Framework 5 nodes,
  against the aggregate-based context those schedules live in.
- Events Axon Server fires are appended to the event store like any Axon Framework 4 event, so Axon Framework 5 reads
  them as such, with the event transformation for formats it cannot read as is.
- `axoniq-legacy` depends optionally on the Axon Server connector.
