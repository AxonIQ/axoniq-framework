# ADR 001: Event schedulers keep Axon Framework 4's packages for stored types and its stored format (issue [#701](https://github.com/AxonIQ/axoniq-framework/issues/701))

Date: 2026-10-23
Status: accepted
Related: [#624](https://github.com/AxonIQ/axoniq-framework/issues/624) (db-scheduler events on Axon Framework 5 nodes), [#3065 ADR 005](../3065-legacy-deadlines/adr-005-deadline-stored-job-format.md) (deadline stored job format), [#3065 ADR 012](../3065-legacy-deadlines/adr-012-deadline-type-names-resolve-through-the-xstream-mapper.md) (type names through the XStream mapper), [ADR 002](adr-002-event-scheduler-builders-on-af5.md) (builders)

## Context

Events scheduled with an Axon Framework 4 `EventScheduler` can be due days or months later. When an application moves
to Axon Framework 5, its Quartz, JobRunr and db-scheduler stores hold events that Axon Framework 4 scheduled, and its
Sagas hold the `ScheduleToken`s to cancel them. During a rolling upgrade, nodes of both versions run on the same store
and fire, cancel and reschedule each other's jobs.

Three kinds of names and layouts are persisted:

- the tokens in Saga state, through Java serialization, Jackson (`@class` property of `ScheduleToken`'s
  `@JsonTypeInfo`) or XStream (class name as element name): `SimpleScheduleToken` (also returned by the Axon Server
  scheduler), `QuartzScheduleToken`, `JobRunrScheduleToken`, `DbSchedulerScheduleToken`, with their
  `serialVersionUID`s;
- the jobs: Quartz's `FireEventJob` class name and its `JobDataMap` keys, JobRunr's job target
  `JobRunrEventScheduler#publish`/`#publishWithRevision` with their parameter lists, db-scheduler's task name and its
  `DbSchedulerBinaryEventData`/`DbSchedulerHumanReadableEventData` classes, which declare no `serialVersionUID`;
- the payload and metadata inside those jobs, written by the configured Axon Framework 4 `Serializer`.

An archived Axon Framework 5 draft of the schedulers moved them to `org.axonframework.messaging.eventhandling.scheduling`
and forked from Axon Framework 4 before 4.13.

## Options considered

**Option A: Axon Framework 5 packages and a new stored format, with a migration step.** Every stored token and job
would need converting before Axon Framework 5 nodes join, and nodes of both versions could not share a store.

**Option B: Axon Framework 4 packages for everything that is called by users or stored, Axon Framework 5 conventions
for the rest, and the 4.13 stored format exactly (chosen).** This mirrors #3065 ADR 005 for the deadline managers.

## Decision

Option B.

- Name, package and structure stay as in Axon Framework 4 for the user-facing API to schedule and cancel events
  (`EventScheduler`, `ScheduleToken`, `SchedulingException`), and for every stored type: the four tokens, `FireEventJob`,
  the JobRunr job target signatures, the db-scheduler task name and data classes. Their packages are
  `org.axonframework.eventhandling.scheduling[.java|.quartz|.jobrunr|.dbscheduler]`, which no Axon Framework 5 jar
  uses.
- Builders, `EventJobDataBinder`, suppliers and helpers follow Axon Framework 5 conventions and may change. The
  test support (`StubEventScheduler`, `ScheduledItem`, `EventSchedulerValidator`) stays in
  `org.axonframework.test.eventscheduler`, as user tests import it.
- The stored format is Axon Framework 4.13.2's, per backend. Tokens pin their Axon Framework 4 `serialVersionUID`s
  and Jackson annotations, and `ScheduleToken` extends `Serializable` again. The db-scheduler data classes pin the
  `serialVersionUID`s the Axon Framework 4 classes computed (binary `5490561342395105748L`, human-readable
  `5479686302073166009L`). Quartz keys are kept, with the timestamp written as an ISO string. Metadata is written as a
  `Map<String, String>` and read untyped. `String` and `byte[]` payloads are written as Axon Framework 4 wrote them,
  and stored type names resolve through the XStream mapper as in #3065 ADR 012.
- The ported code is diffed against Axon Framework 4.13.2, not the archived draft, and takes the later Axon Framework
  4 changes in.
- Quartz's layout from before Axon Framework 3.4 (the whole `EventMessage` in the `JobDataMap`) is not supported. Its
  Axon Framework 4 classes are not on the Axon Framework 5 class path, so Quartz fails loading such a job before any
  Axon code runs, and sets its trigger to `ERROR`.
- The payload and metadata conversion is shared with the deadline managers through an internal
  `StoredMessageConverter`, extracted from `StoredDeadlineConverter`; the deadline managers keep their behaviour.

## Consequences

- Axon Framework 4 and Axon Framework 5 nodes fire, cancel and reschedule each other's events, and Sagas migrated with
  their state keep cancelling with the tokens they hold. Compatibility tests per backend run Axon Framework 4.13.2's
  classes in an isolated class loader, in both directions.
- The scheduler packages are Axon Framework 4 names inside an Axon Framework 5 module; a later move would break stored
  data, so they stay for the life of `axoniq-legacy`.
- Applications with Quartz jobs from before Axon Framework 3.4 reschedule them on Axon Framework 4 before Axon
  Framework 5 nodes join; the migration path says so.
