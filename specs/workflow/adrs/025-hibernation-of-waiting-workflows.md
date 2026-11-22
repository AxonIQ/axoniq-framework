# ADR-025: Hibernation of Waiting Workflows

- Name: Hibernation of waiting workflows
- Status: Proposed
- Date: 2026-11-21

## Context

Every running workflow instance keeps its execution in memory, with a parked virtual thread, for as long as it runs.
Workflows that wait days or months for an event or a deadline dominate memory on nodes that run many of them, and
every broadcast business event is delivered to every resident execution of the segment.

[ADR-009](./009-event-sourced-workflow-state-rehydration.md) already restores an instance from its own events, in a
unit of work of its own ([ADR-015](./015-dcb-append-conditions.md)), whenever a segment is claimed
([ADR-014](./014-workflow-instance-sharding.md)). A waiting instance can therefore leave memory and be restored the same
way when something may move it on.

## Decision

A workflow module can enable hibernation (`WorkflowConfigurer#hibernation`, overridable per module through
`WorkflowModule.OptionalPhase#hibernation`, Spring Boot `axon.workflow.hibernation.*`). It is disabled by default, with
an idle delay of 30 seconds and a wake timeout of 10 seconds.

**Safe point.** An execution hibernates only when its body is blocked, no primitive call is in progress, no task is
queued, no workflow cancellation is pending, and every active step is a running, open wait whose `StepWaitInfo` has a
readable timeout. Running `execute` steps, including retry back-off, keep it in memory. The check runs as a task on the
workflow thread, and only for executions that made no progress (no own event applied) for the idle delay. Combinators
record no step of their own, so waits combined by `anyMatch`/`allMatch`/`noneMatch` hibernate.

**Hibernation is a node-local memory decision and writes nothing.** Under a per-workflow lock and under the claim its
execution was started or restored under, while that claim is current, the engine records the hibernation in memory
(deadline timer, stand-in cancellation, wait rows), removes the execution, marks it checkpoint-safe and stops it as a
segment release does. A stopped execution appends no event, runs no further task and never reports its end, also when
its body catches `Throwable` around a wait and carries on, the only way a body can keep going (see **Wakes**); removals
and cancellation unregistrations only ever affect the execution or hibernation they belong to, never one that replaced
it. Claims keep restoring every
running workflow of a segment; releases, split/merge (the overlap rule of the running-workflows projection) and
shutdown forget the segment's hibernations. The workflow index ([ADR-023](./023-workflow-index.md)) is unaffected: a
hibernated workflow is running.

**Routing.** Broadcast delivery stays at processor level. Within a segment, resident executions receive every event as
before; hibernated workflows are found through an in-memory index of their open waits, keyed by event name and the value
of one `=` association. A row may match more events than the wait accepts, never fewer; predicate waits match every
event of their name. An own event of a hibernated workflow always wakes it.

**Wakes.** A wake sources the workflow in a fresh unit of work holding the resources of the claim's execution context
(except an event-store transaction) and its `Segment`, restores it through the claim restore path (version resolution, consistency marker of its own read), runs it on the execution context of the claim it
hibernated under, delivers the triggering event or cancellation before the body continues, and starts it only while
that claim is current and caught up ([ADR-017](./017-remove-workflow-replay.md)). A wake by an event runs synchronously
on the handler thread under the workflow lock, bounded by the wake timeout. Deadline and cancellation wakes run on the
engine executor; deadlines are enforced by re-entry, so the engine publishes no timeout events. A cancellation of a
hibernated workflow goes to a stand-in that wakes it and then queues the request
([ADR-013](./013-cancellation-and-termination-semantics.md)); when the woken execution leaves memory before it ran the
request, the request is made again through the cancellation service, or fails when the wake failed. A stopped
execution's waits throw an internal `Error`, no `Exception`, so a body that handles failed steps or catches every
exception still unwinds instead of waiting again in a hot loop; a body that catches `Throwable` must rethrow it.

**Failed event wakes** always fail the event; the engine never skips it. The claim that failed it holds its checkpoint
until it ends, so neither a checkpoint nor the release stores a position past the event, and the engine asks the
processor to release the segment, as it does for any event it fails to handle, also when the processor's error handler
swallows the failure. The next claim restores the workflow and handles the event again. What happens in between is up
to the processor's error handling (by default: release, and retry from the stored token). A workflow whose wake keeps
failing therefore keeps its segment from moving past the event; a dead-letter queue for such events is a possible later
addition. A failed delivery to one workflow does not keep a broadcast event from the others. Failed deadline or
cancellation wakes are retried after the idle delay.

**Manager.** Hibernated workflows are live ([ADR-018](./018-workflow-manager-outside-in-api.md)): live queries return
resident executions plus the sourced states of the local hibernated workflows the query may match (the ids it names;
otherwise those the history finds, or all of them when the module projects no history), sourced a bounded number at a
time. Duplicate starts of a hibernated id are ignored, checked under the workflow lock.

ADR-009, ADR-013, ADR-014, ADR-015, ADR-017, ADR-018 and ADR-023 still apply. A stale node that wakes a workflow
another node moved on is still fenced only by the DCB append condition of ADR-015.

## Consequences

- Waiting workflows cost no memory or thread while hibernated; every wake costs a load (cheap with the workflow-state
  snapshots of [ADR-024](./024-workflow-state-snapshots-by-default.md)).
- A broadcast event that matches many hibernated workflows restores them one by one on the handler thread.
- Predicate waits wake on every event of their name and may churn between memory and hibernation.
- Every claim still restores all running workflows of its segment, which then hibernate again after the idle delay.
- A failed event wake costs a segment re-claim, which restores the workflow and handles the event again.
