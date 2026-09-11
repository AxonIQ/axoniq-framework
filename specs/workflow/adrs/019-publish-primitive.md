# ADR-019: Publish Primitive

- Name: Publish primitive
- Status: Accepted
- Date: 2026-09-10

## Context

A workflow that needs to publish a business event had to wrap the publication in `ctx.execute { appender.append(...) }`.
That records three events for one fact: the step started, the business event, and the step completed. The business
event itself carried no workflow metadata, so it was not part of the workflow's durable state, and consumers could not
tell which workflow instance produced it. Workflow-to-workflow, workflow-to-agent and workflow-to-entity communication
needs one event that is both the business event and the workflow's replay-safe checkpoint.

## Decision

`WorkflowContext#publish(PublishCommand)` appends exactly one event: the caller's `EventMessage` enriched with workflow
metadata. Nothing else about the event changes. The event you publish is the event consumers receive.

### Metadata contract

The engine adds `workflowId`, `stepName`, `stepType=COMPLETED` and `stepPrimitive=PUBLISH`. Workflow keys override
caller keys of the same name. `MessageType`, payload, identifier and timestamp are untouched. No event name customizer
applies. The `EventConverter` of the workflow's context is attached, as on every other engine event.

The step name is therefore the only name the author chooses. It is the durable identifier of the publish, never on the
wire: name it after the intent as a verb phrase, unique within the body (`notifyApproved`, `requestQuote`). Two
publishes of the same event type need two step names; the same step name twice is a no-op on the second call.

Because the event carries `stepType=COMPLETED`, the existing state projection registers it as a completed step. The
replay-skip gate (`WorkflowState#containsStep`) and the drift guard (`ReachedSteps#assertNoReplayDrift`) apply
unchanged, so a replay or a post-crash re-run never publishes the event a second time. The event does not modify the
workflow payload.

### Routing exception to ADR-014

ADR-014 sequences every event carrying `workflowId` metadata by that id, delivering it to the owning segment only. A
published event is an exception: it is a business event for every other instance and may start or wake instances
resident in any segment, while the publisher itself must observe it to complete its step. Published events are
therefore always sequenced by `SequencingPolicy.BROADCAST`, never by a start-candidate id. Candidate routing would
deliver the event to one segment and leave a publisher on another segment waiting forever.

`WorkflowEngine#handle` treats a published event as a business event: it runs the start path and delivers the event to
every execution the segment owns. The publisher is one of them, so the same delivery records its step. Both the
sequencing policy and the engine check `MetadataUtils#isPublishStep` before the `workflowId` rule.

### Foreign-instance guard

Broadcast delivers a published event to every owned execution. `SimpleWorkflowExecution#onEvent` evaluates the wait
conditions for every event but evolves the state only from events of its own `workflowId`; the state projection keeps
the same check as a safety net. Without this every instance would register the publisher's step as its own.

### Layering

The primitive follows ADR-001, ADR-002 and ADR-003: one raw runtime method returning `WorkflowStepResult`, a
`PublishStepDefinition(PrimitiveMetadata, EventMessage)` record as the DSL bridge, and `publish` / `awaitPublish`
convenience methods in the Java and Kotlin DSLs. The DSL also accepts a plain payload, resolving the `MessageType`
through the configured `MessageTypeResolver`, as `EventAppender#append(Object)` does.

## Consequences

- One event per publication. The business event is the step, and the step is the checkpoint.
- Consumers see an additional `workflowId` tag on the event. Caller tags resolved by the application's `TagResolver`
  are kept. The append is fenced against the workflow's own stream only, not against consumer streams.
- Each `publish` call is its own ordered append transaction. Two calls are two transactions.
- A publisher whose own `waitForEvent` matches the published event is woken by it.
- A definition starting on the published event derives its own instance id from it. An id equal to an existing
  instance is rejected as a duplicate, so workflows chained through publish must derive distinct ids.
- Correlation data on every engine event, this one included, comes from the workflow's start-event context, not from
  the event that woke the workflow. Tracked as a follow-up in issue #482.
- Multi-tenancy follows the `ProcessingContext`: the tenant resource of the workflow's context is copied into the
  append unit of work, so the event lands in the publisher's tenant. Restoring instances after a segment claim without
  a tenant on the sourcing context remains an engine-wide limitation unrelated to this primitive.
