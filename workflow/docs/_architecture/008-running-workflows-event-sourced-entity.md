# ADR-008: RunningWorkflows Event-Sourced Entity

- Status: Accepted
- Date: 2026-07-08

## Context

[ADR-007](./007-workflow-event-tags-and-waiting-step-projections.md) introduced workflow event-store tags so running-instance
projections can rebuild from a narrow tagged event slice instead of replaying all workflow events.

The first concrete projection needed from that tag contract is `RunningWorkflows`: the set of workflow identifiers that
started and have not yet reached a terminal workflow state.

The desired construction is explicitly event sourced and should use standard AF5 building blocks instead of a custom
projector and custom replay loop.

## Decision

We introduce a singleton AF5 event-sourced entity named `RunningWorkflows`.

### State

The entity stores only the current set of running workflow identifiers.

No workflow payloads or step details are stored in this entity.

### Reconstruction slice

The entity rebuilds from the workflow lifecycle tag slice introduced by ADR-007:

- `workflowEvent=lifecycle`

Those events are the tagged lifecycle markers that exist specifically for this projection.

### Event-sourcing logic

The entity reads only event metadata:

- `workflowId` metadata identifies which workflow instance is affected
- `workflowStatus=STARTED` adds that identifier to the set
- terminal workflow statuses remove that identifier from the set

The payload is ignored.

### AF5 construction

The implementation uses standard AF5 event-sourced entity infrastructure registered declaratively through
`EventSourcedEntityModule`:

- `EventSourcedEntityModule.declarative(String.class, RunningWorkflows.class)` as the reconstruction mechanism
- a messaging model whose entity evolver reads only event metadata and updates the set of running workflow identifiers
- an entity factory that always creates an empty `RunningWorkflows` singleton state
- a criteria resolver that always selects the lifecycle-tagged workflow events

This keeps loading, replay, criteria-based sourcing, and repository behavior inside AF5 instead of reimplementing a
custom tagged-stream reader.

The declarative module registration is intentionally the extension seam for future operational improvements:

- the current default uses the standard AF5 event-sourced entity module wiring with a minimal metadata-only evolver
- snapshot configuration can be added later without changing the entity contract
- repository decoration can later add caching without changing the reconstruction model

### Shape

`RunningWorkflows` is a singleton entity addressed by a constant identifier.

The identifier is only used to load the entity from the AF5 repository. It does not partition the event stream; the
criteria resolver selects the lifecycle-tagged workflow events globally.

## Consequences

- The current set of running workflow identifiers can be reconstructed through the standard AF5 event-sourced repository.
- The implementation depends only on workflow lifecycle metadata, not on workflow event payload shapes.
- The tag contract from ADR-007 now has a direct consumer and proves its intended projection use case.
- We avoid introducing another custom in-memory projector for a concern that fits AF5 event sourcing directly.
