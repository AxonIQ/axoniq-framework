# ADR-008: RunningWorkflows Abstraction and Event-Sourced Implementation

- Status: Accepted
- Date: 2026-07-08

## Context

[ADR-007](./007-workflow-event-tags-and-waiting-step-projections.md) introduced workflow event-store tags so running-instance
projections can rebuild from a narrow tagged event slice instead of replaying all workflow events.

The first concrete projection needed from that tag contract is `RunningWorkflows`: the set of workflow identifiers that
started and have not yet reached a terminal workflow state.

The initial persistence model should use standard AF5 event-sourcing building blocks instead of a custom projector and
custom replay loop. The workflow engine must not depend on that persistence choice, however, because a future
implementation may use a different storage or reconstruction strategy.

## Decision

We introduce `RunningWorkflows` as the abstraction for identifying active workflow instances.

The default implementation is `EventSourcedRunningWorkflows`, a singleton event-sourced entity. It is an
implementation detail of the default workflow store, not part of the `RunningWorkflows` contract.

### State

`RunningWorkflows` exposes only the current set of running workflow identifiers and membership checks.

The current event-sourced implementation stores no workflow payloads or step details.

### Reconstruction slice

`EventSourcedRunningWorkflows` rebuilds from the workflow lifecycle tag slice introduced by ADR-007:

- `workflowEvent=lifecycle`

Those events are the tagged lifecycle markers that exist specifically for this projection.

### Event-sourcing logic

`EventSourcedRunningWorkflows` reads only event metadata:

- `workflowId` metadata identifies which workflow instance is affected
- `workflowStatus=STARTED` adds that identifier to the set
- terminal workflow statuses remove that identifier from the set

The payload is ignored.

### Axon Framework construction

The implementation uses standard AF5 event-sourced entity infrastructure registered declaratively through
`EventSourcedEntityModule`:

- `EventSourcedEntityModule.declarative(String.class, EventSourcedRunningWorkflows.class)` as the reconstruction
  mechanism
- a messaging model whose entity evolver reads only event metadata and updates the set of running workflow identifiers
- an entity factory that always creates an empty `EventSourcedRunningWorkflows` singleton state
- a criteria resolver that always selects the lifecycle-tagged workflow events

This keeps loading, replay, criteria-based sourcing, and repository behavior inside AF5 instead of reimplementing a
custom tagged-stream reader.

The declarative module registration is intentionally the extension seam for future operational improvements:

- the current default uses the standard AF5 event-sourced entity module wiring with a minimal metadata-only evolver
- snapshot configuration can be added later without changing the `RunningWorkflows` contract
- repository decoration can later add caching without changing the reconstruction model
- another `RunningWorkflows` implementation can replace event sourcing without changing engine startup behavior

### Shape

`EventSourcedRunningWorkflows` is a singleton entity addressed by a constant identifier.

The identifier is only used by the event-sourced implementation to load its entity from the AF5 repository. It does
not partition the event stream; the criteria resolver selects the lifecycle-tagged workflow events globally.

## Consequences

- The engine depends on `RunningWorkflows`, rather than a particular persistence model.
- The default implementation reconstructs the current set of running workflow identifiers through the standard AF5
  event-sourced repository.
- The default implementation depends only on workflow lifecycle metadata, not on workflow event payload shapes.
- The tag contract from ADR-007 now has a direct consumer and proves its intended projection use case.
- We avoid introducing another custom in-memory projector for a concern that fits AF5 event sourcing directly.
