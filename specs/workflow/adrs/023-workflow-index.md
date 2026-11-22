# ADR-023: Workflow Index

- Name: Workflow index
- Status: Proposed
- Date: 2026-11-21

## Context

[ADR-008](./008-running-workflows-event-sourced-entity.md) introduced `RunningWorkflows`, a singleton event-sourced
entity holding the ids of every running workflow instance. Since
[ADR-014](./014-workflow-instance-sharding.md), every claim of a segment restores the running instances of that
segment, so every claim sources that entity: it reads every workflow lifecycle event ever written, for every segment,
on every claim. With many running workflows, or a long history, claims and restarts get slow.

[ADR-012](./012-checkpoint-driven-replay-lower-bound.md) made the processor token the only replay lower bound, and
[ADR-017](./017-remove-workflow-replay.md) lets a claimed segment restore its durable state and start workflow bodies
only once it caught up with its backlog. [ADR-015](./015-dcb-append-conditions.md) fences a node that lost its claim
without noticing through append conditions. Any faster way to find the running instances of a segment must keep these
guarantees.

## Decision

The engine keeps a workflow index: per workflow module, the running workflow instances, stored by workflow id, and
selectable per segment. For claims that use an index, it replaces sourcing `RunningWorkflows` (ADR-008).
`RunningWorkflows` stays the fallback, and the source that initializes an index.

### The processor token is the index position

The index has no position of its own. Its contents are valid up to the token the segment's processor stored:

- The engine applies the lifecycle events it handles to in-memory pending changes of the segment, on the handler
  thread (`RunningWorkflowsProjection`).
- It decides on checkpoints as before (decide-then-flush). In `onCheckpointAdvanced`, it flushes every pending change
  of the segment and completes only once they are durable, before the processor stores the token.
- A failed or timed-out flush fails the checkpoint. The processor releases the segment and claims it again later, and
  the claim restores the running workflows from the event store, so no change is lost.
- Entries may be ahead of the stored token (a crash after the flush, before the token was stored). Re-applying the
  same lifecycle events converges, because changes are idempotent set updates.

Index writes therefore never request, delay or hold a checkpoint of their own. With an index, the engine must decide
when the processor stores its tokens: a processor that can store tokens without asking the engine fails to start.

### Claim

A claim reads the index entries of its segment, then streams the event tail from the claimed token and applies its
lifecycle events (`RunningWorkflowsClaimLoader`). Entries plus tail are the running instances of the segment; each is
restored in its own sourcing unit of work, as ADR-015 requires. Entries whose sourced state is terminal are removed.
When the index is unavailable, not initialized, or the claim has no token, the claim falls back to `RunningWorkflows`.
The first claim of an uninitialized index initializes it from `RunningWorkflows`, once per index name.

### JPA index

`JpaWorkflowIndex` stores entries in `WorkflowIndexEntry` and the initialization marker in
`WorkflowIndexInitialization`. When the processor runs its batch in a JPA transaction, the first flush round of a
checkpoint writes within that transaction, so entries and token commit together. A flush that has to wait for a write
in flight, a release, a load and an initialization run in a transaction of their own.

### Valid workflow ids

Every workflow id is 1 to 255 printable ASCII characters. The engine enforces this for every workflow it starts, with
or without an index: a workflow with an invalid id is logged and not started. The JPA index stores the id itself as a
`VARCHAR(255)` key, which needs a case-sensitive collation without trailing-space padding.

### Default

The default is `WorkflowIndex.none()`: no index code runs, and every claim sources `RunningWorkflows` as before. Spring
Boot offers the JPA index as an opt-in.

## Consequences

- A claim with an index reads only its segment's entries and the tail after its token, instead of every lifecycle
  event ever written.
- The index is derived data. The event store stays the source of truth: the index never loses a running workflow, as
  long as every node that runs the workflow module writes it.
- A node that does not write the index (an earlier version, or one without the index) can advance a token while
  workflows start in its segment, and the index misses them. The index must only be enabled once every node writes it;
  emptying its tables while every node is stopped rebuilds it from `RunningWorkflows`.
- A failed index write costs a segment re-claim, not correctness.
- A running workflow with an invalid id, started by an earlier version, stalls its segment under the JPA index.
- ADR-008 remains accepted for claims without an index, and for initializing an index.
