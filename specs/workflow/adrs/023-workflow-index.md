# ADR-023: Workflow Index

- Name: Workflow Index
- Status: Proposed
- Date: 2026-11-24

## Context

A workflow instance belongs to one segment of the engine's pooled streaming event processor, see
[ADR-014](./014-workflow-instance-sharding.md). When a node claims a segment, the engine must learn three things:

1. which workflow instances of that segment are running, to restore them
2. which waits are open, to route an event only to the workflows that wait for it
3. which workflows are hibernated, to keep them out of memory

Without an index, each of the three comes from a different place:

- Running workflows come from `EventSourcedRunningWorkflows`, one event-sourced list for all segments, see
  [ADR-008](./008-running-workflows-event-sourced-entity.md).
- Open waits come from `EventSourcedWaitAssociations`, one event-sourced list for all segments.
- Hibernated workflows come from `HibernatedWorkflows`, one snapshot per segment, which the engine stores when it
  releases the segment.

A segment claim sources both lists in full and keeps the entries of its own segment. The cost of a claim therefore
grows with the whole application, and a node pays it once per claimed segment. The hibernation record is missing or out
of date after a crash, so a claim then loads every hibernated workflow of the segment into memory.

## Decision

The engine has a **workflow index**: a store of rows, cut per segment, written with the token of the
processor. The events stay the source of truth, and the rows are derived from them. A Spring Boot application with
JPA has it by default. Without it, a segment claim sources the two lists and keeps no workflow hibernated.

### Rows

`WorkflowIndex` is the SPI, registered through `WorkflowConfigurer.registerWorkflowIndex(...)`. `InMemoryWorkflowIndex` and
`JpaWorkflowIndex` implement it. One workflow index serves every workflow module, as each row carries the name of its
module.

- one row per running workflow: module, workflow id, segment key, and a nullable `wakesAt`
- one row per open wait: module, workflow id, step name, segment key, event name, associations
- one mark per initialized workflow module

Every change to a row has the same outcome when it is stored twice. A workflow starts once and ends once, and a wait
opens once and closes once, so a change that arrives late or again never hides a workflow or a wait.

### Segment key

`SegmentKeyRange` is the ownership rule of ADR-014 in a form a database can index. The segment key of a workflow id is
its hash with the bits reversed, read as an unsigned number. Reversed, the ids of one segment form one contiguous range
of keys. A split halves the range and a merge joins two neighboring ranges, so the key stored with a row never changes.

### Writing with the token

`Checkpointing.onCheckpointStore(segment, token, processingContext)` is a hook the processor invokes right before it
stores a token, inside the transaction that stores it. The engine notes row changes while it handles events. At the
hook it writes the changes that the token covers. Rows and token are committed together, or neither is, so the token is
the position of the rows. The index keeps no position of its own. Rows ahead of the token, as after a crash between a
write in a transaction of its own and the token, are harmless: applying the events after the token again gives the same
rows.

Only the node that owns a segment commits rows:

- Storing a token checks the owner, and fails the transaction of a node that lost its segment claim.
- A token that did not advance is not stored. The processor then extends the claim in the same transaction, which
  makes the same check.
- On a release, a failed store does not roll back. So the processor checks the claim before it invokes the hook.

### Segment claim

```
claim of segment S, stored token T
  -> read the rows of S                      one range query per table on the segment key
  -> read the events after T                 only those tagged workflowEvent=waitForStep or workflowEvent=lifecycle
  -> apply the events to the rows, and note what they change
  => the running workflows and the open waits of S, as of the claim
```

A split gives both new segments the token of the old segment, and a merge stores
`MergedTrackingToken.merged(lower, upper)`, which resumes from the lower token. So a claim after a split or a merge
reads every event its rows may miss, without a step of its own.

A segment claim sources the two lists instead when the module is not initialized, or when the segment has no stored
token. The sourced workflows and waits of the segment then become its rows, and are stored with the next token, under
its owner check.

### Initialization

An index introduced while the processor already stored tokens, or one that was emptied, misses the workflows that
started before those tokens. So `load` reports nothing for a module until `initialize` marked it. A claim that finds
the module not initialized sources the two lists, and initializes the index with every running workflow and open wait
of the module, in one transaction. An initialization never removes a row, as another claim may already have stored a
later change.

### Start checks

The token can only be the position of the rows while the engine decides when the processor stores a token, and while
the tokens survive a restart whenever the index does. A workflow module with an index therefore refuses to start when
its processor has event handling components of its own, uses another progress strategy, or keeps its tokens in an
`InMemoryTokenStore` while the index is not an `InMemoryWorkflowIndex`.

### Hibernation

When a workflow hibernates, its row gets `wakesAt`, the nearest timeout of its open waits. When it wakes, its row loses
`wakesAt`. A segment claim keeps a workflow out of memory when its row carries `wakesAt` and no event after the token
belongs to it. A workflow that made progress after it hibernated closed a wait or ended, and the claim reads both
events. The rows replace the `HibernatedWorkflows` snapshot per segment: the engine stores none.

`wakesAt` is a fact of one node, not of an event, so no event corrects it. The engine stores it only when the workflow
index reports `storesWithToken()`, which puts it under the owner check. A workflow index in another database than the
token store is configured with `JpaWorkflowIndexConfiguration.storesWithToken(false)`. Its rows are written in a
transaction of their own, before the token. They hold no `wakesAt`, so a segment claim loads the hibernated workflows
of the segment, as it does without a workflow index.

### Alternatives considered

- **Rows for running workflows only, and no rows for waits.** Rejected. Event routing by wait association needs the
  open waits of a segment right after a segment claim, before its workflows have replayed. Without wait rows, every
  segment claim sources the full list of open waits.
- **Reading every event after the token.** Rejected. A segment claim reads only wait and lifecycle events, selected by
  tag. No other event changes a row, and the number of other events grows with the business load.
- **A position per segment, kept apart from the token.** Rejected. It let nodes with and without the index run
  together, at the cost of a third table, a position store every 10 seconds, and a claim rule across a split or a
  merge. Every node runs with the index instead.
- **Keeping the hibernation snapshot per segment next to the rows.** Rejected. It holds the same fact as `wakesAt` in a
  second place, it needs a `SnapshotStore`, and it is missing after a crash. `wakesAt` in the row is stored with the
  token, so it is there after a crash too.

## Consequences

- A segment claim reads the rows of one segment and the few events after its token. Its cost follows the segment, not
  the application.
- Measured with 100,000 hibernated workflows on 16 segments, a JPA token store and a JPA workflow index on PostgreSQL,
  and a clean stop and restart: the restart takes 1.9 s and reads 16 events with the workflow index, against 42.9 s and
  4,936,030 events without it. Peak heap is 451 MB against 2,107 MB. Wake latency and deadline lateness are equal.
- A hibernated workflow stays out of memory across a segment claim on any node, also after a crash, and after a split
  or a merge.
- The workflow index must be in the database of the token store to keep hibernated workflows out of memory.
- `JpaWorkflowIndex` adds three tables. Each stored token also writes the rows that changed.
- A row holds a workflow id of at most 255 characters, a step name of at most 1024 characters, and at most 10,000 bytes
  of associations per wait.
- The first claim with an empty workflow index sources the two lists and initializes the index for the whole module.
  Its rows carry no `wakesAt`, so the segment claims of that start load every running workflow. A split or a merge
  does not source.
- Every node of a workflow module must run with the same index. A rolling upgrade from a version without it is not
  supported: stop every node of the old version first. Turning the index off and on again, or resetting the tokens of
  the workflow processor, means emptying the index, its initialization included.
- A workflow module with an index refuses to start when its processor could store a token past changes the rows do
  not hold.
- `Checkpointing` has one more default method. It does nothing unless a component overrides it.
- An application chooses `JpaWorkflowIndex`, `JdbcWorkflowIndex` or `InMemoryWorkflowIndex`. The JPA and the JDBC index
  share their tables. Spring Boot picks the index of the type of the token store, and JPA when the token store does not
  tell. `WorkflowIndex` and the types of its methods are `@Internal`: they are a storage detail of the engine and may
  change with it.
