# ADR-014: Workflow Instance Sharding

- Name: Workflow Instance Sharding
- Status: Accepted
- Date: 2026-08-07

## Context

The workflow engine ran every instance on a single segment. A node handled the whole event stream, held all
running instances in memory and re-ran their bodies after a restart. Throughput was bounded by one node, and adding
nodes did not divide the work: with a durable token store the single segment kept every workflow on whichever node
claimed it first, leaving the others idle, and with the in-memory default each node processed the whole stream on
its own, duplicating the work.

Axon's pooled streaming event processor already partitions a stream over segments and claims those segments through a
token store. The engine needed a rule that maps a workflow instance to a segment, and a way to make every routing,
start, wake, checkpoint and recovery decision follow that same rule.

## Decision

A workflow instance belongs to exactly one segment, decided by its identifier.

### Ownership

`SegmentedWorkflowRouting` is the single home of the rule: a segment owns the instances whose segment key it matches.
The segment key of a workflow id is the part before the first `#`, so the cross-version disambiguated form
`base#version` shares the segment of its base id and every version of one logical workflow lands together.
`String.hashCode()` is specified by the JLS, so the mapping is stable across JVMs and restarts.

### Routing

The same object is the processor's `SequencingPolicy`:

- events carrying `workflowId` metadata are sequenced by that id's segment key
- a business event with exactly one start candidate is sequenced by the candidate id, so a new instance is created on
  the segment that will own it
- every other event is sequenced by `SequencingPolicy.BROADCAST` and delivered to all segments

Broadcast is the only correct routing for an event that may wake an instance resident in any segment. Sequencing runs
before the event reaches a handler, so the payload conversion a start condition or id provider needs is not always
available; a failed derivation is treated as "no candidate" and broadcasts rather than failing the work package.

Ownership guards turn broadcast delivery back into exactly-once work: `shouldHandle` decides whether a segment may
handle or wake an instance, `shouldStartNewInstance` decides whether it may create one. Every non-owning segment makes the
delivery a no-op.

```
event arrives
  -> has workflowId metadata?
       yes -> sequence by that id's segment key -> owning segment only
  -> exactly one definition would start from it?
       yes -> sequence by the candidate id      -> owning segment only
  -> otherwise
       BROADCAST -> delivered to all 4 segments
                 -> each segment applies its ownership guard
                 -> only the owner acts, the rest no-op
```

### Segment count and claims

The processor is initialized with a configurable segment count, defaulting to four, settable through the enhancer
constructor or the `axoniq.workflow.initial-segment-count` property. It uses the `TokenStore` component registered by
the application; without one it falls back to an in-memory store and logs a warning, because process-local claims mean
single-node operation.

Because segments start from independent tokens, the replay decision uses the earliest token across all of them: the
segment furthest behind determines whether catch-up is still required.

### Checkpointing

Checkpoint holdback is per segment. The engine delays a segment's checkpoint only while the instances that segment
owns still have work in flight; instances of other segments are irrelevant to it, because their progress is tracked by
their own token.

### Instance lifetime follows the segment

Restoration happens on segment claim, not at startup. When the processor claims a segment, the engine sources the
running-workflow set, keeps the ids that segment owns, loads their durable state and starts those executions. When the
segment is released, the engine interrupts and drops them.

Interrupting is deliberate: no cancellation events are written, so the next node to claim the segment resumes each
instance from its persisted state.

```
app starts
  -> engine loads nothing, only sets up replay bookkeeping

processor claims segment 2
  -> engine.claimSegment(2)
       -> read the running workflow ids
       -> keep the ids segment 2 owns
       -> load each one's durable state
       -> create and start those executions

processor releases segment 2      (shutdown, rebalance, or lost claim)
  -> engine.releaseSegment(2)
       -> interrupt those executions
       -> remove them from the repository
```

Failover follows from this without any coordination of its own:

```
node A owns segments 0,1        node B owns segments 2,3
node A dies
  -> its token store claims go stale
  -> node B claims segments 0 and 1
  -> claimSegment(0) and claimSegment(1) run on node B
  -> node B resumes those instances from their persisted state
```

### Only streaming processors are supported

Restoration and release hang off the processor's segment claim lifecycle, the `SegmentChangeListener`, which only the
pooled streaming event processor provides. A `SubscribingEventProcessor` - and with it Axon Server's Persistent
Streams - has no claim and release callbacks, so no node would ever restore or drop instances. Workflows therefore
cannot be backed by Persistent Streams. Whether the segment lifecycle can be integrated there is tracked as a
separate investigation; until then the pooled streaming processor is the only supported event source for workflows.

## Consequences

- Instances are distributed over segments, so nodes divide work instead of duplicating it.
- A workflow id maps to the same segment on every node and after every restart, so start placement, wake routing,
  checkpointing and recovery all agree without coordination.
- Segments carry their instances between nodes. When a node dies, a surviving node claims its segments and resumes the
  owned instances without a restart.
- Events that cannot be attributed to a single instance cost one delivery per segment. Ownership guards keep the work
  exactly-once, so the cost is delivery, never correctness.
- Multi-node operation requires the application to register a durable `TokenStore`. With the in-memory fallback,
  claims never leave the process and the engine runs on one node.
- Workflows require the pooled streaming event processor. Persistent Streams run on a `SubscribingEventProcessor`,
  which has no segment claim lifecycle, so they cannot back workflows.
