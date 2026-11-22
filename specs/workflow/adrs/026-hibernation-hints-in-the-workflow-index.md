# ADR-026: Hibernation Hints in the Workflow Index

- Name: Hibernation hints in the workflow index
- Status: Proposed
- Date: 2026-11-21

## Context

[ADR-025](./025-hibernation-of-waiting-workflows.md) lets a waiting workflow leave memory, but as a node-local
decision: every claim of a segment restores every running workflow of that segment from the event store, and they
hibernate again after the idle delay. After a restart, or when a segment moves to another node, the claim pays the full
restore time and memory peak for workflows that only wait.

The workflow index of [ADR-023](./023-workflow-index.md) already lists the running workflows per segment, and a claim
already streams every event from the token it resumes from. Storing every workflow's open waits, derived from its
events, is unsound: step results do not block, so the events cannot tell whether a body is at a safe point. Only the
runtime safe-point check of ADR-025 can.

## Decision

**An index entry carries an optional hibernation hint**: the open waits (step name, awaited event name or none,
canonical [ADR-006](./006-serializable-association-strings.md) association strings) and the nearest deadline
(`wakeAt`). The engine records the hint when a workflow hibernates at a safe point, and clears it, by writing the entry
without a hint, when the workflow wakes, when any of its own events is handled (not a publish-primitive event), and
when a claim does not adopt it. `initialize` writes no hints. The JPA index stores the waits as readable JSON in a
nullable `hibernationWaits` text column, next to a nullable `wakeAt` timestamp, with an internal codec (no JSON
dependency).

**Validation at claim.** A claim accepts a hint only when the tail it streams from its token holds no own event of the
workflow (any own event, not only lifecycle events), and the workflow still runs after the tail. Accepted workflows are
adopted as hibernations of the claim instead of being restored. Every hint the claim does not adopt (an own event in
the tail, hibernation disabled on the node, adoption refused, any fallback of the claim) is cleared. Correctness does
not depend on the validation: an own event of a hibernated workflow, redelivered from the token, wakes it. Validation
saves restore-then-wake churn.

**Fence against late writes.** A hint is not derived from the events, so a write that lands after a later claim cleared
it could bring back old waits, and events the workflow now awaits would not be routed to it. A delta that *sets* a hint
is therefore only written by `WorkflowIndex.storeWithin(name, delta, context)`, which writes within the transaction in
which the processor stores the checkpoint's token and reports, per call, whether it did (an empty result means nothing
was written). Otherwise the engine writes the delta through `store` with every hint set replaced by a plain running
entry, and keeps the hint pending for a later checkpoint; a release, which writes in the index's own transaction, never
writes a hint, and the claim's end drops pending hints. Clears, additions and removals are written by every write. A
checkpoint decides the changes it writes when it is taken and writes them once the workflows of its segment are safe; a
write within the processor's transaction also writes the hints recorded during that wait. Workflows tend to hibernate
exactly when they turn safe, and a segment that turns idle takes no later checkpoint, so these hints would otherwise
often stay unwritten. That is safe: the processor hands the segment no further event while it waits within its
transaction, so such a hint reflects no more events than one recorded before the decision. With a JPA index and a JPA
token store in one database, the token store refuses the token of a node that lost the claim, which rolls the hint back
with it. A checkpoint that does not advance the token (for example one requested at the token the claim resumed from)
stores no token, so the `CheckpointingProgressStrategy` then extends the claim in the same transaction, which fails the
same way for a node that lost the claim; it does so only when it handed the transaction to the participants and stored
nothing. Both checks fence the hint only when the token store keeps its claims in the index's transaction: a token store
without claims, such as the in-memory one, gives no fence. The in-memory index counts every write as joined: it is
node-local and takes effect at once. An index that does not implement `storeWithin` never stores hints.

**Adoption.** An adopted workflow gets the same in-memory structures as one that hibernated on this node (wait rows,
cancellation stand-in, deadline timer), so routing, duplicate starts, the workflow manager and cancellation treat both
alike. It wakes for an awaited event or an own event also while its claim catches up; its body starts only once the
claim caught up ([ADR-017](./017-remove-workflow-replay.md)). Its deadline and a cancellation wake it only once the
claim caught up; deadlines that passed meanwhile then wake their workflows, at most 16 loads at a time. A workflow that
turns out terminal on wake (only possible through a late entry) is removed from the index.

ADR-023 and ADR-025 still apply. This decision replaces ADR-025's statements that hibernation writes nothing, that
the workflow index is unaffected, and that every claim restores every running workflow of its segment: all three still
hold without an index that stores hints.

## Consequences

- With a JPA index joined to the processor's transaction, a restart or a segment move restores no waiting workflow;
  only those that had events since they hibernated are restored.
- Operators can see in the index which workflows are hibernated, until when, and for which events.
- The fence depends on the index joining the transaction of the token. An index kept in another database must be
  configured with a transactional-executor provider that finds no executor for a processing context, so its writes
  are reported as not joined and it stores no hints; a provider that ignores the processing context would report
  writes in their own transactions as joined, and lose the fence. The index cannot detect that, so it is documented.
  Dropping the join after AxonFramework PR #804 must keep it for hint sets.
- An unjoined index (no processor transaction, or a custom index without `storeWithin`) behaves as ADR-025: claims
  restore everything.
- A hint set by a workflow whose own events trail the processor (lag above the idle delay) is cleared again when those
  events are handled, so such workflows are restored on the next claim.
