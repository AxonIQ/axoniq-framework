# PR #255 walkthrough

Engine changes only. Tests and `pom.xml` left out. Present in this order: each section assumes the ones above it.

One commit, 32 files. The 15 below are the ones worth talking through.

---

## 0. The idea, in one picture

```
one stream:   e1  e2  e3  e4  e5  e6  ...
                |       |       |
   segment 0 ---+-------+       |        token + claim
   segment 1 -----------+-------+        token + claim

   segment 0 -> node A        wf-a, wf-d
   segment 1 -> node B        wf-b, wf-c
```

Each segment has its own **read position** (token) and its own **owner** (claim, a row in the `TokenStore`).
An instance belongs to one segment, decided by `hash(workflowId) & mask`. Nobody coordinates: every node computes the same answer.

---

## 1. `docs/_architecture/014-workflow-instance-sharding.md`

The design record. Start here so the rest reads as "we did what this says".

Covers why hashing beats a lookup table (no shared state, no rebalancing protocol of our own) and what is deliberately out of scope.

---

## 2. `SegmentedWorkflowRouting` — new file

The single ownership rule. Everything else calls into this, so if it is wrong, everything is wrong.

```java
ownedBy(segment, workflowId)   // hash & mask
shouldHandle(workflowId, seg)  // deliver to this segment?
shouldSpawn(baseId, seg)       // create here?
```

**Point to make:** one rule, one file, used everywhere. No second copy of the hashing logic.

### 2b. `segmentKey`, same file

Key is the id up to the first `#`. The engine builds `base#version` for cross-version runs, so `order-7` and `order-7#2.0.0` must land on one segment. Different key would put two versions of one workflow on two nodes.

### 2c. `spawnCandidateIds`, same file

Runs at sequencing time, before the payload is guaranteed convertible. A throw here fails the work package and stalls the whole segment, so it catches and falls back to broadcast. An extra delivery is cheap; a stalled segment is not.

---

## 3. `EventHandlingComponentHandlingAny`

Was hardcoded to one segment and an in-memory token store. Now `anyEventInSegments(n)`, using the application's registered `TokenStore`.

**Say out loud:** without a durable store claims stay inside one JVM, so sharding silently becomes single-node. Hence the warning on fallback.

---

## 4. `WorkflowAutoConfiguration` + `WorkflowConfigurationDefaults`

The config surface, small.

`axoniq.workflow.initial-segment-count`, default 4. Four is a guess at a balance: more parallelism versus more broadcast deliveries per event.

`WorkflowConfigurationDefaults` also moves engine shutdown to a phase strictly below the processor's — see §12.

---

## 5. `WorkflowSpawnRouting`

`hasDerivedWorkflowId` rejects a spawn whose `workflowIdProvider` produced nothing.

Not transient: the same event type derives the same nothing every time. Failing would stall the segment and take down every instance on it, so we skip that one definition and log which one.

---

## 6. `SimpleWorkflowConfigurationRegistry`

A cache, not sharding logic. The highest-version lookup now runs **per streamed event** because routing needs it, so it is precomputed on registration instead of re-derived per call.

Worth a sentence, not a discussion.

---

## 7. `WorkflowEngine.handle` — the two guards

The heart of it.

```java
if (!segmentedRouting.shouldHandle(workflowId, segment)) { ...; return; }   // targeted
...
for (var execution : repository.findAll(ownedBy(segment))) { ... }          // broadcast
```

Broadcast sends an event to all N segments. The guard makes N-1 of them no-op, so **N deliveries, work exactly once**.

---

## 8. `WorkflowEngine.checkAndCreateNewWorkflow` — the spawn guard

Same trick for creation: `shouldSpawn(baseId, segment)`. Without it a broadcast start event would create the same instance on every segment.

Ordering matters: `hasDerivedWorkflowId` runs **before** `shouldSpawn`, because the ownership check hashes the id and a null id would blow up there.

---

## 9. `WorkflowEngine.claimSegment` / `releaseSegment`

Claim: restore only the instances this segment owns, fault-isolated per instance, so one unrestorable workflow cannot orphan the shard.

Release: drain queued work, then interrupt. **Interrupt, not cancel** — no cancellation events written, so the next owner resumes from the last `<Step>Started`.

### 9b. Two contexts, same file

`sourcingContext` carries the event-store transaction and must stay short-lived. `executionContext` parents the restored bodies, which outlive the claim callback.

Expect a question here. The framework maintainer has asked us to remove this hierarchy; see §15.

---

## 10. `WorkflowEngineReplaySupport` — per-segment replay state

Was one flag and one token for the whole engine. Now per segment: `liveSegments`, `segmentTokens`.

Segments catch up at different speeds. A shared flag lets the first segment to finish start bodies belonging to a segment still replaying.

```
startup latest = 1000    seg0 at 40    seg3 at 1000

seg3 hits 1000  ->  seg3 live, seg0 still replaying
start event for a seg0 instance  ->  materialized, body NOT started
seg0 hits 1000  ->  live, deferred bodies start
```

### 10b. `isReplaying(segment, claimedFrom)`

Uses the position the segment is **claimed from**, not what this node observed.

A node claiming a segment it never held observed nothing, and both defaults are wrong: assume live and bodies run at head state during replay; assume replaying and a segment already at the end parks forever.

---

## 11. `WorkflowEngineCheckpointingSupport` — one trigger per segment

`Map<Integer, CheckpointTrigger>`, keyed by segment id.

The framework hands out one trigger per claim. A shared field let a position reached by one segment go through another's trigger, storing a token past events that segment never handled.

Release removes only its own entry.

---

## 12. The checkpoint rule, illustrated

**A segment's token must never pass an event whose effects are not yet durable.** Otherwise the next owner starts after it and the effect is lost.

```
segment 1, token at 499
  event #500 PaymentReceived arrives

  handle() delivers FIRST      -> match enqueued on the instance queue
  handle() then asks for a checkpoint
                               -> holdback: queue non-empty, token STAYS 499
  match runs, wakes the wait   -> completion event enqueued behind it
  completion durable           -> queue empty
                               -> only now may the token move to 500

crash at any point above  ->  new owner reads from 499  ->  #500 redelivered
```

Reverse the two lines in `handle` and the checkpoint is asked for before the work exists: the token moves to 500 and the wake is lost on restart.

### 12b. Two rules that fall out of it

- pending work is filtered by ownership: resolving another segment's id would mark **its** unsafe instance safe
- a materialized-but-not-started instance counts as **unsafe**, having queued work nobody ran

---

## 13. `WorkflowExecutionCheckpointingSupport`

Small but load-bearing: "unsafe" no longer depends on the body running.

An instance restored on claim but not yet started still accumulates queued work. Calling it safe would let the token pass a wake that is sitting in a queue nobody is draining.

---

## 14. `SimpleWorkflowExecution`

Wait matching moved onto the instance's own task queue, off the delivering thread.

A restored body re-registers its wait conditions only once it has replayed back up to them. Matching inside that window reads an empty condition set and **drops the wake permanently**.

---

## 15. `ProcessingContextUtils`

Three lines, and the one thing a reviewer may push back on.

```java
if (!(v instanceof EventStoreTransaction)) { to.putResource(key, v); }
```

The transaction is bound to the unit of work that opened it. A restored instance keeps the claim's short-lived context on its steps, so copying it would route every later append into a unit of work that already committed.

### 15b. Say this before they ask

A workaround. We proposed a framework fix; the maintainer declined, correctly, and asked us to stop holding a `ProcessingContext` for an instance's lifetime instead. That work is designed, not in this PR.

---

## 16. `WorkflowEventProcessingRegistrationEnhancer`

Biggest diff, mostly startup plumbing. Leave it last.

The claim and release listeners are wired here. `onSegmentReleased` returns the engine's drain future rather than an already-completed one, so the processor waits for this node to go quiet.

### 16b. The startup scan

Reads every segment's token without holding claims. `fetchToken` claims on a durable store, so each claim is released before the next read, and segments a live peer owns are skipped. Otherwise two nodes booting at once deadlock each other.

### 16c. Segment count is decided once

The configured value applies only to an empty token store. An existing layout wins, and now logs a warning: a half-finished rolling deploy otherwise looks exactly like a working cluster.

---

## Questions to expect

| Likely question | Short answer |
|---|---|
| Why broadcast instead of a lookup table? | No wait-association table yet. Tracked in #271. |
| Isn't N deliveries wasteful? | Yes, bounded by segment count. Guards make it correct; the table makes it cheap, later. |
| Why interrupt and not cancel on release? | Cancellation writes events. The instance would come back on the new owner already cancelled. |
| Why is `#` reserved? | Cross-version ids are `base#version`, and ownership is decided on the base part. |
| Can two nodes run the same instance? | Not by routing. Zombie-writer fencing is out of scope, also #271. |
| Why the SNAPSHOT in `pom.xml`? | Local framework build. Must be a released version before merge. |
