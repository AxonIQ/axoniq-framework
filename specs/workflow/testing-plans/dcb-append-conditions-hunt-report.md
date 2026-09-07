# DCB Append Conditions — Hunt Report

Change under test: `98b9625d "Introduce DCB append conditions"` cherry-picked onto
`origin/feature/phase-2-sharding-dst-testing`, worktree `axon-flow-spec-dcb-hunt`, branch
`hunt/dcb-append-conditions`. Full per-scenario evidence, oracles and reproduce commands:
`docs/testing-plans/dcb-append-conditions-hunt.md` (the plan file, updated live during the run).

## Bottom line

The change does what it claims on the backends that implement `AppendCondition` correctly
(in-memory, Axon Server): every two-writer race we could manufacture — in-process, DST
split-brain, and across two real JVMs with a SIGSTOP-forced claim loss — ended with every fact
recorded exactly once, the loser fenced with no terminal event, and the step action running
once. Five previously documented duplicate-record gaps flipped closed.

Three caveats matter. (1) The ADR's sentence "duplicate side effects are prevented, not just
detected" is refuted: a stale JVM re-RUNS a step's action after takeover (10/10 multi-JVM
rounds) even though its records are perfectly fenced — a pre-existing TOCTOU in
ExecuteDelegate, not a regression, proven by a parent-commit differential where the same race
also corrupts the log (FND-6). (2) Two real defects live in the new support code: a provable
infinite loop on the failure path (FND-2) and a stack-overflow wedge of the append chain past
~970 queued appends (FND-4). (3) The PostgreSQL DCB store self-fences a single writer under
the transactional path — unusable as a DCB workflow store in this snapshot (FND-5, store-side).

## Per-claim verdicts

| Claim | Verdict | Key evidence |
|---|---|---|
| CL-1 exactly one instance per id, one start | **HOLDS** (in-memory, axon-server; postgres blocked) | S3: 15/15 cross-node races landed, always 1 Started; itest green; DST F-3 flip (redelivered start of a terminated key now rejected); TLA OneStartPerInstance |
| CL-2 lost-claim writer cannot record | **HOLDS** (same backends) | S3 + S5 multi-JVM (see below); DST F-1 flips: split-brain loser fenced+interrupted, duplicate-record gap closed |
| CL-3 step action never runs twice from two writers | **FALSIFIED — FND-6** (records stay exactly-once; the ACTION runs twice) | S5 multi-JVM: 10/10 rounds, `shipFinal action ran 2 times across two writers (A=1 B=1)` while the store audit stayed clean; root-caused TOCTOU in ExecuteDelegate (foreign STARTED indistinguishable from own redelivery) |
| CL-4 sibling appends never false-conflict | **HOLDS on in-memory + axon-server; FAILS on postgres (FND-5)** | S2: 20/20 + 5/5 runs, 0 rejections, negative control red on every backend; postgres: single writer self-fences deterministically |
| CL-5 RETRYING never mistaken for duplicate | **HOLDS** | S2 retry arm (RETRYING ×3 then success) green on in-memory + axon-server; DST Inv8 green over 300 fuzz seeds |
| CL-6 rejection: no marker advance, no terminal event | **HOLDS** | S1 t3 (marker unchanged on failure); DST F-13 flips (duplicate terminal on re-drive now rejected); TLA RejectionNeverAdvancesMarker over 540 rejected transitions; S3/S5: losers published nothing terminal |
| CL-7 restore seed sound | **HOLDS** (in-memory, axon-server; postgres blocked) | S4: 10 in-memory + 3 axon-server restart cycles, every restored instance's first append accepted, zero self-rejection; mixed-position arm green. Mechanism nuance R-4 below |
| CL-8 no regression | **HOLDS** | itests 43/43 green; simulation reds fully partitioned: 5 closed-gap flips + 5 pre-existing base reds (proven by base-commit differential at 2cd512df); 300 fuzz + 60 chaos seeds green |

## Per-backend verdict vector

```
VECTOR sibling_no_false_conflict   in-memory:PASS(20/20)             axon-server:PASS(5/5)             postgres:FAIL(FND-5, 3/3 deterministic)
VECTOR two_node_fencing            in-memory:PASS(10/10 races)       axon-server:PASS(5/5 races)       postgres:BLOCKED(FND-5)
VECTOR id_reuse_origin_anchor      in-memory:PASS                    axon-server:PASS                  postgres:BLOCKED(FND-5)
VECTOR restore_seed                in-memory:PASS(10 cycles+mixed)   axon-server:PASS(3 cycles+mixed)  postgres:BLOCKED(FND-5)
VECTOR condition_enforcement(NC)   in-memory:ENFORCES                axon-server:ENFORCES              postgres:ENFORCES(over-enforces own events)
VECTOR multi_jvm_claim_loss (S5)   axon-server:records PASS(10/10) / action-duplication FAIL(FND-6, 10/10)
```

No backend silently ignores `AppendCondition`: the negative control (foreign tagged append →
next append rejected + execution interrupted) fired on all three. Axon Server rejects with a
server-side `ConsistencyConditionException`; postgres's problem is the opposite — it rejects
the writer's OWN events.

## Findings

### FND-5 — PostgreSQL DCB store self-fences a single writer — HIGH (blocks the postgres backend), deterministic
- `axoniq-postgresql` 5.3.0-shardingfix-SNAPSHOT, postgres:16-alpine, standard per-UoW JDBC
  transaction wiring. The spawn append's afterCommit returns a `ConsistencyMarker` from a
  finalization pass that did NOT include the event itself ("finalizePositions completed with
  latest global index: 1", marker=2, own event finalized later at index 2). Every subsequent
  append of that instance is rejected by the instance's OWN Started event; the execution
  wedges and never completes. CL-4 falsified on this backend by a workload with ONE writer.
- Blame narrowed to the store, not the change: the RAW engine path (`appendEvents(condition,
  null, …)` + commit + afterCommit, marker chained exactly like the workflow layer) does NOT
  reproduce — 50 sequential chained appends green, 200 chained appends with a concurrent
  unrelated-filler thread green (`Fnd5PostgresMinimalReproTest`, both arms). The failure needs
  the transactional processing-context path. The change's own marker handling is proven
  correct (S1 t1/t9) and the identical workload passes on the other two backends.
- Reproducer: `./mvnw -pl examples/integrationtests -am test -Dtest='S2SiblingFalseConflictTest' -Dsurefire.failIfNoSpecifiedTests=false`
  (postgres arm fails 3/3; needs Docker). Diagnostic log with engine DEBUG: /tmp/dcbhunt-s2-pg4.log.
- Consequence for the ADR: it lists PostgreSQL as a supported DCB store; in this snapshot it
  is not usable as one.

### FND-6 — a stale writer re-runs a step's side effect after takeover; the fence stops the RECORD, not the ACTION — HIGH for exactly-once-effect expectations, deterministic (10/10 multi-JVM rounds)
- Channel, root-caused in `ExecuteDelegate.execute` (runtime/.../execution/ExecuteDelegate.java):
  the at-most-once guard snapshots `resumedInFlight` (containsStep && STARTED) BEFORE
  `acceptAllPendingTasksForStep` drains the instance's task queue. If the takeover node's
  foreign `<step>Started` event is applied during that drain, the subsequent
  `!state().containsStep(stepName)` gate sees the step present-and-STARTED, so the stale node
  (a) never publishes its own STARTED — the append condition never gets a chance to fire
  before the effect — and (b) enters the STARTED||RETRYING action block and INVOKES THE
  ACTION, as if its own STARTED had been redelivered. Foreign and own STARTED are
  indistinguishable: events carry no writer identity.
- Observed (A's per-process log, every round): WAITING → FINAL-RUN → BODY-END; A's
  step-COMPLETED skipped locally (B's COMPLETED already made the step terminal), only A's
  workflow-terminal append rejected. Store stayed exactly-once in all rounds — the duplicated
  action's record can never land (any A-side append is conditioned on A's stale marker, and
  the foreign STARTED that triggered the dup is after that marker by definition).
- Verdict on the ADR: "Duplicate side effects are prevented, not just detected" is REFUTED for
  this interleaving. The true guarantee: duplicate side effects are prevented only when the
  loser must publish its OWN STARTED. The ADR's separate orphan-effect caveat covers a
  different case (mid-action at claim move), not this one (the stale node STARTS the action
  fresh, after losing ownership, triggered by the winner's event).
- Pre-existing vs regression: PRE-EXISTING, proven by a parent-commit differential — the
  identical rig on 98b9625d^ (2cd512df, no append conditions) violated 8/8 rounds with the
  SAME duplicate action AND additionally landed a duplicate record in the shared log
  (`started=1 completed=2, duplicateFacts=[S5SlowWorkflowCompleted]`, /tmp/dcbhunt-s5parent.log).
  The change fixes the record half (10/10 exactly-once with fencing); the TOCTOU action race
  is older than the change. What the change adds is the ADR's overclaim that side effects are
  prevented.
- Reproducer: `./mvnw -pl examples/integrationtests -am test -Dtest=S5MultiJvmFencingTest -Dsurefire.failIfNoSpecifiedTests=false`
  (Docker; 8 rounds, every round violates; ~1 min total after container start).
- Fix shape: take the `resumedInFlight` snapshot AFTER the drain (or re-check immediately
  before invoking the action), and/or never run an action whose STARTED this execution did not
  publish without first passing one conditioned append of its own.

### FND-2 — `WorkflowAppendConditions.isAppendRejected` infinite-loops on a cyclic cause chain — LOW likelihood, PROVEN hang
- Guards only self-referential causes; a 2-cycle (`a.initCause(b); b.initCause(a)`, legal)
  never terminates. On the failure path of EVERY append and in `handleWorkflowException`.
- Reproducer: arm t7 of `java -cp "hunt/out:$(cat hunt/cp.txt)" S1MarkerStress` (2 s watchdog
  proves the walker never returns; thread pinned at 100% CPU).
- Fix shape: depth-bounded walk or visited identity set.

### FND-4 — append-chain completion cascade breaks by StackOverflowError at ~970 queued appends; the tail is stranded FOREVER — MEDIUM (low probability, unrecoverable, silent)
- `ConsistencyMarkerSupport.appendSequentially` chains appends via synchronous
  CompletableFuture callbacks; a backlog unwinding in one cascade overflows the stack at
  ~970 nodes, the cascade dies mid-walk, and every append queued behind it never completes
  and never fails — `.join()` callers (PayloadDelegate, TerminateDelegate, step publishes)
  block forever; the instance wedges silently.
- Proof it is stack-bound: `hunt/S1ChainDepth.java` — 979/1000 complete at default stack,
  520/1000 at `-Xss512k`, 5000/5000 at `-Xss16m`. Virtual threads (the engine's context)
  have the SAME ceiling: `hunt/S1ChainDepthVT.java` — 972/1000 … 986/100000, all STALLED.
- Engine reachability: sequential bodies keep chain depth ~1; parallel-step/cancel fan-outs
  reach tens; the per-instance task-queue cap (1000) is the same order as the break (~970).
  Needs a pathological burst (store latency spike + hot instance) — low probability, but the
  failure is permanent and invisible.
- Fix shape: hand over the chain with `whenCompleteAsync(..., executor)` (bounds stack depth
  per cascade step).

### FND-1 — non-atomic read-modify-write in `updateAppendPosition` — THEORETICAL, consequence modelled
- `var current = marker; marker = current == null ? position : current.upperBound(position);`
  is not atomic; concurrent calls can REGRESS the marker. Not observed empirically: 100k
  two-thread barrier races, 2M lockstep rounds, 20k seed-vs-append races — 0 regressions
  (`hunt/S1Focus.java`, `hunt/S1MarkerStress.java`). If it ever fires, TLC proves the
  consequence (`formal/tla/MC_markerchain_regress.cfg`): a lone healthy writer is rejected by
  its OWN history and dead-ends without a terminal event — a liveness hole, not corruption.
- Engine reachability requires restore-seeding to overlap a live append of the same instance.
- Fix shape: `AtomicReference.accumulateAndGet(position, ConsistencyMarker::upperBound)`.

### FND-3 — a never-completing append stalls the whole per-instance chain — documented ceiling, PROVEN
- No timeout exists at this layer; everything queued behind waits forever (arm t5). Consistent
  with the ADR's "safety over liveness". Flag if hanging (rather than failing) stores are an
  operational reality — combined with FND-4 a long stall converts into a permanent wedge once
  the backlog unwinds.

### Closed gaps (the change working as designed — these must be re-pinned before merge)
- F-1 duplicate terminal record under split-brain: CLOSED (F1RecordDuplicationTest red:
  "highest observed was 1").
- F-1 ownership probe: loser now interrupted (F1SplitBrainTest red: 1 owner).
- F-13 / F-13-class duplicate workflow-terminal on re-drive: CLOSED (F13DuplicateCancelRecordTest,
  SagaCompensationTest.failPath red).
- F-3 start-event redelivery restarting a terminated business key: CLOSED (Inv7TerminalIsFinalTest
  probe red: exactly 4 events). The ORIGIN-anchored spawn works.
- Whoever merges this must flip those five expected-gap tests (and the F-1/F-3/F-13 entries in
  the formal docs) to pin the CLOSED state, or the DST suite stays red after the merge.

### S5 — multi-JVM forced claim loss (two real JVMs, one Axon Server DCB store, SIGSTOP)

Rig: `S5NodeMain` (real forked JVMs, own in-memory token stores, shared Axon Server DCB
context) + `S5MultiJvmFencingTest` (SIGSTOP/SIGCONT orchestration, store audit, per-JVM logs).
Protocol per round: A publishes and runs to a parked `waitForEvent`; SIGSTOP A; B boots,
restores the parked instance, parks; a publisher node emits the wake signal; B completes;
SIGCONT A; A races into the final step.

- RECORD fencing (CL-2, CL-6): **HELD 10/10 rounds** — exactly 1 Started, 1 Completed, zero
  duplicate facts; A's post-thaw append rejected, A interrupted, nothing terminal published.
- ACTION duplication (CL-3): **VIOLATED 10/10 rounds** — FND-6 below.
- Fault-landed proof per round: the surviving Completed is B's (A's was rejected in A's log),
  and A provably attempted an append after resume. Fresh JVM pairs per round (distinct pids).

Also learned by S5 v1 (straddling-step variant): the base branch enforces at-most-once
execution — a takeover node does NOT re-run a step that was in-flight on the frozen node
(`StepIndeterminateException`), the instance parks. That matches the ADR's indeterminate-step
consequence; claim-loss during a long-running action costs LIVENESS, not duplicate records.
And harness physics: a raw store append does not wake a parked wait; signals must be published
through an engine's sink (`S5SignalProbeTest`).

## DST tiers (S6)

- New `DcbFencingScenario` + `DcbFencingTest`: real two-engine split-brain per seed; oracle =
  ≤1 Started, ≤1 terminal per step, ≤1 workflow-terminal AND ≥1 observed rejection (fault
  landed). 20/20 conclusive seeds (0–19), zero violations, zero inconclusive.
- Smoke: reds are exactly the known set (5 closed-gap flips + pre-existing base reds). No new red.
- Fuzz: 300 seeds (0–299 in three <15-min chunks), all green. Chaos: 60 seeds, green.

## TLA+ (S7)

`formal/tla/MarkerChain.tla` models the marker-chain protocol (2 writers, conditional appends,
seed-from-head restore, serialized sibling chain, rejection = interrupt).
- Fencing ON (`MC_markerchain.cfg`): all five invariants hold (OneStartPerInstance,
  AtMostOnceRecording, NoWriteAfterOwnershipLoss, SiblingNoFalseConflict,
  RejectionNeverAdvancesMarker); 540 rejected-append transitions covered — not vacuous.
- Fencing OFF (`MC_markerchain_broken.cfg`): OneStartPerInstance then AtMostOnceRecording
  violated — the model is refutable.
- Marker-regression action ON (`MC_markerchain_regress.cfg`): SiblingNoFalseConflict violated —
  the FND-1 race, if it ever fires, produces a self-fence liveness hole.
- Pre-existing bridge re-confirmed: `MC_record.cfg` red / `MC_record_fixed.cfg` green.

## Residuals — what was NOT tested, and why

- R-1: PostgreSQL arms of S3/S4/S5 — BLOCKED by FND-5 (the backend cannot run one writer).
  Retest after the store fix.
- R-2: no fine-grained network partition (Toxiproxy) between node and store; claim loss was
  forced by SIGSTOP of the whole JVM. A partition that drops only append responses
  (write-succeeded-but-unacked) is untested; the marker would not advance and the writer
  would retry nothing (no retry exists) — expected park, but unproven.
- R-3: the synthetic-store restore edge (a store whose sourcing `appendPosition()` returns
  null/ORIGIN would leave the marker unseeded → restored instance permanently self-fences from
  ORIGIN). Unreachable on all three real backends (each returns a real marker); only
  constructible with a mock store.
- R-4 (ADR wording vs mechanism): the restore seed is the **lowerBound across all sourcing
  reads** in the shared claim transaction (AF `DefaultEventStoreTransaction.updateAppendPosition`),
  not "the head of the stream at claim start" as the ADR says. Conservative-low: under an
  ACTIVE takeover (stale owner appending during the new claim's sourcing) the new owner can
  seed below an instance's own last event and self-fence → both stopped, instance parks until
  the next claim. Allowed by the ADR's safety-over-liveness stance, but the ADR sentence "past
  every owned instance's own events" only holds for a quiescent store. Recommend an ADR
  correction and (later) a scenario that appends DURING claim sourcing.
- R-5 (hypothesis, unproven): `executeWorkflow` sends the Completed event with a 5 s `.get()`;
  a per-instance append backlog >5 s would surface as `TimeoutException` → the TIMED_OUT
  branch publishes a terminal event. Under fencing that terminal is itself conditioned (a
  foreign writer still rejects it), so the two-writer safety claim is unaffected — but a slow
  store could produce spurious TIMED_OUT terminals for healthy single-writer instances. Not
  observed in any run; noting for a slow-store scenario.
- Spring Boot production wiring (bike-rental style autoconfiguration) was not exercised; all
  rigs used programmatic configurers.

## Environment / harness traps hit (recorded so nobody pays twice)

- `-pl examples/integrationtests` WITHOUT `-am` resolves a stale
  `axoniq-workflow-engine-0.3.0-SNAPSHOT` from ~/.m2 that predates the change → the fencing
  itest fails convincingly (duplicate Completed, id reuse accepted). Stale-jar artifact, proven
  (jar lacks `WorkflowAppendConditions.class`); every Maven run must use `-am`.
- The connector jars on the test classpath auto-enhance every node (localhost:8124 connect
  storms) unless both ServiceLoader enhancers are `disableEnhancer(...)`-ed.
- `EventSink.publish(null, …)` (no unit of work) delivers only after polling delays; on
  postgres such events stay invisible up to 60 s (no finalization → no NOTIFY). Test helpers
  must publish inside a unit of work (OBS-2).
- A just-closed node's stale stream callback can fail an unrelated publisher's unit of work
  with `RejectedExecutionException` (OBS-1, ~1/12 cycles, in-memory).
- Surefire's per-class `.txt`/XML `time=` attributes under-report long system-out-heavy tests;
  judge by the recorded per-seed output, and count `<testcase` elements after removing the
  whole `target` directory.

## Honest confidence

For the fencing semantics themselves — one instance per id, stale writers fenced at the
STORE, no duplicate facts, no terminal from a loser, siblings never self-conflict, retries
pass — the evidence is strong and multi-layered (unit stress, in-process races, DST
split-brain over 20 seeds, two real JVMs with SIGSTOP-forced claim loss, TLC model with a
refutable twin, 300 fuzz + 60 chaos seeds), on in-memory and Axon Server. I would ship this
change against Axon Server AS A RECORD-CONSISTENCY mechanism — which it fully delivers.

Do not ship the ADR's exactly-once-side-effect claim: FND-6 shows a stale writer still re-runs
a step action after takeover (records stay clean, the effect duplicates). Anyone relying on
"prevented, not just detected" for non-idempotent effects will be burned; the ADR sentence
needs correcting and the ExecuteDelegate TOCTOU closing.

I would NOT enable the PostgreSQL backend until FND-5 is fixed in `axoniq-postgresql` — the
engine self-fences a single healthy writer, which is a total outage of the workflow engine on
that store, found deterministically on the first real run.

Top remaining risks, in order: (1) FND-6 duplicate side effects under claim churn if
consumers believe the ADR's "prevented" claim; (2) FND-5 if postgres is in the deployment
matrix; (3) FND-4's silent permanent wedge under an append burst (fix is one line of intent);
(4) the R-4 active-takeover seed nuance parking instances more often than the ADR text
suggests under churny claims; (5) FND-2's hang if any dependency ever builds a cyclic cause
chain.
