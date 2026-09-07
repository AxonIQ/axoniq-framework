# DCB Append Conditions — Verification Hunt Plan

Change under test: commit `98b9625d` "Introduce DCB append conditions" cherry-picked onto
`origin/feature/phase-2-sharding-dst-testing` in worktree `axon-flow-spec-dcb-hunt`,
branch `hunt/dcb-append-conditions`. This is a falsification campaign: the change passed its
own tests; those prove nothing new. Every scenario below tries to break one of the claims.

Status legend: `planned` / `running` / `PASS` / `FAIL` / `INCONCLUSIVE` (with the reason).
A scenario is PASS only if its fault provably landed and the green-but-broken audit was run.
Results are appended under each scenario as they are produced, never before.

## Claims under attack

| # | Claim | Falsified by |
|---|---|---|
| CL-1 | Exactly one instance per workflow id across two nodes; only one records a start | two `WorkflowStarted` (or duplicate facts) for one id in the shared log |
| CL-2 | A writer that lost its segment claim cannot record events for a taken-over instance | any event from the stale writer in the log after takeover |
| CL-3 | A step action never runs twice as a result of two writers | action-run counter > 1 attributable to a second writer (crash-replay F-0 dupes excluded) |
| CL-4 | Concurrent sibling appends of one instance never reject each other | any `AppendEventsTransactionRejectedException` in a single-writer parallel-step run |
| CL-5 | RETRYING repeats are never mistaken for duplicates | a retry loop rejected/interrupted with only one writer |
| CL-6 | A rejected append never advances the held position and never publishes a terminal event | marker advanced after rejection, or a terminal event from the losing writer |
| CL-7 | The seeded restore position ≥ instance's own last event; first append after restore not self-rejected | a restored instance interrupted by its own history |
| CL-8 | No regression in replay, catch-up, checkpointing, claim/release, id reuse | existing itest / simulation smoke failures |

## Environment facts (verified up front)

- In-memory `InMemoryEventStorageEngine` (AF 5.3.0) enforces conditions: `containsConflicts`
  checks tailMap from marker at insert AND at commit. `AppendCondition.none()` = INFINITY marker
  = never conflicts.
- Simulation harness `ControllableEventStorageEngine` delegates `appendEvents(condition, ...)`
  verbatim to the in-memory engine → the DST backend should honor conditions (to be proven, S0).
- Docker 29.1.3 available. TLC at `formal/tla/tools/tla2tools.jar`.
- Postgres DCB engine: `io.axoniq.framework.postgresql.PostgresqlEventStorageEngine`
  (axoniq-postgresql 5.3.0-shardingfix-SNAPSHOT, local install).
- Fencing silently disables when `eventSink` is not `instanceof EventStore` or
  `appendCondition() == null` — every rig must prove fencing was ACTIVE (vacuity check).
- DST sweeps must be chunked under 15 min (eval-license watchdog kills longer runs).

## Static suspicions from code reading (to be confirmed or dismissed by scenarios)

- SUS-1: `ConsistencyMarkerSupport.updateAppendPosition` is a non-atomic read-modify-write on
  a volatile field; concurrent calls can lose an update and REGRESS the marker → S1.
- SUS-2: `WorkflowAppendConditions.isAppendRejected` walks the cause chain guarding only
  self-referential causes; a 2-cycle loops forever → S1.
- SUS-3: an append future that never completes stalls the instance's whole chain and queues
  unbounded work behind it; no timeout at this layer → S1 (characterize).
- SUS-4: restore seeding ignores null/ORIGIN `appendPosition()`; a store returning ORIGIN
  after sourcing would leave the marker unseeded → first restored append checked from ORIGIN →
  permanent self-fence → S4.
- SUS-5: the test/DST harness may wrap the EventStore in a non-EventStore sink and silently
  disable fencing → S0 vacuity check via F1RecordDuplicationTest.

## Scenarios

### S0 — Baseline regression + harness vacuity check (CL-8, SUS-5) — status: PASS
- ORACLE: exit code of existing suites; every simulation smoke invariant.
- WORKLOAD: `examples/integrationtests` full suite; `simulation` smoke tier.
- EVIDENCE the change is live in the DST harness: `F1RecordDuplicationTest` (asserts the
  duplicate-record GAP is PRESENT) — under real fencing this must now FAIL. If it stays green,
  fencing is inert in the harness and every downstream DST verdict is vacuous.
- AMBIGUITY: a red F1RecordDuplicationTest is the EXPECTED signal (gap closed), not a regression.
- BUDGET: one run each, ≤15 min.
- RESULT (real runs, logs /tmp/dcbhunt-s0.log, /tmp/dcbhunt-itests2.log, /tmp/dcbhunt-base-diff.log):
  - `examples/integrationtests` with `-am`: 43 tests, 0 failures, 1 skipped, EXIT=0. Includes the
    change's own `ConcurrentWriterFencingTest` (both tests green).
  - Simulation smoke: 255 tests, 10 red. Partition proven by a base-commit differential
    (worktree at 2cd512df, same 4 suspect classes re-run): 5 reds are PRE-EXISTING on the base
    (ExternalStepCancellationTest, BackoffCancelSwallowedTest ×2, RollingDeployTest,
    F20InterleavingProbeTest — identical failures without the change) and 5 reds are
    CLOSED-GAP FLIPS caused by the change: F1RecordDuplicationTest ("expected ≥2 terminal
    records, observed 1"), F1SplitBrainTest (expected ≥2 owners, observed 1 — loser fenced+
    interrupted), F13DuplicateCancelRecordTest + SagaCompensationTest failPath (duplicate
    workflow-terminal on re-drive now rejected), Inv7TerminalIsFinalTest F-3 probe
    (start-event redelivery no longer restarts a terminated business key: 4 events, not >4).
  - VACUITY CHECK PASSED: fencing is active in the DST harness — rejection warnings logged
    ("Append of ... was rejected", e.g. workflow 'cancel-A') and the F-1/F-13/F-3 gaps closed.
  - HARNESS TRAP HIT AND CORRECTED: running `-pl examples/integrationtests` WITHOUT `-am`
    resolves a stale `axoniq-workflow-engine-0.3.0-SNAPSHOT` from ~/.m2 that does NOT contain
    WorkflowAppendConditions → fencing itest fails with duplicate Completed/no id-reuse
    rejection. That was a stale-jar artifact of my run, not an engine bug (verified: jar lacks
    the class; reactor jar has it; `-am` re-run green). All subsequent Maven runs use `-am`.

### S1 — Concurrency stress on ConsistencyMarkerSupport (CL-4, CL-6, SUS-1/2/3) — status: FAIL (2 findings: FND-2 cycle hang, FND-4 cascade break; chain-contract arms PASS)
- ORACLE: (a) chain hand-off: every append receives exactly the position the previous
  successful append wrote (integer markers); (b) no lost `updateAppendPosition` (final marker
  == max of all applied); (c) a failed append neither advances the marker nor strands appends
  queued behind it; (d) bounded memory (chain does not retain completed nodes); (e)
  `isAppendRejected` terminates on cyclic causes.
- WORKLOAD: 100–400 platform+virtual threads × 1k–10k ops; adversarial schedules: concurrent
  `appendSequentially` vs `updateAppendPosition`; injected sync throws, async failures,
  never-completing appends; seeded interleavings via latches.
- EVIDENCE: each adversarial arm has a control showing the fault fired.
- AMBIGUITY: a lost update needs 2+ threads in `updateAppendPosition` simultaneously — report
  whether the engine can reach that state, separately from the class-level defect.
- BUDGET: pure-JDK JUnit in `runtime` (test scope only), ≤5 min.
- RESULT (harness `hunt/S1MarkerStress.java`, `hunt/S1Focus.java`, `hunt/S1ChainDepth.java`,
  run standalone against runtime/target/classes):
  - t1 chain hand-off: PASS — 50 rounds × 200 concurrent appends, every append received exactly
    the previous append's written position.
  - t2 lost-update race on `updateAppendPosition`: NOT OBSERVED in 100k two-thread
    barrier races nor 2M lockstep rounds (t2b: finalMarker == expected 200000010). The RMW is
    still non-atomic by inspection; TLA+ regress cfg (S7) shows the consequence IF it fires,
    but empirically the window did not produce a regression on this hardware. Downgraded to
    theoretical; engine-level reachability additionally requires restore-seeding concurrent
    with a live append, which the engine's restore sequencing makes unlikely.
  - t3 failed append: PASS — marker unchanged (100), chain not stranded.
  - t4 sync throw / null-returning append fn: PASS — propagated, chain continued.
  - t5 never-completing append: CONFIRMED stall of everything queued behind it, no timeout at
    this layer (FND-3 characterization, matches ADR "safety over liveness").
  - t6 memory: PASS — 600k sequential appends, ~0 MB retained growth.
  - t7 `isAppendRejected` on a 2-cycle cause chain: **FAIL — infinite loop confirmed**
    (thread still spinning after 2 s watchdog; guard only handles self-cause). FND-2.
  - t8 seed-vs-append race: 0/20k regressions observed (same non-atomicity caveat as t2).
  - t9 deep-chain cascade: **FAIL — `hunt/S1ChainDepth.java` proves the completion cascade
    breaks by StackOverflowError and strands the tail FOREVER**: depth 1000 → 979/1000
    complete (default stack), 520/1000 at -Xss512k, 5000/5000 at -Xss16m (stack-bound proof).
    50k concurrent appends never complete (t9, 120 s). FND-4: any burst of ~1k queued appends
    of ONE instance permanently wedges that instance's append chain; the stranded appends
    never fail and never complete, so callers block forever. VIRTUAL THREADS DO NOT HELP:
    `hunt/S1ChainDepthVT.java` (cascade unwound on a virtual thread) breaks at the same
    threshold — 972/1000, 988/5000, 953/20000, 986/100000 all STALLED. Engine reachability:
  a sequential body keeps chain depth ~1 (each primitive joins its append), parallel-step
  and cancel fan-outs reach tens; the per-instance task queue cap (1000) is the same order
  as the break point (~970), so the wedge needs a pathological but not impossible burst.

### S2 — Sibling-append false conflict, in-engine (CL-4, CL-5) — status: FAIL on postgres (FND-5 self-fence); PASS in-memory + axon-server
- ORACLE: workflow with N parallel `execute` steps + combinators + cancel fan-out + a retry
  loop completes; zero rejected appends; every step recorded exactly once; RETRYING intact.
- WORKLOAD: 8-way parallel steps, ≥10 repeats per backend; retry workflow with 3 retries.
- EVIDENCE fencing active: negative control — a deliberate foreign tagged append mid-run IS
  rejected on the same rig.
- AMBIGUITY: timeout ≠ false conflict; classify separately.
- BUDGET: in-memory ≤2 min; Axon Server + Postgres ≤10 min each (Testcontainers).
- RESULT (test `S2SiblingFalseConflictTest`, rig `DcbFencingBackends`, logs /tmp/dcbhunt-s2-*.log):
  - IN_MEMORY: PASS — 20/20 runs COMPLETED (8 parallel steps ×1 terminal each, DoomedCancelled ×1,
    RetryingRetrying ≥3, RetryingCompleted ×1, workflow terminal ×1); ZERO rejections in all runs.
    Negative control fired: exactly one warning in the whole log —
    "Append of io.axoniq.framework.integrationtests.workflow.Step2Started ... for workflow 's2-nc-IN_MEMORY' was
    rejected" — and no step2/workflow terminal after the foreign append. Fencing proven ACTIVE.
  - AXON_SERVER (axonserver:2025.2.7, DCB context, Testcontainers): PASS — 5/5 runs clean, zero
    rejections; negative control rejected with a REAL server-side
    `io.axoniq.axonserver.eventstore.api.ConsistencyConditionException: Consistency condition is
    not met` → Axon Server ENFORCES AppendCondition.
  - POSTGRES (axoniq-postgresql 5.3.0-shardingfix-SNAPSHOT, postgres:16-alpine, JDBC
    TransactionManager wired per the framework's own EntityManagerTransactionManager /
    TransactionalUnitOfWorkFactory contract): **FAIL — FND-5, deterministic 3/3 runs.**
    Run 1 instance: spawn appends SiblingWorkflowStarted under
    `[consistencyMarker=ORIGIN, criteria=Tag(workflowId)]`; its afterCommit finalization returns
    "latest global index: 1" WITHOUT finalizing the event itself → marker = position 2 EXCLUDES
    the instance's own Started event; the execution then wedges 60 s ("Error waiting for start of
    workflow instance s2-POSTGRES-1"), and every sibling append chained from that marker is
    rejected by the instance's OWN event: Par1..Par4Started all logged
    "was rejected: another writer already recorded events for this instance" with a SINGLE writer.
    CL-4 falsified on this backend. Two secondary latency findings: events stay invisible to
    sourcing/streaming until finalized (bare `publish(null, …)`/plain commit() without
    afterCommit() leaves them invisible for up to 60 s = `getNotifications(60000)` poll), and the
    same sibling workflow that takes ~1 s in-memory never completes on postgres.
    Note: postgres DOES enforce conditions (the rejections prove it) — it over-enforces against
    the writer's own events. Blame split still open: PG engine finalization/afterCommit-marker
    race vs AF UoW nesting resource scoping; needs the connector team, reproducer is
    `S2SiblingFalseConflictTest#postgres` (fails on repeat 1 every time).

### S3 — Two-node spawn & fencing differential, three backends (CL-1, CL-2, CL-3, CL-6) — status: PASS in-memory (10/10 races landed) + axon-server (5/5); postgres BLOCKED by FND-5
- ORACLE: shared store, two in-process engines with separate token stores (the itest topology):
  exactly 1 `WorkflowStarted`, exactly 1 terminal, no duplicate facts, step action runs once,
  loser publishes nothing terminal. Per-backend verdict vector required.
- WORKLOAD: ConcurrentWriterFencingTest topology over in-memory / Axon Server (Testcontainers,
  DCB context) / PostgreSQL DCB (Testcontainers). ≥5 races per backend.
- EVIDENCE: the race actually happened (both nodes spawned/attempted); runs where one node
  never spawned are not counted.
- AMBIGUITY: "both fenced, instance parked" is ALLOWED by the ADR (safety over liveness) —
  counted separately, not a CL-1 violation.
- BUDGET: ≤25 min total.
- RESULT (test `S3TwoNodeFencingTest`, fresh store per round — in-memory new engine, axon-server
  context delete+recreate via AxonServerContainerUtils.purgeEventsFromAxonServer; logs
  /tmp/dcbhunt-s3-*.log):
  - IN_MEMORY: PASS — 10 rounds, race landed 10/10 (loser's rejection warning observed every
    round: "was rejected: another writer already recorded events"). Every round: exactly 1
    RacedWorkflowStarted, no duplicate event names, step action ran exactly once. The loser is
    fenced at its very FIRST (spawn) append, before its body runs — spawns=1 per round, so the
    ADR's "both fenced, instance parked" case never appeared (0 occurrences).
  - AXON_SERVER: PASS — 5 rounds, race landed 5/5, same oracles green. Id-reuse (ORIGIN anchor)
    arm green on both backends: seeded terminated history untouched (2 lifecycle events), body
    never ran, step action count 0.
  - POSTGRES: BLOCKED — the backend cannot complete even a single-writer instance (S2/FND-5
    self-fence on the first chained append), so a two-node differential is meaningless there.
    Not run to avoid a vacuous verdict.

### S4 — Restore-seed soundness (CL-7, SUS-4) — status: PASS in-memory + axon-server; postgres BLOCKED by FND-5
- ORACLE: run instances to mid-flight, stop node, restore on a fresh engine over the same
  store; first append of every restored instance is ACCEPTED; no self-rejection interrupts.
- WORKLOAD: several instances × several steps, restart at varied points; ≥10 restart cycles;
  mixed-position segment (idle instance restored while another appended later events).
- EVIDENCE: restored instances progress after restart (new events land).
- AMBIGUITY: an instance legitimately fenced by a foreign writer is not a seed bug; only
  self-rejection with no foreign events counts.
- BUDGET: ≤15 min, in-memory + one real backend.
- RESULT (test `S4RestoreSeedTest`; each cycle: 5 instances run step1 → park on waitForEvent,
  node closed, FRESH node over the same store restores them, releases published, all must
  complete; logs /tmp/dcbhunt-s4-*.log):
  - IN_MEMORY: PASS — 10 cycles × 5 instances: every restored instance's first append after
    restore ACCEPTED, all 50 completed (AfterReleaseCompleted ×1 each), ZERO "was rejected"
    warnings in the whole run. Mixed-position arm green: A parked EARLY, B appended 6 later
    steps (Busy6Completed) before the restart, and A still completed after restore — the seeded
    position beyond A's own last event did not self-fence A.
  - AXON_SERVER: PASS — 3 cycles + mixed-position arm, same oracles, zero rejections
    (real backend substituted for the planned postgres arm, see below).
  - POSTGRES: BLOCKED by FND-5 — evidence run: cycle 1 never reaches the parked state
    (ConditionTimeout after 30 s at S4RestoreSeedTest.runCycle:72 waiting for
    WaitReleaseStarted), i.e. the same first-append wedge as S2; restore seeding itself was
    never exercised on this backend. Retest after FND-5 is fixed.

### S5 — Multi-JVM rig with forced claim loss (CL-2, CL-3, CL-6) — status: FAIL — FND-6 (CL-3 duplicate action run, 10/10 rounds); CL-2/CL-6 record-fencing HELD in every round
- ORACLE: two real JVMs, one shared DCB store (Axon Server), separate token stores. JVM-A
  claims, starts a slow instance; A is SIGSTOPped past the claim timeout; B takes over and
  finishes; A resumes and tries to keep writing. Log must contain no A-events after B's
  takeover for that instance; no duplicate facts; ≤1 terminal.
- WORKLOAD: ≥3 rounds; instance with a step that straddles the pause.
- EVIDENCE the fault landed: B provably restored the instance AND A provably attempted an
  append after resume (rejection observed). A round without both is INCONCLUSIVE and repeated.
- AMBIGUITY: A's in-flight external action landing once (orphan effect) is the documented
  indeterminate-step consequence, not a violation; a SECOND recorded outcome is.
- BUDGET: ≤40 min including container startup.
- RESULT (files `S5NodeMain.java`, `S5MultiJvmFencingTest.java`, `S5SignalProbeTest.java` in
  examples/integrationtests; logs /tmp/dcbhunt-s5v3.log, -s5v3b.log, -s5v4.log):
  - Rig evolution, for the record: v1 (step straddling the SIGSTOP) is impossible by design on
    this base branch — the at-most-once policy makes the takeover node refuse an in-flight
    step (`StepIndeterminateException`, ExecuteDelegate resumedInFlight), instance parks; the
    ADR's documented indeterminate/orphan consequence, not a violation. v2 (parked
    waitForEvent + signal appended raw to the store) never woke anyone: a raw store append
    does not wake a parked wait — proven by `S5SignalProbeTest` (node.publish wakes in 0.4 s,
    appendDirectly never does). v3 publishes the signal through a dedicated publisher node.
  - v3 protocol per round: A boots, publishes StartSlow, completes `prepare`, parks on
    GoSignal; SIGSTOP A; B boots over the same store (own token store), restores the parked
    instance, parks; publisher publishes GoSignal; B wakes, runs shipFinal, completes the
    workflow; SIGCONT A; A catches up and races into shipFinal.
  - RECORD fencing (CL-2/CL-6): HELD in 10/10 rounds — store audit per round: exactly 1
    S5SlowWorkflowStarted, exactly 1 S5SlowWorkflowCompleted, zero duplicate facts; A's
    workflow-terminal append REJECTED and A interrupted, publishing nothing.
  - ACTION duplication (CL-3): **VIOLATED in 10/10 rounds (1 v3 + 1 v3b + 8/8 in the stats
    run)** — `shipFinal action ran 2 times across two writers (A=1 B=1)`. Fresh JVM pairs per
    round (distinct pids in NODE-UP lines, per-round wfId, coherent per-process logs), so not
    a rig artifact. See FND-6.
  - Fault-landed proof per round: B provably took over (the surviving Completed is B's — A's
    was rejected in A's log) and A provably attempted an append after resume (rejection
    warning in A's log).
  - Reproduce: `./mvnw -pl examples/integrationtests -am test -Dtest=S5MultiJvmFencingTest
    -Dsurefire.failIfNoSpecifiedTests=false` (Docker required).

### S6 — DST scenarios + seeded tiers (CL-1..CL-7 under faults) — status: PASS (20/20 fencing seeds; 300 fuzz seeds green; 60 chaos seeds green)
- ORACLE: new `DcbFencingScenario` (split-brain topology, fencing expected to HOLD now):
  ≤1 terminal record per step, ≤1 Started, loser interrupted, plus the loser's rejection
  observed (fault landed). Existing invariant set over smoke + a bounded fuzz.
- WORKLOAD: smoke seed set; `DstFuzzTest` ≥200 seeds in <15-min chunks; chaos tier ≥100 seeds
  if time allows.
- EVIDENCE: per-seed fault traces; any break carries seed + committed log.
- AMBIGUITY: harness verdict non-reproducibility (documented) — findings pinned by log.
- BUDGET: smoke ≤5 min; each fuzz chunk ≤15 min.
- RESULT so far (files `simulation/.../scenarios/DcbFencingScenario.java`,
  `simulation/.../DcbFencingTest.java`, copied from the DST worktree where they ran):
  - DcbFencingScenario: real two-EngineInstance split-brain over one shared
    ControllableEventStorageEngine; per-seed oracle ≤1 Started, ≤1 terminal/step, ≤1
    workflow-terminal, AND ≥1 observed rejection warning for the instance (fault-landed
    proof via a logback ListAppender). Verdict: **20/20 conclusive seeds (seeds 0–19), each
    with rejections=1, zero safety violations, zero inconclusive** (surefire XML system-out:
    "DcbFencing verdict: 20 conclusive seeds (needed 20), inconclusive seeds: []").
  - Smoke re-run in the DST worktree: red classes are EXACTLY the known set — 5 closed-gap
    flips (F1SplitBrain, F1RecordDuplication, F13DuplicateCancelRecord,
    SagaCompensation.failPath, Inv7 F-3 probe) + 4 pre-existing base reds (BackoffCancel ×2,
    ExternalStepCancellation, RollingDeploy). F20InterleavingProbeTest was green on this pass
    (known nondeterministic; red on base and on the S0 pass — pre-existing either way).
    NO new red.
  - Fuzz chunk 1: DstFuzzTest green, 63.9 s (child-run seeds 0–99).
  - Fuzz chunks 2–3 (coordinator-run, DST worktree): seeds 100–199 and 200–299, both
    EXIT=0, Tests run 1/0 failures each (/tmp/dcbhunt-fuzz2.log, /tmp/dcbhunt-fuzz3.log).
  - Chaos: DstChaosFuzzTest 60 seeds, EXIT=0 (/tmp/dcbhunt-chaos.log). Total: 300 fuzz +
    60 chaos + 20 fencing seeds, no new invariant break anywhere.

### S7 — TLA+ model of the marker-chain protocol — status: PASS (model refutable, all invariants hold under fencing)
- ORACLE: TLC over new `MarkerChain.tla`: `OneStartPerInstance`, `AtMostOnceRecording`,
  `NoWriteAfterOwnershipLoss`, `SiblingNoFalseConflict`, `RejectionNeverAdvancesMarker`.
- WORKLOAD: N=2 writers, one instance, store = sequence with criteria check from per-writer
  marker; actions: seed-from-head, conditional append, serialized sibling chain, lose-claim.
  Violated/fixed cfg pair per repo convention: fencing-ON cfg must be green; fencing-OFF cfg
  MUST produce a counterexample (else the model is vacuous). A third cfg models SUS-1 marker
  regression to check its protocol-level consequence.
- EVIDENCE: the broken cfg's counterexample.
- BUDGET: ≤15 min TLC per cfg.
- RESULT (files `formal/tla/MarkerChain.tla`, `MC_markerchain{,_broken,_regress}.cfg`; TLC run):
  - `MC_markerchain.cfg` (fencing ON): "Model checking completed. No error has been found."
    5771 states / 1791 distinct. Coverage: 540 RejectedAppend transitions fired → green over
    real rejections, not vacuous.
  - `MC_markerchain_broken.cfg` (fencing OFF): `OneStartPerInstance` violated (36 states);
    with OneStart masked, `AtMostOnceRecording` violated (269 states, two writers record the
    same step's terminal). Refutability proven.
  - `MC_markerchain_regress.cfg` (fencing ON + marker-regression action = the SUS-1 race):
    `SiblingNoFalseConflict` violated in 6 steps — a lone healthy writer is rejected by its
    OWN history after a marker regression and dead-ends with no terminal event. Protocol-level
    consequence of FND-1 confirmed (liveness hole, not corruption; dup-record safety held).
  - Bridge re-runs: `MC_record.cfg` red (AtMostOnceRecording, 195 states),
    `MC_record_fixed.cfg` green (2760/973 — matches README). `RejectionNeverAdvancesMarker`
    held as an action property in the fenced run.

## Execution order

1. S0 baseline (decisive vacuity check) — first, everything depends on it
2. S1 marker-support stress (pure JDK) in parallel with S7 TLA+ (no Maven contention)
3. S2 + S3 backend matrix
4. S4 restore-seed
5. S6 DST tiers
6. S5 multi-JVM

## Findings register

- FND-1 (S1, class-level, THEORETICAL — not observed empirically):
  `ConsistencyMarkerSupport.updateAppendPosition` is a non-atomic read-modify-write on a
  volatile (`var current = marker; marker = ...upperBound...`). Two concurrent callers can keep
  the LOWER position (marker regression). Not observed in 100k barrier races + 2M lockstep
  rounds + 20k seed-vs-append races (`hunt/S1Focus.java` t2b, `hunt/S1MarkerStress.java` t2/t8 —
  0 regressions everywhere). Protocol consequence IF it fires proven by TLC:
  `formal/tla/MC_markerchain_regress.cfg` violates SiblingNoFalseConflict (lone writer rejected
  by its OWN history, dead-ends without terminal). Engine reachability needs restore-seeding
  concurrent with a live append of the same instance. Fix shape: `AtomicReference` +
  `accumulateAndGet(position, ConsistencyMarker::upperBound)`.
- FND-2 (S1, PROVEN hang): `WorkflowAppendConditions.isAppendRejected` never terminates on a
  cause CYCLE of length ≥2 (`a.initCause(b); b.initCause(a)` is legal; the loop guards only
  `cause.getCause() == cause`). Sits on the failure path of EVERY workflow append and in
  `SimpleWorkflowExecution.handleWorkflowException`. Reproducer: arm t7 of
  `java -cp "hunt/out:$(cat hunt/cp.txt)" S1MarkerStress` — watchdog shows the walker still
  spinning after 2 s. Fix shape: depth bound or visited identity set.
- FND-3 (S1, PROVEN, documented ceiling): an append whose future never completes stalls the
  instance's whole append chain forever; everything queued behind waits unboundedly (no timeout
  at this layer). Matches the ADR's "safety over liveness". Reproducer: arm t5 of the same run.
- FND-4 (S1, PROVEN at class level, MEDIUM): the append-chain completion cascade unwinds
  synchronous CompletableFuture callbacks and breaks by StackOverflowError at ~970 queued
  appends; every append behind the break NEVER completes and never fails — permanent silent
  wedge, `.join()` callers block forever. Stack-bound proof: `hunt/S1ChainDepth.java` —
  979/1000 at default stack, 520/1000 at `-Xss512k`, 5000/5000 at `-Xss16m`. Virtual threads
  have the SAME ceiling (`hunt/S1ChainDepthVT.java`: 972/1000, 986/100000). Engine
  reachability: sequential bodies keep depth ~1, fan-outs reach tens, per-instance task-queue
  cap (1000) is the same order as the break (~970). Fix shape: complete the hand-off future via
  `whenCompleteAsync(..., executor)` to cut the recursion.
- FND-6 (S5, multi-JVM, deterministic 10/10 rounds): **a stale writer re-runs a step's side
  effect after takeover; the append condition fences the RECORD but not the ACTION.** Channel
  (root-caused in `ExecuteDelegate.execute`): the at-most-once guard snapshots
  `resumedInFlight` (containsStep && STARTED) BEFORE `acceptAllPendingTasksForStep` drains the
  instance's task queue. When the takeover node's foreign `<step>Started` event is applied
  during that drain, the follow-up `!state().containsStep(stepName)` gate sees the step
  present-and-STARTED, so the stale node (a) never publishes its own STARTED — the fence never
  gets a chance to fire before the effect — and (b) falls into the STARTED||RETRYING action
  block and INVOKES THE ACTION, exactly as if its own STARTED had been redelivered. Foreign
  and own STARTED are indistinguishable (events carry no writer identity). Observed sequence
  (A's log): WAITING → FINAL-RUN → BODY-END → only the workflow-terminal append rejected; A's
  step-COMPLETED was skipped locally because B's COMPLETED had made the step terminal.
  The duplicated action's RECORD can never land: any A-side append is conditioned on A's stale
  marker and the foreign STARTED that triggered the dup is by definition after it → always
  rejected (observed in all 10 rounds: store stayed exactly-once).
  Consequence: the ADR sentence "Duplicate side effects are prevented, not just detected" is
  REFUTED for this interleaving; the true guarantee is "duplicate side effects are prevented
  only when the loser must publish its own STARTED". Claim CL-3 (task brief wording: a step
  action never runs twice as a result of two writers) is FALSIFIED.
  Pre-existing vs regression: PRE-EXISTING — identical rig on the parent commit (2cd512df)
  violated 8/8 rounds with the same duplicate action AND landed a duplicate record
  (`completed=2, duplicateFacts=[S5SlowWorkflowCompleted]`, /tmp/dcbhunt-s5parent.log); the
  change fixes the record half, the ADR's side-effect claim is the new overclaim.
  Reproducer: `S5MultiJvmFencingTest` (8/8 rounds, ~20 s/round, Docker required).
  Fix shape: take the `resumedInFlight` snapshot AFTER the drain (or re-check before invoking
  the action), and/or require one conditioned no-op/STARTED append of one's own before running
  any action whose STARTED this execution did not publish.
- FND-5 (S2, postgres backend, deterministic): **DCB append-condition chain self-fences on
  `axoniq-postgresql`.** With the standard JDBC per-unit-of-work connection wiring (the
  framework's own `TransactionalUnitOfWorkFactory` + `attachToProcessingLifecycle` contract,
  same shape as `EntityManagerTransactionManager` / `SpringTransactionManager`), the spawn
  append (`consistencyMarker=ORIGIN, criteria=Tag(workflowId)`) gets an afterCommit
  `ConsistencyMarker` from a finalization run that did NOT include the event itself
  ("finalizePositions completed with latest global index: 1", marker=position 2, own event later
  finalized at index 2). Consequences: (a) execution wedges 60 s in "Error waiting for start of
  workflow instance"; (b) every sibling append chained from that marker is rejected by the
  instance's OWN Started event — Par1..Par4Started "was rejected: another writer already
  recorded events for this instance" with a single writer (CL-4 falsified on this backend);
  (c) the instance never completes. Reproducer: `S2SiblingFalseConflictTest#postgres`
  (fails on repeat 1, 3/3 runs). Secondary: events appended without the after-commit phase
  (bare `EventSink.publish(null, …)`, or `AppendTransaction.commit()` without `afterCommit()`)
  stay invisible to sourcing/streaming for up to 60 s (`getNotifications(60000)` poll fallback) —
  the test rig's publish/appendDirectly were adjusted to drive the full lifecycle.
  Blame split open (PG engine finalization/afterCommit marker race vs AF UoW nesting resource
  scoping) — hand to the connector team. In-memory and Axon Server pass the identical workload.
  NARROWING (coordinator, `Fnd5PostgresMinimalReproTest`, both arms green ×50/×200 iterations):
  the RAW engine path (`appendEvents(condition, null, ...)` + commit + afterCommit, marker
  chained exactly like the workflow layer) does NOT reproduce — neither sequentially (50
  chained conditional appends) nor with a concurrent unrelated-filler thread (200 chained + a
  filler appending in parallel). The failure therefore needs the TRANSACTIONAL unit-of-work
  path (per-UoW JDBC connection, engine-internal finalization racing the UoW commit's
  visibility). Workflow-layer marker handling is separately proven correct (S1 t1, all
  backends green elsewhere), so the defect sits in `axoniq-postgresql` finalization/afterCommit
  semantics under a transactional processing context — not in commit 98b9625d.
- OBS-1 (S4, in-memory, flake ~1/12 cycles): a just-closed node's stale stream callback on the
  shared store fails the PUBLISHER's unit of work — `node.publish` on the FRESH node threw
  `RejectedExecutionException: ... Coordinator$CoordinationTask ... rejected from
  ScheduledThreadPoolExecutor [Shutting down]` (the OLD node's coordinator). A dead subscriber's
  callback failure should not fail an unrelated publisher's commit. Log
  /tmp/dcbhunt-s4-final.log; harness now retries the publish once (DcbFencingBackends.Node#publish).
- OBS-2 (all backends): `EventSink.publish(null, …)` (no unit of work) delivers events to the
  workflow processor only after a polling delay — in-memory the S2 suite went from 132 s (bare
  publish) to 9 s (publish inside a unit of work); on postgres bare-published events stay
  invisible for up to 60 s (no finalization → no NOTIFY). Test helpers must publish through a
  UnitOfWorkFactory.

## Per-backend verdict vectors (S2/S3/S4, backend-matrix agent)

```
VECTOR sibling_no_false_conflict  in-memory:PASS(20/20)  axon-server:PASS(5/5)  postgres:FAIL(FND-5 self-fence, 3/3)
VECTOR two_node_fencing           in-memory:PASS(10/10 races landed)  axon-server:PASS(5/5 races landed)  postgres:BLOCKED(FND-5)
VECTOR id_reuse_origin_anchor     in-memory:PASS  axon-server:PASS  postgres:BLOCKED(FND-5)
VECTOR restore_seed               in-memory:PASS(10 cycles + mixed)  axon-server:PASS(3 cycles + mixed)  postgres:BLOCKED(FND-5)
VECTOR condition_enforcement(NC)  in-memory:ENFORCES(foreign append → rejection)  axon-server:ENFORCES(ConsistencyConditionException)  postgres:ENFORCES(over-enforces: rejects writer's own events)
```
