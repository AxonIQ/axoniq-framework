# DCB Append Conditions — Hunt 2

Engine under test: `git log origin/main..feature/271__dcb-append-conditions` (9 commits), worktree
`axon-flow-spec-dcb-hunt-271`, branch `hunt/dcb-append-conditions-on-271`. The first hunt and its
findings FND-1..FND-6 are in `dcb-append-conditions-hunt-report.md` and are not re-reported here.
On this engine FND-1 (non-atomic marker update) and FND-2 (cyclic cause walk) are closed in code
(`SequencedAppendCondition.updateAppendPosition` uses `accumulateAndGet`,
`AppendFailureClassifier.isRejected` walks with an identity-visited set); FND-4 (append-chain stack
depth) is unchanged and still open.

## Bottom line

No new engine defect was proven. Four claims of the change were confirmed by test, one accepted
liveness gap was pinned, one design choice was model-checked with a refutable twin, and one
intermittent duplicate-terminal observation is recorded without a test because it does not
reproduce in isolation.

| Hypothesis | Verdict | Evidence | Finding |
|---|---|---|---|
| H1 write inside the restore read | HOLDS, both directions | `ClaimRestoreForeignWriteTest`, 2/2 arms pass, 2 runs | — |
| H2 action only after the store accepted STARTED | HOLDS | `FencedStepStartTest` / `FencedStepStartScenario`, effect 0 while fenced, 1 after the claim, 3 runs | — |
| H3 every append goes through the conditioned path | HOLDS | read of `runtime/src/main`, every emitter routes to `SimpleWorkflowExecution.appendWorkflowEvent` | — |
| H4 fenced instance parks until the next claim | GAP PRESENT, pinned | `FencedWriterParkTest`, non-terminal after 24 h simulated, terminal after the claim, 3 runs | — |
| H5 fenced execution takes the terminal cleanup path | ASYMMETRY, confirmed by reading, consequence unproven | `SimpleWorkflowExecution:244/437` vs `:449`, `WorkflowEngine:579-588` | FND-8 (candidate) |
| H6 per-instance claim seed | HOLDS, refutable twin violates | TLC `MC_claimseed.cfg` clean, `MC_claimseed_shared.cfg` violates | — |
| H7 fencing combined with the other features | PARTIAL | deterministic: execute STARTED, execute COMPLETED, parallel branch. Seeded fuzz: withdrawn as a gate | FND-7 (intermittent), R-6 |

## Hypotheses

### H1 — a stale owner appends while the new claim is sourcing that instance

- ORACLE. Durable log per instance. A restored instance whose own sourcing read did not include a
  foreign write must record nothing further (its first append is rejected). A restored instance with
  no foreign write during its read, and a sibling of the same claim, must both reach a terminal
  record.
- WORKLOAD. Two instances of one segment, one empty declarative definition, restored through
  `WorkflowEngine.restoreWorkflowsFor(Segment.ROOT_SEGMENT, ...)` on one claim context.
- EVIDENCE. `ForeignWritingStorageEngine.wroteDuringSourcing()` — asserted true before the oracle is
  read, so a run in which the write never landed fails instead of passing.
- AMBIGUITY. The restored execution is removed from the repository the moment it is fenced, so the
  oracle is the durable log and not the live execution; a run that cannot decide terminality within
  5 s counts as non-terminal, which is the failing side for the control arm.
- BUDGET. 2 arms, deterministic, ~4 s.

Result: both arms pass, twice.

```
H1 aWriteLandingInsideTheRestoreReadFencesTheRestoredExecution PASS
H1 aRestoredExecutionWithNoForeignWriteIsNeverRejectedByItsOwnHistory PASS
H1 DONE
```

This closes the R-4 residual of hunt 1 in the direction that matters. The ADR says a restored
instance is seeded "at or after its own last event". Precisely: at or after the last event its own
read saw. A write that lands inside that read window is not sourced and correctly rejects the
restored execution — safety, at the cost of the park H4 pins. The sibling arm shows the per-instance
unit of work does what commit `94149611` claims: `wf-b` is untouched by what a previous owner wrote
for `wf-a`, which under one shared claim transaction it would not be (H6 proves that half formally).

Label: reproduced by test.

### H2 — a step's action runs only once the store accepted its STARTED

- ORACLE. While the step's own `STARTED` append is rejected: the counted effect for that step is 0,
  the durable log holds no `STARTED` record for it, and the fenced execution published no terminal
  workflow record. After the next claim: the effect ran exactly once and the step has exactly one
  `COMPLETED` record.
- WORKLOAD. One `OrderWorkflow` instance, `reserveInventory`, one seed.
- EVIDENCE. `ControllableEventStorageEngine.fencedInstance()` names the instance the fence wrote
  for, and the rejection warning of `SimpleWorkflowExecution` is counted for that instance. Both are
  asserted, so a run where the fence never fired fails.
- AMBIGUITY. A late action invocation is given a 2 s absence window before the effect count is read.
- BUDGET. 1 seed, deterministic, ~10 s.

Result, 3 runs, identical:

```
H2 Outcome[fencedInstance=order-fenced-start, rejectionsObserved=2, effectRunsWhileFenced=0,
   startedRecordsWhileFenced=0, terminalRecordsWhileFenced=0, effectRunsAfterRestore=1,
   completedRecordsAfterRestore=1]
```

Commit `f01f9a5d` does what it says on this path. Note what it does not cover, and what hunt 1's
FND-6 was about: this fences a run whose own `STARTED` the store rejects. FND-6 is the run that
never publishes a `STARTED` at all because a foreign one arrived during the task drain — that path
is now guarded by `ownStartedSteps` in `ExecuteDelegate`, and re-testing it needs the two-JVM rig of
hunt 1 (R-7 below).

Label: reproduced by test.

### H3 — every workflow event append goes through the conditioned path

Read-only. `grep` over `runtime/src/main/java` for `publish`, `eventSink`, `appendEvents`:

```
WorkflowLifecycleControlDelegate:173,185   -> workflowExecution.appendWorkflowEvent(...)
AbstractStepExecutor:305                   -> workflowExecution.appendWorkflowEvent(...)
VersionDelegate:132                        -> workflowExecution.appendWorkflowEvent(...)
WorkflowContextDelegation:300              -> workflowExecution.appendWorkflowEvent(...)
SimpleWorkflowExecution:275,312,345,354,362,414 -> publishAndWait -> appendWorkflowEvent
```

`SimpleWorkflowExecution.appendWorkflowEvent` (line 576) is the only producer of an append, and it
always goes through `SequencedAppendCondition.appendSequentially` and `WorkflowEventPublisher`. No
`EventSink.publish` call site survives: the `EventSink` imports left in `WaitForDelegate`,
`RetryableExecuteDelegate`, `ExecuteDelegate` and `PayloadDelegate` are unused. The external
cancel/terminate path (`WorkflowLifecycleControlDelegate`), the version path (`VersionDelegate`),
the payload path (`WorkflowContextDelegation`), the failure-path terminal events and the spawn are
all conditioned. No fence hole to write a scenario for.

Label: confirmed by reading.

### H4 — the liveness the ADR trades safety for

- ORACLE. After one late foreign write fences the owner on a single node where no claim moves: the
  instance records no terminal event, and 24 h of simulated time plus the wake event it needs does
  not change that. After a claim: it terminates, with exactly one terminal record.
- WORKLOAD. One `OrderWorkflow` instance, the fence on `chargePayment` `COMPLETED`.
- EVIDENCE. `fencedInstance()` names the instance, asserted.
- AMBIGUITY. "Never terminates" is bounded: 3 s wall clock with virtual time already advanced 24 h.
  Quoted as simulated time, as the harness requires.
- BUDGET. 1 seed, deterministic, ~15 s.

Result, 3 runs, identical:

```
H4 Outcome[fencedInstance=order-parked-by-fence, terminalWhileParked=false, simulatedParkTime=PT24H,
   terminalAfterClaim=true, workflowTerminalRecords=1]
```

`FencedWriterParkTest` is an expected-gap pin: it asserts the gap is present. It quotes the ADR
sentence it pins. It flips if the engine ever recovers a fenced instance without a claim.

Label: reproduced by test.

### H5 — what happens to the fenced execution's checkpoint state

Read-only, with `file:line`.

A rejection reaches `SimpleWorkflowExecution.execute`'s `catch`/`finally`
(`SimpleWorkflowExecution.java:234-248`). `handleWorkflowException` recognises it
(`:335`) and publishes nothing. Then the `finally` calls `finishWorkflow` (`:244`), which calls the
termination handler (`:437-440`). That handler is `WorkflowEngine.execute`'s (`WorkflowEngine.java:579-588`):
it removes the execution from the repository, calls `checkpointWorkIndex.markSafe(workflowId)` and
`checkpointingSupport.requestCheckpoint(segment, executionToken)`.

The publication-timeout path is the deliberate opposite: `stopRuntimeForRecovery`
(`SimpleWorkflowExecution.java:449`), whose Javadoc says the execution "deliberately remains in the
repository and non-terminal so a processing-node restart can restore it from durable history. In
contrast to finishWorkflow(Consumer), this method must not invoke the termination handler because
that handler removes the execution from the engine."

A fenced instance is in the same shape — non-terminal, driver stopped, needs a restore — and takes
the other path. The asymmetry is real and recorded as FND-8. Its consequence is NOT proven: the
token handed to `requestCheckpoint` is the execution's own start token, not the head, so the "the
checkpoint advances past the wake signal the instance still needs" story that motivated the
hypothesis does not follow from the code as read. What is certain is only that a fenced instance is
dropped from the node and marked safe while it is not finished, and that only a claim brings it
back — which H4 measured.

Label: confirmed by reading. Consequence: unproven, see R-5.

### H6 — the claim seed, in TLA+

`formal/tla/ClaimSeed.tla` models a claim restoring two instances while the previous owner keeps
appending, with the seed taken two ways: per instance (each read's own head) or shared (the lowest
of the claim's reads, which is what one transaction gives). `NoSelfFence` says a restored instance is
never rejected by its own history.

```
### MC_claimseed
EXIT=0
Model checking completed. No error has been found.
14775 states generated, 7470 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 8.
### MC_claimseed_shared
EXIT=12
Error: Invariant NoSelfFence is violated.
265 states generated, 224 distinct states found, 157 states left on queue.
The depth of the complete state graph search is 6.
```

The shared-seed counterexample is the ADR's argument, exactly: `i1` is read at length 1, the previous
owner appends for `i2`, `i2` is read at length 2 but seeded at the running lower bound 1, and `i2`'s
own record at index 2 rejects it. The existing `MarkerChain.tla` was left untouched and re-run for
the record (`MC_markerchain.cfg`, EXIT=0, 5771 states generated, 1791 distinct, no error).

Label: model-checked.

### H7 — fencing combined with the other workflow features

Two vehicles were built. One works and is shipped; one was measured and withdrawn as a gate.

**Deterministic per-feature fences (shipped).** `ControllableEventStorageEngine.armForeignWriteBeforeCommitOf(step, status)`
lands a foreign write for the committing instance in the window between the two condition checks of
one commit — the in-memory store checks the condition when the transaction is created
(`InMemoryEventStorageEngine:118`) and again at commit (`:135`), so a write in between is the real
race a peer wins. That covers, deterministically:

| Feature path | Scenario | Result |
|---|---|---|
| execute step, own `STARTED` rejected | `FencedStepStartScenario` | action never runs, next claim runs it once |
| execute step, `COMPLETED` rejected, single node | `FencedWriterParkScenario` | parks, no terminal, claim recovers it |
| parallel sibling steps in flight | `ParallelBranchFenceScenario` | no sibling records past the fence, one terminal at the end |

```
PB run0 Outcome[fencedInstance=comb-fenced-branch, maxStartedRecords=1, maxTerminalRecordsForAStep=0,
   terminalRecordsWhileFenced=0, terminalAfterClaim=true, maxTerminalRecordsAtEnd=1, terminalRecordsAtEnd=1]
```
(identical on 3 runs)

**Seeded fuzz (built, not a gate).** `FaultKind.STALE_WRITER` + `StaleWriterFault` arm the same
fence, nudge virtual time so the timers the instances sit on produce a commit, fall back to a blind
`appendForeign` on a live instance when nothing commits while armed, and then perform the claim that
un-parks whatever got fenced. `StaleWriterFencingScenario` runs the whole smoke workload under it.
Two measurements decided against shipping a test for it:

1. Yield. Over seeds 11..40 the fault landed a write on 5 of 30 seeds. The harness settles to
   quiescence before it injects, so at the moment the fault runs almost nothing is committing.
2. The workload is not green without it. Same 30 seeds, identical fault set minus `STALE_WRITER`:

```
SWEEP SUMMARY fault=true  seeds=30 red=1 seedsWithInjection=5
SWEEP SUMMARY fault=false seeds=30 red=1 seedsWithInjection=0
   1 SWEEP seed=11 fault=true  RED [PayloadReducerSemantics]
   1 SWEEP seed=11 fault=false RED [PayloadReducerSemantics]
```
   and seeds 12, 22 and 37 each break on the same invariant when re-run alone. A gate on this shape
   would be red on a clean engine, which the setup refuses to ship.

Not covered, with the reason: retry backoff while ownership is lost, a parked `waitForEvent` whose
timeout fires on the stale node, version upgrade / migration mid-flight on two writers,
`modifyPayload` reducers against a rejected payload append, external cancel / terminate on the stale
node, and `waitFor` / `sleep` whose `STARTED` was rejected. Each needs its own start event, wake
event and step name against a single-workflow world — the same shape as the three shipped scenarios,
and mechanical to add. Recorded as R-6.

Label: reproduced by test (the three deterministic cases); measured (the fuzz yield and the
differential).

## Findings

### FND-7 — duplicate terminal workflow records under load, no stale writer involved — MEDIUM, INTERMITTENT, not reproduced in isolation

Running the smoke workload over seeds 11..40 back to back, seed 22 ended with **six** instances each
holding the same terminal workflow record twice:

```
DIAG seed 22 fault=false dupTerminalInstances={deploy-p3=2, subs-p2=2, order-wf2=2, order-wf1=2,
                                               loopc-p4=2, order-wf0=2}
    deploy-p3|io.axoniq.framework.workflow.RollingDeployWorkflowFailed x2
    subs-p2|io.axoniq.framework.workflow.SubscriptionRenewalWorkflowFailed x2
    order-wf2|io.axoniq.framework.workflow.OrderWorkflowFailed x2
    order-wf1|io.axoniq.framework.workflow.OrderWorkflowFailed x2
    loopc-p4|io.axoniq.framework.workflow.LoopingPollCounterNamesWorkflowFailed x2
    order-wf0|io.axoniq.framework.workflow.OrderWorkflowFailed x2
```

Seen twice in sweeps of that range (once through `SweepProbe`, once through `DiagProbe`), and
**not** reproducible by running seed 22 on its own — two isolated runs gave an empty duplicate set
and a `PayloadReducerSemantics` break respectively. There is no stale writer and no `STALE_WRITER`
fault in these runs: the fault set is `WORKER_CRASH, MESSAGE_REORDER, RESTART, CLOCK_JUMP`.

Why it matters: "at most one terminal workflow record per instance" is a claim the append condition
is supposed to make structural, and hunt 1 recorded the F-13 duplicate-terminal gap as closed. Six
instances failing it at once, all with `...Failed`, points at a shutdown or recovery storm rather
than a two-writer race — a second execution of the same instance re-publishing its terminal event
after a restore, whose marker legitimately covers the first one, would be accepted by the condition,
because the condition prevents a concurrent writer and not a repeated fact.

No test is shipped: the observation is load-dependent, and a test that is red on a clean engine only
some of the time is worse than the record.

Reproducer (not deterministic; run the whole range, not the seed):
`java -cp <sim classes> SweepProbe 11 30 false`, then look for `maxWfTerm=2`.

Fix shape: unknown, not root-caused. First step is to capture which execution wrote the second
record — the two events carry no writer identity, so it needs a per-execution marker in the trace,
not more sweeping.

Label: measured, not reproduced in isolation.

### FND-8 — a fenced execution is cleaned up as if it were terminal — LOW, candidate, confirmed by reading

See H5. `SimpleWorkflowExecution.java:244` → `:437` → `WorkflowEngine.java:579-588` remove a
non-terminal fenced execution from the repository, mark it safe for checkpointing and request a
checkpoint, while the deliberately-parallel publication-timeout path (`:449`) does none of those and
says in its Javadoc why. Nothing observable was proven to go wrong: the token passed to
`requestCheckpoint` is the execution's own start token, and restoration sources by criteria rather
than from the token, so a later claim still finds the instance (which H4 measured).

Fix shape, if it is one: route the fenced case through `stopRuntimeForRecovery` rather than
`finishWorkflow`, so a fenced instance is treated as what it is — an instance this node no longer
owns and has not finished.

Label: confirmed by reading.

## Precise negatives

- **N-1.** No unconditioned append path exists in `runtime/src/main`. Every emitter, including
  external cancel/terminate, the version path, the payload path, the failure-path terminal events
  and the spawn, routes through `SimpleWorkflowExecution.appendWorkflowEvent`. H3 was looking for a
  fence hole and there is none to write a scenario against.
- **N-2.** A restored instance is not fenced by what a previous owner wrote for a *different*
  instance of the same claim. Both the DST-adjacent unit arm (H1, `wf-b`) and the model (H6) say so,
  and the model's shared-seed twin shows this is a property of commit `94149611` and not of the
  store.
- **N-3.** The `PayloadReducerSemantics` breaks seen throughout this hunt are not caused by the
  stale-writer fault: the same seeds break with the fault removed (H7 differential). They are the
  documented pre-existing red.

## Residuals

- **R-5** (from H5). Whether a fenced instance can lose a wake signal is unproven. Vehicle: a scenario
  that parks an instance on a `waitForEvent`, fences the wait's own `COMPLETED` commit, restores, and
  delivers the signal exactly once — the shipped `FencedWriterParkScenario` deliberately publishes it
  twice, so it cannot answer this.
- **R-6** (from H7). Six feature paths have no fence coverage: retry backoff, a parked wait whose
  timeout fires on the stale node, version migration mid-flight, `modifyPayload` reducers,
  external cancel/terminate on the stale node, and `waitFor`/`sleep` whose `STARTED` was rejected.
  Vehicle: one single-workflow scenario each, on the pattern of `ParallelBranchFenceScenario`.
- **R-7** (from H2). The FND-6 interleaving of hunt 1 — a stale node that never publishes its own
  `STARTED` because a foreign one arrived during the task drain — is not re-tested here.
  `ownStartedSteps` in `ExecuteDelegate` is meant to close it. Vehicle: the hunt-1 two-JVM rig
  (`S5MultiJvmFencingTest`), which needs Docker and Axon Server.
- **R-8.** The seeded stale-writer campaign has no gate. It needs either a quieter workload than the
  default registrations, or the `PayloadReducerSemantics` flake resolved first.
- **R-9.** Nothing in this hunt ran through Maven. See below.

## How everything was run

The base run that owns `~/.m2` never released it within the session, so no Maven command was
executed. Every result above was produced by compiling the sources with `javac` against the
worktree's already-built module classes plus the dependency classpath, and driving the scenarios
from small `main`s, plus TLC directly. That validates behaviour and compilation; it does **not**
validate the Surefire wiring, Checkstyle or the module build. Re-run under Maven before merging:

```
./mvnw -o -pl runtime test -Dtest=ClaimRestoreForeignWriteTest -Dsurefire.failIfNoSpecifiedTests=false
./mvnw -o -f workflow/axoniq-workflow-simulation/pom.xml test -Dtest='FencedStepStartTest,FencedWriterParkTest,ParallelBranchFenceTest' -Dsurefire.failIfNoSpecifiedTests=false
```

---

# Addendum — R-6 feature fences, FND-7 root cause, and H8

Run after Maven became available. Everything below was executed through Surefire; the commands and exit codes are at
the end.

## H8 — an interrupt inside the terminal transition, and what classifies it

### The classification, by `file:line`

| Where the interrupt lands | Classified as | `file:line` |
|---|---|---|
| The `STARTED` gate (`awaitStateChange(stepStatus(step, STARTED))`) | **cancel** — the flag is restored and the run returns `false`; `execute` returns `WorkflowStepResults.canceled(stepName)`. No `STARTED` was accepted, so nothing ran. | `ExecuteDelegate.tryStartStep`, and its caller at `ExecuteDelegate.java:156-158` |
| The step-result wait (`stateBased`) | **failure** — `InterruptedException` becomes `StepInterruptedException`, and `StepInterruptedException extends StepFailedException` | `AbstractStepExecutor.java:350-359` → `StateBasedWorkflowStepResult.java:145-160`; `StepInterruptedException.java:38` |
| A body that catches `StepFailedException` | **durable FAILED**, at the body's own discretion | `OrderWorkflow.java:169-175` calls `ctx.fail(e)` |
| The wait inside a terminal transition | **swallowed** — logged, and the projected state stays non-terminal | `SimpleWorkflowExecution.java:479-483` |

So the hypothesis's mechanism is half right and lands one frame away from where it was placed. An interrupt in the
**gate** cannot produce a FAILED: the gate classifies it as a cancel and no `STARTED` was ever accepted. The FAILED in
the quoted stack comes from the **step-result** wait of an `awaitEvent` (`OrderWorkflow.java:136`), a path this PR does
not touch — `WaitForDelegate`'s only change in the diff is dropping the unused `EventSink` parameter. What the PR does
add is one new way for that interrupt to be delivered: `interruptWorkflowDriver()` on a rejected append
(`SimpleWorkflowExecution.java:586`), the single new interrupt call site in the whole diff of that file.

That new source is safe by construction: it fires only when `AppendFailureClassifier.isRejected` is true, which means a
foreign writer is already past this execution's marker, so the `ctx.fail` that follows is itself rejected and nothing
terminal lands. `FencedWriterParkTest` measures exactly that (`terminalRecords <= 1`, none from the fenced writer).

### The sharper consequence, reproduced deterministically — FND-9

The interrupt that *does* corrupt the log is the one that lands in the terminal transition's own wait.

1. `SimpleWorkflowExecution.transitionToTerminalState` publishes the terminal event and then waits for its own state to
   turn terminal (`:473-484`). That wait is a `taskQueue.take()`, and its `catch` only logs (`:479-483`).
2. `WorkflowLifecycleControlDelegate.failWorkflow` runs the transition and then throws `WorkflowFailedException`
   (`:113-126`).
3. `SimpleWorkflowExecution.handleWorkflowException` catches it and re-checks
   `!this.state().workflowStatus().isTerminal()` (`:341-349`). The state never evolved, so the guard passes and the
   terminal event is published **a second time**.
4. Both appends come from the same execution through `SequencedAppendCondition.appendSequentially`
   (`SequencedAppendCondition.java:46-58`), the second anchored at the marker the first one advanced (`:60-66`). A
   rejection means a *foreign* writer, and there is none. **The append condition cannot see this.**

`InterruptedTerminalTransitionScenario` drives it on purpose: publish a failing instance, then interrupt the driver in
the window where the durable log already holds the terminal record and the live execution's state does not. The window
is short, so the scenario spins on it rather than polling at an interval, and interrupts every time it sees the two
disagree. Two attempts at the injection were measured and discarded before this one: polling at an interval missed the
window entirely under Surefire (0/6 attempts caught it), and interrupting continuously from the step's own failure
caught the window but landed in the terminal publication's wait instead, so the first terminal event never committed
(`terminalRecords=0` on 2 of 3 runs). Gating the interrupt on the first terminal record being durable is what makes it
land where it is meant to.

```
IT run0 Outcome[attempts=1, caughtWindow=true, terminalRecords=2, distinctTerminalTypes=1]
IT run1 Outcome[attempts=1, caughtWindow=true, terminalRecords=2, distinctTerminalTypes=1]
IT run2 Outcome[attempts=1, caughtWindow=true, terminalRecords=2, distinctTerminalTypes=1]
```

First attempt every time: the same terminal fact recorded twice, one execution, both accepted.

**Pre-existing, not PR-caused.** `transitionToTerminalState` is byte-identical to `origin/main`; the diff of
`SimpleWorkflowExecution.java` adds exactly one interrupt call site and it is the rejection path. What the PR changes is
exposure: it adds an interrupt source, and its extra durable round-trip per step start shifts when every other
interrupt lands.

**What it costs the change's claims.** Hunt 1 recorded the F-13 duplicate-terminal gap as CLOSED. That is true only for
the two-writer case. A duplicate terminal record produced by ONE execution publishing twice is outside what an append
condition can detect, by the ADR's own reasoning ("a rejection therefore always means a foreign writer").

Fix shape: make the terminal transition idempotent rather than state-guarded — mark the instance terminal from the
append that was accepted, not from the redelivery, so an interrupt in the wait cannot re-open the guard. Or classify an
interrupt inside `transitionToTerminalState` as a park and stop, never as "publish again".

Pin: `InterruptedTerminalTransitionTest`, an expected-gap test asserting the gap is present (2 records, 1 distinct
type). It flips the day the transition survives the interrupt.

### `DstSmokeTest` — the flip, measured

Two runs, both EXIT=1, and the failing case moves between runs:

```
--- smoke 1
[ERROR] Tests run: 10, Failures: 2, Errors: 2, Skipped: 0
[ERROR]   DstSmokeTest.sameSeedIsReproducible:77 » InvariantViolation [PayloadReducerSemantics] ... (case [1])
[ERROR]   DstSmokeTest.sameSeedIsReproducible:81 » InvariantViolation [DeterministicReplay] ... (case [2])
--- smoke 2
[ERROR] Tests run: 10, Failures: 2, Errors: 1, Skipped: 0
[ERROR]   DstSmokeTest.sameSeedIsReproducible:81 » InvariantViolation [DeterministicReplay] ... (case [1])
```

So `sameSeedIsReproducible` is not deterministically red on case [1] with one symptom; it flaps across cases and across
two different invariants. A single observation of case [1] is one draw from that population, which is why the
attribution had to come from the code path and the deterministic scenario rather than from the smoke run.

## FND-7 — root-caused, and reclassified

### (1) `DstReproduceTest -Ddst.seed=22`, three times

```
=== RUN 1  EXIT=1   DstReproduceTest.reproducesSeed:58 » InvariantViolation [PayloadReducerSemantics]
=== RUN 2  EXIT=1   DstReproduceTest.reproducesSeed:62 » InvariantViolation [DeterministicReplay]
=== RUN 3  EXIT=1   DstReproduceTest.reproducesSeed:58 » InvariantViolation [PayloadReducerSemantics]
```

None of the three shows the duplicate terminal. They abort earlier, on the documented pre-existing reds, before the run
produces the log the duplicate would be in. The isolation control is therefore not clean, and no conclusion can be
drawn from these three runs alone.

### (2) Classification: engine bug

Under a back-to-back sweep of seeds 11..40 in one JVM, seed 22 produced the duplicate in **3 of 3 sweeps**:

```
DIAG seed 22 dupTerminalInstances={deploy-p3=2, subs-p2=2, order-wf2=2, order-wf1=2, loopc-p4=2, order-wf0=2}
DIAG seed 22 dupTerminalInstances={order-wf1=2, loopc-p4=2, order-wf0=2, order-wf2=2, subs-p2=2}
DIAG seed 22 dupTerminalInstances={order-wf0=2, loopc-p4=2, deploy-p3=2, subs-p2=2, order-wf1=2}
```

The evidence that decides is **not** the sweep. It is the deterministic reproduction above: `InterruptedTerminalTransitionScenario`
produces the identical symptom — the same terminal fact twice, one distinct type, from one execution — on demand, and
the code path is readable end to end. The sweep only supplies the interrupt timing that the scenario supplies on
purpose; a load of concurrent shutdowns interrupts several drivers inside the same window, which is why five or six
instances duplicate at once. **FND-7 and FND-9 are the same defect**; FND-7's entry above is superseded, and its
"MEDIUM, intermittent, not reproduced in isolation" grading is withdrawn.

### (3) Can a RESTART give one instance two accepted FAILED appends?

**No, not by that route.** A restart-restored execution takes one of two branches, and neither ends in a second
accepted terminal:

- It **sources** the earlier FAILED. Then `removeTerminalAndStartRestoredWorkflowExecutions` removes it before anything
  starts (`WorkflowEngine.java:552-557`), and `SimpleWorkflowExecution.execute` skips a terminal instance anyway
  (`:221-223`). Restore does filter terminal instances, in those two places.
- It **does not source** it. Then its seed sits below that record and its own FAILED append conflicts and is rejected.

The two branches cannot both be dodged, because the marker and the read come from the same value. On the in-memory
store, `source(...)` fixes `end = lastVisiblePosition.get()` and the marker it hands back is `end + 1` computed from
that same `end` (`InMemoryEventStorageEngine.source`, and `MapBackedSourcingEventMessageStream.lastEntry` building
`new GlobalIndexConsistencyMarker(end + 1)`). A restore therefore cannot get a marker that outruns the events its own
read delivered. Confidence: high — read from the 5.3.1 sources jar, not inferred.

Both FAILED records come from **one** execution, so this is not the F-13 two-writer class re-opened under restart. It is
a third thing the condition was never able to catch.

### `AF-INMEM-SOURCE-TRUNCATION`

**Not present in this worktree.** `grep -rniE "AF-INMEM|truncat|inmem" formal/*.md` finds no such item in
`formal/FOLLOW-UPS.md` or anywhere else here, and no harness workaround references it. The question of whether this
PR's per-instance sourcing unit of work bypasses that workaround cannot be answered from this checkout; if the item
exists it lives in a different one.

What can be said from the code here: `WorkflowEngine.restoreWorkflow` (`:460-490`) reads the seed as
`sourcingContext.component(EventStore.class).transaction(sourcingContext).appendPosition()` — the marker the store
attaches to the read it just performed. On the in-memory store that marker and the read's end are the same value (see
above), so the per-instance unit of work introduces no window between what the restored state saw and what its marker
covers.

## R-6 — the six feature fences

Each is one `*Scenario` + one `*Test`, all deterministic, all green. Every one proves its fault landed: the foreign
event is in the durable log and the instance logged a `"was rejected"` warning.

| # | Combination | Verdict | Evidence | Test class |
|---|---|---|---|---|
| 1 | retry backoff, retry fires on the stale node | **records HOLD, action does not** | `records 5→5`, `terminalRecords=0`, `rejections=1`, `effects 1→2` | `FencedRetryBackoffTest` |
| 2 | parked `waitForEvent`, timeout fires on the stale node | HOLDS | `records 4→4`, `timedOutRecords=0`, `terminalRecords=0`, `rejections=1` (3/3 runs) | `FencedParkedWaitTimeoutTest` |
| 3 | version migration mid-flight | HOLDS | `records 4→4`, `migrationMarkers=1`, `terminalRecords=0`, `rejections=1` | `FencedVersionMigrationTest` |
| 4 | `modifyPayload` write rejected | HOLDS | `records 7→7`, `rejections=1`, `payloadFoldConsistent=true` after the claim | `FencedPayloadWriteTest` |
| 5 | external cancel to the stale node | HOLDS | `records 2→2`, `cancelledRecords=0`, `terminalRecords=0`, `rejections=1` | `FencedExternalCancelTest` |
| 6 | `STARTED` gate per primitive | HOLDS for all three | retryable execute / `waitForEvent` / `sleep`: `startedRecords=0`, `terminalStepRecords=0`, `effectRuns=0`, `terminalRecords=0`, `rejections=2` | `FencedStepPrimitiveStartTest` |

### FND-10 — a retry attempt runs its action without an accepted append of its own — MEDIUM, deterministic

Case 1 is the only one that does not fully hold, and it is a real hole in the guarantee commit `f01f9a5d` introduced.

```
Outcome[recordsBeforeFence=5, recordsAfterFence=5, effectsBeforeFence=1, effectsAfterFence=2,
        terminalRecords=0, rejections=1]
```

The `STARTED` gate only runs when the step is not yet in the state: `execute` calls `tryStartStep` inside
`if (!workflowExecution.state().containsStep(stepName))` (`ExecuteDelegate.java:155-159`). A retry attempt reaches
`execute` with the step already present and status `RETRYING`, so it skips the gate entirely and runs the action with
nothing forcing the store to confirm the attempt is this execution's. Its outcome append is then rejected — the record
half of the fence holds, exactly as case 1 measures — but the external effect has already happened, on a node that no
longer owns the instance.

This is the FND-6 shape of hunt 1 (records fenced, action duplicated) surviving on the retry path. The ADR's
"duplicate side effects are prevented, not just detected" does not hold for a step under a retry policy.

Fix shape: give a retry attempt the same gate — publish and get an accepted `RETRYING` (or a per-attempt marker) before
invoking the action, so a fenced node's attempt is stopped by the store the way a first attempt is.

Pinned as an expected-gap assertion inside `FencedRetryBackoffTest`
(`effectsAfterFence > effectsBeforeFence`), which flips when the gate covers retries.

## Harness additions

- `ControllableEventStorageEngine.appendForeign`, `foreignWritesInLog`, `armForeignWriteBeforeCommitOf`,
  `armForeignWriteBeforeNextCommit`, `disarmFence`, `isFenceArmed`, `fencedInstance`.
- `EngineInstance.liveExecution(workflowId)` — needed to see the engine's projected state disagree with the durable log
  and to interrupt the driver in that window.
- `FenceOracles` — the per-instance counts every fence scenario reads off the durable log, plus the rejection appender.

## Still not tested, and why

- **The "RESTART while inside the `STARTED` gate" scenario H8 asked for.** Not built. The gate classifies an interrupt
  as a cancel and no `STARTED` is accepted in that window (table above), so the FAILED the hypothesis predicted cannot
  originate there; the reproduction budget went to the terminal-transition window, where it does originate and where it
  reproduces on the first attempt. A gate-specific scenario would still be worth having as a pin that the gate's
  classification does not regress.
- **The blind-write variant of case 2** (foreign write while parked, then the timeout fires on its own). Measured
  flaky on the landing evidence only: 1 rejection in 3 runs, with the safety oracle holding all three times. The
  shipped scenario arms the timeout's own commit instead, which makes the same fence deterministic. Recorded in the
  scenario's Javadoc.
- **Two-writer versions of cases 1–6.** All six use one engine plus a foreign write, which is the store-level
  equivalent; a real second engine would additionally exercise the routing and claim paths.
- **Non-in-memory stores.** Everything is `InMemoryEventStorageEngine`. FND-9's window exists independently of the
  store, but its timing does not.

## Commands and exit codes (addendum)

```
./mvnw -o -f workflow/axoniq-workflow-simulation/pom.xml test -Dtest=DstReproduceTest -Ddst.seed=22 -Dsurefire.failIfNoSpecifiedTests=false
    RUN 1 EXIT=1   [PayloadReducerSemantics]
    RUN 2 EXIT=1   [DeterministicReplay]
    RUN 3 EXIT=1   [PayloadReducerSemantics]

./mvnw -o -f workflow/axoniq-workflow-simulation/pom.xml test -Dtest=DstSmokeTest -Dsurefire.failIfNoSpecifiedTests=false
    RUN 1 EXIT=1   Tests run: 10, Failures: 2, Errors: 2  (case [1] PayloadReducerSemantics, case [2] DeterministicReplay)
    RUN 2 EXIT=1   Tests run: 10, Failures: 2, Errors: 1  (case [1] DeterministicReplay)

./mvnw -o -f workflow/axoniq-workflow-simulation/pom.xml test -Dsurefire.failIfNoSpecifiedTests=false -Dtest='FencedRetryBackoffTest,\
FencedParkedWaitTimeoutTest,FencedVersionMigrationTest,FencedPayloadWriteTest,FencedExternalCancelTest,\
FencedStepPrimitiveStartTest,InterruptedTerminalTransitionTest,FencedStepStartTest,FencedWriterParkTest,\
ParallelBranchFenceTest'
    RUN 1 EXIT=0   Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
    RUN 2 EXIT=0   Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
    RUN 3 EXIT=0   Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
```

An earlier revision of `InterruptedTerminalTransitionTest` failed run 1 of that set
(`Tests run: 12, Failures: 1`, "no attempt interrupted the driver inside the window") before the injection was changed
from an interval poll to a gated spin. The failure is recorded rather than overwritten: it is the evidence that the
window is short enough to miss, which is also why the sweep only hits it under load.
