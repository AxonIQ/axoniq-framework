# Deterministic Simulation Testing (DST) for the workflow engine

This module drives the **real** workflow engine through the Phase-3 determinism seams in a seeded,
single-threaded simulator, injecting faults (crash, restart, message reorder, clock jump,
write-then-vanish, plus the heavier **chaos** faults: duplicate-completed, event-store latency jitter,
flapping restart, clock skew, partial batch) and asserting the protocol invariants from
[`formal/INVARIANTS.md`](../formal/INVARIANTS.md) after every step. It is the implementation-level
counterpart to the TLA+ design check in [`formal/tla/`](../formal/tla): the leasing/crash-recovery
invariants (`AtMostOneOwner`, `AtMostOnceRecording`, `CommittedHistorySurvivesCrash`,
`DeterministicReplay`, `EventuallyTerminates`, `EffectAtMostOnce`) are asserted here under the names
`Invariants.assert*` / `Invariants.document*`, plus the DST-only INV-7..13 (`TerminalIsFinal`,
`RetryBound`, `TimeoutsFire`, `OneInstancePerStart`, `VersionRoutingSound`, `MigrateVersionContract`,
and `NoLostPayloadWrites` — e.g. INV-13: the final committed payload reflects every committed step's
contribution, so no committed payload write is lost across crashes/replays — see
[`formal/INVARIANTS.md`](../formal/INVARIANTS.md) for each invariant's TLA+ scope decision).

No production engine behaviour is changed by this module — it is harness + tests + CI only. It reuses
the Phase-3 seams (`ManualWorkflowScheduler`, `MutableClock`, `SeededWorkflowIdGenerator`) from the
`axoniq-workflow-test` module and wires an engine exactly like
`examples/simple/.../WorkflowReplayPreparedStateTest`.

## Layout

| Package | What |
|---|---|
| `harness/` | The seeded loop (`DstSimulation`), the simulated world (`SimulationWorld`, durable `ControllableEventStorageEngine` + `DurableTokenStore`), the engine builder (`EngineInstance`), config/result records, and bounded `Polling`. |
| `faults/` | The injected faults: `WorkerCrashFault`, `RestartFault`, `MessageReorderFault`, `ClockJumpFault`, `WriteThenVanishFault`, plus the **chaos** faults `DuplicateCompletedFault`, `EventStoreLatencyJitterFault`, `FlappingRestartFault`, `ClockSkewFault`, `PartialBatchFault` (drawn only by the chaos campaign — see below). |
| `invariants/` | `Invariants` — the `assert*` checks (names match `INVARIANTS.md` verbatim; INV-1..6 plus `assertTerminalIsFinal` (INV-7), `assertRetryBound` (INV-8), `assertTimeoutsFire` (INV-9), `assertOneInstancePerStart` (INV-10), `assertVersionRoutingSound` (INV-11), `assertMigrateVersionContract` (INV-12) and `assertNoLostPayloadWrites` (INV-13)) and the three `document*` helpers for the expected gaps (F-0 effect, F-1 ownership, F-1 record consequence). |
| `workflow/` | The test workflows: `OrderWorkflow` (reserve → charge → await-confirmation → sleep → ship; the fuzz workhorse, COMPLETED path), `CancellingWorkflow` (INV-7 terminal path), `RetryingWorkflow` (INV-8), `TimeoutWorkflow` (INV-9), `StartOnlyWorkflow` (INV-10), `VersionedOrderWorkflow` (INV-11, two versions), `MigratingOrderWorkflow` (INV-12, in-body `migrateVersion`), `PayloadOrderWorkflow` (INV-13, every step writes a distinct payload key), `CountingEffects` (effect counters that survive a crash), `SimulationEvents`. |
| `scenarios/` | Hand-authored deterministic reproductions: `WriteThenVanishScenario` (F-0 / `MC_effect.cfg`), `SplitBrainScenario` (F-1 / `MC_owner.cfg` + the F-1 record consequence / `MC_record.cfg`), `TerminalIsFinalScenario` (INV-7 across crash/replay; its start-event-redelivery probe surfaces finding **F-3**), and the DST-only INV-8..13 scenarios (`Inv8RetryBoundScenario`, `Inv9TimeoutsFireScenario`, `OneInstancePerStartScenario`, `VersionRoutingSoundScenario`, `MigrateVersionContractScenario`, `NoLostPayloadWritesScenario`). |

## Running it

### Per-PR smoke (fast, the default)

```bash
./mvnw -pl workflow/axoniq-workflow-simulation -am test
```

Runs the fixed smoke seed set plus the F-0 / F-1 documented-gap reproductions and the anti-hang
self-check. Finishes in well under a minute (the simulation tests themselves are ~10 s). The long
fuzz is tagged `fuzz` and excluded from this run.

`-am` is required: the harness depends on the `runtime`, `dsl` and `test` modules being built.

### Reproduce one seed

The single-seed reproduction entry point (the command the harness prints in its diagnostic on any
break) re-runs a seed twice and asserts the run is bit-for-bit reproducible:

```bash
./mvnw -pl workflow/axoniq-workflow-simulation -am test -Dtest=DstReproduceTest -Ddst.seed=42
```

`-Ddst.seed` defaults to `0`. This test is deliberately **not** `@Tag("fuzz")`, so it runs without
having to clear any exclusion.

### Nightly fuzz (many seeds)

The long fuzz explores many seeds across the full fault set (including write-then-vanish). It is
gated behind the `fuzz` JUnit tag, which the module excludes by default via the overridable
`dst.excludedGroups` property. Clear it and scope the run to the fuzz class:

```bash
./mvnw -pl workflow/axoniq-workflow-simulation -am test \
  -Ddst.excludedGroups= -Dtest=DstFuzzTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddst.seeds=2000
```

`-Ddst.seeds` sets how many seeds to explore (default 25 for a quick manual run).
`-Dsurefire.failIfNoSpecifiedTests=false` stops the `-am` dependency modules — which have no
`DstFuzzTest` — from failing the run.

### Chaos fuzz (heaviest campaign)

`DstChaosFuzzTest` is a sibling of `DstFuzzTest` (also `@Tag("fuzz")`, excluded from the default
build) that drives the heaviest configuration, `SimulationConfig.chaos(seed)`: **more instances** (10
`OrderWorkflow` + the 7 fixed singletons = 17 instances), **more steps** (90), a **higher fault
probability** (0.85), a **bigger wall-clock deadline** (240 s — ample headroom so a CI load pause
cannot false-abort the heavier crash-loop work), and the full **chaos fault set** `CHAOS_FAULTS`:
every existing fault plus five new chaos faults:

| Chaos fault | What it does | Stresses |
|---|---|---|
| `DuplicateCompletedFault` | re-delivers the external trigger (start / wakeup) of an **already-terminal** workflow | INV-2 / INV-7 / INV-10 dedup under duplicate delivery |
| `EventStoreLatencyJitterFault` | extra seeded settle nudges + an aggressive pending-batch shuffle | INV-4 / ordering robustness (F-2 surface) |
| `FlappingRestartFault` | several rapid `crashAndRecover` cycles, re-asserting INV-3 after **each** | INV-3 / INV-5 under repeated restarts |
| `ClockSkewFault` | a forward clock jump of a varied magnitude (sub-second to many hours) | INV-5 / `orTimeout` timer math |
| `PartialBatchFault` | delivers only part of this step's pending batch, deferring the tail | INV-2 / ordering |
| `DuplicatedAppendFault` | records the next durable commit TWICE in the recovery log (at-least-once store); the dup surfaces on crash+recovery replay | INV-2 replay-idempotence / INV-4 |

Clock skew is **forward-only**: the `MutableClock` + `ManualWorkflowScheduler` virtual time is
monotonic (a backward jump would violate the scheduler's non-decreasing due-time contract), so skew
is modelled as varied-magnitude forward jumps rather than varied direction.

```bash
./mvnw -pl workflow/axoniq-workflow-simulation -am test \
  -Ddst.excludedGroups= -Dtest=DstChaosFuzzTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddst.seeds=500
```

The chaos fuzz, like the fuzz, asserts every invariant after every step but does **not** claim
bit-for-bit seed equality (the `ALL_FAULTS`/racy-fault precedent — it includes the racy
write-then-vanish and the crash-loop flapping restart). A two-engine-over-shared-store *segment-claim
contention* fault was deliberately **not** added to this set: it would re-surface the known split-brain
gap **F-1** (no durable cross-node lease) and turn the general chaos campaign red over a documented
gap. F-1 stays covered by its dedicated expected-violation tests (`F1SplitBrainTest` /
`F1RecordDuplicationTest` / `SplitBrainScenario`).

## Anti-hang / deadline design

The harness is **structurally incapable of hanging** — it never relies on the JUnit `@Timeout` as
its primary stop. The guards, all in `harness/DstSimulation`:

1. **Hard global wall-clock deadline per `run()`** — the PRIMARY stop. A `Deadline` is created at the
   top of `run()` from `SimulationConfig.wallClockDeadline()` (60 s smoke / 60 s fuzz / 240 s chaos —
   the chaos deadline is large because a chaos run does many crash+recover cycles over 17 instances
   and must have ample headroom against CI load pauses; the smoke deadline was bumped from 30 s to
   60 s purely for headroom as the smoke instance count grew to 10, reducing green-gate flake). The
   seeded loop calls `deadline.checkNotExpired(...)` every iteration (main loop and horizon); on
   expiry it throws an enriched `InvariantViolation` (`HARNESS ABORT`) carrying the seed, the fault
   trace, the committed log and the reproduce command — it never blocks past the deadline.
2. **Settle-aware max-steps cap** — the second guard. `SimulationConfig.maxSteps()` bounds the main
   loop in **effective steps**: only an iteration whose `settle(...)` reached genuine quiescence
   (committed log stable for the quiet window, pending timers drained or established never-due — the
   settle's own outcome) is charged against the cap; an iteration where the engine's durably-async
   work still lagged under machine load is not (the fix for the former nightly "seed-339 class"
   load-flake, where a loaded fuzz could spuriously abort at the cap on a seed that passes in
   isolation). Quiescence feeds **only** this abort accounting — the iteration structure and the
   per-iteration seeded fault draws are untouched, so seed reproducibility is preserved by
   construction. Exhausting the cap (or the absolute backstop of 10× `maxSteps` raw iterations, which
   keeps the loop provably finite even if quiescence detection were wrong) with any instance still
   non-terminal aborts with the same enriched diagnostic, treated as a potential liveness finding
   (a spin or stall), not a silent success.
3. **Every wait/poll is bounded with a short budget, clamped to the deadline.** `settle(...)` waits
   at most `SETTLE_BUDGET` (4 s) and `Deadline.clamp(...)` further trims that to whatever remains of
   the global deadline, so no single wait can outlive the hard stop. If a settle does not quiesce
   before the global deadline it aborts with the diagnostic; if only the per-settle budget elapsed
   (e.g. a transient CI pause) it logs and continues, and the loop's next `checkNotExpired` catches a
   genuine stall. The crash/recovery "pump" inside settle nudges virtual time a bounded number of
   times (`MAX_NUDGES_PER_SETTLE`) so a never-due timer cannot make it spin.
4. **JUnit `@Timeout` is only a last-resort backstop**, set comfortably *above* the harness deadline
   (e.g. 120–180 s on the smoke tests vs. the 30 s harness deadline). It should never be what stops a
   run; `DstSmokeTest.wallClockDeadlineAbortsRatherThanHangs` proves the harness deadline fires first.

## Determinism level (honest)

A seed fixes the **inputs**: every fault choice and parameter (single seeded `Random`), every workflow
id (`SeededWorkflowIdGenerator`), and every timer (virtual-time `ManualWorkflowScheduler` +
`MutableClock` advanced in lock-step). Each instance's per-instance FIFO task queue still evolves that
instance's state in delivery order.

A seed does **not** fix the **verdict**. The engine's event processor runs one work package per
segment and defaults to more than one, so several delivery threads race the body threads; which
deliveries land, and when, varies between runs of the same seed. Two runs of one seed can therefore
reach different outcomes. Measured, not assumed: in a single JVM, seed 1 left three different sets of
instances non-terminal across `DstSmokeTest.allInvariantsHold`, `.sameSeedIsReproducible` and
`.smokeSeedSetSummary`, and `F20BuggifyInterleavingProbeTest` reported per-schedule duplicate counts of
`{0=26, 1=29, 2=50, 3=50, 4=0, 5=50, 6=50, 7=0}` on one pass and `{0=0, 1=50, 2=50, 3=50, 4=1, 5=0,
6=0, 7=1}` on the next.

Pinning `DurableTokenStore` to a single segment was measured as the remedy and **rejected**: on the
same 40-class set it leaves 14 classes red against 9 at the engine default, and the 9 are a strict
subset. Restoring seed-to-verdict reproducibility needs the segment count to be settable on the
processor the harness builds, not clamped in the token store.

Older residuals / what was already not claimed:

- **Cross-instance interleaving of the single global log is deliberately factored out** (it is the
  F-2 surface — `findAll()` Set order, body thread scheduling; `ARCHITECTURE.md` §8/§11). Distinct
  instances are independent, so determinism is asserted *per instance* (`fingerprint` /
  `perWorkflowEvents` group by `workflowId`), not on the raw global order.
- **The body executor is left at the engine default (virtual-thread-per-task), not a same-thread
  executor.** The engine's body blocks on its per-instance task queue waiting for events the
  processor thread delivers, so a literal same-thread executor deadlocks the body against its own
  queue. The `settle(...)` wall-clock quiet window absorbs *some* of the resulting scheduling jitter
  — not all of it, which is the second half of the verdict non-reproducibility above. This is the one
  place the harness uses bounded
  polling rather than pure logical stepping. (Documented on `EngineInstance`.)
- ~~Two Phase-3 seams stay un-injected~~ **Closed by Phase 1' (determinism seams)**: `ExecuteDelegate`'s
  per-attempt timeout now goes through `WorkflowScheduler.withTimeout` (a default method built on
  `delayedExecutor`), so `ManualWorkflowScheduler` fires it on virtual-time advance instead of the JDK's
  real-time `orTimeout` Delayer; and `ProcessingContextUtils` now honors the caller-supplied UoW id
  (the workflowId / a seeded `WorkflowIdGenerator` id) instead of minting `UUID.randomUUID()`. Phase 1'
  also made multi-match wait-condition dispatch registration-ordered (`EventWaitConditions`) and
  `findAll()` iteration workflowId-ordered (`InMemoryWorkflowExecutionRepository`), removing the two
  hash-order nondeterminism sources called out in `ARCHITECTURE.md` §8/§11.
- **The event-timestamp era and the virtual-clock era differ by default (the D5 era gap) — with an
  OPT-IN aligned mode.** Committed event timestamps come from the JVM-global static Axon clock
  (`GenericEventMessage.clock`, wall era) while the injected engine clock starts at the Unix epoch,
  so the engine's recorded-STARTED-vs-now timer math sees ~+56 years of remaining time — which is
  exactly what keeps the default workload's 5 s per-attempt step timeouts from ever firing under the
  CLOCK_SKEW/CLOCK_JUMP faults (the default-mode scenarios *depend* on the gap; it is not changed).
  Scenarios that need exact, multi-step timeout pacing opt in to the **aligned-clock mode**
  (`SimulationWorld(seed, registration, true)`): the static event-timestamp clock is pointed at the
  world's `MutableClock` for the world's lifetime (restored in `close()`), making both eras one, so
  a 10 s wait timeout fires on a plain 10 s `advanceTime` — no era anchoring — and several timeouts
  in one body are deterministically drivable (pinned by the aligned-clock multi-iteration pin in
  `LoopAndStormTest`). The static is JVM-global, so an aligned world must not run concurrently with
  any other world in the same JVM (the suites are single-threaded per JVM).

## Phase 2: the deterministic carrier (opt-in)

`SimulationConfig.smoke(seed).withDeterministicCarrier(interleavingSeed)` runs every engine-executor task on
ONE carrier thread (`DeterministicVirtualThreadExecutor`, test-module fakes): `null` = FIFO pick,
a seed = reproducible seeded-interleaving fuzz. Measured level (see `POC-TLA-DST.adoc` Phase 2'): a
single-instance world is **globally** bit-for-bit under FIFO; the full multi-instance loop keeps the
per-instance contract (content identical to the production executor) while the global order still varies with
Axon processor delivery timing — scheduling the processor side onto the carrier is the next increment.
Requires `--add-opens java.base/java.lang=ALL-UNNAMED` (wired in this module's surefire config).

Phase 3 on top of it: `SimulationResult.globalFingerprint()` (whole-log order metric),
`SimulationResult.virtualElapsed()` (virtual-time liveness accounting; the smoke workload consumes ~3 virtual
minutes in under a second of wall time), the per-PR `DstCarrierSmokeTest` (carrier is content-neutral), and the
nightly `DstInterleavingFuzzTest` (`-Ddst.seeds=<N>` seeded schedules, world seed fixed;
`-Ddst.startSeed=<S>` chunks a large sweep, the `DstFuzzTest` precedent).

Phase 4 widening: `SimulationConfig.swarm(seed)` (the SHAPE — instances, steps, probability, fault subset,
carrier mode — is a pure function of one seed; campaign `DstSwarmFuzzTest`), the `DUPLICATED_APPEND` chaos
fault (untargeted `armDuplicateNextCommit` + targeted `armDuplicateCommitFor(stepName, status)` for depth
probes — see `DuplicatedAppendDepthProbeTest` / finding F-24), and the BUGGIFY seam
(`Buggify.activate(seed, p)` → in-engine scheduling-bias points
`engine.live-switch` + `execution.append-task`; production = no-op). Per-PR pins: `DstWideningSmokeTest`.
The BUGGIFY-amplified campaign is `DstBuggifyFuzzTest` (`@Tag("fuzz")`, `-Ddst.seeds`/`-Ddst.startSeed`):
the full-fault fuzz shape with BUGGIFY active around every world. The strict F-20 acceptance probe
(`F20BuggifyInterleavingProbeTest`, per-PR) drives the reused-names live-lock under carrier + BUGGIFY
simultaneously — the combination that reopens the F-20 duplicate-terminal window (the engine fix flips it
to exactly-1 per schedule). Its "reproducibly" premise no longer holds: the carrier pins the body side
only, and the processor now runs one work package per segment, so the per-schedule counts move between
passes (see "Determinism level"). The window still reopens; the count no longer repeats.

## The expected gaps (documented, not failures)

These are **expected** protocol gaps confirmed by TLA+; the tests assert the gap is *present* so the
per-PR build stays green, and a future fix that closes a gap will make the corresponding test fail —
the intended signal to re-evaluate. Each bridges a named TLA+ counterexample cfg in
[`formal/tla/`](../formal/tla/README.md#tla--dst-bridge).

- **F-0 / `EffectAtMostOnce`** (`F0EffectDuplicationTest` + `WriteThenVanishScenario`, seeds 1/2/3 →
  TLA+ `MC_effect.cfg`). A crash in the window after an `execute` action ran but before its
  `COMPLETED` event committed re-runs the action on replay: the effect is at-least-once, not
  at-most-once. The scenario arms the durable store to drop the `chargePayment` `COMPLETED` commit,
  crashes, recovers, and asserts the charge effect ran **exactly twice** while at-most-once
  *recording* (INV-2) still holds (≤1 terminal record). This is the implementation-level match of the
  TLA+ `EffectAtMostOnce` counterexample.
- **F-1 / `AtMostOneOwner`** (`F1SplitBrainTest` + `SplitBrainScenario`, seed 0 → TLA+
  `MC_owner.cfg`). Two engine instances over a shared event store, each with its own non-durable
  in-memory processor token, both claim segment 0 and both drive the same instance — split-brain. The
  test asserts ≥2 concurrent owners.
- **F-1 consequence / `AtMostOnceRecording`** (`F1RecordDuplicationTest` + `SplitBrainScenario`,
  seed 0 → TLA+ `MC_record.cfg`). INV-2 holds only *given* INV-1. The same two-engine split-brain
  setup, with no append-condition on the `COMPLETED` write (the `ExecuteDelegate` FIXMEs), drives the
  first `execute` step (`reserveInventory`) to `COMPLETED` on **both** engines, so two terminal
  records for one step land on the shared durable log (`⟨STARTED, COMPLETED, COMPLETED⟩`). The test
  asserts ≥2 terminal records — the implementation-level match of the TLA+ `AtMostOnceRecording`
  counterexample. The optimistic append-condition fix (`MC_record_fixed.cfg`) would close it.

## CI

[`.github/workflows/dst.yml`](../.github/workflows/dst.yml) adds two jobs (it does not alter the
existing module build in `main.yml` / `pullrequest.yml`, which already compiles this always-on
module):

- `dst-smoke` — on every pull request and push to `main`, JDK 21 + 25: runs the fast smoke suite.
- `dst-fuzz` — nightly cron (and manual dispatch): clears the fuzz exclusion and explores many seeds.
