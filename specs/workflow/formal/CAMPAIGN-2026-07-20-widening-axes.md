# Hunting campaign — widening axes (2026-07-20)

> Status 2026-09-07: `F20BuggifyInterleavingProbeTest` (section 5) was removed. The ≥2 duplicate mode it promoted is unreachable since the cancellation and termination rework; see the F-20 status note in `POC-TLA-DST.adoc`.

Branch `poc/tla_dst` (fast-forwarded onto the determinism-hardening phases 1-4 machinery,
commit `107e3540`). One variable at a time over the new DST machinery: the virtual-timeout
seam, the deterministic carrier, the schedule axis, and the widening axes
(swarm / storage faults / BUGGIFY). Engine untouched throughout (harness + docs + pins only).

## Verdicts at a glance

| # | Probe | Verdict |
|---|-------|---------|
| 1 | Schedule-axis sweep, 200 schedules | **All green** (precise negative) |
| 2 | Swarm sweep, 300 shapes | **All green**, zero max-steps aborts (precise negative) |
| 3 | BUGGIFY-amplified fuzz, 200 seeds | **All green** (precise negative — boundary bias alone reopens no duplicate) |
| 4 | DUPLICATED_APPEND depth | **Finding F-24**: engine replay idempotence HOLDS; INV-8/INV-12 assertions were not duplicate-robust — hardened |
| 5 | F-20 strict attempt (carrier + BUGGIFY) | **Promoted**: reproducible duplicates on every schedule — `F20BuggifyInterleavingProbeTest` is the strict acceptance |

## 1. Schedule-axis sweep (DstInterleavingFuzzTest)

World seed fixed at 41; interleaving seeds 0–199, chunked 4×50 (each chunk its own JVM,
under the eval-license wall-clock). `dst.startSeed` support added to the test (mirrors
`DstFuzzTest`).

```
./mvnw -pl workflow/axoniq-workflow-simulation -am test -Dtest=DstInterleavingFuzzTest -Ddst.excludedGroups= \
  -Ddst.seeds=50 -Ddst.startSeed=<0|50|100|150> -Dsurefire.failIfNoSpecifiedTests=false
```

Result: 200/200 green, chunk exits 0/0/0/0. No schedule-only invariant break on the smoke
workload. Nothing to triage (no red ever observed).

## 2. Swarm sweep (DstSwarmFuzzTest)

Seeds 0–299, chunked 6×50. Shapes drawn: 1–8 instances, 30–90 steps, p 0.2–0.9,
seed-chosen chaos-fault subsets, ~143 non-carrier / ~157 carrier (FIFO + seeded) modes.

```
./mvnw -pl workflow/axoniq-workflow-simulation -am test -Dtest=DstSwarmFuzzTest -Ddst.excludedGroups= \
  -Ddst.seeds=50 -Ddst.startSeed=<0..250 step 50> -Dsurefire.failIfNoSpecifiedTests=false
```

Result: 300/300 green; **no shape aborted at the max-steps cap with a non-terminal
instance** (no HARNESS-ABORT anywhere in the sweep). No liveness finding to quote
`virtualElapsed()` for.

## 3. BUGGIFY-amplified fuzz (DstBuggifyFuzzTest, new)

`DstFuzzTest`-shaped worlds (full fault set incl. write-then-vanish) with
`Buggify.activate(seed, 0.25)` around every world, deactivated in a `finally` (JVM-global
seam). Seeds 0–199, chunked 4×50.

```
./mvnw -pl workflow/axoniq-workflow-simulation -am test -Dtest=DstBuggifyFuzzTest -Ddst.excludedGroups= \
  -Ddst.seeds=50 -Ddst.startSeed=<0|50|100|150> -Dsurefire.failIfNoSpecifiedTests=false
```

Result: 200/200 green. `execution.append-task` perturbed ~165–180×/world;
`engine.live-switch` fired in the crashing seeds (19/150 sampled worlds drew it).
Precise negative: biasing the replay→live boundary and the enqueue-vs-drain window on the
standard workload reopens **no** F-7/F-13-class duplicate — the corruption class needs the
reused-names loop shape (see 5), not just scheduling pressure.

(One chunk initially failed on a compilation error — a build racing this campaign's own
in-flight harness edit, zero seeds executed; re-run clean. Not a finding.)

## 4. DUPLICATED_APPEND depth (DuplicatedAppendDepthProbeTest, new) → finding F-24

Harness extension: `ControllableEventStorageEngine.armDuplicateCommitFor(stepName, status)`
(the targeted-vanish pattern applied to the duplicate fault). Compositions probed, each
across crash+recovery (seeds 21–24, deterministic):

- duplicated `RETRYING` attempt record (terminal instance) — replay appends nothing, no
  effect re-runs;
- duplicated `RETRYING` **and** vanished terminal `FAILED` (non-terminal recovery), with a
  vanish-only control — the recovered engine behaves **identically with and without the
  duplicate** (one resume attempt, `FAILED`, workflow completes); the duplicate never
  changed an engine decision;
- duplicated `migrateVersion` marker — applied once (`putIfAbsent`), migrated branch ran
  exactly once, v1 branch never;
- duplicated combinator-decision record — decision stable, INV-14 green (it derives
  decisions, never counts records).

**F-24:** `Invariants.assertRetryBound` (INV-8) and `assertMigrateVersionContract`
(INV-12) counted raw log occurrences, so the store-level duplicate ALONE tripped them with
zero engine misbehaviour — a latent false-red in every campaign carrying
`DUPLICATED_APPEND` (chaos/swarm). The invariants' formal wording already stated set
semantics (`Cardinality({ e ∈ history(w) : … })`); the assertions were stricter than the
wording. Hardened (harness only): both count distinct committed events by event
identifier — an at-least-once store duplicate shares the identifier (counted once); a
genuine engine re-publish mints a NEW identifier (F-7/F-13/F-20 class stays detected).
Wording synced in `INVARIANTS.md`; pinned per-PR by the probe test.

## 5. F-20 strict acceptance (F20BuggifyInterleavingProbeTest, new)

Carrier (seeded interleaving) + BUGGIFY active simultaneously over the reused-names
live-lock (`LoopAndStormScenario.reusedNamesLiveLock`, buggify seed == interleaving seed,
p = 0.25). Where the plain-carrier probe yields 0 duplicates on all 8 schedules,
carrier+BUGGIFY reopens the window on **every** schedule.

Measured at promotion: 5 JVM runs × 2 passes × 8 schedules = **80/80 observations with
≥2 duplicate `retryDelay` TIMED_OUT terminals** (min 12, max 50 = `MAX_SPINS`, i.e. every
loop re-entry duplicated; never 0, never the fixed-engine 1). Schedule 0 saturated at 50
in 10/10 passes. Exact counts remain load-dependent (the documented enqueue-timing
residual); the presence is 100 % reproducible. Promoted seeds {0, 4, 7} pinned ≥2 in both
passes + the F-19/F-20 gap alphabet (≠1) on all schedules. The F-19+F-20 engine fix flips
this probe to exactly-1 everywhere — it is the strict acceptance test.

## Invariants across the whole campaign

Every always-on invariant (INV-1…, per `INVARIANTS.md`) held across all 700 fuzz/swarm/
schedule worlds and all depth probes. The only assertion-level trips observed were the
pre-hardening INV-8/INV-12 record-count trips over store-duplicated logs (F-24,
harness-caused, engine exonerated by the controls).

## Regression seeds

No fuzz/swarm/schedule seed ever went red, so `RegressionSeedsTest` gains no new seeds
from this campaign. The deterministic pins live in the two new probe tests
(`DuplicatedAppendDepthProbeTest` seeds 21–24; `F20BuggifyInterleavingProbeTest`
interleaving seeds 0–7, promoted {0, 4, 7}).
