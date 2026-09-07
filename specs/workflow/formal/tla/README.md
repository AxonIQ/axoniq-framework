# TLA+ design check — task leasing + crash recovery (Phase 2)

A small, abstract TLA+ model of the `axon-flow-spec` engine's **task-leasing +
crash-recovery** protocol, checked with TLC. It encodes the six invariants from
[`../INVARIANTS.md`](../INVARIANTS.md) (using their exact `MachineName`s) and
reproduces, at the design level, the three Phase-0 findings F-0/F-1/F-2.

This models **only** leasing + recovery (not the whole engine), at tiny scope (2
processes, 3 ordered steps, one workflow instance, bounded log + bounded crash
count) so TLC terminates in ~1 s per run. It is a **design model**, not a
transliteration of the Java code; it follows
[`../ARCHITECTURE.md`](../ARCHITECTURE.md) §2 (leasing), §5 (replay/recovery),
§7 (idempotency/dedup) and §11 (nondeterminism).

## Files

| File | Purpose |
|---|---|
| `Sanity.tla` / `Sanity.cfg` | Step A smoke test: a 1-variable counter + trivial invariant that proves the TLC + `.cfg` wiring before the real model. |
| `WorkflowLeaseRecovery.tla` | The protocol model: state, actions, flags, and the six invariants. |
| `MC.tla` | Model-checking harness: pins the tiny scope and the process-symmetry set. |
| `MC_safe.cfg` | INV-2/3/4 under single-writer + fix on. Expect **No error**. |
| `MC_effect.cfg` | `EffectAtMostOnce` with a crash in the apply→commit window, fix **off**. Expect **VIOLATED** (F-0). |
| `MC_effect_fixed.cfg` | Same with the outbox/append-condition fix **on**. Expect **No error**. |
| `MC_owner.cfg` | `AtMostOneOwner` with the non-durable lease. Expect **VIOLATED** (F-1). |
| `MC_owner_fixed.cfg` | Same with a durable lease. Expect **No error**. |
| `MC_record.cfg` | `AtMostOnceRecording` under split-brain + no append-condition. Expect **VIOLATED** (F-1 consequence). |
| `MC_record_fixed.cfg` | Same with the append-condition on (lease still broken). Expect **No error**. |
| `MC_live.cfg` | `EventuallyTerminates` (temporal, under fairness). Expect **No error**. |
| `CombinatorReplay.tla` | Standalone tiny model of the combinator categorization across crash/replay (F-22). Run with `-deadlock` (it terminates). |
| `MC_combinator.cfg` | `CategoriesReplayStable`, snapshot fix **off**. Expect **VIOLATED** (F-22). |
| `MC_combinator_fixed.cfg` | Same with the record-at-decision fix **on**. Expect **No error**. |
| `HookOnCompletion.tla` | Standalone tiny model of the terminal COMPLETED hook drop on the happy completion path (F-4). Run with `-deadlock` (it terminates). |
| `MC_hook.cfg` | `StatusHookFiresOncePerStatus`, await-terminal-state fix **off**. Expect **VIOLATED** (F-4). |
| `MC_hook_fixed.cfg` | Same with the await-terminal-state fix **on**. Expect **No error**. |
| `PayloadRepublish.tla` | Standalone tiny model of the `modifyPayload` duplicate terminal record across crash + re-run (F-7). Run with `-deadlock` (it terminates). |
| `MC_payload.cfg` | `AtMostOnceRecording`, `!containsStep` publish gate **off**. Expect **VIOLATED** (F-7). |
| `MC_payload_fixed.cfg` | Same with the `!containsStep` publish gate **on**. Expect **No error**. |
| `BackoffCancel.tla` | Standalone tiny model of a cancellation racing a retry-backoff launch (F-23). Run with `-deadlock` (it terminates). |
| `MC_backoffcancel.cfg` | `CancelledStepDoesNotRun`, backoff-cancellation wiring **off**. Expect **VIOLATED** (F-23). |
| `MC_backoffcancel_fixed.cfg` | Same with `CANCEL_COVERS_BACKOFF` **on**. Expect **No error**. |
| `MarkerChain.tla` | Standalone model of the DCB append-condition fencing (per-writer `ConsistencyMarker` chain, spawn from ORIGIN, restore seeds from head, rejection interrupts) — the richer successor of the `MC_record` pair. `CHECK_DEADLOCK FALSE` is in the cfgs. |
| `ClaimSeed.tla` | Standalone model of the position a segment claim seeds each restored instance with: per instance (its own read) versus one transaction shared by the claim (the lowest of its reads). `CHECK_DEADLOCK FALSE` is in the cfgs. |
| `MC_markerchain.cfg` | Fencing **on** (conditional appends). `OneStartPerInstance`, `AtMostOnceRecording`, `NoWriteAfterOwnershipLoss`, `SiblingNoFalseConflict` + `RejectionNeverAdvancesMarker` (PROPERTY). Expect **No error**. |
| `MC_markerchain_broken.cfg` | Fencing **off** (`ConditionalAppends=FALSE`, unconditional appends). Expect **VIOLATED** (`OneStartPerInstance` at the shallowest state; `AtMostOnceRecording` violable too — refutability proof). |
| `MC_markerchain_regress.cfg` | Fencing **on** + `MarkerRegress` (the `ConsistencyMarkerSupport.updateAppendPosition` lost-update race moves a marker backward). Expect `SiblingNoFalseConflict` **VIOLATED** (a lone writer is rejected by its own history). |

`tools/tla2tools.jar` (TLC2 Version 2.19) is the checker. `states/` (the TLC
metadir) and `*.toolbox` are git-ignored.

## Reproduce every run

All commands run **from the worktree root**, no `cd` (TLC resolves `EXTENDS`ed
modules from the spec file's directory):

```sh
# Step A — smoke test (expect: Model checking completed. No error has been found.)
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/Sanity.cfg formal/tla/Sanity.tla

# Step C.1 — INV-2 / INV-3 / INV-4 hold under single-writer + fix on (No error)
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_safe.cfg formal/tla/MC.tla

# Step C.2 — F-0: EffectAtMostOnce VIOLATED (crash in the STARTED->COMMIT window)
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_effect.cfg formal/tla/MC.tla

# Step C.3 — F-1: AtMostOneOwner VIOLATED (non-durable lease admits two claimers)
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_owner.cfg formal/tla/MC.tla

# F-1 consequence — AtMostOnceRecording VIOLATED under split-brain + no append-condition
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_record.cfg formal/tla/MC.tla

# Step C.4 — EventuallyTerminates (temporal, under fairness) (No error)
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_live.cfg formal/tla/MC.tla

# Step C.5 — the fixes work (all No error):
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_effect_fixed.cfg formal/tla/MC.tla   # F-0 fix
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_owner_fixed.cfg formal/tla/MC.tla    # F-1 fix
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_record_fixed.cfg formal/tla/MC.tla   # append-condition closes the record race
```

## Model overview

One workflow instance is driven by up to two processes (`Procs = {p1, p2}`) over
three ordered steps (`Steps = {1, 2, 3}`).

**Durable substrate (survives a crash)**
- `log` — an append-only sequence of step records `[step |-> s, status |-> st]`,
  `status ∈ {STARTED, COMPLETED}`. This is the Axon `EventStore`
  (ARCHITECTURE §4). Recovery rebuilds state from it.

**Volatile (lost on a crash)**
- `holds[p]` — does `p` hold the single-segment processing lease (ARCHITECTURE
  §2)? Modelled **per-process** (not a single holder) because the engine's
  processor `TokenStore` is hardcoded in-memory and per-process
  (`AllEventEventHandlingComponent.java:70`): each node has its own token, so
  without a durable lease two nodes can each believe they own segment 0.
- `vol[p]` — `p`'s in-memory view of step records, rebuilt from `log` on
  recovery.
- `applied[p]` — the set of steps whose external effect **this incarnation** of
  `p` has run. Volatile and **not** rebuilt from the log — this is the crux of
  F-0.

**Watched counter**
- `effect[s]` — how many times step `s`'s external side effect has run
  (`ExecuteDelegate.java:148` `action.apply`). `EffectAtMostOnce` watches it.

**Actions** (`Next`)
- `Claim(p)` / `Recover(p)` — acquire the lease and rebuild `vol[p]` from `log`
  (cold start vs. post-crash; identical rebuild, kept distinct for trace
  labels). With `DURABLE_LEASE` they require that no other process holds the
  lease; otherwise the acquisition is permissive (split-brain).
- `ApplyEffect(p, s)` — the owner runs step `s`'s side effect: records `STARTED`
  in `log` (if absent), increments `effect[s]`, marks `s` in `applied[p]`. Kept
  **separate** from the commit so a crash can interleave between them (the F-0
  window). On a post-crash resume the step is still `STARTED` in `log` while
  `applied[p]` is empty, so the effect runs again — unless `APPEND_CONDITION`
  binds the effect to the (idempotent) commit.
- `CommitCompleted(p, s)` — append `COMPLETED(s)` (the at-most-once *recording*
  point). Terminal-ness is decided from `p`'s **own** view, mirroring the
  engine's per-process publish/evolve guards. With `APPEND_CONDITION` an
  optimistic guard additionally requires `vol[p] = RebuildFromLog` and a fresh
  durable check, rejecting a stale split-brain writer.
- `Crash(p)` — drop `p`'s `vol`, `applied`, and lease; keep `log`. Bounded by
  `MaxCrashes`.
- `Done` — terminal idle: stutters once every step is `COMPLETED`, so TLC does
  not flag the legitimate terminal state as a deadlock.

**Flags (CONSTANTS, toggled per `.cfg`)**
- `DURABLE_LEASE` — `TRUE` ⇒ lease acquisition is mutually exclusive (durable
  single-writer); `FALSE` ⇒ the non-durable in-memory token admits a second
  claimer (F-1).
- `APPEND_CONDITION` — `TRUE` ⇒ the FIXME fix: the `COMPLETED` append is
  conditional on the log not having changed under the writer (optimistic
  append, `ExecuteDelegate.java:163…`), **and** the effect is bound to the
  commit (an outbox), so a replayed `STARTED` step does not re-run its effect.
  `FALSE` ⇒ today's behaviour (F-0, and the F-1 record race).

**Invariants** (names verbatim from `../INVARIANTS.md`): `AtMostOneOwner`,
`AtMostOnceRecording`, `CommittedHistorySurvivesCrash`, `DeterministicReplay`,
`EffectAtMostOnce` (safety), `EventuallyTerminates` (liveness, temporal under
`FairSpec`). `CommittedHistorySurvivesCrash` is an action property
(`[][Crash ⇒ log'=log]`), so it is declared as a `PROPERTY`, not an `INVARIANT`.

## TLA+ ↔ DST bridge

Each TLA+ counterexample is reproduced at the implementation level by a named, seeded DST scenario in
the `simulation` module (Phase 4), asserted by the matching `Invariants.*` method whose name is the
invariant's `MachineName` verbatim. The three *expected-violation* runs (F-0, F-1, the F-1 record
consequence) are asserted in DST as **documented gaps** — the test expects the gap to be *present* so
the per-PR build stays green; the corresponding `*_fixed.cfg` shows the fix, and a future engine fix
that closes the gap will make the DST test fail, the intended signal to re-evaluate. This table is the
authoritative co-located bridge; the [`../INVARIANTS.md` Cross-reference
contract](../INVARIANTS.md#cross-reference-contract) is its per-invariant twin.

| TLA+ cfg / counterexample | INV (MachineName) | DST scenario class | Test | Seed(s) | Expected outcome |
|---|---|---|---|---|---|
| `MC_effect.cfg` — `EffectAtMostOnce` **VIOLATED** (crash in STARTED→COMMIT, fix off) | INV-6 `EffectAtMostOnce` (F-0) | `WriteThenVanishScenario` | `F0EffectDuplicationTest` | 1, 2, 3 | effect runs **exactly 2×** (at-least-once), while INV-2 still holds (≤1 terminal record) — `documentEffectAtMostOnceGap` |
| `MC_effect_fixed.cfg` — same, fix **on** (No error) | INV-6 `EffectAtMostOnce` | — (fix not yet in engine) | — | — | the F-0 fix (outbox / commit-bound effect); DST test flips to failing once landed |
| `MC_owner.cfg` — `AtMostOneOwner` **VIOLATED** (non-durable lease) | INV-1 `AtMostOneOwner` (F-1) | `SplitBrainScenario` | `F1SplitBrainTest` | 0 | **2** engines both own segment 0 and route the start event — `documentSplitBrainOwnership` |
| `MC_owner_fixed.cfg` — same, durable lease (No error) | INV-1 `AtMostOneOwner` | — (fix not yet in engine) | — | — | the F-1 fix (durable, claim-fenced lease) |
| `MC_record.cfg` — `AtMostOnceRecording` **VIOLATED** (split-brain + no append-condition) | INV-2 `AtMostOnceRecording` (F-1 consequence) | `SplitBrainScenario` | `F1RecordDuplicationTest` | 0 | the same step gets **2** terminal `COMPLETED` records on the shared durable log (`⟨STARTED, COMPLETED, COMPLETED⟩`) — `documentDuplicateRecordingUnderSplitBrain` |
| `MC_record_fixed.cfg` — same, append-condition **on** (No error) | INV-2 `AtMostOnceRecording` | — (fix not yet in engine) | — | — | the optimistic append-condition closes the record race independent of the lease |
| `MC_safe.cfg` — INV-2/3/4 hold under single-writer + fix (No error) | INV-2/3/4 | `WriteThenVanishScenario` (INV-2 holds despite F-0); DST harness | `F0EffectDuplicationTest`; `DstReproduceTest` / `DstSmokeTest` | 1,2,3; harness seeds | `assertAtMostOnceRecording`, `assertCommittedHistorySurvivesCrash`, `assertDeterministicReplay` all pass |
| `MC_live.cfg` — `EventuallyTerminates` (temporal, fairness) (No error) | INV-5 `EventuallyTerminates` | DST harness | `DstSmokeTest` / `DstReproduceTest` | harness seeds | every instance terminal by the virtual-time horizon — `assertEventuallyTerminates` |
| `MC_combinator.cfg` — `CategoriesReplayStable` **VIOLATED** (categories recomputed per run, snapshot off) | F-22 (INV-14's categorization facet) | `CombinatorReplayScenario` | `CombinatorReplayTest.allMatch_…` | 0 | the live pass sees the in-flight branch in `unmatched()`; the recovered re-run re-categorizes it into `matched()` |
| `MC_combinator_fixed.cfg` — same, record-at-decision **on** (No error) | F-22 | — (fix not yet in engine) | — | — | recording the categorization durably at decision time makes the replayed observation identical |
| `MC_hook.cfg` — `StatusHookFiresOncePerStatus` **VIOLATED** (the happy path's `taskQueue.clear()` drops the queued notify task — the COMPLETED hook fires **zero** times; the awaited fail/cancel/timeout exception path holds); `MC_hook_fixed.cfg` (`AWAIT_TERMINAL_STATE=TRUE`) → **No error** | INV-17 `StatusHookFiresOncePerStatus` (**F-4**) | `StatusHookFiresOncePerStatusScenario` | `Inv17StatusHookFiresOncePerStatusTest` | 0 | the terminal COMPLETED hook is droppable on the happy path — `documentTerminalHookMayBeDropped` — while the no-re-fire / at-most-once facet holds; the modeled fix (await the terminal state change before `finishWorkflow`, symmetric with the awaited terminal paths) closes the drop |
| `MC_payload.cfg` — `AtMostOnceRecording` **VIOLATED** (models the old behaviour: crash between the step's COMPLETED commit and the workflow's own terminal commit; the re-run re-publishes the ungated `modifyPayload` step's COMPLETED — the gated execute-style contrast step stays at 1); `MC_payload_fixed.cfg` (`CONTAINS_STEP_GATE=TRUE`) → **No error** | INV-2 `AtMostOnceRecording` (**F-7 — FIXED**) | `DuplicateTerminalPayloadRecordScenario` | `F7DuplicatePayloadRecordTest` | 0 | **F-7 fixed:** `modifyPayload` now gates its publish on `!containsStep` (the replay-skip gate the other primitives have), so the post-crash live re-run keeps the `finalizePayload:COMPLETED` count at 1 and `assertAtMostOnceRecording` **holds**; the same window on an `execute` step likewise stays at 1 (the fixed `modifyPayload` matches it). `MC_payload.cfg` retains the pre-fix violated counterexample. |
| **DST-only — combinators out of model scope** | INV-14 `CombinatorConsistency` | `CombinatorConsistencyScenario` | `Inv14CombinatorConsistencyTest` | 0 | each combinator's recorded decision (post-combinator step + reconstructed-payload boolean) equals the documented short-circuit semantics over the branches' committed outcomes, stable across crash/replay — `assertCombinatorConsistency`. Not modelled in TLA+ (no combinators / parallel branch steps / `matched()`/`unmatched()` in the leasing + crash-recovery spec — see "Scope & limits"). |
| **DST-only — event correlation out of model scope** | INV-15 `EventCorrelationExact` | `EventCorrelationExactScenario` | `Inv15EventCorrelationExactTest` | 0 | an `associate(...)`-correlated event wakes EXACTLY the matching waiting instance (every completed wait recorded `matchedKey == its own key` — no cross-wakeup) and completes at most once for the right key; uncorrelated/duplicate events produce no spurious completion; stable across crash/replay — `assertEventCorrelationExact`. Not modelled in TLA+ (no event correlations / associations / `waitForEvent` association key / multi-instance event routing in the leasing + crash-recovery spec — see "Scope & limits"). |
| **DST-only — failure propagation out of model scope** | INV-16 `FailurePropagation` | `FailurePropagationScenario` | `Inv16FailurePropagationTest` | 0 | a step failure propagates to a terminal FAILED workflow status (failing step recorded terminally-failed, instance reaches FAILED — never COMPLETED, never stuck — and no step after the failing one begins), stable across crash/replay — `assertFailurePropagation`. Covers both the no-retry uncaught-exception path and the retry-exhaustion path, propagated via `ctx.fail`. Not modelled in TLA+ (no workflow-level FAILED status / step-failure status / `ctx.fail` terminal primitive in the leasing + crash-recovery spec — see "Scope & limits"). Deterministic scenario only, scenario-pinned like INV-9. |
| **DST-only — lifecycle hooks out of model scope** | INV-17 `StatusHookFiresOncePerStatus` (**F-4** candidate) | `StatusHookFiresOncePerStatusScenario` | `Inv17StatusHookFiresOncePerStatusTest` | 0 | a registered status-change hook fires **at most once** per status and is **NOT re-fired on crash/replay** (the lifecycle-hook analogue of INV-6/F-0 — HOLDS), STARTED fires exactly once — `assertStatusHookFiresOncePerStatus` + `assertStartedHookFiredExactlyOnce`; the terminal **COMPLETED** hook is at-most-once but can be silently **dropped** on the happy path (candidate finding **F-4**, a *lost* hook fire — not a re-fire — from a `finishWorkflow` queue-clear race) — `documentTerminalHookMayBeDropped` (mirrors `documentEffectAtMostOnceGap`). Not modelled in TLA+ (no status-change listeners / `@WorkflowStatusChangedHandler` / `setStatus`→`notify` callback path in the leasing + crash-recovery spec — see "Scope & limits"). Deterministic scenario only, scenario-pinned like INV-9/16. |
| **DST-only — replay drift guard out of model scope** | INV-18 `DriftGuardPausesCleanly` | `DriftGuardPausesCleanlyScenario` | `Inv18DriftGuardPausesCleanlyTest` | 0 | when replay drift is detected (`guardAgainstReplayDrift` throws `WorkflowReplayDriftException` — new code runs past a recorded-terminal step WITHOUT `ctx.migrateVersion`), the engine pauses the instance **NON-TERMINALLY** and **CLEANLY**: no terminal workflow-status event, no spurious drifted-step event, committed history intact (a per-instance multiset superset of the pre-drift snapshot) — `assertDriftGuardPausesCleanly`. The drift-paused instance is exactly the documented INV-5 (`EventuallyTerminates`) non-termination carve-out (its safety twin). Drift is induced **LIVE**: a v1-recorded instance is replayed under a structurally-divergent v2 body (via the test-scope `SimulationWorld.crashAndRecoverWith`), tripping the guard; **HOLDS** (no anomaly — no terminal event, no corrupted history). Not modelled in TLA+ (no runtime step-reference book / `unreferencedTerminalSteps` comparison / workflow versions / `ctx.migrateVersion` in the leasing + crash-recovery spec — see "Scope & limits"). Deterministic scenario only, scenario-pinned like INV-9/16/17. |
| **DST-only — payload reducer semantics out of model scope** | INV-19 `PayloadReducerSemantics` (**F-6** candidate) | `PayloadReducerSemanticsScenario` | `Inv19PayloadReducerSemanticsTest` | 0 | each payload reducer produces its documented merge into the engine's reconstructed payload — `global_only` DISCARDS the result (keys absent), `combine_local_and_global` MERGES it key-by-key (keys present), `local_only` REPLACES the whole payload (prior keys dropped), and `parameterPayloadReducer` governs the step's input view **independently of the result write** — and the result equals the documented reducer fold of the committed log, stable across crash/replay — `assertPayloadReducerSemantics`. Extends INV-13 (`NoLostPayloadWrites`). Now also pins the reducer **edge cases**: (b) parameter-view vs result-write are independent knobs (combine input + global_only write → result discarded), (c) last-writer-wins on a same-key combine (per the instance's step sequence, not the F-2 global-append order), (d) a combine that omits a key keeps the prior global value. **HOLDS** across the fuzz (full fault set) for the supported reducer/value space (edges (b)/(c)/(d) folded into the per-step fuzz like INV-13, content-based / F-2-robust). Edge (a) — a **`null` value under combine** — surfaced candidate finding **F-6** (the combine accepts the null but `EventSourcedWorkflowState.payload()`'s `Map.copyOf` rejects it, the completion-path NPE is not turned into a terminal status → the instance is wedged non-terminally) — characterized via `runNullEdge` / `ReducerWorkflow.nullEdge` and pinned (`nullValueUnderCombine_wedgesTheInstance_candidateFindingF6`), kept OUT of the always-on fuzz, not patched. Not modelled in TLA+ (no workflow payload / payload reducers / the three reducer behaviours / parameter-vs-result reducer distinction in the leasing + crash-recovery spec — see "Scope & limits"). |
| **DST-only — versioning edges out of model scope** | INV-20 `VersioningEdges` | `VersioningEdgesScenario` | `Inv20VersioningEdgesTest` | 0 | the versioning edges INV-11/INV-12 do not cover, for a workflow registered at THREE versions whose body does two forward `ctx.migrateVersion` bumps under distinct `changeId`s + a downgrade attempt: a downgrade migration is REJECTED (`IllegalArgumentException`) and NEVER recorded (no marker for the downgrade `changeId`, no version stamped below the started one); multiple distinct `changeId`s each record AT MOST ONCE and monotonic non-decreasing (valid semver); a fresh start spawns at the HIGHEST registered version; and an instance recorded (post-migration) at a no-longer-registered version, recovered under a reduced registry, routes via the 4/5-pass lookup to the CLOSEST registered sibling ≤ its recorded state — never 0 (stranded), never 2 — and completes (the routing found a runnable body) — `assertVersioningEdges`. Extends INV-11 (`VersionRoutingSound`, with 3-version depth) + INV-12 (`MigrateVersionContract`, with the downgrade-reject + multi-`changeId` edges). **HOLDS** across the fuzz (full fault set) — the downgrade-rejection was genuinely exercised (the engine threw, the body caught it, no marker recorded). Not modelled in TLA+ (no workflow versions / multi-version registry / `migrateVersion` / downgrade-reject rule / multi-pass replay-routing lookup in the leasing + crash-recovery spec — see "Scope & limits"). The downgrade-rejection + multi-`changeId` edges are folded into the per-step fuzz set; the deeper closest-sibling routing facet is driven by the deterministic test via `crashAndRecoverWith` a reduced registry (the way INV-18 induces drift), since the fixed-registration fuzz loop cannot drop a registered version mid-run. |
| **DST-only — retry/timeout edges out of model scope** | INV-21 `RetryTimingAndExhaustionEdges` | `RetryTimingAndExhaustionEdgesScenario` | `Inv21RetryTimingAndExhaustionEdgesTest` | 0 | the retry/timeout edges INV-8 `RetryBound` and INV-9 `TimeoutsFire` do not cover: a step under `BackoffStrategy.fixed`/`linear`/`exponential` retries on the schedule RECONSTRUCTED FROM THE RECORDED `RETRYING` TIMESTAMPS (monotonic in committed time, constant gaps for `fixed` / non-decreasing for `linear`/`exponential`, exactly the strategy-dictated `RETRYING` count, resolves to a terminal record) — so the schedule SURVIVES crash/replay; a `RetryPolicy.retryWhile(predicate)` STOPS retrying when the predicate says so (strictly FEWER attempt records than maxRetries+1); an `onRetry` handler side effect fires EXACTLY ONCE per actual retry decision and is NOT RE-FIRED on crash/replay (the F-0 analogue for retry handlers — `assertOnRetryFiredOncePerRetry`); and a per-attempt `execute` timeout reaches a terminal `TIMED_OUT` (the D5 residual INV-9 left to the wait path), total budget `(retries+1)×timeout`, never hanging — `assertRetryTimingAndExhaustionEdges`. Extends INV-8 (attempt-record ceiling) + INV-9 (timeout produces a terminal outcome). **Both headline probes HELD**: the `onRetry`-not-re-fired probe (crash-surviving fire count unchanged across crash/replay) and the per-attempt-`execute`-timeout-fires probe (slow `execute` reached `TIMED_OUT`). Not modelled in TLA+ (no time/timeouts / retries / backoff timers / retry predicate / retry handler in the leasing + crash-recovery spec — see "Scope & limits"). Deterministic scenario only, scenario-pinned like INV-9 (the per-attempt `execute` timeout rides the `orTimeout` residual, Phase-3 D5). |
| **DST-only — event-name customization out of model scope** | INV-22 `EventNameCustomizationSound` | `EventNameCustomizationSoundScenario` | `Inv22EventNameCustomizationSoundTest` | 0 | a workflow registered with a custom `eventNameCustomizer` (custom namespace + `workflowBaseName` + a `stepCompleted("Done")` status-suffix override) records every committed step/status event under the CUSTOMIZED wire name the customizer dictates — each event's `MessageType.qualifiedName()` equals the customizer-derived expected (custom namespace + `capitalize(stepName)`/`capitalize(workflowBaseName)` base + the status suffix, incl. the `Done` override on step COMPLETED) — the instance reaches a terminal status under customization, and a crash + replay reproduces the IDENTICAL customized names (a replayed present step emits nothing) — `assertEventNameCustomizationSound`. **HOLDS** across the fuzz (full fault set) — the customized names were applied, stable across replay, and routing/replay stayed sound under customization. Not modelled in TLA+ (no event-name customization / `EventNameCustomizer` / wire-level event name / namespace / status suffixes in the leasing + crash-recovery spec — see "Scope & limits"). Folded into the per-step fuzz set (the assertion is content-based / F-2-robust). |
| **DST-only — engine self-protection at the abuse surfaces out of model scope** | INV-23 `EngineSelfProtection` (**F-5** candidate) | `EngineSelfProtectionScenario` | `Inv23EngineSelfProtectionTest` | 0 | the engine PROBED at its two self-protection surfaces never CORRUPTS/TEARS an instance's committed history — the worst it does is stall non-terminally or throw cleanly. **(1) Nested primitive** (§3.1-forbidden — a primitive called from inside another primitive's action): the engine has NO up-front guard (no `appendTask` re-entrancy check) — under the engine's default virtual-thread body executor it SILENTLY COMPLETES (inner action gets its own thread; terminal, no corruption), under a single-threaded body executor it SILENTLY DEADLOCKS the per-instance task queue (inner `appendTask` never consumed; stuck non-terminal, only `STARTED` committed, no corruption — observed within a SHORT wall-clock window). The misuse is NEVER surfaced as a clear up-front error (candidate gap **F-5**) — `documentNestedPrimitiveNotGuarded`. **(2) Task-queue overflow** (driven against the REAL `SimpleWorkflowExecution.appendTask`, real `ArrayBlockingQueue<>(1000)`): the (bound+1)-th append throws a clean `RuntimeException("Too many tasks…")` BEFORE publishing anything, the already-queued ≤1000 tasks stay intact + FIFO + consumable — a clean failure, not corruption. The no-corruption contract HOLDS at both — `assertEngineSelfProtection` (the committed history is either well-formed-complete or a clean non-terminal prefix — never torn / duplicate-terminal / orphan). Not modelled in TLA+ (no per-instance task queue / body executor / `appendTask` re-entrancy-or-bound / nested primitive call in the leasing + crash-recovery spec — see "Scope & limits"). Deterministic scenario only, scenario-pinned like INV-9/16/17/18 (a nested-primitive instance deliberately stalls non-terminally, which the liveness horizon would read as a hang). |

## Design holes found

### F-0 — `EffectAtMostOnce` (INV-6) is violated → reproduces the headline code finding

- **Run:** `MC_effect.cfg` (`DURABLE_LEASE=TRUE`, `APPEND_CONDITION=FALSE`).
- **TLC:** `Error: Invariant EffectAtMostOnce is violated.`
- **Trace (the violating tail):**
  1. `Claim(p1)` — p1 owns the segment.
  2. `ApplyEffect(p1, 1)` — runs step 1's effect: `log = ⟨STARTED(1)⟩`,
     `effect[1] = 1`, `applied[p1] = {1}`.
  3. `Crash(p1)` — crash **after** the effect ran but **before** `COMPLETED(1)`
     committed. `vol`/`applied` are lost; `log` keeps `STARTED(1)`.
  4. `Claim(p1)` (recover) — rebuilds `vol` with `STARTED(1)`, but `applied[p1]`
     is empty.
  5. `ApplyEffect(p1, 1)` — the step is still `STARTED` and the cached-result
     path only protects `COMPLETED` steps, so the effect **runs again**:
     `effect[1] = 2`.
- **Design hole:** the external side effect (`ExecuteDelegate.java:148`) is not
  bound to the `COMPLETED` event commit — there is no transactional outbox /
  exactly-once QoS. A crash in the `STARTED → COMPLETED` window re-runs the
  effect on replay (at-least-once, not at-most-once). The code flags it:
  `ExecuteDelegate.java:130` `// FIXME -> consider to use QOS`.
- **Suggested fix:** bind the effect to the commit — a transactional outbox or
  an append-condition that makes `STARTED + effect + COMPLETED` atomic / dedups
  a replayed `STARTED` step.
- **Fix verified:** `MC_effect_fixed.cfg` (`APPEND_CONDITION=TRUE`) →
  `Model checking completed. No error has been found.` Toggling the fix flag
  closes the hole.
- **Implemented in the engine (F-0 FIXED).** The shipped fix is *at-most-once via
  skip-and-resolve*: the `ExecuteDelegate` overload snapshots "STARTED at the
  execute entry" (which on a live run is impossible, so it can only be a replayed
  in-flight attempt) and, instead of re-running the action, routes the interrupted
  attempt through the regular error flow — no retry → step FAILED (cause
  `StepIndeterminateException`); retry → RETRYING + next attempt. This satisfies
  `EffectAtMostOnce` (`effect[s] ≤ 1` per attempt), the same invariant
  `APPEND_CONDITION=TRUE` makes hold. It is *weaker than* the modeled
  outbox/append-condition (which is exactly-once — the step still COMPLETES); the
  implemented point is lighter and needs no outbox. DST pins: `F0EffectDuplicationTest`
  (no-retry → FAILED, effect == 1) + `AtMostOnceRetryResumeTest` (retry → RETRYING).

### F-1 — `AtMostOneOwner` (INV-1) is violated → split-brain ownership

- **Run:** `MC_owner.cfg` (`DURABLE_LEASE=FALSE`, `APPEND_CONDITION=TRUE`).
- **TLC:** `Error: Invariant AtMostOneOwner is violated.`
- **Trace (3 states):**
  1. `Claim(p1)` — `holds = (p1 ↦ TRUE, p2 ↦ FALSE)`.
  2. `Claim(p2)` — p2 acquires the segment **while p1 still holds it**, because
     the non-durable in-memory token does not forbid it:
     `holds = (p1 ↦ TRUE, p2 ↦ TRUE)`, both `pc = "ready"`.
- **Design hole:** the processor `TokenStore` is hardcoded in-memory and
  per-process (`AllEventEventHandlingComponent.java:70`), so it provides no
  cross-node single-writer lease — two nodes can both own segment 0.
- **Suggested fix:** a durable, claim-fenced lease for the processor token
  (injectable durable `TokenStore` with proper claim/expiry), so segment
  acquisition is mutually exclusive across nodes.
- **Fix verified:** `MC_owner_fixed.cfg` (`DURABLE_LEASE=TRUE`) →
  `No error has been found.`

#### F-1 consequence — `AtMostOnceRecording` (INV-2) is violable when INV-1 breaks

INV-2 holds **given** INV-1. `MC_record.cfg` (`DURABLE_LEASE=FALSE`,
`APPEND_CONDITION=FALSE`) shows the dependency the ExecuteDelegate FIXMEs warn
about:

- **TLC:** `Error: Invariant AtMostOnceRecording is violated.`
- **Trace:** `Claim(p1)` → `ApplyEffect(p1,1)` (STARTED(1)) → `Claim(p2)`
  (split-brain; p2 rebuilds with STARTED(1)) → `CommitCompleted(p1,1)`
  (COMPLETED(1)) → `CommitCompleted(p2,1)`: p2's **stale** view still shows the
  step open, and with no append-condition the duplicate lands →
  `log = ⟨STARTED(1), COMPLETED(1), COMPLETED(1)⟩`.
- **Fix verified:** `MC_record_fixed.cfg` (`APPEND_CONDITION=TRUE`, lease still
  broken) explores 973 distinct split-brain states and reports
  `No error has been found.` — the optimistic append-condition restores the
  recording guarantee **independent of** fixing the lease.

### F-2 — `DeterministicReplay` (INV-4) holds in this model (the deterministic core)

- **Run:** `MC_safe.cfg` (also exercised in every No-error run).
- **TLC:** `No error has been found.`
- The model rebuilds state as a **pure function of the committed log**
  (`RebuildFromLog`) applied synchronously, mirroring the engine's synchronous
  `evolve` (ARCHITECTURE §5). `DeterministicReplay` asserts (a) the rebuild
  loses nothing and (b) no process fabricates/drops/rewrites a committed
  terminal record — both hold here. F-2's real risk surface (timestamp ties in
  `workflowStepNames()`, multi-match wait dispatch order, the live/replay flip)
  lives at a level of detail this abstract model deliberately leaves out (see
  Scope below); it is the seam Phase 4 DST probes directly. The model confirms
  the **design** is deterministic where the log fully orders events.

### ClaimSeed — the position a claim seeds each restored instance with

`MarkerChain.tla` models one instance, so it cannot express this choice. A claim restores several
instances while the previous owner keeps appending, and the seed is either each instance's own read
or the lowest read of one shared transaction.

`NoSelfFence`: a restored instance is never rejected by its own history — only a write that lands
after its own sourcing read may stop it, and that is a foreign writer.

```bash
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC -workers 2 -deadlock \
  -config formal/tla/MC_claimseed.cfg formal/tla/ClaimSeed.tla          # No error
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC -workers 2 -deadlock \
  -config formal/tla/MC_claimseed_shared.cfg formal/tla/ClaimSeed.tla   # VIOLATED
```

The shared-seed counterexample is the argument the change is built on: `i1` is read at length 1, the
previous owner appends for `i2`, `i2` is read at length 2 but seeded at the running lower bound 1,
and `i2`'s own record rejects it. The green run is worth something because its twin breaks.

### MarkerChain — DCB append-condition fencing (successor of the `MC_record` pair)

`MarkerChain.tla` models the shipped DCB protocol ("Introduce DCB append
conditions"): 2 writers, 1 instance, a global store (sequence of records, each
carrying the marker it was appended under), per-writer `ConsistencyMarker`
(`-1` = unseeded), spawn checked from ORIGIN, `Restore` seeding the marker from
the store head + rebuilding the writer's program position by replay, and a
rejected writer interrupted without a terminal event. Ownership loss is
deliberately **not** writer-visible — a stale writer keeps appending on its old
marker; only the condition check stops it. `NoWriteAfterOwnershipLoss` is the
condition-semantics observable: every committed record's `mkr >= position - 1`.

- **`MC_markerchain.cfg`** (fencing on): all invariants + the
  `RejectionNeverAdvancesMarker` action property →
  `Model checking completed. No error has been found.` (5771 states, 1791
  distinct, depth 13).
- **`MC_markerchain_broken.cfg`** (`ConditionalAppends=FALSE`): TLC reports
  `Error: Invariant OneStartPerInstance is violated.` (both writers spawn —
  two `Started` records); with only `AtMostOnceRecording` listed the deeper
  duplicate-record trace lands too (stale second writer commits a second
  `StepTerm` for the same step, `mkr` 1 behind a store of length 3). The
  refutability proof — the model is not vacuous.
- **`MC_markerchain_regress.cfg`** (fencing on + `MarkerRegress`, the
  `ConsistencyMarkerSupport.updateAppendPosition` lost-update race: the
  non-atomic read-`upperBound`-write of the volatile marker lets a concurrent
  update clobber a newer position with an older one): TLC reports
  `Error: Invariant SiblingNoFalseConflict is violated.` — Spawn → Retrying ×2
  → `MarkerRegress` (marker 3 → 0) → the lone writer's own next append is
  REJECTED against its own history and the instance is interrupted for no
  reason (116 states, 98 distinct). A **liveness** hole, not a safety one: the
  safety invariants still hold under the regress.

Reproduce (same invocation style; the cfgs carry `CHECK_DEADLOCK FALSE`):

```sh
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_markerchain.cfg formal/tla/MarkerChain.tla         # No error
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_markerchain_broken.cfg formal/tla/MarkerChain.tla  # VIOLATED
java -XX:+UseParallelGC -cp formal/tla/tools/tla2tools.jar tlc2.TLC \
  -workers auto -metadir formal/tla/states \
  -config formal/tla/MC_markerchain_regress.cfg formal/tla/MarkerChain.tla # VIOLATED
```

## Scope & limits (what is abstracted away)

- **2 processes, 3 ordered steps, 1 instance, `MaxLogLen=6`, `MaxCrashes=2`.**
  Every set is bounded so the state space is finite and TLC finishes in ~1 s.
- **Time / scheduling are not modelled.** No clock, timeouts, retries, backoff,
  or timer firing order. Steps are ordered (`t < s ⇒ Committed(t)`) instead of
  carrying timestamps, so the F-2 *timestamp-tie* and *multi-match wait
  dispatch* nondeterminism (ARCHITECTURE §11) is out of scope here — it is a
  Phase-4 DST concern. The model checks the coarser design property that replay
  is a pure function of the committed log.
- **Liveness fairness:** `EventuallyTerminates` is checked under `FairSpec`
  (weak fairness on `Claim`/`Recover` and on `Progress`). `Crash` is adversarial
  and **bounded** (`MaxCrashes`), not fair — after the last crash the fair
  actions carry the instance to terminal. The model does **not** carve out the
  ADR-005 drift-pause sink (no version drift is modelled here); the drift-pause
  exclusion noted in INVARIANTS.md INV-5 is therefore not exercised at this
  scope. Symmetry is disabled for the liveness run (TLC warns it can miss
  liveness violations).
- **The lease is modelled as a per-process boolean**, abstracting Axon PSEP
  segment-claim / heartbeat / expiry machinery down to "holds it or not", which
  is the property INV-1 cares about.
- **The effect/outbox and the optimistic append-condition are modelled as two
  boolean flags**, not as the concrete mechanism. The model shows *that* binding
  the effect to the commit (F-0) and gating the append on an unchanged log (F-1
  record race) close the holes; it does not prescribe the implementation.
- **No workflow-level status, step-failure status / `ctx.fail` failure
  propagation, retries/backoff/timeouts, retry predicates (`retryWhile`) /
  retry handlers (`onRetry`), per-attempt `execute` timeouts, workflow versions
  / `migrateVersion`, spawn-dedup routing, workflow payload / payload reducers,
  combinators / parallel branch steps, event correlations / associations /
  multi-instance event routing, the runtime replay-drift guard, or event-name
  customization (`EventNameCustomizer` / wire-level event names / namespaces /
  status suffixes).**
  The `log` is a sequence of `<<step, status>>` step records with `status ∈
  {STARTED, COMPLETED}` only — no workflow-level FAILED status, no step-failure
  status, no payload map, no parallel-step fan-out or combinator decision, no
  per-instance association key or correlated event delivery, no workflow
  status-change listeners / `@WorkflowStatusChangedHandler` registration / the
  engine's `setStatus`→`notify` callback path, and no runtime step-reference
  "book" (`referencedStepNames`) / event-sourced-vs-runtime book comparison
  (`unreferencedTerminalSteps` / `guardAgainstReplayDrift`) that the drift guard
  fires on. The
  implementation/DST-only safety properties built on those concepts — INV-7
  `TerminalIsFinal`, INV-8 `RetryBound`, INV-9 `TimeoutsFire`, INV-10
  `OneInstancePerStart`, INV-11 `VersionRoutingSound`, INV-12
  `MigrateVersionContract`, INV-13 `NoLostPayloadWrites`, INV-14
  `CombinatorConsistency` (a combinator's decision is consistent with the
  documented short-circuit semantics, a pure function of its branch steps'
  committed terminal outcomes, and stable across crash/replay), INV-15
  `EventCorrelationExact` (an `associate(...)`-correlated event wakes exactly the
  matching waiting instance — no cross-wakeup — and duplicate/uncorrelated events
  produce no spurious wait completion, stable across crash/replay), INV-16
  `FailurePropagation` (a step failure propagates to a terminal FAILED workflow
  status — the workflow never silently hangs or completes when a step fails;
  failing step recorded terminally-failed, instance reaches FAILED, no step after
  the failing one begins, stable across crash/replay), and INV-17
  `StatusHookFiresOncePerStatus` (a registered status-change hook fires at most
  once per status and is not re-fired on crash/replay — the lifecycle-hook analogue
  of INV-6/F-0, which holds; surfaces the candidate finding **F-4** that the
  terminal COMPLETED hook can be silently *dropped* on the happy path), and INV-18
  `DriftGuardPausesCleanly` (when the replay drift guard fires, the engine pauses
  the instance non-terminally and cleanly — no terminal workflow status, no
  spurious drifted-step event, committed history intact; the safety twin of the
  INV-5 drift-pause non-termination carve-out, induced live by replaying a
  v1-recorded instance under a structurally-divergent v2 body), and INV-19
  `PayloadReducerSemantics` (each payload reducer produces its documented merge
  into the workflow payload — `global_only` discards the result,
  `combine_local_and_global` merges it key-by-key, `local_only` replaces the
  whole payload, and the `parameterPayloadReducer` governs the step's input view
  — stable across crash/replay; extends INV-13), and INV-20 `VersioningEdges`
  (the versioning edges INV-11/INV-12 do not cover — a `ctx.migrateVersion`
  downgrade is rejected and never recorded, multiple distinct `changeId`s each
  record at most once and monotonic non-decreasing, and a deeper THREE-version
  registry routes a fresh start to the highest version and an instance recorded
  at an older version to the closest registered sibling, never 0 / never 2;
  extends INV-11 with depth + INV-12 across `changeId`s), and INV-21
  `RetryTimingAndExhaustionEdges` (the retry/timeout edges INV-8/INV-9 do not
  cover — a step under a `BackoffStrategy` retries on the schedule reconstructed
  from the recorded `RETRYING` timestamps so it survives crash/replay, a
  `retryWhile` predicate stops retrying with strictly fewer attempt records than
  maxRetries+1, an `onRetry` handler fires once per actual retry decision and is
  NOT re-fired on crash/replay — the F-0 analogue for retry handlers, which
  holds — and a per-attempt `execute` timeout reaches a terminal `TIMED_OUT`;
  extends INV-8 + INV-9), and INV-22 `EventNameCustomizationSound` (a workflow
  registered with a custom `eventNameCustomizer` records its step/status events
  under the customized wire names the customizer dictates — namespace + base +
  step + suffix, incl. a `stepCompleted` override — those names are stable
  across crash/replay as a pure function of history, and the engine still
  routes/replays correctly under customization), and INV-23
  `EngineSelfProtection` (the engine PROBED at its two abuse surfaces — a §3.1
  nested primitive and a task-queue overflow — never CORRUPTS/TEARS an
  instance's committed history: the overflow throws cleanly without publishing
  anything, and the nested primitive either silently completes (default
  virtual-thread executor) or silently DEADLOCKS the per-instance queue
  (single-threaded executor), never a clean up-front rejection — candidate gap
  **F-5**) — are
  deliberately asserted in the DST harness only (see each invariant's "Scope
  decision" in INVARIANTS.md); adding them here would bloat a model kept tiny so
  TLC finishes in seconds. The TLA+ model has no per-instance task queue, body
  executor, `appendTask` re-entrancy/bound, or nested primitive call to model.
