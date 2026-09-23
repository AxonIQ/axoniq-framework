# DST / TLA+ docs-coverage matrix

Maps every **documented guarantee** of the axon-flow-spec engine — the `axon-flow-workflow` skill
(§3 non-negotiables, §4 primitives, §5 step customization, §7 recipes, §10 anti-patterns), the ADRs
(000–005, plus **ADR-014 workflow instance sharding** in §10), and the protocol invariants
(`INVARIANTS.md` INV-1..36) — to the **invariant + scenario/test**
that exercises it in the TLA+ model and/or the DST harness, or an explicit **out-of-scope / not-modelled**
note with rationale.

Status legend: ✅ held · 🔴 finding (engine) · 🟠 design-question finding · 🟢 false-alarm / works-as-expected ·
⚪ out-of-scope (with rationale).

This file is the human-facing index; the authoritative per-invariant wording lives in
[`INVARIANTS.md`](INVARIANTS.md) (the Cross-reference contract) and the run-by-run bridge in
[`tla/README.md`](tla/README.md#tla--dst-bridge). Findings detail is in
[`POC-TLA-DST.adoc`](POC-TLA-DST.adoc).

---

## 1. The invariant spine (INV-1..36)

| INV | MachineName | TLA+ | DST assertion | DST scenario · test | Verdict |
|---|---|---|---|---|---|
| 1 | `AtMostOneOwner` | `MC_owner` (VIOLATED) / `MC_owner_fixed` | `assertAtMostOneOwner` / `documentSplitBrainOwnership` | `SplitBrainScenario` · `F1SplitBrainTest` | 🔴 **F-1** (split-brain; in-mem token) |
| 2 | `AtMostOnceRecording` | `MC_record` (VIOLATED under split-brain) / `MC_safe` | `assertAtMostOnceRecording` (step-status terminals only) / `documentDuplicateRecordingUnderSplitBrain` | `WriteThenVanishScenario`; `F1RecordDuplicationTest`; `DuplicateTerminalPayloadRecordScenario` · `F7DuplicatePayloadRecordTest` | ✅ (given INV-1); F-1 consequence; 🔴 **F-7** single-node (`modifyPayload` re-records on post-crash live re-run); ⚠️ **F-13 coverage gap**: counts only step-status terminals, so a duplicate `<workflow>:CANCELLED` is NOT caught here (only by INV-7) |
| 3 | `CommittedHistorySurvivesCrash` | `MC_safe` PROPERTY | `assertCommittedHistorySurvivesCrash` (per-`workflowId`) | crash/restart faults in `DstSimulation`; `RegressionSeedsTest` | ✅ (F-2 refinement: single-source log) |
| 4 | `DeterministicReplay` | `MC_safe` | `assertDeterministicReplay` (per-instance **multiset**) | replay-twice in `DstReproduceTest`/`DstSmokeTest` | 🟢 **F-2** (per-instance content det.; global-append order non-det. by design) |
| 5 | `EventuallyTerminates` | `MC_live` (temporal, `FairSpec`) | `assertEventuallyTerminates` | virtual-time horizon in `DstSimulation` | ✅ (drift-pause is the documented carve-out → INV-18) |
| 6 | `EffectAtMostOnce` | `MC_effect` (VIOLATED) / `MC_effect_fixed` | `assertEffectAtMostOnce` / `documentEffectAtMostOnceGap` | `WriteThenVanishScenario` · `F0EffectDuplicationTest` | 🔴 **F-0** (effect at-least-once) |
| 7 | `TerminalIsFinal` | DST-only | `assertTerminalIsFinal` (no NEW work after terminal; tolerates a re-published *identical* terminal status = F-13 gap) | `TerminalIsFinalScenario` · `Inv7TerminalIsFinalTest`; `DuplicateCancelTerminalRecordScenario` · `F13DuplicateCancelRecordTest` | ✅ for NEW-work finality; 🔴 **F-13** cancel-path duplicate `<workflow>:CANCELLED` (workflow-terminal analogue of F-7); surfaced 🟠 **F-3**; F-2 late-event tolerance |
| 8 | `RetryBound` | DST-only | `assertRetryBound` (≤ maxRetries+1) | `Inv8RetryBoundScenario` · `Inv8RetryBoundTest`; `BackoffOverflowScenario` · `S3BackoffExponentialOverflowTest` | ✅ record bound holds; surfaced 🟠 **F-9** (S-3) `BackoffStrategy.exponential` `Duration` overflow at large attempt counts → liveness wedge (extreme-config/LOW likelihood) |
| 9 | `TimeoutsFire` | DST-only | `assertTimeoutsFire` | `Inv9TimeoutsFireScenario` · `Inv9TimeoutsFireTest` | ✅ (scenario-pinned; `orTimeout` residual) |
| 10 | `OneInstancePerStart` | DST-only | `assertOneInstancePerStart` | `OneInstancePerStartScenario` · `Inv10…Test` | ✅ (live case; 🟠 F-3 after-terminal re-spawn tolerated) |
| 11 | `VersionRoutingSound` | DST-only | `assertVersionRoutingSound` | `VersionRoutingSoundScenario` · `Inv11…Test` | ✅ |
| 12 | `MigrateVersionContract` | DST-only | `assertMigrateVersionContract` | `MigrateVersionContractScenario` · `Inv12…Test` | ✅ |
| 13 | `NoLostPayloadWrites` | DST-only | `assertNoLostPayloadWrites` | `NoLostPayloadWritesScenario` · `Inv13…Test` | ✅ |
| 14 | `CombinatorConsistency` | DST-only | `assertCombinatorConsistency` | `CombinatorConsistencyScenario` · `Inv14…Test`; `AnyMatchNoMatchWinnerScenario` · `S5AnyMatchNoMatchWinnerTest` | ✅ decision facet holds; surfaced 🟠 **F-10** (S-5) `anyMatch` no-match-all-completed winner reads `results[0]` (minor; winner-result-semantics gap) |
| 15 | `EventCorrelationExact` | DST-only | `assertEventCorrelationExact` | `EventCorrelationExactScenario` · `Inv15…Test` | ✅ |
| 16 | `FailurePropagation` | DST-only | `assertFailurePropagation` | `FailurePropagationScenario` · `Inv16…Test` | ✅ (pins intended no-auto-fail-on-uncaught) |
| 17 | `StatusHookFiresOncePerStatus` | DST-only | `assertStatusHookFiresOncePerStatus` / `documentTerminalHookMayBeDropped` | `StatusHookFiresOncePerStatusScenario` · `Inv17…Test` | ✅ no-re-fire; surfaced 🔴 **F-4** (COMPLETED hook dropped) |
| 18 | `DriftGuardPausesCleanly` | DST-only | `assertDriftGuardPausesCleanly` | `DriftGuardPausesCleanlyScenario` · `Inv18…Test` (live drift) | ✅ (INV-5 carve-out's safety twin) |
| 19 | `PayloadReducerSemantics` | DST-only | `assertPayloadReducerSemantics` | `PayloadReducerSemanticsScenario` · `Inv19…Test` | ✅ |
| 20 | `VersioningEdges` | DST-only | `assertVersioningEdges` | `VersioningEdgesScenario` · `Inv20…Test` | ✅ (downgrade-reject + multi-`changeId` + closest-sibling routing) |
| 21 | `RetryTimingAndExhaustionEdges` | DST-only | `assertRetryTimingAndExhaustionEdges` / `assertOnRetryFiredOncePerRetry` | `RetryTimingAndExhaustionEdgesScenario` · `Inv21…Test`; `BackoffOverflowScenario` · `S3BackoffExponentialOverflowTest` | ✅ (scenario-pinned; backoff timing + `retryWhile` + onRetry-not-re-fired + per-attempt-`execute`-timeout, all HELD); surfaced 🟠 **F-9** (S-3) exponential-backoff `Duration` overflow at large attempt counts → liveness wedge (extreme-config/LOW) |
| 22 | `EventNameCustomizationSound` | DST-only | `assertEventNameCustomizationSound` | `EventNameCustomizationSoundScenario` · `Inv22…Test` | ✅ (custom names applied + stable across replay + routing sound; folded into the fuzz) |
| 23 | `EngineSelfProtection` | DST-only | `assertEngineSelfProtection` / `documentNestedPrimitiveNotGuarded` | `EngineSelfProtectionScenario` · `Inv23…Test` (real `appendTask` overflow + both executor models) | ✅ no-corruption at both abuse surfaces; surfaced 🟠 **F-5** (nested primitive not guarded — silent deadlock/silent-success, never a clean rejection) |
| 24 | `NoWorkAfterRelease` | `sharding/SegmentClaim.tla` — `SegmentClaim_join_violated_seq` / `_restart_violated` / `_release_violated` / `_steal_violated` (VIOLATED) vs their `_fixed` pairs | **none — TLA+ + unit tests + rig only**; the harness models no segments or nodes (`SimulationConfig` has no field for either; `InMemoryTokenStore` has a no-op `releaseClaim`; `assertAtMostOneOwner` is called with a literal `1`) | `NodeJoinIT`, `MultiNodeStartupTest`, `WholeClusterRestartIT`, `ScaleDownAndUpIT`, `RollingDeployIT`, `StoreOutageIT` + mocked-`TokenStore` unit tests | ✅ claim protocol holds (**F-25/F-31/F-32 FIXED**); ⚠️ release window open — **S-9**, model-confirmed, no failing test |
| 25 | `OneInstanceOneOwner` | `sharding/WfShard.tla` — `cfg_d2_owner` (VIOLATED, 317 distinct) / `cfg_fixed` | **none** — see INV-24 | `WorkflowEngineSegmentClaimStartScopeTest` (both call sites); `CrossSegmentSpawnIT`, `ScaleDownAndUpIT` | ✅ (**F-27 FIXED** at both call sites — the second was found by the model, not by reading) |
| 26 | `NoWorkWhileReplaying` | `sharding/WfShard.tla` — `cfg_live_trap` (temporal VIOLATED, State 17 stuttering, 9,154 distinct) / `cfg_live_fixed3` | **none** — see INV-24 | `WorkflowEngineSegmentLiveModeScopeTest`, `WorkflowEngineClaimDuringReplayTest` | ✅ (**F-28 + F-33 FIXED together** — splitting them trades a safety bug for a hang); ⚠️ residual: cross-node migration still starts at head, blocked upstream on **FW-2** |
| 27 | `TokenNeverPassesUnappliedEvent` | `sharding/Holdback.tla` — `C2_*`, `C4_shutdown_clears_first` (VIOLATED) vs `C3*`/`C4b`; `M1`/`M2`/`M3` mutation arms | **none** — see INV-24 | `WorkflowEngineCrossSegmentCheckpointTest`, `WorkflowConfigurationDefaultsTest`, `WorkflowEngineReplayTest`; `DurableWaitAcrossRebalanceIT` (~1000 observations, zero losses) | ✅ (**F-26 + F-35 + F-36 FIXED**); ⚠️ out of its scope: **F-34b** (no wait was ever published, so no barrier exists) and **FW-4** (the framework never clamps an over-high request) |
| 28 | `SpawnExactlyOnce` (+ `WakeExactlyOnce`) | `sharding/WfShard.tla` — hold with **all four modelled defects enabled**, 1,722,131 distinct states, depth 31 (`cfg_asis3_spawn`/`cfg_asis3_wake`); fail in 8 steps under a guard mutation (`cfg_mut`/`cfg_mut_wake`) | **none** — see INV-24 | `DuplicateStartEventsIT`, `TimerAcrossHandoverIT`, `IdleClusterSparseTrafficIT`, `CrossSegmentSpawnIT`, `SustainedLoadHandoverIT` | 🟢 **works as expected** — the campaign's strongest precise negative: broadcast fan-in is genuinely safe; ⚠️ untested adjacent hazard **S-10** (`#` silently load-bearing in the segment key) |
| 29 | `NoForeignStepRecorded` | `MC_publish` (No error) / `MC_publish_noguard` (VIOLATED) | `assertNoForeignStepRecorded` (always-on) | `PublishChainScenario` · `Inv29PublishPrimitiveTest` | ✅ works as expected (P6) |
| 30 | `PublisherObservesOwnPublish` | `MC_publish` (No error) / `MC_publish_candidate`, `MC_publish_nogate` (VIOLATED) | `assertPublisherObservesOwnPublish` (always-on) | `PublishChainScenario`, `FencedPublishScenario`, `PublishAppendFailureScenario` · `Inv29PublishPrimitiveTest`, `FencedPublishTest`, `PublishAppendFailureTest` | ✅ works as expected (P6); candidate 🟠 **F-41** on a failed engine append |
| 31 | `ManagerVisibilityMonotonic` | `MC_managerview` (VIOLATED, no crash) / `MC_managerview_retain` (VIOLATED, crash) / `MC_managerview_fixed` | `assertManagerVisibilityMonotonic` | `ManagerVisibilityProbeScenario` (`VISIBILITY`) · `ManagerVisibilityProbeTest` | ✅ **F-42 fixed** (non-live ids sourced from the store; the pin flipped and now guards the fix) |
| 32 | `ManagerStatusMonotonic` | `MC_managerview_status` (VIOLATED) / `MC_managerview_fixed` | `assertManagerStatusMonotonic` | `ManagerVisibilityProbeScenario` (`STATUS`) · `ManagerVisibilityProbeTest` | ✅ **F-42 fixed**, second facet (a completed id is answered COMPLETED while the projection still says STARTED) |
| 33 | `ManagerViewMatchesLog` | DST-only | `assertManagerViewMatchesLog` (always-on after settle + horizon) | smoke/fuzz/chaos; `ManagerQueryEquivalenceScenario` · `ManagerQueryEquivalenceTest` | ✅ held on every settled step and horizon; mutation-checked (detached steps dropped → caught) |
| 34 | `ManagerOneStatePerId` | DST-only | `assertManagerOneStatePerId` (always-on) | as INV-33 | ✅ held; mutation-checked (dedupe deleted → caught in smoke) |
| 35 | `ManagerCancelTargetsLiveOnly` | DST-only | `assertManagerCancelTargetsLiveOnly` | `ManagerCancellationScenario` · `ManagerCancellationTest` | ✅ held; surfaced and closed **F-43** (a fenced node answered `true` for a cancellation it never recorded; `cancelStep` now answers false on an interrupted wait — `FencedManagerCancelTest` guards it) |
| 36 | `ManagerLiveWins` | DST-only (the model's `View` takes live by construction) | `assertManagerLiveWins` | `ManagerLiveWinsScenario` · `ManagerLiveWinsTest` | ✅ held; added because canary (a) — history wins over live — escaped every settle-time oracle |

---

## 2. `axon-flow-workflow` skill §4 — primitives

| Primitive / guarantee | Covered by | Notes |
|---|---|---|
| `execute` action side effect (at-most-once across crash) | INV-6 `EffectAtMostOnce` | 🔴 F-0: at-least-once today |
| `execute`/`awaitExecute` records a step (at-most-once) | INV-2 `AtMostOnceRecording` | ✅ |
| `waitForEvent`/`awaitEvent` + `associate(...)` correlation | INV-15 `EventCorrelationExact` | ✅ wakes exactly the matching instance; dup/uncorrelated ignored |
| `waitForEvent` timeout fires | INV-9 `TimeoutsFire` | ✅ (wait-timeout path; `orTimeout` residual scenario-pinned) |
| `sleep` durable delay | INV-5 + INV-4 (driven by the virtual-time scheduler; every workflow with a `sleep` step — e.g. `OrderWorkflow.settleDelay` — is driven through the fuzz) | ⚪ no dedicated "resumes-exactly-once" invariant: a sleep is a recorded step, so its at-most-once resume is INV-2/INV-4; folding a standalone check was skipped (overlaps determinism), noted in INV-19 |
| `modifyPayload`/`setPayload` | INV-13 `NoLostPayloadWrites` + INV-19 `PayloadReducerSemantics` | ✅ (local_only replace); 🔴 **F-7**: re-records a duplicate `<step>:COMPLETED` on a post-crash live re-run (INV-2 violation) — the only state-publishing primitive missing the replay-skip gate + uses the unguarded direct `eventSink.publish` (`PayloadDelegate.java:80,84,97` vs `ExecuteDelegate.java:117` + `sendStepEvent`'s guard `AbstractStepExecutor.java:204-212`); `F7DuplicatePayloadRecordTest` |
| `migrateVersion` contract | INV-12 `MigrateVersionContract` | ✅ at-most-once / monotonic / replay-stable |
| `fail` / `cancel` terminal | INV-7 `TerminalIsFinal` (cancel) + INV-16 `FailurePropagation` (fail) | ✅ finality of NEW work; 🔴 **F-13**: the cancel path re-publishes a duplicate `<workflow>:CANCELLED` across a crash/replay (workflow-terminal analogue of F-7) — `TerminateDelegate.cancelled`'s unguarded direct `eventSink.publish` (`:178`), guarded on replay only by the upstream `execute()` short-circuit + `switchToLiveMode` terminal-eviction filter; `F13DuplicateCancelRecordTest` |
| combinators `anyMatch`/`allMatch`/`noneMatch` (+ `matched()`/`unmatched()`, short-circuit) | INV-14 `CombinatorConsistency` | ✅ across crash/replay (decision + `matched()`/`unmatched()` facet); 🟠 **F-10** (S-5): on the `anyMatch` no-predicate-match-but-all-completed path the winner-derived accessors (`success()`/`result()`/`resultAs()`) read `fallback.orElse(results[0])` (the first completed branch, `AnyMatchCombinatorDelegate.java:114-116`) rather than unmatched-explicit (minor; `matched()`/`unmatched()` themselves correct); `S5AnyMatchNoMatchWinnerTest` |
| parallel execution (multiple in-flight `execute`) | INV-14 (folds parallel branches) | ⚠ genuinely-overlapping in-flight branches deferred to the engine example suite (`AnyRaceWorkflow` etc.) — DST per-instance determinism (INV-4) forbids racing same-instance commits; see INV-14 notes |
| `publish` / `awaitPublish` (ADR-019): one event that is both the business event and the publisher's COMPLETED step; starts and wakes other workflows; reaches plain Axon handlers | INV-29 `NoForeignStepRecorded` + INV-30 `PublisherObservesOwnPublish` (+ INV-2 for the record, INV-10/INV-28 facets for the fan-out start, INV-15 for the wake) | ✅ **works as expected**: `PublishChainScenario` (1 event → 2 workflows started + 1 pre-registered wait woken, stable across crash), `PublishWaitOrderScenario` (wait-before woken once; wait-after not woken — documented live-delivery semantics, `LateWaitNeverWakes`), `PublishFanOutWakeScenario` (running-to-running 1:N: three live waiters each woken once by one event a fifth running workflow publishes, a control waiter on another id untouched), `PublishCrashScenario` (crash after commit: no re-publish; vanished commit + crash: exactly one record — the effect *is* the record, so no F-0 class divergence exists for this primitive), `FencedPublishScenario` (stale writer fenced, nothing published, nobody started), `PublishAppendFailureScenario` (hung commit → resolution timeout → restart publishes once; failed commit → nothing durable → restart publishes once; 🟠 candidate **F-41**, engine-wide: a failed append drops the instance from the live set without a recovery marker until the next restart, same on `modifyPayload`), `PublishToEventHandlerScenario` (regular `EventHandlingComponent` receives each published event once). Always-on in the fuzz loop via the chain in `defaultRegistrations()`. ⚪ cross-segment delivery is TLA+-only (`PublishRouting.tla`, `MC_publish_candidate*.cfg` show the hang candidate routing would cause) |
| `WorkflowStepResult` blocking/non-blocking handle | exercised throughout (every workflow uses `execute`/`awaitExecute`) | ⚪ no dedicated invariant (a handle is the API surface, not a protocol guarantee) |

---

## 3. §5 — step customization

| Concern | Covered by | Notes |
|---|---|---|
| `timeout(Duration)` (per-attempt) | INV-9 `TimeoutsFire` + INV-21 `RetryTimingAndExhaustionEdges` | ✅ wait-timeout (INV-9); per-attempt `execute` `orTimeout` → `TIMED_OUT` pinned by INV-21 (the D5 residual, scenario-pinned) |
| `retryPolicy(maxRetries(n))` + backoff | INV-8 `RetryBound` + INV-21 `RetryTimingAndExhaustionEdges` | ✅ ≤ n+1 attempt records (`STARTED`/`RETRY_STARTED`) and ≤ n `RETRYING` across crash/replay (INV-8); backoff schedule reconstructed from recorded `RETRYING` → `RETRY_STARTED` gaps (INV-21) |
| `BackoffStrategy.fixed`/`linear`/`exponential` | INV-21 `RetryTimingAndExhaustionEdges` (+ INV-8) | ✅ constant / non-decreasing recorded `RETRYING` → `RETRY_STARTED` gaps, survives crash/replay; 🟠 **F-9** (S-3): `exponential` overflows `Duration` at large attempt counts (`base × 2^(attempt-1)`, cap applied AFTER the multiply, `BackoffStrategy.java:72-78`) → `ArithmeticException` on the workflow thread wedges the instance non-terminally (extreme-config/LOW); `S3BackoffExponentialOverflowTest` |
| `RetryPolicy.retryWhile(predicate)` | INV-21 `RetryTimingAndExhaustionEdges` | ✅ stops retrying early (strictly fewer attempts than maxRetries+1) |
| `RetryPolicy.onRetry(handler)` | INV-21 `RetryTimingAndExhaustionEdges` | ✅ fires once per actual retry decision, NOT re-fired on crash/replay (F-0 analogue, HELD) |
| `parameterPayloadReducer` (default LocalOnly) | INV-19 `PayloadReducerSemantics` | ✅ parameter-side view |
| `resultPayloadReducer` (GlobalOnly default / Combine / LocalOnly) | INV-19 + INV-13 | ✅ discard / merge / replace |
| parameter-view vs result-write interplay (independent knobs) | INV-19 `PayloadReducerSemantics` edge (b) | ✅ combine input + global_only write → result discarded (the two reducers are independent) — fuzz `ReducerWorkflow.STEP_INTERPLAY` |
| `combine` last-writer-wins on the same key | INV-19 `PayloadReducerSemantics` edge (c) | ✅ the later same-key combine write wins (per the instance's step sequence, not the F-2 global-append order) — fuzz `ReducerWorkflow.STEP_LWW_FIRST/SECOND` |
| `combine` missing key (step omits a key the payload holds) | INV-19 `PayloadReducerSemantics` edge (d) | ✅ the omitted key keeps its prior global value (`putAll` merges only present keys) — rides every fuzz combine step |
| `combine` with a `null` map value | INV-19 `PayloadReducerSemantics` edge (a) | 🔴 **F-6** (candidate): combine accepts the null but `EventSourcedWorkflowState.payload()`'s `Map.copyOf` rejects it → the next payload read NPEs → `handleWorkflowException` `default` records no terminal → instance wedged non-terminally. Characterized + flagged (`runNullEdge`), **not** patched (test/docs-only). |
| `eventNameCustomizer` (wire names) | INV-22 `EventNameCustomizationSound` | ✅ covered by INV-22: a workflow registered with a custom `eventNameCustomizer` (custom namespace + `workflowBaseName` + a `stepCompleted("Done")` override) records every committed step/status event under the customizer-derived wire name, stable across crash/replay, routing/replay sound under customization — `assertEventNameCustomizationSound` (`CustomNamedWorkflow`, folded into the fuzz). (Changing a *shipped* workflow's customizer namespace is still treated like a step rename — a forbidden migration; INV-22 pins that the engine HONOURS the registered customizer, not that it may change.) |
| `stepName` durable identity | INV-2 (step name = dedup key) implicitly | ✅ relied on by every per-step assertion |

---

## 4. §7 — recipes

| Recipe | Covered by |
|---|---|
| 7.1 sequential happy path | every workflow (`OrderWorkflow` etc.); INV-2/3/4/13 |
| 7.2 race (`anyMatch`, first wins) | INV-14 `CombinatorConsistency` |
| 7.3 guard (`allMatch`) | INV-14 |
| 7.4 retry with backoff | INV-8 `RetryBound` (`OrderWorkflow.shipOrder`, `RetryingWorkflow`) + INV-21 `RetryTimingAndExhaustionEdges` (backoff timing / `retryWhile` / `onRetry` / per-attempt-`execute`-timeout edges via `RetryTimingWorkflow`) |
| 7.5 wait-for-event with correlation | INV-15 `EventCorrelationExact` |
| 7.6 multi-version fork (`migrateVersion`) | INV-11 `VersionRoutingSound` + INV-12 `MigrateVersionContract` |
| 7.7 loop with event wait | ⚪ partially: the correlated-wait + retry loops are exercised (INV-15/INV-8); a dedicated "loop determinism" check was skipped (covered by INV-4 per-instance replay determinism + INV-15), noted in INV-19 |

---

## 5. §3 non-negotiables & §10 anti-patterns

| Rule / anti-pattern | Covered by | Notes |
|---|---|---|
| §3.3 determinism between primitives | INV-4 `DeterministicReplay` | ✅ per-instance content |
| §3.4 step names durable | INV-2 | ✅ dedup key |
| §3.5 terminal primitives are terminal | INV-7 `TerminalIsFinal` + INV-16 `FailurePropagation` | ✅ |
| §3.6 drift guard before state-publishing | INV-18 `DriftGuardPausesCleanly` | ✅ live-induced |
| §3.7 `migrateVersion` contract | INV-12 `MigrateVersionContract` | ✅ |
| §10 `awaitEvent` without `associate(...)` wakes every waiter | INV-15 (directly tests correlation scopes the wakeup) | ✅ |
| §10 combinator `.unmatched()` before `.await()` | INV-14 (reads after `.await()`) | ✅ by construction |
| §10 step timeout is per-attempt | INV-8/INV-9 model `(retries+1)×timeout`; INV-21 drives a per-attempt `execute` timeout past its window to a terminal `TIMED_OUT` | ✅ |
| §10 / §13.4 task queue is bounded (`appendTask` "Too many tasks" at 1000) | INV-23 `EngineSelfProtection` (overflow probe) | ✅ probed: the (bound+1)-th `appendTask` throws cleanly BEFORE publishing — no torn/duplicate committed record; queued ≤1000 tasks stay intact + FIFO (real `SimpleWorkflowExecution.appendTask`) |
| §3.1 no nested primitives | INV-23 `EngineSelfProtection` (nested-primitive probe) | 🟠 **probed** (the RUNTIME effect, not the design-time rule): the engine has NO up-front guard — under the default virtual-thread executor the nested primitive silently COMPLETES, under a single-threaded executor it silently DEADLOCKS the per-instance task queue, NEVER a clean rejection (candidate gap **F-5**); in both cases committed history is uncorrupted. The §3.1 *authoring* rule itself remains a design-time DSL constraint |
| §3.2 Java/Kotlin DSL parity; ADR-001/002/003 layering | — | ⚪ out-of-scope: compile-/design-time DSL constraints, not runtime protocol invariants observable in the event log. Enforced by the DSL build + the `axon-flow-workflow` skill; DST drives the runtime, not the DSL author surface |

---

## 6. ADRs 000–005

| ADR | Covered by | Notes |
|---|---|---|
| 000 ADR conventions | — | ⚪ doc convention |
| 001 command-mode primitives | — | ⚪ design-time API shape; INV-* drive the runtime through the DSL |
| 002 DSL definition records | — | ⚪ design-time DTO shape |
| 003 canonical+convenience DSL | — | ⚪ design-time; DSL parity is a build concern |
| 004 replay safe-point tokens | INV-3 `CommittedHistorySurvivesCrash` (recovery rides `SafePointStore` + replay) | ✅ exercised by every crash/restart fault; 🟠 **F-11**: the safe point (`lowerBound` over every live execution's restart token) is **pinned** forever by a single non-terminal instance (no non-terminal eviction path) → unbounded replay/retention while it lives (INV-3 correctness HOLDS — replay ≥ necessary); `F11SafePointPinnedByNonTerminalTest`. 🔴 **F-12**: the *opposite* over-advance — a graceful-shutdown race removes a still-non-terminal instance and the empty-repository branch (`determineEngineSafePoint` `:319-320`) then persists the **LATEST** token, so on restart `requiresReplay` is false and the in-flight instance is never re-driven (lost recovery; INV-3 history survives but the re-drive fails; graceful-shutdown only, a hard crash cannot hit it); `F12LostRecoveryOnShutdownRaceTest` |
| 005 workflow versioning (semver, `migrateVersion`, four-pass routing) | INV-11 `VersionRoutingSound` + INV-12 `MigrateVersionContract` | ✅; 🔴 **F-37** is the sharding-side consequence: rehydration resolved the recorded definition by **exact version only** while replay falls back to the closest registered one, so a migrated instance could not be restored — and the restore pass failed as a whole, orphaning the rest of the shard. FIXED; `WorkflowEngineRestoreFaultIsolationTest` |
| 014 workflow instance sharding | §10 below (INV-24…INV-28) | see §10 |

> **Stale-statement correction (2026-08-11).** The ADR-004 row above, and `ARCHITECTURE.md`, describe a
> `SafePointStore` engine. **There is no `SafePointStore` in `runtime/src/main` any more** — the base branch replaced
> the safe-point machinery with the framework's checkpointing (ADR-012 checkpoint-driven replay lower bound,
> Axoniq Framework 5.3.0 `Checkpointing`/`CheckpointTrigger`). INV-3 and findings F-11/F-12/F-17 were written against
> the safe-point engine and their mechanisms need re-deriving against checkpointing before they are re-cited as
> current. ADRs 006–013 are not mapped by this matrix at all.

---

## 7. Findings

| Finding | Verdict | Surfaced by | Reproduce |
|---|---|---|---|
| **F-0** `EffectAtMostOnce` | 🔴 engine gap (effect at-least-once) | INV-6 (TLA+ `MC_effect` + DST) | `F0EffectDuplicationTest` (seeds 1,2,3) |
| **F-1** `AtMostOneOwner` | 🔴 multi-node gap (no durable lease) | INV-1 (TLA+ `MC_owner` + DST) | `F1SplitBrainTest` / `F1RecordDuplicationTest` (seed 0) |
| **F-2** `DeterministicReplay` residual | 🟢 works-as-expected (refined: per-instance global-append order is non-det too; harness asserts content) | INV-3/4/7 fuzz | harness-only; pinned seeds 18/252/370 in `RegressionSeedsTest` |
| **F-3** terminated-id re-spawn | 🟠 idempotency design-question | INV-7/INV-10 | `Inv7TerminalIsFinalTest` (F-3 test, seed 0) |
| **F-4** terminal COMPLETED hook dropped | 🔴 engine race (lost lifecycle hook) | INV-17 | `Inv17StatusHookFiresOncePerStatusTest` (non-deterministic; ~half the time) |
| **F-5** nested primitive not guarded | 🟠 robustness gap (no up-front guard — silent deadlock/silent-success, never a clean rejection) | INV-23 | `Inv23EngineSelfProtectionTest` (default executor → completes; single-threaded executor → deadlocks; seed 0) |
| **F-6** `null` payload value wedges the instance | 🟠 robustness gap (reducer/payload boundary) | INV-19 reducer-edge | `Inv19PayloadReducerSemanticsTest` (null-under-combine wedges; seed 0) |
| **F-7** `modifyPayload` re-records a duplicate terminal step | 🔴 engine bug (INV-2 `AtMostOnceRecording` violation; single-node, deterministic) | S-1 investigation (INV-2 surface) | `F7DuplicatePayloadRecordTest` via `DuplicateTerminalPayloadRecordScenario` + `DuplicatePayloadWorkflow` (seed 0; finalizePayload terminal count 1→2 across crash/recover) |
| **F-11** non-terminal instance pins the engine safe point | 🟠 liveness / unbounded-retention (replay cost + memory grow without bound; INV-3 correctness HOLDS) | safe-point machinery (`determineEngineSafePoint` `lowerBound` over `findAll()`, no non-terminal eviction); INV-3/INV-5 cross-ref | `F11SafePointPinnedByNonTerminalTest` via `SafePointPinnedByNonTerminalScenario` (seed 0; safe point pinned at parked A's early token == its restart token while N later instances complete + are evicted and the log head advances ≥ N past it; `crashAndRecover` replays from the pin) |
| **F-12** shutdown/eviction race advances the safe point to LATEST — loses an in-flight instance | 🔴 lost recovery / HIGH (committed in-flight work silently abandoned on recovery; **graceful-shutdown only** — a hard `kill -9` cannot hit it) | safe-point machinery (`shutdown()` interrupts a non-terminal instance → `finishWorkflow` `remove` + `persistEngineSafePoint` while STARTED → empty-repo branch stores LATEST → `requiresReplay` false → straight to live); INV-3/INV-5 cross-ref | `F12LostRecoveryOnShutdownRaceTest` via `LostRecoveryOnShutdownRaceScenario` through the scenario-only **unmasked** `SimulationWorld.crashAndRecoverPersistingEngineSafePoint` (seed 0; safe point after recovery == log head LATEST, in-flight A not re-created, its committed `<workflow>:STARTED` still on the durable log; the shared masked `crashAndRecover()` hides it). **Re-derived 2026-09-23: does not reproduce on the checkpointing engine** (claim restore is event-sourced); `RecoveryAfterNonTerminalExitTest.GracefulShutdown` is green on the baseline and on the #479 fix |
| **F-25** second node cannot start against a shared durable token store | ✅ **FIXED** — the startup scan claimed every token it only needed to read and rethrew when a peer held one | INV-24; confirmed on real infrastructure + confirmed by reading | `NodeJoinIT`, `MultiNodeStartupTest` |
| **F-31** mutual boot livelock — whole cluster fails to start | ✅ **FIXED at the root** (sequential scan, no claim held while reading) | INV-24; **found by TLA+**, then reproduced on real infrastructure | `SegmentClaim_join_violated_seq` (196 distinct) / `_fixed_seq` (124) |
| **F-32** crash during boot poisons the next boot, single node | ✅ **FIXED** — same change; the model pins the cause on `nodeId` defaulting to the pid | INV-24; **found by TLA+**, then reproduced on real infrastructure | `SegmentClaim_restart_violated` / `_fixed` |
| **F-30** instances restored on segment claim can never make progress | ✅ **FIXED** — `copyResources` no longer copies the `EventStoreTransaction` | INV-27 (restored work must be able to commit); reproduced by test | `MigratedInstanceProgressIT` |
| **F-29** catch-up after node loss silently strands waiting instances | ✅ **FIXED for 22 of 24** by the F-30 fix (same root cause); residue is F-34b | reproduced by test, with a live-node control | `CatchUpResumeTest` (both arms) |
| **F-34a** resume event dropped before a restored body re-registers its wait | ✅ **FIXED** — matching moved onto the instance's own task queue | INV-27; reproduced by test | `WorkflowEngineClaimGapWakeTest` |
| **F-34b** wake lost before the wait was ever durably published | 🔴 **OPEN — design-level, not closable here.** No engine or framework change can close it; needs the durable wait-association table tracked in issue **#271**. Vehicle: #271, **not** a gap-pin | outside INV-27 (no wait ⇒ no barrier); reproduced by test | `CatchUpResumeTest` (intermittently red; contiguous-tail signature, distinct from F-34a's scattered losses) |
| **F-26** one `CheckpointTrigger` and one coalesced token served N segments | ✅ **FIXED** — highest severity of the set: **silent event loss** after restart | INV-27; confirmed by reading, cross-checked against `axoniq-event-streaming` 5.3.0 sources | `WorkflowEngineCrossSegmentCheckpointTest` (expected-gap → red → inverted) |
| **F-27** start-on-claim not scoped by segment (**two** call sites) | ✅ **FIXED** at both | INV-25; confirmed by reading + canary, second call site **found by TLA+** | `WorkflowEngineSegmentClaimStartScopeTest`; `cfg_d2_owner` (317 distinct) |
| **F-28** `liveMode` / `currentTrackingToken` engine-wide | ✅ **FIXED** with F-33 | INV-26; confirmed by reading | `WorkflowEngineSegmentLiveModeScopeTest` |
| **F-33** claim-time restore ignored live mode | ✅ **FIXED, with a stated residual** (cross-node migration still starts at head — FW-2) | INV-26; **found by TLA+**, which also proved F-28/F-33 must not be split | `cfg_live_trap`; `WorkflowEngineClaimDuringReplayTest` |
| **F-35** engine shutdown shared a lifecycle phase with the processor | ✅ **FIXED** — a wake was lost on every graceful restart, the ordinary operation | INV-27; **found by TLA+**, then reproduced | `C4_shutdown_clears_first` / `C4b`; `WorkflowConfigurationDefaultsTest` |
| **F-36** materialized-but-not-started execution dropped events and under-reported pending work | ✅ **FIXED** — two halves, **neither works alone** | INV-27; confirmed by reading, then model-checked | `C2_NoSilentDrop`, `M3_restore_ignores_completion`; `WorkflowEngineReplayTest`, `DurableWaitAcrossRebalanceIT` |
| **F-37** one unrestorable instance silently orphaned an entire shard | ✅ **FIXED** — rehydration now resolves like replay, and a restore failure is isolated to its own instance | INV-25 / ADR-005; confirmed by reading | `WorkflowEngineRestoreFaultIsolationTest` |
| **F-13** cancel path re-records a duplicate workflow-terminal `<workflow>:CANCELLED` | 🔴 duplicate durable workflow-terminal record (corruption class; the cancel-path / workflow-terminal analogue of F-7). Same ungated publish, two triggers: **intermittent** crash/replay re-drive race (~1-in-4 crashes — the `Inv7TerminalIsFinalTest` cancel flake) and **deterministic** F-3 start-event redelivery restart. Plus a confirmed **INV-2 coverage gap**: INV-2 counts only step-status terminals, so this duplicate falls solely to INV-7 | INV-7 (the intermittent `Inv7TerminalIsFinalTest` cancel flake); `TerminateDelegate.cancelled`'s unguarded direct `eventSink.publish` (`:178`) + `SimpleWorkflowExecution.handleWorkflowException` CANCELLED branch (`:255-271`), guarded on replay only by `execute()` short-circuit (`:149`) + `switchToLiveMode` eviction filter (`:158-163`) | `F13DuplicateCancelRecordTest` via `DuplicateCancelTerminalRecordScenario`: drive `CancellingWorkflow` to terminal CANCELLED, crash+recover via the shared, unchanged `crashAndRecover()` (count stays 1), then redeliver the start event (deterministic F-3 re-drive) → per-`workflowId` `<workflow>:CANCELLED` count 1→2 (seed 0, every run). `assertTerminalIsFinal` now tolerates a re-published identical terminal status as the F-13 gap so the gated build is deterministically green |

Engine never modified: F-0/F-1/F-4 carry modelled/candidate fixes for a reviewed follow-up; F-3/F-5/F-6 are
documented design/robustness questions; **F-11** is the parked complement of F-6/S-4 (its wedge throws → removed →
safe point advances; F-11 parks → never evicted → pins the floor), a liveness/retention candidate (bound retention /
non-terminal-aware safe-point floor / evict-or-checkpoint long-parked instances) — INV-3 still holds (replay ≥
necessary, never less); **F-12** is the *opposite* over-advance of the same safe-point machinery — a graceful-shutdown
race removes a still-non-terminal instance and the empty-repo branch then persists the LATEST token, so recovery skips
replay and silently abandons the in-flight instance (lost recovery / HIGH; graceful-shutdown only — masked in the
default scenarios by `crashAndRecover()`'s freeze/pin, reached via the scenario-only unmasked recovery path); candidate
fix: do not store the empty-repo 'latest' safe point on a non-terminal removal, or compute the safe point before
removing a non-terminal instance; **F-7** is a confirmed single-node INV-2 violation with a candidate fix
(gate `PayloadDelegate`'s append on `!containsStep` and/or route its publish through the guarded `sendStepEvent`
path like the other primitives); **F-13** is the cancel-path / workflow-terminal analogue of F-7 — the cancel path's
unguarded direct `<workflow>:CANCELLED` publish re-records a duplicate workflow-terminal record across a crash/replay
(an intermittent state-timing race), and INV-2 does NOT catch it because it counts only step-status terminals (confirmed
coverage gap), so it falls solely to INV-7; candidate fix: gate the workflow-terminal publish on "already terminal"
(the F-7-class fix) and/or extend `assertAtMostOnceRecording` to count workflow-status terminal duplicates;
F-2 is harness-only and fixed. (INV-23's no-corruption safety
property HOLDS at both abuse surfaces; F-5 is the *gap* that the engine never surfaces the §3.1 misuse as a
clear error — caught only by liveness/the bounded deadline.)

---

## 8. Explicit out-of-scope / not-modelled (with rationale)

- **Nested primitives §3.1 — the RUNTIME effect is now probed by INV-23** `EngineSelfProtection`: the engine self-protection at this surface (silent complete under the default virtual-thread executor / silent deadlock under a single-threaded executor, never a clean rejection — candidate gap **F-5**; no committed-history corruption either way). Only the §3.1 *authoring* rule itself (you must not write it) stays a compile-/design-time DSL constraint, as do **Java/Kotlin DSL parity §3.2 and ADR-001/002/003 layering** (enforced by the DSL build, not observable as a runtime event-log invariant).
- **`eventNameCustomizer` wire format** (§5): now **covered by INV-22** `EventNameCustomizationSound` (a workflow registered with a custom `eventNameCustomizer` records its step/status events under the customized wire names, stable across crash/replay, routing/replay sound under customization). Only the *migration* concern remains out of scope — changing a shipped workflow's customizer namespace is treated like a step rename (a forbidden migration), which is a design-time constraint, not a runtime invariant.
- **Per-attempt `execute` `orTimeout`** (Phase-3 D5 residual): a non-injectable JDK timer; INV-9 is exercised on the injectable wait-timeout path and pinned by a scenario, not the per-step fuzz set. INV-21 `RetryTimingAndExhaustionEdges` additionally pins the per-attempt `execute` timeout itself — its scenario pre-advances the lock-step `MutableClock` past the per-attempt window so the engine records `TIMED_OUT` via the immediate negative-remaining branch (no real wall-clock waiting), the same scenario-pinned treatment as INV-9.
- **Cross-instance global-log ordering** (F-2): non-deterministic by design; all assertions are per-`workflowId`.
- **Genuinely-overlapping in-flight same-instance branches** (parallel `execute` without await): DST asserts per-instance replay determinism (INV-4), which racing same-instance commits would violate; covered by the engine's own example suite (`AnyRaceWorkflow`/`AllMatchGuardWorkflow`/`NoneMatchGuardWorkflow`).
- **Framework defects are out of this repo's scope, and are not folded into engine workarounds.** Four findings in
  Axon Framework / Axoniq Framework **5.3.0** — **FW-1** (`Context#resources()` mixes plain data with lifecycle-bound
  handles; the root cause of F-30, avoided by a one-type deny-list, not fixed), **FW-2** (claim callbacks omit the
  token while release callbacks carry it; blocks the cross-node half of F-33), **FW-3** (`initializeTokenSegments`
  raises a different exception type per store), **FW-4** (a checkpoint request beyond `lastConsumedToken` is stored
  verbatim rather than clamped; what gave F-26 its blast radius) — need an **upstream vehicle**. Written up as
  ready-to-file issues; **none is filed yet**. Checkpointing is new in 5.3.0 and unproven, so a defect that looks like
  engine misuse may be partly the framework's; where a fix makes the engine work around framework behaviour, that is
  recorded as a framework finding in its own right, because a workaround that hides one leaves the next caller to
  rediscover it.
- **TLA+ scope**: the *original* model covers leasing + crash-recovery only (2 procs / 3 steps / 1 instance); INV-7..23 are DST-only by deliberate scope decision (no workflow status, retries, backoff, retry predicates/handlers, timeouts, versions, payload, combinators, correlations, hooks, drift, event-name customization, per-instance task queue / body executor in the tiny model) — see each INV's "Scope decision" in `INVARIANTS.md`.
  **Superseded in one direction:** three further models now live in `formal/tla/sharding/` — `SegmentClaim.tla`
  (claim / release / expiry / join / boot), `WfShard.tla` (instance ownership, spawn, wake, broadcast, replay
  boundary) and `Holdback.tla` (checkpoint holdback, handover, shutdown ordering) — and they carry INV-24…INV-28. What
  *they* do not model: token positions and the `lowerBound` computation itself, real time (expiry is a fair action,
  not a clock), segment split/merge, more than one handover, DCB consistency markers, the real `Segment.matches` mask,
  and multiple instances per segment except where `Holdback.tla` varies `InstPerSeg`.
- **Segments and nodes are not in the DST harness at all** (INV-24…INV-28 therefore have **no DST assertion**).
  `SimulationConfig` has no segment or node field, segment count cannot be set from outside the engine,
  `InMemoryTokenStore` has a no-op `releaseClaim` and no owner column, and one JVM shares `ClockUtils.instant()` and
  the default `pid@host` node identity — so every ownership, expiry and steal arm would pass **vacuously**. The
  sharding invariants are carried by TLA+, `runtime` unit tests, and the multi-JVM rig instead. Adding the segment
  dimension is the largest outstanding gap in this matrix.

---

## 9. Chaos coverage

The mixed-workload chaos campaign (`SimulationConfig.chaos` / `DstChaosFuzzTest`, 17 instances) drives all of the
above invariants every step under an expanded fault set: worker-crash, message reorder/delay/duplicate, restart,
clock-jump, write-then-vanish **plus** duplicate-completed, event-store latency/jitter, flapping-restart, clock-skew,
partial-batch. Held across the scaled-seed campaign — **2000 distinct normal seeds (0..1999) + 1000 chaos seeds
(0..999)** this round (via the additive `-Ddst.startSeed` chunking, each chunk <15 min), on top of the two
1000-seed harness-fix sweeps and the per-invariant 500-seed runs: **0 new findings**, every invariant held (see
`POC-TLA-DST.adoc` Phase 7/8). The `SegmentClaimContention` fault is deliberately omitted (it would re-surface
F-1, already covered by `F1*Test`).

---

## 10. ADR-014 — workflow instance sharding

Each guarantee ADR-014 states, mapped to the invariant that pins it and the vehicle that exercises it, or an explicit
out-of-scope note. Findings F-25…F-37 and FW-1…FW-4 all come from this surface; see the S1 section of
[`POC-TLA-DST.adoc`](POC-TLA-DST.adoc).

| ADR-014 guarantee | Covered by | Notes |
|---|---|---|
| "A workflow instance belongs to exactly one segment, decided by its identifier" — `SegmentedWorkflowRouting` is the single home of the rule; `String.hashCode()` is JLS-specified, so the mapping is stable across JVMs and restarts | INV-28 `SpawnExactlyOnce` | ✅ stability confirmed on real infrastructure: after a 120 s idle gap the next instance still routes **by hash**, not to the warm node (`IdleClusterSparseTrafficIT`) |
| ADR-019 exception: "Published events are always sequenced by `SequencingPolicy.BROADCAST`, never by a start-candidate id" and are handled as business events (start path + every owned execution, the publisher included) | INV-30 `PublisherObservesOwnPublish` + INV-28 `WakeExactlyOnce` | ✅ model-checked: `MC_publish.cfg` holds; `MC_publish_candidate.cfg` shows the hang candidate routing would cause (the publisher's segment skips its own event), `MC_publish_candidate_wake.cfg` the lost wake on a third segment. Engine-level two-segment routing pinned by `NewInstanceCandidateRoutingTest` on `main`. Not DST-reachable (single segment) |
| "The segment key of a workflow id is the part before the first `#`" | INV-28 | ⚠️ **S-10, untested and undocumented outside the ADR**: `#` is a common id separator, so a `workflowIdProvider` returning `order#123` collapses **every** order onto one segment and sharding silently degrades to one segment; an id *starting* with `#` always maps to segment 0. There is no validation at configuration time. No test pins this |
| "Ownership guards turn broadcast delivery back into exactly-once work" — `shouldHandle` / `shouldSpawn`, every non-owning segment a no-op | INV-28 `SpawnExactlyOnce` + `WakeExactlyOnce` | 🟢 **works as expected, and this is the campaign's strongest precise negative**: both hold with all four modelled defects enabled at 1,722,131 distinct states / depth 31, and fail in 8 steps once the guards are deleted. Duplicate start events are exactly-once on both paths (`DuplicateStartEventsIT`). The duplication risk lives in the release/claim window, **not** in broadcast routing |
| "A failed derivation is treated as no candidate and broadcasts rather than failing the work package" | INV-28 | ✅ by construction; exercised on the broadcast arm of `DuplicateStartEventsIT` |
| "The processor … uses the `TokenStore` component registered by the application; without one it falls back to an in-memory store and logs a warning" | INV-24 | 🟢 **S-8 REFUTED** — the processor's unnamed lookup and the enhancer's named `TokenStore["<module>"]` lookup resolve to the **same instance**, so there is no silent in-memory fallback while a durable store is configured. This was the item most likely to invalidate every multi-node result |
| "The replay decision uses the earliest token across all segments" | INV-24 | 🔴 **F-25/F-31/F-32** — computing it claimed every token it only needed to read. FIXED; a peer-owned segment is now skipped, and all-segments-held yields a null token so the node starts live owning nothing, picking work up on a genuine claim |
| "Checkpoint holdback is per segment" | INV-27 `TokenNeverPassesUnappliedEvent` | 🔴 **F-26** — the holdback *was* scoped; the trigger and pending token acting on it were not, so one segment's request advanced another's stored token past events it never handled (**silent event loss**). FIXED. ⚠️ **FW-4**: the framework stores an over-high request verbatim, so the next caller to make this mistake gets the same silence |
| "Restoration happens on segment claim, not at startup … load their durable state and start those executions" | INV-25, INV-26, INV-27 | 🔴 four findings on this one sentence: **F-30** (restored instances held an already-committed context and were inert), **F-27** (the pass was repository-global at two call sites), **F-33** (it restored from event-store head while the segment token was behind), **F-37** (one unrestorable instance failed the pass as a whole and orphaned the shard). All FIXED |
| "When the segment is released, the engine interrupts and drops them … no cancellation events are written, so the next node resumes each instance from its persisted state" | INV-24 `NoWorkAfterRelease`; INV-27 `WakeSurvivesHandover` | ✅ resumption confirmed on real infrastructure — the durable wait survives a rebalance including `kill -9` (~1000 instance-level observations, zero losses, `DurableWaitAcrossRebalanceIT`), and timers **resume at their original deadline** rather than restarting or being dropped (`TimerAcrossHandoverIT`). ⚠️ **S-9 open**: `releaseSegment` returns before interrupted bodies drain and the framework releases the claim before notifying the engine — model-confirmed double start and double completion, no failing test |
| "Failover follows from this without any coordination of its own" | INV-24 | 🔴 **F-25** made this unreachable in practice — a replacement node could not boot inside the claim timeout, so no scale-up, no rolling deploy and no failover. FIXED and proven end to end by `NodeJoinIT`, `RollingDeployIT`, `WholeClusterRestartIT`, `ScaleDownAndUpIT` |
| "Multi-node operation requires the application to register a durable `TokenStore`" | — | ⚪ out of scope as a runtime invariant, but recorded as a **usability defect**: `docs/reference/**` and `docs/getting-started/**` contain zero occurrences of `segment`, `shard`, `token store`, `node`, `cluster` or `scal*`, and the standard Axon property `axon.eventhandling.processors.Workflow.initial-segment-count` is **silently ignored** in favour of `axoniq.workflow.initial-segment-count`. Both are documentation/configuration issues, not protocol invariants |
| Segment count is configurable and "only applies the first time the token store is initialized" | — | ⚪ **not stated in ADR-014 and untested**: `initializeTokenSegments` runs only when `fetchSegments` is empty, with no validation and no warning on mismatch, so two nodes configured with different counts silently run whichever was written first (**S-12**). Segment counts 1, 2, 3, 7, 8, 32, 64 and 100 were exercised single-node (24/24 instances shipped at every count, zero duplicate steps — non-powers-of-two are clean), but never with mismatched counts across nodes |
