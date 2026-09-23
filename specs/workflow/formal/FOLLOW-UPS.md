# Roadmap — what to do next

Prioritized backlog after the full campaign: TLA+ design model + DST harness, invariants **INV-1..23**,
chaos + scaled-seed fuzz, Kotlin DSL parity, a docs-coverage matrix, and a read-only **engine bug-hunt**.

**Findings so far: F-0…F-22 — plus 1 false alarm (F-2) and 1 unconfirmed suspect (S-6).**
(F-14/F-15 from the P1 production-realism saga campaign, F-16 from the P2 parked-subscription campaign, F-17/F-18
from the P3 rolling-deploy campaign, F-19/F-20/F-21 from the P4 retry-loop/storm campaign, F-22 from the P5
combinator replay deep-dive — see the P1…P5 sections in `POC-TLA-DST.adoc`.)
All are DST-reproduced (or design-modelled), pinned by tests, and documented in
[`POC-TLA-DST.adoc`](POC-TLA-DST.adoc). **F-0 is now FIXED** (a reviewed engine change: `execute` effects
are at-most-once — a crash-interrupted in-flight attempt is not re-run, it resolves through the regular
error flow: no retry → FAILED w/ `StepIndeterminateException`, retry → RETRYING + next attempt; only
`ctx.fail` terminates a workflow, by design). The remaining findings are unfixed (every one ships a
candidate fix + a ready-made acceptance test that flips when the fix lands).

**F-0 follow-up — CLOSED (issue #408 / FND-10):** at-most-once used to be per-*attempt* for retried
steps because the durable log could not distinguish "backoff pending" from "attempt in flight". Every retry
attempt now publishes its own `RETRY_STARTED` (payload `StepRetryInfo`, `attempt` = the attempt starting)
through the same accepted-append gate as the first attempt, and `RETRYING` never runs the action any more.
A crash during `RETRY_STARTED` resumes as an indeterminate attempt with the recorded attempt number, and a
node that lost the instance during the backoff has its `RETRY_STARTED` rejected and never runs the action
(`FencedRetryBackoffTest`, inverted from its expected-gap form).

Vehicle legend: ✅ DST harness as-is · 🧪 a different vehicle (real store / Spring Boot / Kotlin / real threads) ·
🔧 a reviewed engine change.

| Finding | What | Severity | Acceptance test (flips on fix) |
|---|---|---|---|
| F-0 | ~~effect at-least-once on crash-replay~~ → **FIXED (at-most-once)** | 🔧 done | `F0EffectDuplicationTest` (effect == 1, FAILED) + `AtMostOnceRetryResumeTest` (retry → RETRYING) |
| F-1 | no durable cross-node lease → split-brain | — | `F1SplitBrainTest` (single owner) |
| F-2 | per-instance global-append order non-deterministic | **false alarm** | n/a (harness asserts content, not order) |
| F-3 | terminated workflowId can re-spawn | — | dedup decision |
| F-4 | happy-path COMPLETED status-hook dropped | bug | `Inv17StatusHookFiresOncePerStatusTest` |
| F-5 | nested primitive not guarded (executor-dependent) | robustness | `Inv23EngineSelfProtectionTest` |
| F-6 | `null` payload value under `combine` wedges the instance | liveness-wedge | `Inv19PayloadReducerSemanticsTest` (null edge) |
| F-6′ (S-4) | **any** between-primitives `RuntimeException` wedges (same sink) | liveness-wedge | `Inv19PayloadReducerSemanticsTest` (throwing modifier) |
| F-7 | `modifyPayload` double-records a terminal event on post-crash re-run | **corruption (INV-2)** | `F7DuplicatePayloadRecordTest` (count 2→1) |
| F-8 | ~~timeout mis-classified as `StepFailedException(null)`~~ → **FIXED on main** (rebase): all blocking-convenience paths now surface `StepTimedOutException` (a subtype of `StepFailedException`) | 🔧 done | `F8BlockingAwaitTimeoutSurfaceTest` (now asserts the fixed behavior) |
| F-9 | `exponential` backoff overflow at large `maxRetries` | low (extreme-config) | `S3BackoffExponentialOverflowTest` |
| F-10 | `anyMatch` no-match returns `results[0]` as winner | minor | `S5AnyMatchNoMatchWinnerTest` |
| F-13 | cancel path re-records a duplicate `<workflow>:CANCELLED` on crash/replay (cancel-path / workflow-terminal analogue of F-7; INV-2 does NOT catch workflow-status duplicates). **P1 addendum: the FAIL path is the same ungated publish, empirically pinned** (`SagaCompensationTest.failPath_…`, duplicate `<workflow>:FAILED` count 2→1) | **corruption (workflow-terminal; intermittent race)** | `F13DuplicateCancelRecordTest` (count 2→1) + `SagaCompensationTest.failPath_…` |
| F-14 | past-deadline `execute` attempt is dispatched anyway: the action RUNS, its result is DISCARDED, the step records TIMED_OUT (`ExecuteDelegate` checks `remainingTimeout.isNegative()` only AFTER launching the action; no completion handler on the negative path). Production trigger: forward clock jump (the RETRYING-resume trigger is gone since #408: the per-attempt timeout now starts at the attempt's own `RETRY_STARTED`). Retry amplification: every doomed relaunch re-runs the effect | **effect-vs-record divergence (F-0 family)** | `SagaCompensationTest.doomedCompensation_…` (effect 1→0-or-skip, COMPLETED 0, TIMED_OUT) |
| F-15 | a compensation step failing INSIDE a catch block (incl. the engine's own `StepIndeterminateException`/`StepTimedOutException` resolutions) escapes uncaught → `default`-sink wedge, saga left half-compensated and non-terminal (composes the F-0-fix resolution path with the saga pattern); retry-policy compensation survives the same window | **liveness-wedge (F-6/S-4 family, new route)** | `SagaCompensationTest.crashMidCompensation_noRetry_…` (wedge → CANCELLED) + `…_withRetryPolicy_…` (contrast) |
| F-16 | committed signal wakes are matched only at LIVE delivery: a wake lost in a crash window (match→commit) or recovery window (pre-re-registration) is never re-delivered — the replay branch never evaluates wait conditions and nothing re-evaluates the committed suffix; the instance stalls until its (day-scale) wait timeout and takes the WRONG branch. Producer-retry rescue works (pinned) | **lost wake / HIGH (liveness + wrong-branch)** | `ParkedSubscriptionTest.lostWake_…` (park → wake) |
| F-17 | a drift-paused instance is evicted as if finished (`finishWorkflow` runs the termination handler unconditionally — the `:335` TODO) and, repo empty, the LATEST safe point is persisted — the documented drift remedy ("revert/fix and replay") restores NOTHING; the instance is permanently abandoned | **abandoned recovery / HIGH** | `RollingDeployTest.badDeploy_…` (restoredByRollForward false → true) |
| F-18 | drift guard fires only on unreferenced TERMINAL steps, so a structural change without `migrateVersion` runs SILENTLY on an instance parked on a non-terminal wait (no pause, no marker, version-invisible — the injected event inherits the instance's pinned version); its terminal record then poisons a rollback (drift pause on the restored body → F-17 abandonment) | **guard blind spot (HIGH in composition with F-17)** | `RollingDeployTest.badDeploy_…` (fraudCheckRanSilently true → false/pause) |
| F-19 | the blocking-looking `ctx.sleep(String, Duration)` convenience does NOT block (Javadoc says "Blocks"; it delegates to the non-blocking `sleep` and discards the handle; `awaitSleep` is the real blocking variant) — every pacing/backoff built on it is a silent no-op (incl. the canonical example) | **API-contract / silent pacing loss** | `LoopAndStormTest.reusedNamesLoop_…` (body re-enters while sleep STARTED) |
| F-20 | re-entering a still-STARTED timed wait (routine in loops, given F-19) publishes DUPLICATE TIMED_OUT terminals — the publish is gated only on lagging in-memory state (INV-2 violation, F-7/F-13-family). **Phase-1' note:** the duplication REQUIRED the real-time timer race; under the virtual-time `withTimeout` seam the expression became bimodal, so `LoopAndStormTest.reusedNamesLoop_…` is re-pinned to `isNotEqualTo(1)` — bimodal 0 (timer never fires before spin exhaustion) or ≥2 (the duplicate race, load-dependent); exactly 1 = fixed engine. The ungated publish is UNCHANGED (production-reachable via `SystemWorkflowScheduler` real timers). **Hunting-campaign note:** the strict acceptance EXISTS now — carrier + BUGGIFY simultaneously reopens the window on every schedule (44–50 duplicates/run, ≥2 in 80/80 observations; `F20BuggifyInterleavingProbeTest`, promoted seeds {0,4,7}) | **corruption (duplicate step terminals; DST-reproduced under carrier+BUGGIFY, production-reachable)** | `F20BuggifyInterleavingProbeTest` (the strict acceptance: fix flips every schedule to exactly 1) + `LoopAndStormTest.reusedNamesLoop_…` at exactly 1 | ~~F20BuggifyInterleavingProbeTest~~ removed 2026-09-07: the ≥2 mode is closed by the terminal-transition queue discard; `F20InterleavingProbeTest` and `LoopAndStormTest` pin exactly 0 (F-19 open) |
| F-21 | the documented retry-loop recipe AS AUTHORED IN THE CANONICAL EXAMPLE (reused step names) cannot retry: cached-terminal spin, wait never re-registers (blind to fresh signals), no engine guard against step-name reuse; production bodies spin forever invisibly | **broken documented recipe / authoring trap** | `LoopAndStormTest.reusedNamesLoop_…` (1 durable registration vs 50 body iterations) + `counterNamesLoop_…` (contrast) |
| F-22 | combinator `matched()`/`unmatched()` categorization is NOT replay-stable (recomputed per body run from CURRENT branch states; a branch completing after the live short-circuit re-categorizes on the recovered run — body logic keyed on membership diverges live-vs-replay). The DECISION (verdict / anyMatch winner / allMatch violator) IS replay-stable (durable first-completed order). Confirmed by reading + DST + a dedicated TLA+ model (`CombinatorReplay.tla`, `MC_combinator[_fixed].cfg`) | **replay-divergent API surface** | `CombinatorReplayTest.allMatch_…` (re-categorization → stable) |
| F-23 | ~~cancellation landing in a retry BACKOFF window is not honored: the backoff-launch future has no cancellation-to-publish wiring and its scheduled launch is never unscheduled (`RetryableExecuteDelegate.scheduleRetryAttempt`). In-body `ctx.cancel`/`ctx.fail` is held hostage in `cancelAllRunningSteps.awaitStateChange(allTerminal)` for the full backoff and the "cancelled" attempt still RUNS its effect; external/handle `cancelRunningStep` is silently swallowed (step stays durably RETRYING, retry completes as if never cancelled); the drained-failure-task ordering strands a RETRYING step inside a CANCELLED workflow. Confirmed by reading + DST + a dedicated TLA+ model (`BackoffCancel.tla`, `MC_backoffcancel[_fixed].cfg`) ~~ → **FIXED (cancellation redesign, extension-workflow#267)** | **cancellation not honored / effect-after-cancel + stalled terminate** | `BackoffCancelHonoredTest` (effect 1→2 after cancel → cancel lands in the window; no step CANCELLED → CANCELLED) |
| S-6 | `EventWaitConditions` lock-free cross-thread race | **unconfirmed** | needs a real-threads vehicle (Track B) |
| F-24 | the record-COUNTING invariant assertions (INV-8 `assertRetryBound`, INV-12 `assertMigrateVersionContract`) counted raw log occurrences, so an at-least-once store duplicate (`DUPLICATED_APPEND`) ALONE tripped them with zero engine misbehaviour — a latent false-red in every chaos/swarm sweep carrying the fault. Engine verdict across all depth compositions (duplicated `RETRYING` / version marker / combinator decision + crash/recovery, dup+vanished-terminal + control): replay idempotence HOLDS, marker applied once, resume identical with/without the duplicate. **RESOLVED (harness):** both assertions now count distinct committed events by event identifier (a genuine engine re-publish mints a NEW identifier and stays detected); wording synced in `INVARIANTS.md` | **works as expected (engine) / harness hardened (F-2 class)** | `DuplicatedAppendDepthProbeTest` (5 deterministic per-PR probes) |
| F-42 | ~~Workflow Manager two-source read: a finished id vanishes, or is answered STARTED, until the history projection catches up (INV-31/32)~~ → **FIXED** (non-live ids sourced from the `WorkflowStore`) | 🔧 done | `ManagerVisibilityProbeTest` (`afterRemoval` is `COMPLETED`; was `null` / `STARTED`) |
| F-43 | ~~`requestStepCancellation` answers `true` on a fenced node although nothing was recorded~~ → **FIXED** (`cancelStep` answers false when the driver is interrupted before the terminal record, else whether the step is terminal) | 🔧 done | `FencedManagerCancelTest` (`lastAnswer` is `false`; was `true`) |
| S-7 | engine `shutdown()`/`interrupt()` durably FAILS an in-flight execute action, contradicting the shutdown Javadoc's own contract ("no Cancelled events are emitted … the step resumes on the next app start", `WorkflowEngine.java:268-276`): `interrupt()` completes running-step futures with `InterruptedException` (`SimpleWorkflowExecution.java:461-468`), which `AbstractStepExecutor.isCancellation` does NOT recognize (`AbstractStepExecutor.java:242-247`), so `ExecuteDelegate`'s `whenComplete` routes it to the FAILURE handler (`ExecuteDelegate.java:203-209`) → a durable `<step>:FAILED` (no retry policy) is initiated during shutdown while the action may still be running; on restart the step is FAILED (cached), not resumed — a transient shutdown becomes a permanent step failure (and, uncaught, an F-6-family wedge). WAIT and BACKOFF futures are unaffected (their handlers ignore non-cancellation causes), so the doc contract holds for them. NOT DST-reachable today: the same-thread executor completes every execute action synchronously, so no action future is ever in flight at shutdown | **unconfirmed on a running system (confirmed by reading)** | needs a real-threads vehicle (Track B — the F-12 Spring Boot + real Axon Server IT is the natural host: park an action on a latch, `context.close()`, assert no `<step>:FAILED` lands and the step resumes on restart) |

---

## Track A — Fix the confirmed findings + prove (🔧 highest leverage; crosses into engine changes)
Each fix flips its acceptance test from "gap present" to "gap closed". Two cheap wins close most of the wedge family:
- **`handleWorkflowException` drive-to-FAILED** — uncomment the `failedWorkflow(...)` publish in the `default`
  branch (`SimpleWorkflowExecution.java:311-323`) so an unexpected `RuntimeException` between primitives drives
  the instance to FAILED instead of wedging it. Closes **F-6 + F-6′(S-4) + F-9's throw mode** in one change.
  **Done by the #479 fix** (backport `poc/tla_dst-479-backport`): the S-4 and F-15 pins flipped to FAILED.
- **Paused instance drops wakes (F-16 family, after #479).** A paused execution (drift pause, recoverable exception,
  failed append) evolves only its own events and reports no checkpoint work, so a wake delivered during the pause is
  passed by the token and lost to the restored wait. Candidate fix, model-checked by `Holdback.tla`
  `P3_paused_queues_wakes`: queue a matching wake on the paused execution and count it towards the barrier, as F-36 did
  for a restored-not-started execution. Cost: the paused instance then holds its segment's checkpoint back until the
  next start or claim (the F-11 retention shape). Acceptance: the
  `wakeDeliveredDuringThePauseIsNotReEvaluatedAfterRestart_asExpectedGap` pin flips.
- **F-7 `!containsStep` gate** — gate `PayloadDelegate`'s `appendTask` on `!state().containsStep(stepName)` and/or
  route its publish through the guarded `sendStepEvent` path, like `Execute`/`WaitFor`/`Version`. Closes the
  corruption-class duplicate terminal record.
- **F-13 workflow-terminal "already terminal" gate** — gate the workflow-terminal (`cancel`/`fail`/`timeout`) publish
  on "already terminal" / route `TerminateDelegate`'s direct `eventSink.publish` through a guarded path (the F-7-class
  fix), so a recovered re-run cannot re-publish a duplicate `<workflow>:CANCELLED`. AND/OR extend
  `assertAtMostOnceRecording` (INV-2) to also count workflow-status terminal duplicates, closing the confirmed INV-2
  coverage gap (today a duplicate `<workflow>:CANCELLED` is caught only by INV-7). Flips `F13DuplicateCancelRecordTest`
  (count 2→1).
- **F-14 deadline-before-dispatch** — in `ExecuteDelegate`, check `remainingTimeout.isNegative()` BEFORE launching the
  action (skip execution, go straight to the timeout handler) and attach the completion/cleanup handler on every path.
  Flips `SagaCompensationTest.doomedCompensation_…` (effect 1 → 0, the wedge disappears with the F-6 fix).
- **F-23 backoff-window cancellation wiring** — in `RetryableExecuteDelegate.scheduleRetryAttempt`, give the
  REGISTERED backoff future the same cancellation handler the action/wait futures have: on a cancellation cause,
  publish the step's terminal CANCELLED through the guarded `sendStepEvent` path and drop the pending launch (gate the
  scheduled launch task on "not cancelled", not just "not terminal"); and refuse to schedule a retry once a
  cancellation for the step/workflow is in flight (closes the drained-failure-task ordering that strands a RETRYING
  step inside a CANCELLED workflow). Flips `BackoffCancelHonoredTest` (effect stays 1 after cancel; the step records
  CANCELLED; the in-body cancel is no longer held hostage for the backoff). Design contract: `BackoffCancel.tla`
  `MC_backoffcancel_fixed.cfg`.
- **F-15** — closed by the same `handleWorkflowException` drive-to-FAILED fix as F-6/F-6′ (the saga then ends FAILED —
  terminal + alarmable — instead of wedging). Independent docs follow-up: document retry policies on compensation
  steps as the saga idiom (the `…_withRetryPolicy_…` pin shows it survives the crash window cleanly).
- **F-16 re-evaluate-on-registration** — on wait (re-)registration, re-evaluate the committed-event suffix after the
  wait's STARTED for matches (deterministic, bounded scan of the durable log); or evaluate wait conditions in the
  replay `onEvent` branch too (guarded for at-most-once completion publish); or durably record match-intent before
  the completion publish. Flips `ParkedSubscriptionTest.lostWake_…` (recovery alone would wake the instance).
  Caveat for the F-6 drive-to-FAILED fix: a shutdown interrupt surfaces as `RuntimeException(InterruptedException)`
  and lands in the same `default` sink — the fix must special-case wrapped interrupts or every parked instance would
  be driven FAILED on every graceful shutdown.
- **F-17 non-terminal-aware finish** — `finishWorkflow` / the termination handler must distinguish a non-terminal
  completion (drift pause, wedge, shutdown interrupt) from a genuine terminal: retain the instance in the repository
  (or at least never advance the safe point past a removed NON-terminal instance — the F-12-fix sibling; one fix can
  close both). Flips `RollingDeployTest.badDeploy_…` (`restoredByRollForward` false → true).
- **F-18 widen the drift predicate** — detect structural divergence BEFORE a recorded non-terminal step (an
  unreferenced *recorded* step of any status when a NEW step name publishes mid-history), or document that the guard
  does not protect parked instances and the bad-deploy remedy is roll-FORWARD (a `migrateVersion`-gated fix), never
  roll-back.
- **F-19/F-20/F-21 loop-recipe set** — make the `void` `ctx.sleep` convenience actually block (delegate to
  `awaitSleep`, matching its Javadoc); gate the timed-out publish on DURABLE step state / route through the guarded
  `sendStepEvent` (the F-7-class fix — closes F-20); add a per-body-execution guard that rejects re-invoking a
  primitive with an already-terminal step name (catches the loop trap at its first spin — closes F-21's silent
  degeneration); fix the canonical `PaymentWorkflow` example (counter-derived step names + a genuinely blocking
  backoff). Flips `LoopAndStormTest.reusedNamesLoop_…`.
- **F-22 snapshot-at-decision** — record the combinator categorization durably when the combinator resolves (the
  modeled `SNAPSHOT_CATEGORIES` fix — `MC_combinator_fixed.cfg` passes), or freeze the categories at resolution
  time, or document that `matched()`/`unmatched()` must not drive control flow / payload writes. Flips
  `CombinatorReplayTest.allMatch_…`.
- **F-4** — `awaitStateChange(s -> s.workflowStatus().isTerminal())` on the happy completion path, symmetric with
  the fail/cancel/timeout/STARTED paths, so the queued COMPLETED evolve+listener-notify runs before
  `finishWorkflow`'s `taskQueue.clear()`.
- **F-8** — special-case `result.timeout()` → `StepTimedOutException` in `AbstractDSLWorkflowContext.resolveStepPayload`
  / the `awaitExecute` helpers, symmetric with the typed `awaitEvent` fix.
- **F-10** — make the `anyMatch` winner empty / `success()==false` on the no-match-all-completed path.
- **F-9** — clamp the backoff shift/factor (or compute the capped delay without an overflowing `multipliedBy`).
- **F-0 / F-1** — the original modelled fixes (transactional outbox / optimistic append-condition; durable
  single-writer `TokenStore`). TLA+ already corroborates them (`MC_effect_fixed` / `MC_owner_fixed` /
  `MC_record_fixed` pass). These hinge on the real store → pair with Track B.
- **F-3** — decision: dedup spawns against the durable log / a dedup window, vs. accept restart.

## Track B — Untested vehicles (🧪 DST's blind spots)
- **Real Axon Server DCB event store** (Testcontainers) — everything ran on `InMemoryEventStorageEngine`; the
  F-0/F-1 fixes hinge on the real store's **append-condition / optimistic concurrency**, never exercised. Re-run
  the crash/split-brain scenarios on the `examples/bike-rental` Testcontainers + Axon Server path.
- **The Workflow Manager's real projector window (F-42, PR #452)** — the gap is induced deterministically through
  `LaggingHistoryRepository`; the unheld arm saw 0 stale answers on the dev machine because the pooled streaming
  processor delivers the projector's events faster than a finished body can be followed by a manager read. A vehicle
  that pauses the processor's work package, or a `Buggify` fire point on `WorkflowHistoryProjector.handle`, would make
  the live window reproducible under seeds. Acceptance stays `ManagerVisibilityProbeTest` (held arms).
- **Cross-segment publish delivery (ADR-019)** — a published event must reach the publisher's own segment, the
  segment of every instance it starts and the segment of every waiter, which is why the engine sequences `PUBLISH`
  events as `BROADCAST`. The harness runs one segment, so DST proves the single-segment half only; the multi-segment
  half is carried by `PublishRouting.tla` (`MC_publish_candidate*.cfg` show the hang and the lost wake candidate
  routing would cause) and by the engine's two-segment routing unit test on `main` (`NewInstanceCandidateRoutingTest`).
  Vehicle for a real pin: the explicit `initialSegmentCount` harness capability named in `COVERAGE-MATRIX.md` §8, or
  the multi-JVM rig.
- **Real multi-node split-brain** (F-1) — DST fakes it with two engines over a shared in-mem store.
- **Kotlin via `@Workflow` annotation + Spring Boot auto-config** — F-4 especially should be re-checked through
  the real `@WorkflowStatusChangedHandler` annotation + id-property discovery (we registered programmatically).
- **S-6 concurrency stress test** — a real multi-threaded hammer (or jcstress) pounding
  `EventWaitConditions.register()` vs `evaluateAndApply()` to confirm/refute the lock-free race the
  single-threaded simulator cannot reach.
- **SP-5 — `storeSafePointToken` is a non-atomic last-writer-wins** (real threads; from the safe-point hunt). Under
  concurrent async terminations, an out-of-order store can persist a stale `lowerBound`; not reachable by the
  single-threaded harness. Needs a real-threads/jcstress vehicle (like S-6). Caveat: hunted on the older
  `poc/tla-dta` checkout — re-verify on `poc/tla_dst`.
- **Spring Boot `@Workflow` discovery / auto-config validation gaps (A-1…A-5)** — from the spring-boot hunt;
  all 🧪 need a Spring Boot integration test or a registry unit test (the DST harness registers programmatically,
  so none are DST-reachable). Caveat: hunted on `poc/tla-dta`, re-verify on `poc/tla_dst`.
  - **A-1** — `@Workflow` id-property validation is dead for the default provider: a workflow with neither
    `idProperty` nor a custom `idPropertyProvider` registers fine but NPEs at the first start event
    (`PayloadPropertyWorkflowIdProvider("")` → `findById(null)`). The `validateAttributes` guard is gated on
    `WorkflowIdProvider.class` which is never the annotation default. *Proven live by two committed ITs that
    pass without an id provider.* Vehicle: a Spring Boot IT asserting a clear up-front error.
  - **A-2** — a custom `idPropertyProvider` lacking a public no-arg ctor is silently swallowed (`createDefault
    Instance`, with a `// FIXME -> HACK -> Ask Steven`) and falls back to `PayloadPropertyWorkflowIdProvider` →
    NPE or silent mis-routing.
  - **A-3** — a `@WorkflowStatusChangedHandler` method missing its `WorkflowStatus` parameter is silently NOT
    wired (no warn) — a discovery-time sibling of F-4's lost-hook symptom (distinct cause).
  - **A-4** — two same-`workflowName`+same-parsed-version `@Workflow` beans silently collapse via
    `parsedVersionsFor`'s `toMap (a,b)->a` merge → a sibling unreachable on replay. Cheap registry unit test.
  - **A-5** — config-traps: custom `WorkflowContext` subtype without a matching factory; prototype-scoped factory
    missed; `startOnConditions` `=`-only + no type coercion (value always the raw String); a
    `@WorkflowStatusChangedHandler(NONE/STARTED)` accepted but never meaningfully fires.

### Track B — Increment 1 (DONE): first pass on the real store
Vehicle: two Testcontainers ITs in `examples/bike-rental` (`RealStoreRecoveryIT`, `RealStoreSplitBrainIT`) driving
the REAL Axon Server DCB event store + the real Spring Boot `@Workflow` path with manually-managed application
contexts, plus one plain registry unit test in `runtime` (`SameVersionDuplicateRoutingCollapseTest`). The ITs are
`*IT`-named and excluded from the default build exactly like `BikeRentalIT` (failsafe is pluginManagement-only,
never bound); run them explicitly:
`./mvnw -pl examples/bike-rental -am test-compile failsafe:integration-test failsafe:verify -Dit.test=RealStore... -Dfailsafe.failIfNoSpecifiedTests=false`.

- **F-12 — CONFIRMED on real infra, upgraded from "candidate race" to DETERMINISTIC on the graceful-shutdown
  path** (`RealStoreRecoveryIT`, green pin). Park one instance (PaymentWorkflow in its `anyMatch`), close the
  context gracefully: shutdown logs "interrupting running steps of 1 workflow instance(s)", but the durable JDBC
  safe point ends at the stream head (`GlobalSequenceTrackingToken {"globalIndex":12}` == LATEST) — the
  interrupt-unwound `finishWorkflow` persists the empty-repo safe point AFTER `shutdown()`'s good persist, inside
  the issue-#125 executor drain. The restarted app sees safePoint==latest, skips the replay ("No running workflow
  instances found", logged before the Workflow processor even starts) and the instance is silently lost. The late
  wake (`acceptPayment` after restart) is durably committed and projected (payment APPROVED) but rescues nothing —
  the rental never reaches RENTED. **F-16's producer-retry mitigation does NOT extend to F-12**: it wakes a
  surviving instance, not a lost one. When the F-12 fix lands, flip the IT's assertions (restart must log
  "Restored 1 running workflow instances"; the late wake must drive RENTED).
  **2026-09-23:** on `poc/tla_dst`'s checkpointing engine the DST analogue does not reproduce: a claim restores every
  non-terminal instance from its own history, so a graceful restart restores the parked instance and a late wake
  completes it, before and after the #479 fix (`RecoveryAfterNonTerminalExitTest.GracefulShutdown`). The IT lives in the
  old repo's `examples/bike-rental` and has not been re-run here; re-run it on the real store before quoting F-12 as open.
- **F-1 — CONFIRMED on real infra** (`RealStoreSplitBrainIT`, green pin). Two app nodes (separate H2s, separate
  ports) against ONE Axon Server: segment claims are client-side `TokenStore` state in Axon Framework and the
  engine hardcodes `new InMemoryTokenStore()` (`AllEventEventHandlingComponent.ANY_EVENT_IN_ONE_SEGMENT`), so
  Axon Server does NOT arbitrate ownership — both nodes claim segment 0, both engines spawn the SAME workflow id
  for one `BikeRequestedEvent` (two "Creating a new workflow '<same-id>'" lines) and one rental commits TWO
  `PaymentPreparedEvent`s (distinct payment ids, same reference) to the shared store — two PENDING payments for
  one rental, visible from both nodes. The duplicate effect is durable in the real store, refuting the hope that
  server-side PSEP coordination would mask F-1 on this topology.
- **Production SafePointStore — ANSWERED**: the engine default is `InMemorySafePointStore` (an `AtomicReference`
  — NOT durable; a default Spring Boot app loses the safe point on every restart and recovery anchors to
  `firstToken()`, i.e. a full replay). `TokenStoreSafePointStore` is used only when the app registers a
  `TokenStore` bean named `WorkflowEngineSafePointTokenStore` (`WorkflowConfigurationDefaults
  .registerSafePointStore`); nothing in the spring-boot module auto-config registers one — the bike-rental
  example does it by hand (JDBC/H2, non-JPA table `WF_TOKEN_ENTRY`, survives `ddl-auto: create`). Note the
  sharpened irony: the durable safe point is exactly what makes the F-12 loss sticky (the overwritten LATEST
  survives the restart and suppresses the replay), while the non-durable default trades it for a full replay.
- **A-4 — pinned** (`SameVersionDuplicateRoutingCollapseTest`, runs in the normal `runtime` build): two
  same-name+same-parsed-version registrations are accepted silently; the spawn path sees both
  (`getHighestVersionConfigurations`, run-in-parallel semantics) but ALL version-routing lookups
  (`findByWorkflowNameAndVersion` / `findClosestRegisteredVersion` / `findClosestHigherRegisteredVersion`)
  collapse to the FIRST registration via `parsedVersionsFor`'s `toMap((a,b)->a)` — the second sibling is
  unreachable on replay.

Remains for later increments: A-1…A-3, A-5 (Spring Boot discovery/validation ITs); S-6 + SP-5 (real-threads /
jcstress vehicle); F-4 re-check through the real `@WorkflowStatusChangedHandler` annotation path; and the
F-0/F-1 **fix validation** on the real store's append-condition / a durable shared workflow-processor token store
(needs the fixes implemented first — the ITs above pin today's broken behavior and document the flips to make).

## Test-infra residuals (DST harness, not engine findings)
- **Multi-iteration loop pacing — RESOLVED (opt-in aligned-clock mode)**. `SimulationWorld` gained an OPT-IN
  aligned-clock constructor (`SimulationWorld(seed, registration, alignedEventClock=true)`): for the world's
  lifetime the JVM-global static Axon event-timestamp clock (`GenericEventMessage.clock`, AF 5.1.1 — explicitly
  documented as test-settable) is pointed at the world's `MutableClock` (restored in `close()`), so committed event
  timestamps and the injected engine clock share ONE virtual era and the recorded-STARTED-vs-now timer math
  (`WaitForDelegate`) is exact — multi-step timeout sequences are deterministically drivable with plain
  `advanceTime` calls, no era anchoring. Pinned by
  `LoopAndStormTest.alignedClock_counterNamesLoop_pacesMultipleGenuineIterations_deterministically` /
  `LoopAndStormScenario.alignedCounterNamesMultiIterationPacing`: three genuine counter-names iterations (poll#1
  TIMED_OUT on a 10s advance, the 2s sleep paced by its own advance, poll#2 the same, poll#3 COMPLETED on the
  signal; per-iteration records each exactly once, iteration effect = 3). Deliberately NOT the default: the whole
  existing workload depends on the D5 era gap (default 5s per-attempt step timeouts must keep "never firing" under
  CLOCK_SKEW/CLOCK_JUMP), so default-mode behaviour is byte-for-byte unchanged; the static is JVM-global, so
  aligned-mode worlds must not run concurrently with other worlds (the suites are single-threaded per JVM).
- **F-16 door-(b) lacks a deterministic pin** — the recovery-window drop (a signal delivered after `crashAndRecover`
  returns but before the recovered body re-registers its wait) is async-raced and only observed, not pinned (the
  P2 churn scenario now deliberately gates on the re-registered wait before delivering). A live-switch seam (a hook
  exposing "engine recovered, body re-runs not yet started") would make it pinnable deterministically — the
  aligned-clock seam does not help here (the window is an async live-switch race, not a timer-era problem).
- **Nightly-fuzz max-steps load-residual (seed-339 class) — RESOLVED (settle-aware step counter)**. The
  `DstSimulation` main loop now charges the max-steps cap in EFFECTIVE steps: only an iteration whose settle reached
  genuine quiescence (committed log stable for the quiet window, pending timers drained or established never-due —
  the settle's own outcome) counts against `config.maxSteps()`; an iteration where the engine's durably-async work
  still lagged under machine load does not burn the cap — exactly the load-lag that made a loaded 500-seed nightly
  occasionally trip an `EventuallyTerminates` HARNESS-ABORT on a seed that passes in isolation. Reproducibility is
  preserved by construction: the iteration structure and the per-iteration seeded fault draws are untouched (one
  draw per iteration, same order); quiescence feeds ONLY the abort accounting, never the RNG sequence
  (`DstSmokeTest.sameSeedIsReproducible` stays green). Termination stays provable: the wall-clock deadline remains
  the primary hard stop, and an absolute iteration backstop (10× maxSteps) bounds the loop even if quiescence
  detection were wrong. The existing maxSteps caps are unchanged.

## Track C — More hunting / coverage (✅ DST-doable) — **DONE**
A second read-only bug-hunt (3 parallel hunters: combinators, Spring Boot auto-config, safe-point/replay) +
serialized DST reproduction. Yielded **F-11** (safe-point pinned by a non-terminal instance), **F-12** (lost
recovery on a graceful-shutdown race, HIGH), **F-13** (cancel-path duplicate `<workflow>:CANCELLED`, corruption,
+ the INV-2 workflow-status coverage gap) — all reproduced, pinned, committed. The remaining suspects are MINOR
and were **characterized but not DST-pinned** (a deliberate scope decision — the high-value findings are landed):

- **C-2 — `noneMatch(WorkflowStepResult::failure)` passes on a CANCELLED/TIMED_OUT branch** (✅ DST-reachable;
  INV-14; minor, predicate-semantics foot-gun). `StateBasedWorkflowStepResult.failure()` is true only for
  `FAILED`, but `StepStatus.isTerminal()` includes CANCELLED+TIMED_OUT, so `NoneMatchCombinatorDelegate`'s
  all-completed check reports `success()==true` when a branch was cancelled or timed out — a guard meant to catch
  "no branch failed" silently passes. The Javadoc never calls out cancel/timeout. Pin: a combinator with a
  timed-out branch (force via the `MutableClock`, INV-9/21 style) under `noneMatch(failure)`, assert `success()`.
  Candidate fix: doc the predicate's cancel/timeout semantics, or treat them as failures.
- **SP-3 — a null-restart-token instance freezes the engine safe-point persist** (MAYBE DST-reachable; INV-3/INV-5;
  low). `WorkflowEngine.determineEngineSafePoint` returns `null` (skips `storeSafePointToken`) if ANY active
  execution's `restartToken()` is null; the first-ever instance (no prior processed event) has a null restart
  token (tests `firstWorkflowStartWithoutPreviousEventTokenDoesNotStoreEngineSafePoint`). So while a null-token
  instance is live, EVERY `persistEngineSafePoint` is a no-op engine-wide → the safe point freezes stale (more
  replay on recovery). Sibling of F-11 (both keep `lowerBound` from advancing). Needs a no-prior-token start seam.
- **C-4 — duplicate branch step-name breaks the `matched()+unmatched()==all-inputs` contract** (✅ DST-reachable;
  INV-14; minor, constructible). `CombinatorSupport.computeCategories` keys matched names in a `Set<String>` and
  `findFirstByPredicate` filters by `getStepName().equals(name)`, so passing the same `WorkflowStepResult` twice
  (or two branches that resolve to the same step name) into a combinator can pull both into `matched`/drop one
  from `unmatched`, violating the documented size identity. Unlikely in normal DSL use (distinct steps).
- Also surfaced by the combinator hunter (documented, not findings): **C-1** (allMatch/noneMatch success path
  `result()`==empty — by-design, the F-10 mirror), **C-3** (a `findFirstByPredicate` array-order fallback that is
  dead code for the production state-backed result, latent), **C-5** (short-circuit `await()` leaves still-running
  branches in `unmatched()` — documented behavior). **SP-4** (restart token = previous event's token — a
  conservative over-replay by one event, benign). **SP-5** → Track B (real threads).

## Track D — Extend the TLA+ model (🔧/🧪 design-level corroboration) — **DONE**
F-4 and F-7 now have the same two-way TLA+↔DST corroboration as F-0/F-1/F-22, via two standalone tiny models
(run with `-deadlock`, ~1 s each): `HookOnCompletion.tla` — `MC_hook.cfg` violates `StatusHookFiresOncePerStatus`
with the exact clear-before-notify happy-path counterexample, `MC_hook_fixed.cfg` (`AWAIT_TERMINAL_STATE=TRUE`)
passes; `PayloadRepublish.tla` — `MC_payload.cfg` violates `AtMostOnceRecording` with the exact crash-then-re-publish
counterexample (the gated execute-style contrast step stays at 1), `MC_payload_fixed.cfg` (`CONTAINS_STEP_GATE=TRUE`)
passes. Bridge rows added to `formal/tla/README.md`.

---

## Top priorities by risk reduction
1. **Track A — `handleWorkflowException` drive-to-FAILED + F-7 gate** (two near-free engine fixes; close a
   corruption-class finding + the whole non-terminal-wedge family; acceptance tests already written).
2. **Track B — real Axon Server store** (the append-condition is load-bearing for F-0/F-1, totally unexercised).
3. **Track C — DONE** (combinators + auto-config + safe-point): yielded F-11/F-12/F-13; C-2/SP-3/C-4 characterized
   (minor, not pinned); spring-boot A-1…A-5 + SP-5 deferred to Track B; one nightly-fuzz max-steps residual logged.
