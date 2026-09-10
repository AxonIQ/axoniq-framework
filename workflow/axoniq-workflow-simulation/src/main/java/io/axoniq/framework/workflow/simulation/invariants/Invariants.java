/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.simulation.invariants;

import io.axoniq.framework.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.simulation.workflow.CombinatorWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CorrelatedWaitWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CustomNamedWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.PublishChainWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.StatusHookFires;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The protocol invariants from {@code formal/INVARIANTS.md}, asserted against the committed event log and the
 * workflow history read-model. Each method's name is the invariant's {@code MachineName} verbatim (the
 * Cross-reference contract that Phase 5 verifies), and each method's intent is worded identically to INVARIANTS.md.
 * INV-1..6 are the original leasing/crash-recovery set; INV-7 ({@code TerminalIsFinal}), INV-8 ({@code RetryBound}),
 * INV-9 ({@code TimeoutsFire}), INV-10 ({@code OneInstancePerStart}), INV-11 ({@code VersionRoutingSound}), INV-12
 * ({@code MigrateVersionContract}), INV-13 ({@code NoLostPayloadWrites}), INV-14 ({@code CombinatorConsistency}),
 * INV-15 ({@code EventCorrelationExact}), INV-16 ({@code FailurePropagation}), INV-17
 * ({@code StatusHookFiresOncePerStatus}), INV-18 ({@code DriftGuardPausesCleanly}), INV-19
 * ({@code PayloadReducerSemantics}), INV-20 ({@code VersioningEdges}) and INV-21
 * ({@code RetryTimingAndExhaustionEdges}) are DST-checked safety properties
 * (no
 * step/status event after an instance's
 * terminal workflow status — see {@link #assertTerminalIsFinal}; at most {@code maxRetries+1} attempt records per
 * retrying step — see {@link #assertRetryBound}; a step whose configured timeout window elapses reaches a
 * {@code TIMED_OUT} outcome — see {@link #assertTimeoutsFire}; at most one LIVE instance per {@code workflowId} at a
 * time, with the after-terminal re-spawn F-3 tolerated — see {@link #assertOneInstancePerStart}; under multi-version
 * registration, every instance routes to exactly one definition at the highest registered version, deterministically
 * across replay — see {@link #assertVersionRoutingSound}; {@code ctx.migrateVersion} records a {@code changeId}'s
 * version at most once, monotonic non-decreasing, stable across replay — see {@link #assertMigrateVersionContract}; the
 * final committed payload reflects every committed step's recorded contribution, so no committed payload write is lost
 * across crashes/replays modulo a later same-key overwrite — see {@link #assertNoLostPayloadWrites}; a combinator's
 * decision is consistent with the documented short-circuit semantics, a pure function of its branch steps' committed
 * terminal outcomes, and stable across crash/replay — see {@link #assertCombinatorConsistency}; and an
 * {@code associate(...)}-correlated event wakes exactly the matching waiting instance — never a non-matching one (no
 * cross-wakeup) — and duplicate/uncorrelated events produce no spurious wait completion — see
 * {@link #assertEventCorrelationExact}; a step failure propagates to a terminal FAILED workflow status — the workflow
 * never silently hangs or completes when a step fails — see {@link #assertFailurePropagation}; a registered
 * status-change hook fires at most once per status and is not re-fired on crash/replay — the lifecycle-hook analogue of
 * INV-6/F-0 — see {@link #assertStatusHookFiresOncePerStatus}; and when replay drift is detected
 * ({@code guardAgainstReplayDrift} throws) the engine pauses the instance non-terminally and cleanly — no terminal
 * workflow status, no spurious drifted-step event, committed history intact (the documented INV-5 carve-out) — see
 * {@link #assertDriftGuardPausesCleanly}; and each payload reducer produces its documented merge into the engine's
 * reconstructed payload — {@code global_only} discards the result, {@code combine_local_and_global} merges it key-by-key,
 * {@code local_only} replaces the whole payload, and the {@code parameterPayloadReducer} governs the step's input view —
 * stable across crash/replay (extending INV-13) — see {@link #assertPayloadReducerSemantics}; and the versioning edges
 * INV-11/INV-12 do not cover — a {@code ctx.migrateVersion} downgrade is rejected and never recorded, multiple distinct
 * {@code changeId}s each record at most once and monotonic non-decreasing, and a deeper 3-version registry routes a
 * fresh start to the highest version and an instance recorded at an older version to the closest registered sibling
 * (never 0, never 2 definitions) — see {@link #assertVersioningEdges}; and the retry/timeout edges INV-8/INV-9 did not
 * cover — a step under a {@code BackoffStrategy} retries on the schedule reconstructed from the recorded {@code RETRYING}
 * timestamps (survives crash/replay), a {@code retryWhile} predicate stops retrying with strictly fewer attempt records
 * than {@code maxRetries + 1}, every retrying step resolves to a terminal record, a per-attempt {@code execute} timeout
 * reaches {@code TIMED_OUT}, and an {@code onRetry} handler fires once per actual retry decision and is NOT re-fired on
 * crash/replay (the retry-handler analogue of INV-6/F-0) — see {@link #assertRetryTimingAndExhaustionEdges} and
 * {@link #assertOnRetryFiredOncePerRetry}; and a workflow registered with a custom {@code eventNameCustomizer} records
 * its step/status events under the CUSTOMIZED wire names, those names are stable across crash/replay (a pure function of
 * history), and the engine still routes/replays correctly under customization — see
 * {@link #assertEventNameCustomizationSound} (INV-22); and the engine self-protects at its two abuse surfaces — a
 * §3.1-forbidden nested primitive and a task-queue overflow — by never CORRUPTING/TEARING an instance's committed
 * history (the worst it does is stall non-terminally or throw cleanly), see {@link #assertEngineSelfProtection} and
 * {@link #documentNestedPrimitiveNotGuarded} (INV-23)).
 * <p>
 * These are checked after every simulation step. A genuine break throws {@link InvariantViolation} (the harness then
 * prints the seed + full log and re-runs to confirm reproducibility). The <em>expected</em> gaps confirmed by TLA+ —
 * {@code EffectAtMostOnce}/F-0 ({@code MC_effect.cfg}), {@code AtMostOneOwner}/F-1 ({@code MC_owner.cfg}), and the F-1
 * consequence on {@code AtMostOnceRecording} under split-brain ({@code MC_record.cfg}) — are checked here as the
 * engine's actual guarantee (effect at-least-once; ownership within a single process; ≤1 terminal record only while
 * INV-1 holds) so the smoke run stays green; the {@link #documentEffectAtMostOnceGap},
 * {@link #documentSplitBrainOwnership} and {@link #documentDuplicateRecordingUnderSplitBrain} helpers assert each gap
 * is present so a future fix re-breaks the documented expectation. INV-17's finding <strong>F-4</strong> (the terminal
 * COMPLETED lifecycle hook was fired at-most-once but could be silently <em>dropped</em> on the happy path) is now
 * <strong>FIXED</strong>: the engine's happy completion path now {@code awaitStateChange}s on the terminal status before
 * {@code finishWorkflow} (symmetric with the awaited fail/cancel/timeout paths), so the COMPLETED hook fires reliably
 * exactly once — enforced by {@link #assertCompletedHookFiredExactlyOnce} (alongside
 * {@link #assertStartedHookFiredExactlyOnce}), while its no-re-fire facet (the F-0 analogue) remains enforced by
 * {@link #assertStatusHookFiresOncePerStatus}; {@link #documentTerminalHookMayBeDropped} is retained as the re-fire
 * guard / drop detector (post-fix it always returns false). INV-23 surfaces a
 * further candidate finding <strong>F-5</strong> (the engine has no up-front guard against a §3.1-forbidden nested
 * primitive — it surfaces only as a silent deadlock under a single-threaded body executor, or a silent success under
 * the default virtual-thread executor, never a clean rejection); {@link #documentNestedPrimitiveNotGuarded} captures it
 * the same way, while {@link #assertEngineSelfProtection} enforces the no-corruption safety property that DOES hold.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class Invariants {

    private Invariants() {
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-1: AtMostOneOwner (Safety, Partial, F-1)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-1 — At-most-one owner of an instance's processing: at any instant, at most one process is actively driving a
     * given workflow instance's body and appending its step events; two processes must never concurrently advance the
     * same instance ("split-brain").
     * <p>
     * Holds trivially within a single engine process (single segment, single-consumer task queue). The harness passes
     * the number of engine processes that currently believe they own segment 0; this asserts it is at most one.
     *
     * @param activeOwnerCount number of engine processes holding the processing claim right now.
     */
    public static void assertAtMostOneOwner(int activeOwnerCount) {
        if (activeOwnerCount > 1) {
            throw new InvariantViolation(
                    "AtMostOneOwner",
                    "two or more processes concurrently own segment 0 (count=" + activeOwnerCount + ")");
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-2: AtMostOnceRecording (Safety, Yes)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-2 — No step recorded twice (at-most-once recording): within one instance's history, a given step name
     * reaches a terminal outcome at most once — no step is recorded as COMPLETED (or FAILED/CANCELLED/TIMED_OUT)
     * twice.
     * <p>
     * Asserted directly against the committed event log: for every {@code (workflowId, stepName)} the number of
     * committed terminal step records is at most one.
     *
     * @param committedLog the committed workflow event log.
     */
    public static void assertAtMostOnceRecording(List<EventMessage> committedLog) {
        Map<String, Integer> terminalCounts = new LinkedHashMap<>();
        for (EventMessage event : committedLog) {
            var stepStatus = MetadataUtils.getStepStatus(event.metadata());
            if (stepStatus.isEmpty() || !stepStatus.get().isTerminal()) {
                continue;
            }
            var key = MetadataUtils.getWorkflowId(event.metadata()) + "/" + MetadataUtils.getStepName(event.metadata());
            int count = terminalCounts.merge(key, 1, Integer::sum);
            if (count > 1) {
                throw new InvariantViolation(
                        "AtMostOnceRecording",
                        "step '" + key + "' recorded a terminal outcome " + count + " times");
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-3: CommittedHistorySurvivesCrash (Safety, Yes)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-3 — Committed history survives a crash (durability): any workflow event that was durably committed to the
     * event store before a crash is still present, in the same order, after recovery — recovery never loses or
     * reorders committed history.
     * <p>
     * Asserted <strong>per instance</strong>: for every {@code workflowId}, its pre-crash committed subsequence must be
     * a prefix (order preserved) of its post-recovery subsequence — no committed event of that instance is lost or
     * reordered relative to that instance's own history. The cross-instance interleaving of the single global log is
     * deliberately <em>not</em> asserted here: it is the F-2 surface (ARCHITECTURE.md §8/§11 — {@code findAll()} Set
     * order, body-thread scheduling, processor delivery timing) and is order-independent because distinct instances are
     * independent. Two independent instances swapping their relative position in the merged global log across a recovery
     * is therefore not a durability break; losing or reordering an event <em>within</em> one instance's history is. This
     * is the same per-{@code workflowId} notion of order used by {@link #assertDeterministicReplay} (INV-4) and the
     * harness's intra-run prefix-stability check. A raw global-index comparison would spuriously flag the
     * non-deterministic cross-instance interleaving as a violation (the harness bug fixed alongside the
     * {@code RestartFault}/{@code WorkerCrashFault} regression seeds — see {@code RegressionSeedsTest}).
     *
     * @param committedBeforeCrash snapshot of the committed log taken immediately before the crash.
     * @param afterRecovery        committed log after the engine recovered.
     */
    public static void assertCommittedHistorySurvivesCrash(List<EventMessage> committedBeforeCrash,
                                                           List<EventMessage> afterRecovery) {
        var before = perWorkflowSubsequences(committedBeforeCrash);
        var after = perWorkflowSubsequences(afterRecovery);
        for (var entry : before.entrySet()) {
            String workflowId = entry.getKey();
            List<String> beforeEvents = entry.getValue();
            List<String> afterEvents = after.getOrDefault(workflowId, List.of());
            if (afterEvents.size() < beforeEvents.size()) {
                throw new InvariantViolation(
                        "CommittedHistorySurvivesCrash",
                        "post-recovery history for '" + workflowId + "' (" + afterEvents.size() + " events) is shorter "
                                + "than its pre-crash committed history (" + beforeEvents.size() + " events) — committed "
                                + "events were lost across recovery");
            }
            for (int i = 0; i < beforeEvents.size(); i++) {
                if (!beforeEvents.get(i).equals(afterEvents.get(i))) {
                    throw new InvariantViolation(
                            "CommittedHistorySurvivesCrash",
                            "committed event for '" + workflowId + "' at index " + i + " changed across recovery: "
                                    + "before=" + beforeEvents.get(i) + " after=" + afterEvents.get(i)
                                    + " — that instance's committed history was lost or reordered");
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-4: DeterministicReplay (Safety, Partial, F-2)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-4 — Deterministic replay: replaying the same committed history always rebuilds the same instance state and
     * drives the same sequence of step decisions — replay never diverges from the run that produced the log, and
     * re-running it twice yields the same result.
     * <p>
     * Asserted at run granularity, <strong>per instance</strong>: two independent runs of the same seed must produce
     * the identical committed event <em>set</em> for every workflow id (same step names + statuses, same multiplicity).
     * Compared as a per-instance <strong>multiset</strong> (each instance's descriptors sorted canonically) rather than
     * by global-append order: the single global log's interleaving is the F-2 non-deterministic surface (ARCHITECTURE.md
     * §8/§11: {@code findAll()} Set order, thread scheduling) — and that non-determinism is not only <em>cross</em>-
     * instance but, for events an instance commits close together, <em>intra</em>-instance too, because the engine
     * publishes durably-async, so a step's terminal record can be globally appended just before or after the
     * workflow-completion commit run-to-run. What is deterministic per instance is the <em>content</em> (which events,
     * with what multiplicity), not their global-append order; this asserts that content. A genuine replay divergence —
     * an instance gaining, losing, or changing an event between two runs of the same seed — is still caught (the
     * multisets differ).
     *
     * @param firstRunLog  committed log from the first run of a seed.
     * @param secondRunLog committed log from a second, independent run of the same seed.
     */
    public static void assertDeterministicReplay(List<EventMessage> firstRunLog,
                                                List<EventMessage> secondRunLog) {
        var first = perWorkflowMultisets(firstRunLog);
        var second = perWorkflowMultisets(secondRunLog);
        if (!first.equals(second)) {
            throw new InvariantViolation(
                    "DeterministicReplay",
                    "two runs of the same seed produced different per-instance committed event sets (F-2):\n  run1="
                            + first + "\n  run2=" + second);
        }
    }

    /**
     * Per-instance committed event <em>multiset</em>: each {@code workflowId}'s descriptors sorted canonically, so the
     * comparison is independent of the F-2 (cross- and intra-instance) global-append order. Two logs with the same
     * per-instance events (same multiplicity) compare equal regardless of the order those events landed in the global
     * log. Used by {@link #assertDeterministicReplay}.
     */
        private static Map<String, List<String>> perWorkflowMultisets(List<EventMessage> committedLog) {
        var byWorkflow = perWorkflowSubsequences(committedLog);
        byWorkflow.values().forEach(java.util.Collections::sort);
        return byWorkflow;
    }

        private static Map<String, List<String>> perWorkflowSubsequences(List<EventMessage> committedLog) {
        var byWorkflow = new java.util.TreeMap<String, List<String>>();
        for (EventMessage event : committedLog) {
            byWorkflow.computeIfAbsent(MetadataUtils.getWorkflowId(event.metadata()),
                                       k -> new ArrayList<>()).add(describe(event));
        }
        return byWorkflow;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-5: EventuallyTerminates (Liveness, Partial)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-5 — Every workflow eventually completes (no permanent stall): a started workflow does not get stuck forever;
     * under fair scheduling and given its awaited events/timers eventually fire, every instance eventually reaches a
     * terminal status (COMPLETED, FAILED, CANCELLED, or TIMED_OUT) — modulo intended waits
     * ({@code waitForEvent}/{@code sleep}), which resolve once their event/timeout arrives.
     * <p>
     * Asserted at the virtual-time horizon: after all awaited events have been delivered and virtual time advanced
     * past every scheduled timeout, no workflow instance may remain live (a still-live instance at horizon is a
     * liveness failure unless it is the intended drift-pause sink, which this workflow never enters).
     *
     * @param liveWorkflowIdsAtHorizon ids of instances still live after the run reached its time horizon.
     */
    public static void assertEventuallyTerminates(List<String> liveWorkflowIdsAtHorizon) {
        if (!liveWorkflowIdsAtHorizon.isEmpty()) {
            throw new InvariantViolation(
                    "EventuallyTerminates",
                    "workflow instance(s) " + liveWorkflowIdsAtHorizon + " did not reach a terminal status by the "
                            + "virtual-time horizon (every awaited event delivered and all timers fired)");
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-6: EffectAtMostOnce (Safety, No, F-0)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-6 — Effect at-most-once vs at-least-once (the F-0 gap): a recorded step's external side effect (its
     * {@code execute} action) should run at most once across the workflow's whole lifetime, including across crashes
     * and replays. Today it does not: a crash between the action running and its COMPLETED event committing re-runs
     * the effect on replay (at-least-once, not at-most-once).
     * <p>
     * This method asserts the <em>target</em> property (effect count ≤ 1). It is expected to FAIL once the
     * write-then-vanish fault has fired — callers that intend to observe F-0 must use
     * {@link #documentEffectAtMostOnceGap} instead, which asserts the gap is present so the smoke run stays green.
     *
     * @param workflowId the workflow instance.
     * @param stepName   the step whose effect is checked.
     * @param effectCount how many times that step's external effect actually ran.
     */
    public static void assertEffectAtMostOnce(String workflowId, String stepName, int effectCount) {
        if (effectCount > 1) {
            throw new InvariantViolation(
                    "EffectAtMostOnce",
                    "external effect for '" + workflowId + "/" + stepName + "' ran " + effectCount + " times (F-0): "
                            + "the action is not bound to the COMPLETED commit, so a crash in the STARTED→COMMIT "
                            + "window re-runs it on replay");
        }
    }

    /**
     * Documents the F-0 gap as an <em>expected</em> violation: asserts the effect ran at least once (the engine's real
     * guarantee) and, when {@code expectDuplicate} is set, that it ran exactly twice — i.e. the write-then-vanish
     * crash window did re-run the effect. This is the implementation-level confirmation of the TLA+
     * {@code EffectAtMostOnce} counterexample, captured as a passing check rather than a build break.
     *
     * @param workflowId      the workflow instance.
     * @param stepName        the step whose effect is documented.
     * @param effectCount     how many times the effect actually ran.
     * @param expectDuplicate {@code true} if the write-then-vanish window was injected for this step, so a second run
     *                        is expected.
     */
    public static void documentEffectAtMostOnceGap(String workflowId, String stepName,
                                                  int effectCount, boolean expectDuplicate) {
        if (effectCount < 1) {
            throw new InvariantViolation(
                    "EffectAtMostOnce",
                    "external effect for '" + workflowId + "/" + stepName + "' never ran — even at-least-once is "
                            + "violated, which is unexpected");
        }
        if (expectDuplicate && effectCount < 2) {
            throw new InvariantViolation(
                    "EffectAtMostOnce",
                    "expected the write-then-vanish (F-0) window to re-run effect '" + workflowId + "/" + stepName
                            + "' (count≥2) but it ran only " + effectCount + " time(s)");
        }
    }

    /**
     * Documents the F-1 gap: under the engine's non-durable, per-process processor token, two engine processes can
     * both believe they own segment 0 (split-brain). Asserts the observed split-brain is present (count ≥ 2) so the
     * scenario passes by expecting the gap rather than breaking the build.
     * <p>
     * This is the implementation-level confirmation of the TLA+ {@code MC_owner.cfg} / {@code AtMostOneOwner}
     * counterexample (INVARIANTS.md INV-1, F-1).
     *
     * @param activeOwnerCount number of engine processes that concurrently own segment 0.
     */
    public static void documentSplitBrainOwnership(int activeOwnerCount) {
        if (activeOwnerCount < 2) {
            throw new InvariantViolation(
                    "AtMostOneOwner",
                    "expected split-brain (≥2 concurrent owners of segment 0) for the F-1 scenario but observed "
                            + activeOwnerCount);
        }
    }

    /**
     * Documents the F-1 <em>consequence</em> on recording: {@code AtMostOnceRecording} (INV-2) holds only
     * <em>given</em> {@code AtMostOneOwner} (INV-1). When the lease breaks (split-brain) and the {@code COMPLETED}
     * write carries no append-condition ({@code ExecuteDelegate.java:163…} FIXMEs), two owners each drive the same step
     * to a terminal record from their own {@code EventSourcedWorkflowState}, and both append it to the shared durable
     * log — two terminal records for one {@code (workflowId, stepName)}.
     * <p>
     * This is the implementation-level confirmation of the TLA+ {@code MC_record.cfg} / {@code AtMostOnceRecording}
     * counterexample (its violating tail {@code log = ⟨STARTED, COMPLETED, COMPLETED⟩}). It asserts the duplicate is
     * <em>present</em> (a step recorded a terminal outcome ≥2 times) so the scenario passes by expecting the gap; the
     * optimistic append-condition fix ({@code MC_record_fixed.cfg}) would close it and make this expectation fail —
     * the intended signal to re-evaluate. Note this is the deliberate contrast with the single-owner crash path, where
     * {@link #assertAtMostOnceRecording} (INV-2) still holds (≤1 terminal record) even though the effect duplicates
     * (F-0).
     *
     * @param maxTerminalRecordsForAStep the highest number of terminal records the shared log holds for any single
     *                                   {@code (workflowId, stepName)} under the split-brain.
     */
    public static void documentDuplicateRecordingUnderSplitBrain(int maxTerminalRecordsForAStep) {
        if (maxTerminalRecordsForAStep < 2) {
            throw new InvariantViolation(
                    "AtMostOnceRecording",
                    "expected the split-brain (F-1 consequence) to append a duplicate terminal record (≥2 terminal "
                            + "records for one step) but the highest observed was " + maxTerminalRecordsForAStep);
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-7: TerminalIsFinal (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-7 — Termination is final: once a workflow instance records a terminal workflow status (COMPLETED, FAILED,
     * CANCELLED, or TIMED_OUT), no further step or status events are ever recorded for that instance — termination is
     * final, even across a crash/replay or the live-switch boundary.
     * <p>
     * Asserted <strong>per {@code workflowId}</strong>: the committed log is grouped by instance (each group keeps its
     * own append order); within a group, the first terminal workflow-status event is the terminal boundary, and no
     * <em>new work</em> may appear after it — concretely, a workflow-status event reaching a <em>different</em> terminal
     * status than the boundary (e.g. {@code CANCELLED} then {@code FAILED} — termination changed), or a step event whose
     * step has <em>no</em> committed event before the terminal boundary (a step appearing wholly after terminal — work
     * the engine's guards should have forbidden). Two kinds of after-terminal record are <strong>tolerated</strong>:
     * <ul>
     *   <li><strong>F-2</strong> (intra-instance append order): a late-appended event (its <em>STARTED</em> or its
     *       terminal record) of a step that <em>began before</em> the workflow's terminal status. The engine publishes
     *       events durably-async, so a step committed close to the workflow-completion commit can have its own commit
     *       land in the global log just <em>after</em> it (fuzz seeds 252 = a late COMPLETED, 18 = a late STARTED).
     *       Because the engine's terminal guards forbid <em>starting</em> new work after terminal, a step that already
     *       began before terminal cannot be new work; its late-appended event is a recording/ordering artifact, not a
     *       finality break.</li>
     *   <li><strong>F-13</strong> (re-published identical terminal status — see {@code formal/POC-TLA-DST.adoc}): a
     *       SECOND, identical {@code <workflow>:STATUS} terminal record (the SAME terminal status, e.g. a second
     *       {@code <workflow>:CANCELLED}) for an instance already terminal at that status. The cancel (and fail/timeout)
     *       path publishes the terminal {@code <workflow>:STATUS} event DIRECTLY (TerminateDelegate's
     *       {@code eventSink.publish} / {@code SimpleWorkflowExecution.handleWorkflowException}) WITHOUT an "already
     *       terminal" gate at the publish site — only the upstream {@code execute()} short-circuit
     *       ({@code SimpleWorkflowExecution.java:149}) and the {@code switchToLiveMode} terminal-eviction filter guard
     *       it. On a crash/replay (the body re-runs and re-reaches {@code ctx.cancel()} before the rebuilt in-memory
     *       state reflects the committed terminal status — an intermittent race) or an F-3 start-event redelivery
     *       (a deterministic restart), a second identical {@code <workflow>:CANCELLED} is re-published — a duplicate
     *       durable terminal record. Because the re-published status is identical, no NEW lifecycle state is reached;
     *       this is treated like the F-2 late-record tolerance so the running engine stays green while the gap is OPEN.
     *       A future engine fix (gating the workflow-terminal publish on "already terminal", the F-7-class fix) flips it
     *       back to a single record. This duplicate is <strong>not</strong> caught by INV-2 ({@code AtMostOnceRecording}),
     *       which counts only <em>step</em>-status terminals (it skips events carrying no step status, i.e. all
     *       workflow-status events) — so for a duplicate <em>workflow</em>-status terminal, INV-7 is the only invariant
     *       that observes it. That INV-2 coverage gap is itself part of finding F-13.</li>
     * </ul>
     * (A <em>duplicate step</em> terminal record — same {@code (workflowId, stepName)} twice — is independently caught by
     * INV-2.) Comparing per-instance is required because the single global log interleaves independent instances
     * non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11) — a different instance's event landing after this
     * one's terminal status in the merged global log is not a violation; this instance starting new work after its own
     * terminal status is. This mirrors the engine's terminal guards (axon-flow-workflow skill §3.5:
     * {@code ctx.fail}/{@code ctx.cancel} end the workflow immediately; {@code AbstractStepExecutor.sendStepEvent}
     * refuses to publish once the workflow is terminal, {@code :197-203}) and the replay cached-result path that
     * re-reaches a terminal primitive as a no-op.
     *
     * @param committedLog the committed workflow event log (oldest first).
     */
    public static void assertTerminalIsFinal(List<EventMessage> committedLog) {
        // Per-workflow append-ordered view: index 0..n-1 is this instance's own committed order.
        var byWorkflow = new LinkedHashMap<String, List<EventMessage>>();
        for (EventMessage event : committedLog) {
            byWorkflow.computeIfAbsent(MetadataUtils.getWorkflowId(event.metadata()), k -> new ArrayList<>())
                      .add(event);
        }
        for (var entry : byWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            List<EventMessage> events = entry.getValue();
            int terminalIndex = -1;
            io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus terminalStatus = null;
            for (int i = 0; i < events.size(); i++) {
                var workflowStatus = MetadataUtils.getWorkflowStatus(events.get(i).metadata());
                if (workflowStatus.isPresent() && workflowStatus.get().isTerminal()) {
                    terminalIndex = i;
                    terminalStatus = workflowStatus.get();
                    break;
                }
            }
            if (terminalIndex < 0) {
                continue; // instance never reached a terminal workflow status — nothing to enforce.
            }
            // Step names with at least one committed event BEFORE the terminal status. A step in this set began before
            // the workflow terminated (the engine's terminal guards forbid STARTING new work afterwards), so any of its
            // events landing AFTER the terminal status — its STARTED or its terminal record — is the F-2 intra-instance
            // append-order artifact: the engine publishes events durably-async, so a step committed close to the
            // workflow-completion commit can have its own commit globally appended just after it (fuzz seeds 252, 18).
            // That is a recording/ordering artifact, not new work, and is tolerated.
            var stepsBeforeTerminal = new HashSet<String>();
            for (int i = 0; i < terminalIndex; i++) {
                if (MetadataUtils.getStepStatus(events.get(i).metadata()).isPresent()) {
                    stepsBeforeTerminal.add(MetadataUtils.getStepName(events.get(i).metadata()));
                }
            }
            for (int i = terminalIndex + 1; i < events.size(); i++) {
                var later = events.get(i).metadata();
                boolean isStepEvent = MetadataUtils.getStepStatus(later).isPresent();
                var laterWorkflowStatus = MetadataUtils.getWorkflowStatus(later);
                boolean isStatusEvent = laterWorkflowStatus.isPresent();
                // A re-published IDENTICAL terminal workflow status (the SAME terminal status for an instance already
                // terminal at that status) is the documented finding F-13 gap, NOT a NEW-work break, so it is TOLERATED
                // here (exactly as a late-appended event of a pre-terminal step is tolerated for F-2). The engine's cancel
                // (and fail/timeout) path publishes the terminal <workflow>:STATUS event directly (TerminateDelegate's
                // eventSink.publish / SimpleWorkflowExecution.handleWorkflowException) WITHOUT an "already terminal" gate
                // at the publish site — only the upstream execute() short-circuit (SimpleWorkflowExecution.java:149) and
                // the switchToLiveMode terminal-eviction filter guard it. On a crash/replay (or an F-3 start-event
                // redelivery) the body can re-run and re-reach ctx.cancel(), re-publishing a SECOND, identical
                // <workflow>:CANCELLED for the same workflowId — a duplicate durable terminal record. Because the
                // re-published status is identical to the terminal status, no NEW lifecycle state is reached; treating it
                // like the F-2 late-record tolerance keeps the running engine green while the gap is OPEN, and a future
                // engine fix (gating the workflow-terminal publish on "already terminal", the F-7-class fix) flips it back
                // to a single record. This duplicate is NOT caught by INV-2 (AtMostOnceRecording), which counts only
                // step-status terminals (it skips events with no step status), so INV-7 is the only invariant that sees a
                // duplicate workflow-status terminal — that INV-2 coverage gap is itself documented under F-13.
                boolean duplicateOfSameTerminalStatus =
                        isStatusEvent && laterWorkflowStatus.get() == terminalStatus;
                // Genuine new work after terminal is the break: a workflow-status event reaching a DIFFERENT lifecycle
                // status than the terminal one (e.g. CANCELLED then FAILED — termination changed), or a step event whose
                // step has NO pre-terminal event (a step appearing wholly after terminal — work the engine's guards
                // should have forbidden). A late-appended event of a step that began before terminal is tolerated (F-2
                // intra-instance append order); a re-published IDENTICAL terminal status is tolerated (F-13 gap).
                boolean lateEventOfPreTerminalStep =
                        isStepEvent && stepsBeforeTerminal.contains(MetadataUtils.getStepName(later));
                boolean differentStatusAfterTerminal = isStatusEvent && !duplicateOfSameTerminalStatus;
                if (differentStatusAfterTerminal || (isStepEvent && !lateEventOfPreTerminalStep)) {
                    throw new InvariantViolation(
                            "TerminalIsFinal",
                            "instance '" + workflowId + "' recorded a "
                                    + (isStatusEvent ? "different workflow-status" : "new step ('"
                                            + MetadataUtils.getStepName(later) + "', no pre-terminal event)") + " event ("
                                    + describe(later) + ") after its terminal workflow status ("
                                    + describe(events.get(terminalIndex).metadata())
                                    + ") — termination must be final (no NEW work after terminal); a late-appended event "
                                    + "of a step that began before terminal is tolerated (F-2 intra-instance append "
                                    + "order), a re-published identical terminal status is tolerated (F-13 gap)");
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-8: RetryBound (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-8 — Retry bound: for a step configured with {@code RetryPolicy.maxRetries(n)}, the number of attempt records
     * for that {@code (workflowId, stepName)} in the committed history is at most {@code n + 1} — the engine never
     * records more attempts than the policy allows, even across crashes/replays.
     * <p>
     * An <em>attempt record</em> is the start record of one attempt that the store accepted to run: the
     * {@code STARTED} of the first attempt, or the {@code RETRY_STARTED} of each retry attempt ({@code ExecuteDelegate}
     * publishes both through the same accepted-append gate). A {@code RETRYING} record is a <em>retry decision</em>,
     * emitted by {@code RetryableExecuteDelegate} as it evaluates the {@code RetryPolicy}; there are at most
     * {@code maxRetries} of them. Each {@code RETRYING} and {@code RETRY_STARTED} record carries the attempt number it
     * belongs to ({@code StepRetryInfo.attempt}); per step every number appears at most once per record kind and stays
     * within the policy ({@code RETRYING}: {@code 1..maxRetries}, {@code RETRY_STARTED}: {@code 2..maxRetries + 1}), so a
     * crash-resumed or fenced attempt is never decided or started twice. The terminal outcome
     * ({@code COMPLETED}/{@code FAILED}/{@code TIMED_OUT}/{@code CANCELLED}) is <strong>not</strong> an attempt and is
     * excluded. This is a <strong>record-level</strong>
     * count — the deliberate contrast with INV-6 ({@code EffectAtMostOnce}/F-0): a crash in the apply→commit window can
     * make a step's <em>effect</em> run more than once, but the recorded <em>attempt count</em> for the step must still
     * stay within the policy bound.
     * <p>
     * Asserted <strong>per {@code (workflowId, stepName)}</strong> against the committed event log, for each step whose
     * configured bound is supplied in {@code maxRetriesByStep} (steps with no entry — those that carry no retry policy —
     * are not bounded by this invariant and are skipped). The bound is taken from the workflow's policy configuration so
     * the assertion is not trivially self-satisfied. Counted over <strong>distinct committed events (by event
     * identifier)</strong> — the set semantics the formal wording states: an at-least-once durable store may
     * legitimately hold the SAME committed event twice (the {@code DUPLICATED_APPEND} fault — a retried append whose
     * first attempt landed), and that store-level duplicate is one attempt, not two; a genuine engine re-publish mints
     * a NEW event identifier and is still counted (the F-7/F-13/F-20 duplicate-record corruption class stays detected).
     * A genuine break (more than {@code maxRetries + 1} distinct attempt records for one step) throws
     * {@link InvariantViolation}; that would be a new finding (the engine recording more attempts than the policy
     * permits), to be triaged per the POC rules — not silently tolerated.
     *
     * @param committedLog     the committed workflow event log.
     * @param maxRetriesByStep configured {@code maxRetries} per retrying step name (e.g.
     *                         {@code shipOrder -> 2}); steps absent from the map carry no retry bound and are skipped.
     */
    public static void assertRetryBound(List<EventMessage> committedLog,
                                        Map<String, Integer> maxRetriesByStep) {
        Map<String, Integer> attemptCounts = new LinkedHashMap<>();
        Map<String, Integer> retryDecisionCounts = new LinkedHashMap<>();
        // Attempt numbers carried by RETRYING / RETRY_STARTED payloads, per (key, status): each number at most once.
        Map<String, Set<Integer>> attemptNumbers = new LinkedHashMap<>();
        var seenIdentifiers = new HashSet<>();
        for (EventMessage event : committedLog) {
            if (!seenIdentifiers.add(event.identifier())) {
                continue; // an at-least-once store duplicate of an already-counted committed event, not a new attempt.
            }
            var stepStatus = MetadataUtils.getStepStatus(event.metadata());
            // Only the non-terminal step events count: STARTED / RETRY_STARTED are attempts, RETRYING is a decision.
            if (stepStatus.isEmpty() || stepStatus.get().isTerminal()) {
                continue;
            }
            String stepName = MetadataUtils.getStepName(event.metadata());
            Integer maxRetries = maxRetriesByStep.get(stepName);
            if (maxRetries == null) {
                continue; // step carries no configured retry bound — INV-8 does not constrain it.
            }
            String key = MetadataUtils.getWorkflowId(event.metadata()) + "/" + stepName;
            if (event.payloadAs(Object.class) instanceof StepRetryInfo info) {
                // RETRYING(n) is the decision after attempt n failed (1 <= n <= maxRetries); RETRY_STARTED(n) is the
                // start of attempt n (2 <= n <= maxRetries + 1). A repeated number is a re-recorded decision or a
                // second writer starting the same attempt — both are what the append condition must prevent.
                int lowest = stepStatus.get() == StepStatus.RETRYING ? 1 : 2;
                int highest = stepStatus.get() == StepStatus.RETRYING ? maxRetries : maxRetries + 1;
                if (info.attempt() < lowest || info.attempt() > highest) {
                    throw new InvariantViolation(
                            "RetryBound",
                            "step '" + key + "' recorded " + stepStatus.get() + " for attempt " + info.attempt()
                                    + " but its policy only allows attempts " + lowest + ".." + highest
                                    + " for that record — the engine numbered an attempt outside the retry policy");
                }
                if (!attemptNumbers.computeIfAbsent(key + "/" + stepStatus.get(), k -> new HashSet<>())
                                   .add(info.attempt())) {
                    throw new InvariantViolation(
                            "RetryBound",
                            "step '" + key + "' recorded " + stepStatus.get() + " for attempt " + info.attempt()
                                    + " twice — the same attempt was decided or started more than once");
                }
            }
            if (stepStatus.get() == StepStatus.RETRYING) {
                int retries = retryDecisionCounts.merge(key, 1, Integer::sum);
                if (retries > maxRetries) {
                    throw new InvariantViolation(
                            "RetryBound",
                            "step '" + key + "' recorded " + retries + " RETRYING record(s) but its policy allows at "
                                    + "most maxRetries = " + maxRetries + " — the engine recorded more retry decisions "
                                    + "than the retry policy permits");
                }
                continue;
            }
            int attempts = attemptCounts.merge(key, 1, Integer::sum);
            if (attempts > maxRetries + 1) {
                throw new InvariantViolation(
                        "RetryBound",
                        "step '" + key + "' recorded " + attempts + " attempt(s) (STARTED/RETRY_STARTED) but its "
                                + "policy allows at most maxRetries+1 = " + (maxRetries + 1) + " — the engine recorded "
                                + "more attempts than the retry policy permits");
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-9: TimeoutsFire (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-9 — Timeouts fire: a step that exceeds its configured timeout reaches a {@code TIMED_OUT} outcome (recorded) —
     * a timeout never silently hangs or vanishes; the workflow always gets a terminal step outcome it can act on.
     * <p>
     * Asserted <strong>per {@code (workflowId, stepName)}</strong> against the committed event log, for each step whose
     * configured finite timeout is supplied in {@code timeoutByStep} (steps with no entry — those whose timeout is
     * effectively infinite or unconfigured — are not constrained and are skipped). For an instance that recorded a
     * {@code STARTED} for such a step at committed timestamp {@code t0}, once the run's virtual-time clock {@code now}
     * has reached at least {@code t0 + timeout} the step's timeout window has provably elapsed, so the committed history
     * must contain a <em>terminal</em> step record for it (normally {@code TIMED_OUT}; any terminal status — e.g. a
     * {@code COMPLETED} that landed just in time, or a {@code CANCELLED}/{@code FAILED} — also satisfies the property
     * that the step did not stay {@code STARTED} forever). A step still in {@code STARTED} whose window has elapsed is
     * the break: the timeout silently failed to fire.
     * <p>
     * The check is sound (no false positives): it only flags a step whose timeout window has <em>provably</em> elapsed
     * in the run's own virtual time (the same clock the engine's {@code WaitForDelegate} measures the timeout against),
     * and a step still legitimately waiting (window not yet elapsed) or with no configured finite timeout is skipped.
     * Taken per {@code (workflowId, stepName)} (never pooled across instances). A genuine break throws
     * {@link InvariantViolation}; that would be a new finding (a configured timeout that does not fire / a step that
     * hangs), to be triaged per the POC rules — not silently tolerated.
     *
     * @param committedLog  the committed workflow event log.
     * @param timeoutByStep configured finite timeout per timing-out step name (e.g. {@code awaitConfirmation -> 5s});
     *                      steps absent from the map carry no finite timeout bound and are skipped.
     * @param now           the run's current virtual-time instant (the harness's {@code MutableClock} instant), used to
     *                      decide whether a step's timeout window has elapsed.
     */
    public static void assertTimeoutsFire(List<EventMessage> committedLog,
                                          Map<String, java.time.Duration> timeoutByStep,
                                          java.time.Instant now) {
        // For each (workflowId, stepName) of a timeout-bearing step, track the STARTED timestamp and whether any
        // terminal step record exists. A step that went STARTED, whose timeout window has elapsed by `now`, and that has
        // no terminal record is a timeout that failed to fire.
        record StepTiming(java.time.Instant startedAt, boolean terminalSeen) {

        }
        Map<String, StepTiming> timings = new LinkedHashMap<>();
        for (EventMessage event : committedLog) {
            var stepStatus = MetadataUtils.getStepStatus(event.metadata());
            if (stepStatus.isEmpty()) {
                continue;
            }
            String stepName = MetadataUtils.getStepName(event.metadata());
            if (!timeoutByStep.containsKey(stepName)) {
                continue; // step carries no configured finite timeout — INV-9 does not constrain it.
            }
            String key = MetadataUtils.getWorkflowId(event.metadata()) + "/" + stepName;
            StepTiming current = timings.get(key);
            if (stepStatus.get() == StepStatus.STARTED) {
                var startedAt = current == null ? event.timestamp() : current.startedAt();
                timings.put(key, new StepTiming(startedAt, current != null && current.terminalSeen()));
            } else if (stepStatus.get().isTerminal()) {
                var startedAt = current == null ? null : current.startedAt();
                timings.put(key, new StepTiming(startedAt, true));
            }
        }
        for (var entry : timings.entrySet()) {
            StepTiming timing = entry.getValue();
            if (timing.startedAt() == null || timing.terminalSeen()) {
                continue; // no STARTED to time from, or already terminal — nothing to enforce.
            }
            String key = entry.getKey();
            String stepName = key.substring(key.indexOf('/') + 1);
            var timeout = timeoutByStep.get(stepName);
            var windowEnd = timing.startedAt().plus(timeout);
            if (!now.isBefore(windowEnd)) {
                throw new InvariantViolation(
                        "TimeoutsFire",
                        "step '" + key + "' has been STARTED since " + timing.startedAt() + " with a configured timeout "
                                + "of " + timeout + " (window ended " + windowEnd + ") but the virtual-time clock is now "
                                + now + " and no terminal (TIMED_OUT) record exists — the timeout did not fire (the step "
                                + "is hanging)");
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-10: OneInstancePerStart (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-10 — One instance per start: a single workflow start for a given business key ({@code workflowId}) yields
     * exactly one live instance at a time: duplicate/redelivered start events do not create a <em>second concurrent</em>
     * live instance. While an instance is LIVE the engine dedups a redelivered start (it consults the in-memory
     * spawn-dedup repository and rejects the spawn), so at most one non-terminal instance exists per {@code workflowId}.
     * The complementary case — a start redelivered <em>after</em> the instance has terminated re-spawns the key — is the
     * separate documented finding <strong>F-3</strong> (an idempotency / dedup-window design question), <strong>not</strong>
     * an INV-10 violation: INV-10 constrains <em>concurrent / live</em> duplicates only.
     * <p>
     * Asserted <strong>per {@code workflowId}</strong>: the committed log is grouped by instance (each group keeps its
     * own append order); within a group, scanning in order, a workflow-status {@code STARTED} that is not preceded by a
     * terminal workflow status since the previous {@code STARTED} is a second concurrent live instance — the break. A
     * {@code STARTED} that follows a terminal status (COMPLETED/FAILED/CANCELLED/TIMED_OUT) is the F-3 after-terminal
     * re-spawn and is deliberately <strong>tolerated</strong> (a fresh lifecycle of a finished key, not two live ones).
     * Comparing per-instance is required because the single global log interleaves independent instances
     * non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11) — another instance's {@code STARTED} landing here
     * is irrelevant; this instance opening a second live lifecycle is what is forbidden. This mirrors the engine's
     * spawn-dedup ({@code NewWorkflowInstanceRouting.resolveWorkflowIdForNewInstance} returns {@code null} for a live
     * duplicate at the same or a lower version) and the after-terminal eviction that
     * produces F-3 ({@code WorkflowEngine.java:161,194}). A genuine break (a second {@code STARTED} with no intervening
     * terminal — a real dedup failure distinct from F-3) throws {@link InvariantViolation}; that would be a new finding,
     * to be triaged per the POC rules — not silently tolerated.
     *
     * @param committedLog the committed workflow event log (oldest first).
     */
    public static void assertOneInstancePerStart(List<EventMessage> committedLog) {
        // Per-workflow append-ordered view: index 0..n-1 is this instance's own committed order.
        var byWorkflow = new LinkedHashMap<String, List<EventMessage>>();
        for (EventMessage event : committedLog) {
            // Group by the business key: a cross-version sibling spawned as "<id>#<version>" is the same start, so two
            // live lifecycles under one base id must fail this invariant whatever suffix the engine gave them.
            byWorkflow.computeIfAbsent(businessKey(MetadataUtils.getWorkflowId(event.metadata())), k -> new ArrayList<>())
                      .add(event);
        }
        for (var entry : byWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            // Walk this instance's own committed subsequence. A workflow-status STARTED reached while a prior lifecycle
            // is still LIVE (we have seen a STARTED but no terminal status since) is two concurrent live instances —
            // the break. A STARTED reached after a terminal status is the tolerated F-3 after-terminal re-spawn (a fresh
            // lifecycle of a finished key), so we reset the "live" tracker on every terminal status.
            boolean liveLifecycleOpen = false;
            for (EventMessage event : entry.getValue()) {
                var workflowStatus = MetadataUtils.getWorkflowStatus(event.metadata());
                if (workflowStatus.isEmpty()) {
                    continue; // step event — not a workflow-level start/terminal transition.
                }
                if (workflowStatus.get().isTerminal()) {
                    liveLifecycleOpen = false; // this lifecycle ended; a later STARTED is the tolerated F-3 re-spawn.
                } else if (workflowStatus.get() == WorkflowStatus.STARTED) {
                    if (liveLifecycleOpen) {
                        throw new InvariantViolation(
                                "OneInstancePerStart",
                                "instance '" + workflowId + "' recorded a second workflow-status STARTED with no "
                                        + "intervening terminal status — two concurrent LIVE instances for one "
                                        + "workflowId (a duplicate/redelivered start was not deduped while the instance "
                                        + "was live). This is a genuine dedup failure, distinct from the documented "
                                        + "after-terminal re-spawn F-3 (which INV-10 tolerates)");
                    }
                    liveLifecycleOpen = true;
                }
            }
        }
    }

    private static String businessKey(String workflowId) {
        int suffix = workflowId.indexOf('#');
        return suffix < 0 ? workflowId : workflowId.substring(0, suffix);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-11: VersionRoutingSound (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-11 — Version routing is sound: when multiple versions of a workflow are registered, every start/correlated
     * event routes to <strong>exactly one</strong> definition/instance (never 0, never 2); a new instance spawns at the
     * <strong>highest</strong> registered version; and routing is <strong>deterministic across replay</strong> (the
     * same committed history resolves the same version for each instance).
     * <p>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log, scoped to the versioned
     * workflow's instances (those whose id starts with {@code versionedIdPrefix}; single-version instances are not
     * constrained and are skipped). The observable is each event's {@code MessageType.version()} — the version of the
     * definition that emitted it (stamped by {@code EventMessageUtils} from {@code WorkflowContext.workflowVersion()},
     * which {@code EventSourcedWorkflowState} pins from the instance's {@code STARTED} event). Two facets are checked:
     * <ul>
     *   <li><strong>no dropped start (never 0)</strong>: every id in {@code expectedStartedWorkflowIds} (the ids the
     *       harness knows it started) must appear in the committed log with at least one event carrying a resolvable
     *       version — an expected start that produced no versioned instance routed to <em>0</em> definitions. (Pass an
     *       empty set for the per-step always-on call, where an instance may not have started yet.)</li>
     *   <li><strong>exactly one definition handled it, at the highest version</strong>: for every versioned instance
     *       <em>present</em> in the log that has recorded a workflow-status {@code STARTED}, the set of distinct versions
     *       carried by that instance's own committed events has size exactly 1 (a set of <em>≥2</em> distinct versions
     *       means two definitions both drove the same instance — routed to <em>2</em>), and that single resolved version
     *       equals {@code highestRegisteredVersion} (a fresh start picked the highest version, not an older sibling).
     *       (This workflow never calls {@code ctx.migrateVersion}, so a single lifecycle legitimately carries exactly one
     *       version.)</li>
     * </ul>
     * The <strong>deterministic-across-replay</strong> facet is enforced jointly with INV-4 ({@code DeterministicReplay}):
     * because each version stamp lives on the instance's committed subsequence, and that subsequence is a pure function
     * of the instance's history (the per-{@code workflowId} prefix-stability check in {@code DstSimulation} and the
     * run-level {@link #assertDeterministicReplay}), replaying the same history resolves the same per-instance version.
     * Taken per {@code workflowId} (never pooled): the single global log interleaves independent instances
     * non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11), so another instance's differently-versioned event
     * landing here is irrelevant; a single instance carrying two versions is what is forbidden. A genuine break (an
     * expected start routed to 0, an instance routed to 2 versions, or a fresh spawn not at the highest version) throws
     * {@link InvariantViolation}; that would be a high-value versioning finding (the routing resolved the wrong number
     * of definitions, or not the highest version), to be triaged per the POC rules — not silently tolerated.
     *
     * @param committedLog              the committed workflow event log (oldest first).
     * @param versionedIdPrefix         the id prefix of the versioned workflow's instances (e.g. {@code vorder-}); only
     *                                  instances whose id starts with it are checked.
     * @param highestRegisteredVersion  the highest registered semver version of the versioned workflow (what a fresh
     *                                  start must spawn at, e.g. {@code 1.0.1}).
     * @param expectedStartedWorkflowIds versioned ids the caller knows it started; each must be present in the log
     *                                  (else routed to 0). Empty for the per-step always-on call (no presence required).
     */
    public static void assertVersionRoutingSound(List<EventMessage> committedLog,
                                                 String versionedIdPrefix,
                                                 String highestRegisteredVersion,
                                                 Set<String> expectedStartedWorkflowIds) {
        // Per-versioned-instance: collect whether it recorded a workflow-status STARTED and the set of distinct
        // MessageType.version() values its own committed events carry.
        var startedByWorkflow = new LinkedHashMap<String, Boolean>();
        var versionsByWorkflow = new LinkedHashMap<String, Set<String>>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(versionedIdPrefix)) {
                continue; // not a versioned instance — INV-11 does not constrain it.
            }
            versionsByWorkflow.computeIfAbsent(workflowId, k -> new java.util.LinkedHashSet<>())
                              .add(event.type().version());
            var workflowStatus = MetadataUtils.getWorkflowStatus(event.metadata());
            if (workflowStatus.isPresent() && workflowStatus.get() == WorkflowStatus.STARTED) {
                startedByWorkflow.put(workflowId, true);
            }
        }
        // Facet 1 — never 0: a start the harness issued must have produced a versioned instance in the log.
        for (String expected : expectedStartedWorkflowIds) {
            if (versionsByWorkflow.getOrDefault(expected, Set.of()).isEmpty()) {
                throw new InvariantViolation(
                        "VersionRoutingSound",
                        "expected versioned start '" + expected + "' produced no committed instance carrying a "
                                + "resolvable version — the start event routed to 0 definitions");
            }
        }
        // Facet 2 — exactly one definition, at the highest version, for every present started instance.
        for (var entry : versionsByWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            if (!startedByWorkflow.getOrDefault(workflowId, false)) {
                continue; // instance never recorded a workflow-status STARTED — nothing to enforce yet.
            }
            Set<String> versions = entry.getValue();
            if (versions.size() > 1) {
                throw new InvariantViolation(
                        "VersionRoutingSound",
                        "versioned instance '" + workflowId + "' has committed events carrying " + versions.size()
                                + " distinct versions " + versions + " — two definitions handled the same instance "
                                + "(routed to 2). A single lifecycle of this workflow must resolve to exactly one "
                                + "version");
            }
            String resolved = versions.iterator().next();
            if (!resolved.equals(highestRegisteredVersion)) {
                throw new InvariantViolation(
                        "VersionRoutingSound",
                        "fresh versioned instance '" + workflowId + "' resolved to version '" + resolved + "' but a new "
                                + "spawn must use the highest registered version '" + highestRegisteredVersion + "'");
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-12: MigrateVersionContract (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-12 — {@code ctx.migrateVersion(changeId, v)} obeys its contract across crashes/replays: the recorded
     * migration version for a {@code changeId} is set <strong>at most once</strong> and is <strong>monotonic
     * non-decreasing</strong> (first-writer-wins, never downgrades), and <strong>replaying the same history yields the
     * same recorded version</strong> (idempotent — replay does not re-apply or change it).
     * <p>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log, scoped to the migrating
     * workflow's instances (those whose id starts with {@code migratingIdPrefix}; other instances are not constrained
     * and are skipped). The observable is the <em>migration-step</em> events: a {@code migrateVersion} call that records
     * a marker publishes a {@code COMPLETED} step event carrying the {@code versionChangeId} ({@code = changeId}) and
     * {@code version} metadata keys ({@code MetadataUtils.createVersionMigrationStep}; {@code MetadataUtils.isVersionMigrationStep}
     * flags it), which {@code EventSourcedWorkflowState} applies first-writer-wins
     * ({@code versions.putIfAbsent(changeId, version)}; bump {@code workflowDefinitionVersion} only if strictly greater
     * — {@code EventSourcedWorkflowState.java:293-298}). Three facets are checked:
     * <ul>
     *   <li><strong>recorded at most once per {@code (workflowId, changeId)}</strong>: the committed log holds at most
     *       one migration-step event for each {@code (workflowId, changeId)}. The delegate's
     *       {@code state.hasMigrateVersionStep(changeId)} guard ({@code VersionDelegate.java:103}) and {@code evolve}'s
     *       already-terminal-step guard ({@code EventSourcedWorkflowState.java:195-199}) make a replay re-reach the call
     *       as a no-op; a <em>second</em> migration record for the same {@code changeId} — or two records carrying
     *       <em>different</em> recorded versions — is the break (the version was re-applied / changed on replay);</li>
     *   <li><strong>never downgrades (monotonic non-decreasing)</strong>: scanning one instance's migration-step events
     *       in committed (append) order, each recorded version is {@code >=} the previous one. The first-writer-wins
     *       record + the strictly-greater bump rule means the workflow's recorded version only moves forward; a recorded
     *       version that is strictly less than an earlier-recorded one for the same instance is the break (a downgrade
     *       was recorded, which {@code VersionDelegate} should have rejected with {@code IllegalArgumentException});</li>
     *   <li><strong>each recorded version is a valid semver</strong> (it must pass {@code Version.validate} — a
     *       malformed recorded version means the marker carried a value the primitive should have rejected).</li>
     * </ul>
     * The <strong>replay-stability</strong> facet is enforced jointly with INV-4 ({@code DeterministicReplay}): because
     * the migration-step event lives on the instance's committed subsequence, and that subsequence is a pure function of
     * the instance's history (the per-{@code workflowId} prefix-stability check in {@code DstSimulation} and the
     * run-level {@link #assertDeterministicReplay}), replaying the same history resolves the same recorded version — the
     * marker is neither re-applied (at-most-once above) nor mutated. Taken per {@code workflowId} (never pooled): the
     * single global log interleaves independent instances non-deterministically (the F-2 surface, ARCHITECTURE.md
     * §8/§11), so another instance's migration record landing here is irrelevant; one instance recording the same
     * {@code changeId} twice, recording a different/downgraded version, or recording an invalid version is the break.
     * Counted over <strong>distinct committed events (by event identifier)</strong> — the set semantics the formal
     * wording states: an at-least-once durable store may legitimately hold the SAME committed marker twice (the
     * {@code DUPLICATED_APPEND} fault), and that store-level duplicate is one recorded marker, not a re-apply; a
     * genuine engine re-publish mints a NEW event identifier and is still counted. A genuine break throws
     * {@link InvariantViolation}; that would be a high-value versioning finding (the migration record was applied
     * twice, downgraded, or resolved to a different version on replay), to be triaged per the POC rules — not silently
     * tolerated.
     *
     * @param committedLog      the committed workflow event log (oldest first).
     * @param migratingIdPrefix the id prefix of the migrating workflow's instances (e.g. {@code vmig-}); only instances
     *                          whose id starts with it are checked.
     */
    public static void assertMigrateVersionContract(List<EventMessage> committedLog,
                                                    String migratingIdPrefix) {
        // Per (workflowId, changeId): how many migration-step events were committed, and the distinct recorded versions.
        // Per workflowId: the ordered list of recorded versions (append order) for the monotonicity / downgrade check.
        var recordCountByKey = new LinkedHashMap<String, Integer>();
        var versionsByKey = new LinkedHashMap<String, Set<String>>();
        var orderedVersionsByWorkflow = new LinkedHashMap<String, List<String>>();
        var seenIdentifiers = new HashSet<>();
        for (EventMessage event : committedLog) {
            if (!seenIdentifiers.add(event.identifier())) {
                continue; // an at-least-once store duplicate of an already-counted committed event, not a re-apply.
            }
            var metadata = event.metadata();
            if (!MetadataUtils.isVersionMigrationStep(metadata)) {
                continue; // not a migrateVersion marker — INV-12 does not constrain it.
            }
            String workflowId = MetadataUtils.getWorkflowId(metadata);
            if (!workflowId.startsWith(migratingIdPrefix)) {
                continue; // not a migrating instance — INV-12 does not constrain it.
            }
            String changeId = MetadataUtils.getVersionChangeId(metadata).orElse(null);
            String recordedVersion = MetadataUtils.getVersion(metadata).orElse(null);
            if (changeId == null || recordedVersion == null) {
                continue; // not a well-formed migration marker.
            }
            // Each recorded version must be a valid semver — the primitive validates before recording.
            try {
                Version.validate(recordedVersion);
            } catch (IllegalArgumentException e) {
                throw new InvariantViolation(
                        "MigrateVersionContract",
                        "migration record for '" + workflowId + "/" + changeId + "' carries an invalid recorded version '"
                                + recordedVersion + "' — migrateVersion must record a valid semver");
            }
            String key = workflowId + "/" + changeId;
            int count = recordCountByKey.merge(key, 1, Integer::sum);
            versionsByKey.computeIfAbsent(key, k -> new java.util.LinkedHashSet<>()).add(recordedVersion);
            orderedVersionsByWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(recordedVersion);
            // Facet 1 — recorded at most once per (workflowId, changeId): a second migration record for the same
            // changeId means replay re-applied the marker (or two writers recorded it).
            if (count > 1) {
                throw new InvariantViolation(
                        "MigrateVersionContract",
                        "migration for '" + key + "' was recorded " + count + " times (versions " + versionsByKey.get(key)
                                + ") — the recorded migration version must be written at most once per changeId; a "
                                + "replay must re-reach the call as a no-op, never re-apply it");
            }
        }
        // Facet 2 — monotonic non-decreasing (never downgrades) within one instance: recorded versions in append order
        // must only move forward. (With at most one record per changeId, this checks across distinct changeIds.)
        for (var entry : orderedVersionsByWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            List<String> ordered = entry.getValue();
            for (int i = 1; i < ordered.size(); i++) {
                Version previous = Version.of(ordered.get(i - 1));
                Version current = Version.of(ordered.get(i));
                if (current.compareTo(previous) < 0) {
                    throw new InvariantViolation(
                            "MigrateVersionContract",
                            "instance '" + workflowId + "' recorded migration version '" + ordered.get(i) + "' after '"
                                    + ordered.get(i - 1) + "' — the recorded version must be monotonic non-decreasing "
                                    + "(first-writer-wins, never downgrades); a downgrade must be rejected, not recorded");
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-13: NoLostPayloadWrites (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-13 — No lost payload writes: for a workflow instance, the final committed payload reflects <strong>every</strong>
     * committed step's recorded contribution — no committed step's payload write is lost or silently dropped across
     * crashes/replays (modulo intended overwrites by a later step on the same key); replaying the same history rebuilds
     * the identical payload.
     * <p>
     * Asserted <strong>per {@code workflowId}</strong>, scoped to the payload workflow's instances (those whose id starts
     * with {@code payloadIdPrefix}; other instances are not constrained and are skipped). It cross-checks <strong>two
     * independent reconstructions</strong> of each instance's payload:
     * <ul>
     *   <li><strong>(A) the committed event log</strong> — each payload-bearing event carries a {@code payloadReducer}
     *       (the {@code modifyPayload} metadata key) naming the {@link io.axoniq.framework.workflow.runtime.api.payload.PayloadReducer}
     *       the engine applies in {@code EventSourcedWorkflowState#evolvePayload}, with the event's payload map as the
     *       step's local contribution. The three reducers map to the engine's three payload behaviours:
     *       {@link CombineGlobalAndLocalPayloadReducer#NAME combine_local_and_global} (an {@code execute} step with
     *       {@code CombineGlobalAndLocalPayloadReducer.INSTANCE}, or the workflow {@code STARTED} event) merges every
     *       {@code k=v} of the event's payload into the running payload — each such {@code k=v} is a recorded
     *       contribution; {@link LocalOnlyPayloadReducer#NAME local_only} ({@code setPayload}/{@code modifyPayload})
     *       replaces the whole payload with the event's map — each {@code k=v} is a recorded contribution and the replace
     *       is itself an intended overwrite of the previous payload; {@link GlobalOnlyPayloadReducer#NAME global_only}
     *       (a default {@code execute} step) discards the result — no contribution;</li>
     *   <li><strong>(B) the engine's actual final payload</strong> — {@code actualPayloadsById}, the payload the engine
     *       itself reconstructed for the instance (the workflow-history read-model's {@code state().payload()}, built by
     *       the {@code WorkflowHistoryProjector} consuming the event stream — an evolution path independent of the raw
     *       log fold in (A)).</li>
     * </ul>
     * The no-lost-write check: for every committed contribution {@code (k, v)} in (A), the engine's actual payload (B)
     * must contain {@code k=v}, <strong>unless</strong> a <em>later</em> committed step of the same instance overwrote
     * key {@code k} (a later {@code combine} whose map contains {@code k}, or any later {@code local_only} replace —
     * which rewrites the whole payload and so decides {@code k}'s fate). A committed contribution that the engine's
     * actual payload does not reflect, and that no later committed step overwrote, was <em>lost</em> — the engine
     * dropped a payload write its own committed log records. This is non-vacuous precisely because (A) and (B) are
     * reconstructed independently: a replay that rebuilt a different payload, an out-of-order or skipped reducer
     * application, or a dropped contribution would make (B) diverge from (A) and trip the check.
     * <p>
     * The <strong>replay-stability</strong> facet ("replaying rebuilds the identical payload") is enforced jointly with
     * INV-4 ({@code DeterministicReplay}): both (A) and (B) are pure functions of the instance's committed subsequence,
     * which is itself a pure function of the instance's history (the per-{@code workflowId} prefix-stability check in
     * {@code DstSimulation} and the run-level {@link #assertDeterministicReplay}), so replaying the same history rebuilds
     * the same payload — and (B) is re-derived by the projector across the crash/replay, so a replay that lost a write
     * would show here. Taken per {@code workflowId} (never pooled): the single global log interleaves independent
     * instances non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11), so another instance's payload write
     * landing here is irrelevant; one instance losing one of its own committed contributions is the break. An instance
     * not yet present in {@code actualPayloadsById} (its start not yet projected) is skipped — there is nothing to
     * cross-check until the engine has reconstructed a payload for it. A genuine break throws {@link InvariantViolation};
     * that would be a new finding (the engine's reconstructed payload does not reflect a committed payload write), to be
     * triaged per the POC rules — not silently tolerated.
     *
     * @param committedLog       the committed workflow event log (oldest first).
     * @param payloadIdPrefix    the id prefix of the payload workflow's instances (e.g. {@code payload-}); only
     *                           instances whose id starts with it are checked.
     * @param actualPayloadsById the engine's actual final payload per {@code workflowId} (the workflow-history
     *                           read-model's reconstructed {@code state().payload()}); an instance absent from this map
     *                           has no reconstructed payload yet and is skipped.
     */
    public static void assertNoLostPayloadWrites(List<EventMessage> committedLog,
                                                 String payloadIdPrefix,
                                                 Map<String, Map<String, Object>> actualPayloadsById) {
        // Per-workflow append-ordered view: index 0..n-1 is this instance's own committed order.
        var byWorkflow = new LinkedHashMap<String, List<EventMessage>>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(payloadIdPrefix)) {
                continue; // not a payload-workflow instance — INV-13 does not constrain it.
            }
            byWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(event);
        }
        for (var entry : byWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            Map<String, Object> actualPayload = actualPayloadsById.get(workflowId);
            if (actualPayload == null) {
                continue; // engine has not reconstructed a payload for this instance yet — nothing to cross-check.
            }
            assertNoLostPayloadWritesForInstance(workflowId, entry.getValue(), actualPayload);
        }
    }

    /**
     * Per-instance INV-13 check: derive this instance's committed payload contributions (A) from its committed events
     * (mirroring {@code EventSourcedWorkflowState#evolvePayload}'s reducer dispatch), then verify each contribution is
     * reflected in the engine's actual reconstructed payload (B) unless a later same-key committed write overwrote it.
     */
    private static void assertNoLostPayloadWritesForInstance(String workflowId,
                                                             List<EventMessage> events,
                                                             Map<String, Object> actualPayload) {
        // (A) Each payload-bearing committed event, in append order, as (reducerName, contributedMap).
        var contributions = new ArrayList<PayloadContribution>();
        for (EventMessage event : events) {
            var reducerName = MetadataUtils.payloadReducer(event.metadata()).orElse(null);
            if (reducerName == null) {
                continue; // event carries no payload reducer — it contributes nothing to the payload.
            }
            if (!CombineGlobalAndLocalPayloadReducer.NAME.equals(reducerName)
                    && !LocalOnlyPayloadReducer.NAME.equals(reducerName)) {
                // global_only (result discarded) or an unknown reducer: evolvePayload leaves the payload unchanged and
                // nothing is contributed.
                continue;
            }
            contributions.add(new PayloadContribution(reducerName, payloadMapOf(event)));
        }
        // No-lost-write check: every committed contribution (k=v) must be reflected in the engine's ACTUAL payload (B)
        // UNLESS a later committed event of this instance overwrote key k (a later combine carrying k, or any later
        // local_only replace, which rewrites the whole payload and thus decides k's fate). A contribution that is
        // neither present in (B) nor later-overwritten was lost by the engine.
        for (int i = 0; i < contributions.size(); i++) {
            PayloadContribution contribution = contributions.get(i);
            for (var write : contribution.values().entrySet()) {
                String key = write.getKey();
                Object value = write.getValue();
                if (actualPayload.containsKey(key) && java.util.Objects.equals(actualPayload.get(key), value)) {
                    continue; // reflected in the engine's actual payload exactly as written — not lost.
                }
                if (overwrittenLater(contributions, i, key)) {
                    continue; // a later same-key write (combine on k, or a local_only replace) — intended overwrite.
                }
                throw new InvariantViolation(
                        "NoLostPayloadWrites",
                        "instance '" + workflowId + "' lost a committed payload write: step contribution " + key + "="
                                + value + " (reducer " + contribution.reducerName() + ") is absent from the engine's "
                                + "reconstructed final payload " + actualPayload + " and no later committed step "
                                + "overwrote key '" + key + "' — a committed payload write was dropped across "
                                + "crash/replay");
            }
        }
    }

    /**
     * {@code true} iff a contribution <em>after</em> index {@code i} wrote key {@code key}: a later
     * {@link CombineGlobalAndLocalPayloadReducer#NAME combine_local_and_global} whose map contains {@code key}, or any
     * later {@link LocalOnlyPayloadReducer#NAME local_only} replace (a whole-payload rewrite decides every key's fate).
     * Either is an intended overwrite of {@code key}.
     */
    private static boolean overwrittenLater(List<PayloadContribution> contributions, int i,
                                            String key) {
        for (int j = i + 1; j < contributions.size(); j++) {
            PayloadContribution later = contributions.get(j);
            if (LocalOnlyPayloadReducer.NAME.equals(later.reducerName()) || later.values().containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reconstructs the final committed payload for {@code workflowId} by folding its committed events in append order,
     * applying each payload-bearing event's named reducer exactly as {@code EventSourcedWorkflowState#evolvePayload}
     * does ({@code combine_local_and_global} merges, {@code local_only} replaces, {@code global_only}/unknown leave the
     * payload unchanged). This is the per-instance final payload INV-13 reasons about and the value a replay rebuilds;
     * exposed so a scenario/test can assert the rebuilt payload directly.
     *
     * @param committedLog the committed workflow event log (oldest first).
     * @param workflowId   the instance whose final payload to rebuild.
     * @return the reconstructed final payload for the instance (empty if it has no committed payload-bearing events).
     */
        public static Map<String, Object> rebuildPayload(List<EventMessage> committedLog,
                                                     String workflowId) {
        Map<String, Object> payload = new LinkedHashMap<>();
        for (EventMessage event : committedLog) {
            if (!workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))) {
                continue;
            }
            var reducerName = MetadataUtils.payloadReducer(event.metadata()).orElse(null);
            if (reducerName == null) {
                continue;
            }
            Map<String, Object> local = payloadMapOf(event);
            if (CombineGlobalAndLocalPayloadReducer.NAME.equals(reducerName)) {
                payload.putAll(local);
            } else if (LocalOnlyPayloadReducer.NAME.equals(reducerName)) {
                payload = new LinkedHashMap<>(local);
            }
            // global_only / unknown: payload unchanged.
        }
        return payload;
    }

    /**
     * Reads an event's payload as a {@code Map<String, Object>} for the payload fold. The simulator's in-memory event
     * store holds the original {@code GenericEventMessage} whose payload is the actual map the step published (no
     * serialization round-trip), so the raw {@code payload()} is the map; a non-map payload (no payload-bearing step
     * carries one) yields an empty map.
     */
        @SuppressWarnings("unchecked")
    private static Map<String, Object> payloadMapOf(EventMessage event) {
        Object payload = event.payload();
        if (payload instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Map.of();
    }

    /**
     * One committed payload-bearing event's recorded contribution: the reducer the engine applies and the event's
     * payload map (the step's local contribution). Used by INV-13's no-lost-write fold and overwrite check.
     *
     * @param reducerName the named {@link io.axoniq.framework.workflow.runtime.api.payload.PayloadReducer} the COMPLETED step
     *                    event carries ({@code combine_local_and_global} or {@code local_only}).
     * @param values      the event's payload map — the keys/values this step contributed.
     */
    private record PayloadContribution(String reducerName, Map<String, Object> values) {

    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-19: PayloadReducerSemantics (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-19 — Payload reducer semantics: each payload reducer produces its <strong>documented merge</strong> into the
     * workflow payload — and that result is stable across crash/replay. Extends INV-13 ({@code NoLostPayloadWrites},
     * {@link #assertNoLostPayloadWrites}): INV-13 covered combine + local_only on the result side for no-lost-write;
     * INV-19 pins each reducer <em>type</em>'s documented MERGE semantics, on both the parameter and result sides.
     * Concretely, for a step's recorded payload contribution:
     * <ul>
     *   <li><strong>{@link GlobalOnlyPayloadReducer#NAME global_only}</strong> (the default {@code resultPayloadReducer}):
     *       the step's result is DISCARDED — the payload is unchanged (the result's keys do NOT appear from this step);</li>
     *   <li><strong>{@link CombineGlobalAndLocalPayloadReducer#NAME combine_local_and_global}</strong>: the step's result
     *       map is MERGED key-by-key into the running payload (each {@code k=v} added/overwritten);</li>
     *   <li><strong>{@link LocalOnlyPayloadReducer#NAME local_only}</strong> (used by {@code setPayload}/{@code modifyPayload}):
     *       the payload is REPLACED wholesale by the step's map;</li>
     *   <li><strong>{@code parameterPayloadReducer}</strong> (default {@code local_only}) governs what the step's action
     *       SEES as its input payload (the step's local view) vs the global — the documented parameter-side default; its
     *       observable effect is recorded as a result-side contribution and so is captured by the same payload check.</li>
     * </ul>
     * The engine's reconstructed final payload for the instance must reflect <strong>exactly</strong> these documented
     * behaviours: a {@code global_only} step's result keys are absent (unless written elsewhere), a {@code combine} step's
     * keys are present, a {@code local_only} replace drops prior keys not in the replacement.
     * <p>
     * The check additionally pins the reducer EDGE cases (all per {@code workflowId}, content-based, F-2-robust): (a) a
     * {@code combine} step whose result map carries a <strong>{@code null} value</strong> — per the documented combine
     * fold ({@code new HashMap<>(global); putAll(local)}, where {@code HashMap.putAll} inserts a null value) the key is
     * PRESENT with value {@code null} (combine OVERWRITES with null; it does NOT skip it nor keep a prior global value);
     * (b) the <strong>parameter-view and result-write reducers are independent knobs</strong> — a step whose
     * {@code parameterPayloadReducer} gives it a combined INPUT view but whose {@code resultPayloadReducer} is
     * {@code global_only} writes nothing back (its result is discarded); (c) <strong>last-writer-wins on the same
     * key</strong> — when two steps write the same payload key under combine, the later (in this instance's own step
     * sequence — the deterministic order, never the global-append order) write wins in the reconstructed payload; (d)
     * <strong>missing keys under combine</strong> — a combine step's result map that OMITS a key the running payload
     * already holds keeps that key's prior (global) value (only the present keys are merged); and (e) the whole fold is
     * <strong>crash/replay-stable</strong> — replaying the same history re-applies the same reducers to the same recorded
     * results, rebuilding the identical payload (no double-merge, no lost write, even with a null value).
     * <p>
     * The check is non-vacuous precisely because two payloads are reconstructed independently: <strong>(A)</strong> the
     * EXPECTED payload, folded from the committed log by {@link #rebuildPayload} applying each event's named reducer
     * exactly as {@code EventSourcedWorkflowState#evolvePayload} does ({@code combine} merges, {@code local_only}
     * replaces, {@code global_only}/unknown leave it unchanged — the <em>documented</em> reducer semantics); and
     * <strong>(B)</strong> the engine's OWN reconstructed payload ({@code actualPayloadsById}, the workflow-history
     * read-model's {@code state().payload()}, built by the {@code WorkflowHistoryProjector} — an evolution path
     * independent of the fold in (A)). INV-19 requires {@code (A) == (B)}: the engine applied each reducer exactly as
     * documented. A reducer mis-application — a {@code global_only} result that WRONGLY appears, a {@code combine} key
     * MISSING, a {@code local_only} replace that did NOT drop a prior key, or any value divergence — makes (B) diverge
     * from (A) and trips the check. (A genuine such divergence would be a high-value finding — the engine mis-applying a
     * documented reducer — to be triaged per the POC rules, not silently tolerated.)
     * <p>
     * The <strong>replay-stability</strong> facet is enforced jointly with INV-4 ({@code DeterministicReplay}): both (A)
     * and (B) are pure functions of the instance's committed subsequence (the per-{@code workflowId} prefix-stability
     * check in {@code DstSimulation} and the run-level {@link #assertDeterministicReplay}), so replaying the same history
     * rebuilds the same payload — and (B) is re-derived by the projector across the crash/replay, so a replay that
     * mis-applied a reducer would show here. Taken per {@code workflowId} (never pooled): the single global log
     * interleaves independent instances non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11), so another
     * instance's payload is irrelevant — content-based, never asserting an instance's own global-append order. An
     * instance not yet present in {@code actualPayloadsById} (its start not yet projected) is skipped — there is nothing
     * to cross-check until the engine has reconstructed a payload for it.
     *
     * @param committedLog       the committed workflow event log (oldest first).
     * @param reducerIdPrefix    the id prefix of the reducer workflow's instances (e.g. {@code reducer-}); only instances
     *                           whose id starts with it are checked.
     * @param actualPayloadsById the engine's actual final payload per {@code workflowId} (the workflow-history
     *                           read-model's reconstructed {@code state().payload()}); an instance absent from this map
     *                           has no reconstructed payload yet and is skipped.
     */
    public static void assertPayloadReducerSemantics(List<EventMessage> committedLog,
                                                     String reducerIdPrefix,
                                                     Map<String, Map<String, Object>> actualPayloadsById) {
        var workflowIds = new java.util.LinkedHashSet<String>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (workflowId.startsWith(reducerIdPrefix)) {
                workflowIds.add(workflowId);
            }
        }
        for (String workflowId : workflowIds) {
            Map<String, Object> actual = actualPayloadsById.get(workflowId);
            if (actual == null) {
                continue; // engine has not reconstructed a payload for this instance yet — nothing to cross-check.
            }
            // (A) the EXPECTED payload, folding each event's named reducer exactly as evolvePayload does (the documented
            // reducer semantics), but in the instance's DETERMINISTIC LOGICAL step order (the order its body issued the
            // steps), NOT the global-append order. (B) the engine's OWN reconstructed payload. They must be equal: each
            // reducer applied as documented (global_only discards, combine merges, local_only replaces).
            Map<String, Object> expected = rebuildPayloadInLogicalStepOrder(committedLog, workflowId);
            assertPayloadReducerSemanticsForInstance(workflowId, expected, actual);
        }
    }

    /**
     * Reconstructs {@code workflowId}'s final payload like {@link #rebuildPayload}, but folds each step's payload
     * contribution in the instance's <strong>deterministic logical step order</strong> (the order the body issued the
     * steps) rather than the non-deterministic global-append order — making INV-19's expected fold (A)
     * <strong>F-2-robust against the post-{@code RESTART} re-append interleaving</strong>, the seed-150/317-class
     * load-flake.
     * <p>
     * <strong>Why this is needed.</strong> A {@code RESTART} fault crashes mid-instance and recovery re-drives the body;
     * the re-emitted events can append to the single global event store in an interleaving that differs from the logical
     * step order — e.g. a {@code local_only} {@code modifyPayload} COMPLETED event landing in the global log
     * <em>before</em> an earlier {@code combine} step's COMPLETED event that was delayed by the crash. The plain
     * append-order fold {@link #rebuildPayload} would then apply {@code local_only} (whole-payload replace) first and the
     * earlier {@code combine} second, resurrecting a key the replace was supposed to drop (the observed {@code seedKey}
     * divergence). The engine's read-model (B) does not have this problem — it applies each step's payload in the
     * instance's logical order — so a raw-append (A) diverges from a correct (B). INV-19's contract is explicitly
     * <em>content-based, never asserting the instance's own global-append order</em> (ARCHITECTURE.md §8/§11, the F-2
     * surface), so (A) must be reconstructed in logical order to honour that.
     * <p>
     * <strong>Logical order.</strong> The body awaits its steps in declaration order, so each step's earliest committed
     * event (the {@code STARTED} for {@code execute}; the directly-published {@code COMPLETED} for {@code modifyPayload},
     * which has no {@code STARTED}) appears in the global log in logical order even when the crash re-orders the later
     * {@code COMPLETED}s. Ordering each step by the <em>minimum committed-log index among that step's events</em> therefore
     * recovers the logical step order; the payload-bearing {@code COMPLETED} contributions are then folded in that order
     * (combine merges, local_only replaces, global_only/unknown leave unchanged), matching the engine's deterministic
     * evolution. This is <strong>not</strong> a neutering: it changes only the FOLD ORDER of (A) to the deterministic
     * one, so a genuine reducer mis-application (a global_only result that leaked in, a combine key missing, a local_only
     * replace that kept a prior key, a wrong last-writer) still makes (A) diverge from (B) and trips the check.
     */
        private static Map<String, Object> rebuildPayloadInLogicalStepOrder(List<EventMessage> committedLog,
                                                                        String workflowId) {
        // Earliest committed-log index per step name for this instance — its logical position in the body sequence.
        var earliestIndexByStep = new LinkedHashMap<String, Integer>();
        // The payload-bearing COMPLETED contributions, by step name (combine merge / local_only replace). A step's
        // payload lands on its COMPLETED event, which carries the reducer name; we fold them in logical step order.
        var contributionByStep = new LinkedHashMap<String, PayloadContribution>();
        for (int i = 0; i < committedLog.size(); i++) {
            EventMessage event = committedLog.get(i);
            if (!workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))) {
                continue;
            }
            String stepName = MetadataUtils.getStepName(event.metadata());
            if (stepName == null) {
                continue; // the workflow-status start/terminal events carry no step name (no per-step payload to order).
            }
            earliestIndexByStep.putIfAbsent(stepName, i);
            var reducerName = MetadataUtils.payloadReducer(event.metadata()).orElse(null);
            if (reducerName == null) {
                continue; // not a payload-bearing event for this step (e.g. its STARTED) — only marks logical position.
            }
            if (!CombineGlobalAndLocalPayloadReducer.NAME.equals(reducerName)
                    && !LocalOnlyPayloadReducer.NAME.equals(reducerName)) {
                continue; // global_only / unknown: contributes nothing to the fold (the result is discarded).
            }
            // A step's payload-bearing COMPLETED event; keep it keyed by step (one terminal record per step).
            contributionByStep.put(stepName, new PayloadContribution(reducerName, payloadMapOf(event)));
        }
        // Fold the contributions in logical step order (by each step's earliest committed index).
        var orderedSteps = new ArrayList<>(contributionByStep.keySet());
        orderedSteps.sort(Comparator.comparingInt(step -> earliestIndexByStep.getOrDefault(step, Integer.MAX_VALUE)));
        Map<String, Object> payload = new LinkedHashMap<>();
        for (String stepName : orderedSteps) {
            PayloadContribution contribution = contributionByStep.get(stepName);
            if (CombineGlobalAndLocalPayloadReducer.NAME.equals(contribution.reducerName())) {
                payload.putAll(contribution.values());
            } else { // local_only: whole-payload replace.
                payload = new LinkedHashMap<>(contribution.values());
            }
        }
        return payload;
    }

    /**
     * Per-instance INV-19 check: the engine's reconstructed payload (B) must equal the documented-reducer fold (A) of the
     * instance's committed events — key for key. Reports the first divergence and which documented reducer behaviour it
     * violates (a {@code global_only} result that leaked in, a {@code combine}/value key that is missing or wrong, a
     * {@code local_only} replace that failed to drop a prior key).
     */
    private static void assertPayloadReducerSemanticsForInstance(String workflowId,
                                                                 Map<String, Object> expected,
                                                                 Map<String, Object> actual) {
        // A key the engine's payload carries that the documented fold does not (e.g. a global_only result that was NOT
        // discarded) — the engine applied a reducer more permissively than documented.
        for (var entry : actual.entrySet()) {
            if (!expected.containsKey(entry.getKey())) {
                throw new InvariantViolation(
                        "PayloadReducerSemantics",
                        "instance '" + workflowId + "' reconstructed payload carries key '" + entry.getKey() + "'="
                                + entry.getValue() + " that the documented reducer fold does not — a reducer produced a "
                                + "key it should have discarded (e.g. a global_only result that was not discarded, or a "
                                + "local_only replace that kept a key it dropped). expected=" + expected + " actual="
                                + actual);
            }
        }
        // A key the documented fold has that the engine's payload lacks or carries a different value for (e.g. a combine
        // merge that did not land, or a local_only replacement value the engine did not apply).
        for (var entry : expected.entrySet()) {
            String key = entry.getKey();
            Object expectedValue = entry.getValue();
            if (!actual.containsKey(key)) {
                throw new InvariantViolation(
                        "PayloadReducerSemantics",
                        "instance '" + workflowId + "' reconstructed payload is missing key '" + key + "'="
                                + expectedValue + " that the documented reducer fold produced — a reducer did not apply "
                                + "its documented merge (e.g. a combine key not merged, or a local_only replacement key "
                                + "not applied). expected=" + expected + " actual=" + actual);
            }
            if (!java.util.Objects.equals(actual.get(key), expectedValue)) {
                throw new InvariantViolation(
                        "PayloadReducerSemantics",
                        "instance '" + workflowId + "' reconstructed payload has key '" + key + "'=" + actual.get(key)
                                + " but the documented reducer fold produced '" + key + "'=" + expectedValue
                                + " — a reducer applied a different value than documented. expected=" + expected
                                + " actual=" + actual);
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-14: CombinatorConsistency (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-14 — Combinator consistency: a combinator's decision is consistent with the documented short-circuit
     * semantics and is a <strong>pure function of its branch steps' committed terminal outcomes</strong> — stable
     * across crash/replay. For a workflow that runs N parallel {@code execute} branch steps and then a combinator over
     * them:
     * <ul>
     *   <li><strong>{@code anyMatch}</strong> (race / first-match-wins): the workflow proceeds on the FIRST branch (in
     *       declaration order) whose committed outcome satisfies the predicate; the result is "matched" iff ≥1 branch
     *       satisfied it;</li>
     *   <li><strong>{@code allMatch}</strong> (guard / all-or-first-fail): the result is "matched" iff ALL branches
     *       satisfied the predicate; short-circuits on the first non-matching branch;</li>
     *   <li><strong>{@code noneMatch}</strong> (fail-fast): the result is "matched" iff NO branch satisfied the
     *       predicate.</li>
     * </ul>
     * The combinator's observable downstream effect (the post-combinator step the body recorded, and the payload key it
     * wrote capturing matched/unmatched) MUST equal what the documented semantics dictate given the branches' committed
     * terminal outcomes; and it MUST be unchanged after a crash + replay (enforced jointly with INV-4 exactly as
     * INV-11/12/13 document the replay-stability facet).
     * <p>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log, scoped to the combinator
     * workflow's instances (those whose id starts with {@code combinatorIdPrefix}; other instances are not constrained
     * and are skipped). For each present instance the check re-derives, from that instance's own committed branch
     * outcomes, the expected decision of each combinator under the {@code CombinatorWorkflow}'s
     * {@link CombinatorWorkflow#VOTED_YES} predicate ("the branch's committed result voted {@code yes}"):
     * <ul>
     *   <li>a branch satisfies the predicate iff its terminal step record is {@code COMPLETED} carrying
     *       {@code vote=yes} in its committed result payload (a {@code FAILED}/non-{@code yes} branch does not);</li>
     *   <li>{@code anyMatch} expected-matched iff <em>any</em> branch satisfied it; {@code allMatch} expected-matched
     *       iff <em>all</em> (and all three branches are present and terminal); {@code noneMatch} expected-matched iff
     *       <em>none</em> did.</li>
     * </ul>
     * It then reads each combinator's <strong>recorded</strong> decision two independent ways and requires both to
     * equal the expected decision: (A) the engine's reconstructed payload ({@code actualPayloadsById}, the
     * workflow-history read-model's {@code state().payload()}, built by an independent projector) carries
     * {@code anyMatched}/{@code allMatched}/{@code noneMatched} booleans; and (B) the committed log records exactly one
     * of the two mutually-exclusive post-combinator step names per combinator ({@code anyMatchMatched} XOR
     * {@code anyMatchUnmatched}, etc.) and that recorded step encodes the same decision. A recorded decision that
     * disagrees with the semantics-derived expectation — or both/neither post-combinator step recorded, or the two
     * reconstructions disagreeing — is the break. This is non-vacuous precisely because the expected decision is
     * computed from the branches' committed outcomes (not read from the same place as the recorded one), so a
     * combinator that resolved the wrong decision, short-circuited incorrectly, or rebuilt a different decision on
     * replay would trip it.
     * <p>
     * The <strong>replay-stability</strong> facet is enforced jointly with INV-4 ({@code DeterministicReplay}): both the
     * recorded post-combinator steps and the recorded payload keys live on the instance's committed subsequence, which
     * is a pure function of the instance's history (the per-{@code workflowId} prefix-stability check in
     * {@code DstSimulation} and the run-level {@link #assertDeterministicReplay}); and (B) is re-derived by the projector
     * across the crash/replay, so a replay that resolved a different combinator decision would show here. Taken per
     * {@code workflowId} (never pooled): the single global log interleaves independent instances non-deterministically
     * (the F-2 surface, ARCHITECTURE.md §8/§11), so another instance's combinator step landing here is irrelevant; one
     * instance recording a combinator decision inconsistent with its own branch outcomes is the break. An instance that
     * has not yet recorded all three branch terminal outcomes (still mid-flight) is skipped — there is nothing to
     * cross-check until the branches have committed. A genuine break throws {@link InvariantViolation}; that would be a
     * new finding (a combinator resolving a decision inconsistent with its branches' committed outcomes), to be triaged
     * per the POC rules — not silently tolerated.
     *
     * @param committedLog        the committed workflow event log (oldest first).
     * @param combinatorIdPrefix  the id prefix of the combinator workflow's instances (e.g. {@code comb-}); only
     *                            instances whose id starts with it are checked.
     * @param actualPayloadsById  the engine's reconstructed final payload per {@code workflowId} (the workflow-history
     *                            read-model's {@code state().payload()}); an instance absent from this map has no
     *                            reconstructed payload yet and is skipped.
     */
    public static void assertCombinatorConsistency(List<EventMessage> committedLog,
                                                    String combinatorIdPrefix,
                                                    Map<String, Map<String, Object>> actualPayloadsById) {
        // Per-workflow append-ordered view: index 0..n-1 is this instance's own committed order.
        var byWorkflow = new LinkedHashMap<String, List<EventMessage>>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(combinatorIdPrefix)) {
                continue; // not a combinator instance — INV-14 does not constrain it.
            }
            byWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(event);
        }
        for (var entry : byWorkflow.entrySet()) {
            assertCombinatorConsistencyForInstance(entry.getKey(), entry.getValue(),
                                                    actualPayloadsById.get(entry.getKey()));
        }
    }

    /**
     * Per-instance INV-14 check: derive each branch's committed terminal outcome (did it vote {@code yes}?), compute the
     * semantics-mandated decision of each combinator, and require both the recorded post-combinator step name and the
     * engine's reconstructed payload to agree with it.
     */
    private static void assertCombinatorConsistencyForInstance(String workflowId,
                                                               List<EventMessage> events,
                                                               @org.jspecify.annotations.Nullable
                                                               Map<String, Object> actualPayload) {
        // Branch terminal outcomes from the committed log: a branch satisfies the predicate iff its terminal step record
        // is COMPLETED carrying vote=yes. Track presence/terminality so a still-mid-flight instance is skipped.
        var branchSatisfied = new LinkedHashMap<String, Boolean>();
        for (EventMessage event : events) {
            var stepStatus = MetadataUtils.getStepStatus(event.metadata());
            if (stepStatus.isEmpty()) {
                continue;
            }
            String stepName = MetadataUtils.getStepName(event.metadata());
            if (!isBranchStep(stepName)) {
                continue;
            }
            if (stepStatus.get() == StepStatus.COMPLETED) {
                Object vote = payloadMapOf(event).get(CombinatorWorkflow.KEY_VOTE);
                branchSatisfied.put(stepName, CombinatorWorkflow.VOTE_YES.equals(vote));
            } else if (stepStatus.get().isTerminal()) {
                branchSatisfied.putIfAbsent(stepName, false); // FAILED/CANCELLED/TIMED_OUT branch does not vote yes.
            }
        }
        // Skip until all three branches have a committed terminal outcome — nothing to cross-check yet.
        if (!branchSatisfied.containsKey(CombinatorWorkflow.BRANCH_A)
                || !branchSatisfied.containsKey(CombinatorWorkflow.BRANCH_B)
                || !branchSatisfied.containsKey(CombinatorWorkflow.BRANCH_C)) {
            return;
        }
        boolean a = branchSatisfied.get(CombinatorWorkflow.BRANCH_A);
        boolean b = branchSatisfied.get(CombinatorWorkflow.BRANCH_B);
        boolean c = branchSatisfied.get(CombinatorWorkflow.BRANCH_C);
        // Expected decisions from the documented short-circuit semantics, given the branches' committed outcomes.
        boolean expectedAny = a || b || c;   // anyMatch: matched iff ≥1 branch satisfied the predicate.
        boolean expectedAll = a && b && c;   // allMatch: matched iff ALL branches satisfied it.
        boolean expectedNone = !(a || b || c); // noneMatch: matched iff NO branch satisfied it.

        // (B-step) Each combinator's recorded post-combinator step: exactly one of the matched/unmatched pair, encoding
        // the recorded decision; cross-check it equals the expected decision.
        assertRecordedCombinatorStep(workflowId, events, "anyMatch", expectedAny,
                                     CombinatorWorkflow.STEP_ANY_MATCHED, CombinatorWorkflow.STEP_ANY_UNMATCHED);
        assertRecordedCombinatorStep(workflowId, events, "allMatch", expectedAll,
                                     CombinatorWorkflow.STEP_ALL_MATCHED, CombinatorWorkflow.STEP_ALL_UNMATCHED);
        assertRecordedCombinatorStep(workflowId, events, "noneMatch", expectedNone,
                                     CombinatorWorkflow.STEP_NONE_MATCHED, CombinatorWorkflow.STEP_NONE_UNMATCHED);

        // (B-payload) The engine's reconstructed payload must carry each decision as a boolean equal to the expected one
        // (skip until projected). The anyMatch winner, when matched, must be one of the matching branches.
        if (actualPayload == null) {
            return; // engine has not reconstructed a payload for this instance yet — nothing to cross-check.
        }
        assertRecordedDecisionPayload(workflowId, actualPayload, "anyMatch", CombinatorWorkflow.KEY_ANY_MATCHED,
                                      expectedAny);
        assertRecordedDecisionPayload(workflowId, actualPayload, "allMatch", CombinatorWorkflow.KEY_ALL_MATCHED,
                                      expectedAll);
        assertRecordedDecisionPayload(workflowId, actualPayload, "noneMatch", CombinatorWorkflow.KEY_NONE_MATCHED,
                                      expectedNone);
        if (expectedAny) {
            Object winner = actualPayload.get(CombinatorWorkflow.KEY_ANY_WINNER);
            if (!isMatchingBranch(winner, a, b, c)) {
                throw new InvariantViolation(
                        "CombinatorConsistency",
                        "instance '" + workflowId + "' recorded anyMatch winner '" + winner + "' but the first matching "
                                + "branch (in declaration order, given branch votes A=" + a + " B=" + b + " C=" + c
                                + ") must be the winner — the race did not proceed on a branch that satisfied the "
                                + "predicate");
            }
        }
    }

    /**
     * Cross-checks one combinator's recorded post-combinator step: exactly one of {@code matchedStep}/
     * {@code unmatchedStep} must be present in the instance's committed log, and which one must equal the
     * semantics-derived {@code expectedMatched}.
     */
    private static void assertRecordedCombinatorStep(String workflowId, List<EventMessage> events,
                                                     String combinator, boolean expectedMatched,
                                                     String matchedStep, String unmatchedStep) {
        boolean recordedMatched = hasStep(events, matchedStep);
        boolean recordedUnmatched = hasStep(events, unmatchedStep);
        if (recordedMatched == recordedUnmatched) {
            // Either both recorded (contradictory) or neither yet recorded. Neither is mid-flight tolerance — by the
            // time we get here all branches are terminal, so the body has run past the combinator and must have recorded
            // exactly one decision step.
            if (!recordedMatched) {
                return; // post-combinator step not yet committed (the body is still recording it) — skip, not a break.
            }
            throw new InvariantViolation(
                    "CombinatorConsistency",
                    "instance '" + workflowId + "' recorded BOTH the matched (" + matchedStep + ") and unmatched ("
                            + unmatchedStep + ") post-" + combinator + " steps — a combinator resolves exactly one "
                            + "decision");
        }
        boolean decision = recordedMatched;
        if (decision != expectedMatched) {
            throw new InvariantViolation(
                    "CombinatorConsistency",
                    "instance '" + workflowId + "' recorded the post-" + combinator + " step '"
                            + (decision ? matchedStep : unmatchedStep) + "' (decision matched=" + decision + ") but the "
                            + "documented semantics over its committed branch outcomes require matched=" + expectedMatched
                            + " — the combinator's decision is inconsistent with its branches' committed terminal "
                            + "outcomes");
        }
    }

    /**
     * Cross-checks one combinator's recorded decision in the engine's reconstructed payload equals the expected one.
     */
    private static void assertRecordedDecisionPayload(String workflowId,
                                                      Map<String, Object> actualPayload,
                                                      String combinator, String key,
                                                      boolean expectedMatched) {
        if (!actualPayload.containsKey(key)) {
            return; // decision not yet projected into the payload — skip, not a break.
        }
        Object recorded = actualPayload.get(key);
        if (!Boolean.valueOf(expectedMatched).equals(recorded)) {
            throw new InvariantViolation(
                    "CombinatorConsistency",
                    "instance '" + workflowId + "' reconstructed payload records " + combinator + " decision " + key
                            + "=" + recorded + " but the documented semantics over its committed branch outcomes require "
                            + key + "=" + expectedMatched + " — the recorded combinator decision is inconsistent with "
                            + "its branches' committed terminal outcomes");
        }
    }

    /**
     * {@code true} iff {@code stepName} is one of the {@link CombinatorWorkflow} parallel branch steps.
     */
    private static boolean isBranchStep(@org.jspecify.annotations.Nullable String stepName) {
        return CombinatorWorkflow.BRANCH_A.equals(stepName)
                || CombinatorWorkflow.BRANCH_B.equals(stepName)
                || CombinatorWorkflow.BRANCH_C.equals(stepName);
    }

    /**
     * {@code true} iff the recorded {@code anyMatch} winner step name is one of the branches that satisfied the
     * predicate (so the race proceeded on a matching branch).
     */
    private static boolean isMatchingBranch(@org.jspecify.annotations.Nullable Object winner, boolean a, boolean b,
                                            boolean c) {
        return (a && CombinatorWorkflow.BRANCH_A.equals(winner))
                || (b && CombinatorWorkflow.BRANCH_B.equals(winner))
                || (c && CombinatorWorkflow.BRANCH_C.equals(winner));
    }

    /**
     * {@code true} iff this instance's committed events contain a step event named {@code stepName}.
     */
    private static boolean hasStep(List<EventMessage> events, String stepName) {
        for (EventMessage event : events) {
            if (MetadataUtils.getStepStatus(event.metadata()).isPresent()
                    && stepName.equals(MetadataUtils.getStepName(event.metadata()))) {
                return true;
            }
        }
        return false;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-15: EventCorrelationExact (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-15 — Event correlation is exact: an {@code associate(...)}-correlated event wakes EXACTLY the matching waiting
     * instance — never a non-matching one (no cross-wakeup) — and duplicate/uncorrelated events produce no spurious wait
     * completion. Concretely, for a workflow whose {@code waitForEvent} step correlates on a per-instance key: an
     * instance's wait step transitions to COMPLETED only on delivery of an event whose correlation key equals THAT
     * instance's key; an instance's recorded wait completion must never be attributable to a foreign key; an uncorrelated
     * event (no matching waiter) creates no wait completion / no spurious step record; a duplicate of an already-matched
     * event does not produce a second wait completion (the wait completes at most once for the right key — the
     * correlation facet of at-most-once recording).
     * <p>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log, scoped to the correlated-wait
     * workflow's instances (those whose id starts with {@code correlatedIdPrefix}; other instances are not constrained
     * and are skipped). Each {@link CorrelatedWaitWorkflow} instance records, in its committed log, its own correlation
     * key (the {@code setup} step's {@link CorrelatedWaitWorkflow#KEY_OWN_KEY} payload) and — once its wait completes —
     * the key of the event that woke it (the {@code recordMatch} step's {@link CorrelatedWaitWorkflow#KEY_MATCHED_KEY}
     * payload, which the body sets from the matched event's key). For each instance whose wait has <em>completed</em>
     * (its {@code awaitSignal} step recorded a terminal COMPLETED outcome <em>and</em> its {@code recordMatch} step has
     * committed) the check requires:
     * <ul>
     *   <li><strong>no cross-wakeup</strong>: the recorded {@code matchedKey} equals the instance's own correlation key
     *       — read two independent ways and both must agree: (A) the committed {@code recordMatch} step's payload and (B)
     *       the engine's reconstructed payload ({@code actualPayloadsById}, the history read-model's
     *       {@code state().payload()}, built by an independent projector). A {@code matchedKey} different from
     *       {@code ownKey} means an event correlated to another instance's key woke THIS instance — the §10 anti-pattern
     *       ("an associated event must NOT wake every waiter") — the break;</li>
     *   <li><strong>wait completes at most once for the right key</strong>: the instance committed at most one terminal
     *       record for its {@code awaitSignal} wait step and at most one {@code recordMatch} record — a duplicate of the
     *       already-matched signal (the MESSAGE_REORDER duplicate mode) must not complete the wait a second time (the
     *       correlation facet of at-most-once recording, INV-2).</li>
     * </ul>
     * An instance whose wait has <strong>not</strong> completed (mid-flight: no terminal {@code awaitSignal} record /
     * no {@code recordMatch} yet) is skipped — there is nothing to cross-check until a correlated event has woken it; in
     * particular an instance whose matching key was never delivered (or only an uncorrelated key was) records no wait
     * completion, exactly as the property requires (no spurious completion). The replay-stability facet is enforced
     * jointly with INV-4 ({@code DeterministicReplay}): the recorded {@code matchedKey} lives on the instance's committed
     * subsequence, a pure function of its history (the per-{@code workflowId} prefix-stability check in
     * {@code DstSimulation} and the run-level {@link #assertDeterministicReplay}), and (B) is re-derived by the projector
     * across the crash/replay, so a replay that resolved a different matched key would show here. Taken per
     * {@code workflowId} (never pooled): the single global log interleaves independent instances non-deterministically
     * (the F-2 surface, ARCHITECTURE.md §8/§11), so another instance's wait completion landing here is irrelevant; one
     * instance recording a foreign matched key, or completing its wait twice, is the break. A genuine break throws
     * {@link InvariantViolation}; that would be a high-value finding (a real engine cross-wakeup — an associated event
     * waking a non-matching waiter), to be triaged per the POC rules — not silently tolerated.
     *
     * @param committedLog       the committed workflow event log (oldest first).
     * @param correlatedIdPrefix the id prefix of the correlated-wait workflow's instances (e.g. {@code corr-}); only
     *                           instances whose id starts with it are checked.
     * @param actualPayloadsById the engine's reconstructed final payload per {@code workflowId} (the workflow-history
     *                           read-model's {@code state().payload()}); an instance absent from this map has no
     *                           reconstructed payload yet and is cross-checked against the committed log only.
     */
    public static void assertEventCorrelationExact(List<EventMessage> committedLog,
                                                   String correlatedIdPrefix,
                                                   Map<String, Map<String, Object>> actualPayloadsById) {
        // Per-workflow append-ordered view: index 0..n-1 is this instance's own committed order.
        var byWorkflow = new LinkedHashMap<String, List<EventMessage>>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(correlatedIdPrefix)) {
                continue; // not a correlated-wait instance — INV-15 does not constrain it.
            }
            byWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(event);
        }
        for (var entry : byWorkflow.entrySet()) {
            assertEventCorrelationExactForInstance(entry.getKey(), entry.getValue(),
                                                   actualPayloadsById.get(entry.getKey()));
        }
    }

    /**
     * Per-instance INV-15 check: derive the instance's own correlation key and the matched key (the key of the event
     * that woke its wait) from the committed log, count its wait completions, and require no cross-wakeup (matchedKey ==
     * ownKey, agreeing across the committed log and the engine's reconstructed payload) and at-most-once wait completion.
     */
    private static void assertEventCorrelationExactForInstance(String workflowId,
                                                               List<EventMessage> events,
                                                               @org.jspecify.annotations.Nullable
                                                               Map<String, Object> actualPayload) {
        String ownKey = null;          // the key the setup step recorded — the key this instance correlates on.
        String matchedKey = null;      // the key recorded by recordMatch — the event key that actually woke the wait.
        int waitCompletions = 0;       // terminal records for the awaitSignal wait step.
        int recordMatchRecords = 0;    // terminal records for the recordMatch step.
        for (EventMessage event : events) {
            var stepStatus = MetadataUtils.getStepStatus(event.metadata());
            if (stepStatus.isEmpty()) {
                continue; // workflow-status event — not a step record.
            }
            String stepName = MetadataUtils.getStepName(event.metadata());
            if (CorrelatedWaitWorkflow.STEP_SETUP.equals(stepName)
                    && stepStatus.get() == StepStatus.COMPLETED) {
                Object recorded = payloadMapOf(event).get(CorrelatedWaitWorkflow.KEY_OWN_KEY);
                if (recorded != null) {
                    ownKey = String.valueOf(recorded);
                }
            } else if (CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL.equals(stepName) && stepStatus.get().isTerminal()) {
                waitCompletions++;
            } else if (CorrelatedWaitWorkflow.STEP_RECORD_MATCH.equals(stepName) && stepStatus.get().isTerminal()) {
                recordMatchRecords++;
                Object recorded = payloadMapOf(event).get(CorrelatedWaitWorkflow.KEY_MATCHED_KEY);
                if (recorded != null) {
                    matchedKey = String.valueOf(recorded);
                }
            }
        }
        // At-most-once wait completion for the right key (the correlation facet of INV-2): a duplicate of the matched
        // signal must not complete the wait — or re-run recordMatch — a second time.
        if (waitCompletions > 1) {
            throw new InvariantViolation(
                    "EventCorrelationExact",
                    "instance '" + workflowId + "' completed its correlated wait step '"
                            + CorrelatedWaitWorkflow.STEP_AWAIT_SIGNAL + "' " + waitCompletions + " times — a "
                            + "correlated event (incl. a duplicate of an already-matched one) must complete the wait at "
                            + "most once for the right key");
        }
        if (recordMatchRecords > 1) {
            throw new InvariantViolation(
                    "EventCorrelationExact",
                    "instance '" + workflowId + "' recorded its post-wait step '"
                            + CorrelatedWaitWorkflow.STEP_RECORD_MATCH + "' " + recordMatchRecords + " times — a "
                            + "duplicate/redelivered signal must not produce a second wait completion");
        }
        // Mid-flight: the wait has not completed (no matched key recorded yet). Nothing to cross-check — in particular an
        // instance whose matching key was never delivered records no completion, exactly as the property requires (no
        // spurious completion). Skip.
        if (matchedKey == null || waitCompletions == 0) {
            return;
        }
        if (ownKey == null) {
            return; // own key not yet projected from the setup step — cannot cross-check yet.
        }
        // (A) No cross-wakeup, committed-log source: the matched key must equal the instance's own correlation key. A
        // foreign matched key means an event correlated to ANOTHER instance's key woke THIS instance — the §10 anti-
        // pattern (an associated event must NOT wake every waiter) — a cross-wakeup break.
        if (!ownKey.equals(matchedKey)) {
            throw new InvariantViolation(
                    "EventCorrelationExact",
                    "instance '" + workflowId + "' (correlation key '" + ownKey + "') completed its wait on a FOREIGN "
                            + "matched key '" + matchedKey + "' — an associate(...)-correlated event woke a non-matching "
                            + "waiter (cross-wakeup); a correlated event must wake EXACTLY the matching instance");
        }
        // (B) No cross-wakeup, engine-reconstructed-payload source (an independent reconstruction): once projected, the
        // engine's own final payload must also record matchedKey == ownKey, so a replay that resolved a different matched
        // key would diverge from (A) and trip here.
        if (actualPayload != null && actualPayload.containsKey(CorrelatedWaitWorkflow.KEY_MATCHED_KEY)) {
            Object projectedMatched = actualPayload.get(CorrelatedWaitWorkflow.KEY_MATCHED_KEY);
            if (!ownKey.equals(String.valueOf(projectedMatched))) {
                throw new InvariantViolation(
                        "EventCorrelationExact",
                        "instance '" + workflowId + "' (correlation key '" + ownKey + "') has engine-reconstructed "
                                + "matchedKey '" + projectedMatched + "' — the projector resolved a foreign matched key "
                                + "(cross-wakeup); a correlated event must wake EXACTLY the matching instance");
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-16: FailurePropagation (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-16 — Failure propagation: a step failure propagates to a terminal FAILED workflow status — the workflow never
     * silently hangs or completes when a step fails. Concretely, for a workflow whose step throws an uncaught exception
     * (with no or exhausted retries): the failing step is recorded with a terminal failure status (FAILED or TIMED_OUT),
     * the instance reaches a terminal FAILED workflow status (not COMPLETED, not stuck non-terminal at the horizon), and —
     * complementing INV-7 ({@code TerminalIsFinal}) — no step after the failing one begins.
     * <p>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log, scoped to the failing workflow's
     * instances (those whose id starts with {@code failingIdPrefix}; other instances are not constrained and are skipped).
     * It is the deliberate contrast with INV-7 (which covers {@code ctx.cancel}&rarr;CANCELLED finality) and INV-8 (which
     * bounds retry <em>attempt records</em>): INV-16 asserts the FAILURE actually <em>propagates</em> to a FAILED terminus
     * deterministically, across crash/replay. For each instance that has reached a <em>terminal workflow status</em> the
     * check requires, content-based (presence/count of events and the terminal statuses reached — not the global-append
     * order between this instance's own events, which is the F-2 intra-instance non-determinism — see below):
     * <ul>
     *   <li><strong>the failing step is recorded terminally-failed</strong>: a terminal step record exists for the
     *       {@code failingStepName} with a <em>failure</em> status ({@code FAILED} or {@code TIMED_OUT}) — the failure did
     *       not silently vanish;</li>
     *   <li><strong>the workflow terminated FAILED</strong>: the instance's terminal workflow status is {@code FAILED},
     *       never {@code COMPLETED} (a step failure that surfaced as a completed workflow would be a silent-completion
     *       break) and never {@code CANCELLED} (that is INV-7's path);</li>
     *   <li><strong>no step after the failing one begins</strong>: the instance recorded no step record at all for the
     *       {@code afterFailureStepName} (the step the body would run if termination were not final) — the failure
     *       propagation, jointly with INV-7, stopped the body at the failing step.</li>
     * </ul>
     * An instance that has <strong>not</strong> yet reached a terminal workflow status (mid-flight) is skipped — there is
     * nothing to enforce until the failure has had a chance to propagate; once it terminates the three facets are
     * required. The check is robust to <strong>F-2</strong>: it asserts per-instance CONTENT (a terminal step-failure
     * record exists, the terminal workflow status is FAILED, the after-failure step is absent) rather than any
     * append-order between the instance's own events — the engine publishes durably-async, so the failing step's terminal
     * record can land just before/after the workflow-FAILED commit run-to-run (intra-instance, here harmless because both
     * are required to be present, not ordered). The replay-stability facet is enforced jointly with INV-4
     * ({@code DeterministicReplay}): the failing-step record and the FAILED workflow status live on the instance's
     * committed subsequence, a pure function of its history (the per-{@code workflowId} prefix-stability check in
     * {@code DstSimulation} and the run-level {@link #assertDeterministicReplay}), so a replay re-reaches the already-FAILED
     * primitive as a no-op and the FAILED terminus is stable. Taken per {@code workflowId} (never pooled): the single
     * global log interleaves independent instances non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11), so
     * another instance's event landing here is irrelevant; this instance failing to propagate its own step failure to a
     * FAILED terminus is the break. A genuine break throws {@link InvariantViolation}; that would be a high-value finding
     * (a silent hang or a step failure that completed the workflow), to be triaged per the POC rules — not silently
     * tolerated.
     *
     * @param committedLog         the committed workflow event log (oldest first).
     * @param failingIdPrefix      the id prefix of the failing workflow's instances (e.g. {@code fail-}); only instances
     *                             whose id starts with it are checked.
     * @param failingStepName      the always-throwing step that must be recorded terminally-failed (FAILED/TIMED_OUT)
     *                             (e.g. {@code failingStep}).
     * @param afterFailureStepName the step that must NOT begin once the failing step has propagated (e.g.
     *                             {@code afterFailure}); any step record for it is a break.
     */
    public static void assertFailurePropagation(List<EventMessage> committedLog,
                                                String failingIdPrefix,
                                                String failingStepName,
                                                String afterFailureStepName) {
        // Per-instance content: did it reach a terminal workflow status (and which), is the failing step recorded with a
        // terminal FAILURE status, and did any after-failure step begin? Content over append order (F-2 robust).
        var terminalWorkflowStatus = new LinkedHashMap<String, WorkflowStatus>();
        var failingStepTerminallyFailed = new LinkedHashMap<String, Boolean>();
        var afterFailureStepBegan = new LinkedHashMap<String, Boolean>();
        for (EventMessage event : committedLog) {
            var metadata = event.metadata();
            String workflowId = MetadataUtils.getWorkflowId(metadata);
            if (!workflowId.startsWith(failingIdPrefix)) {
                continue; // not a failing instance — INV-16 does not constrain it.
            }
            var workflowStatus = MetadataUtils.getWorkflowStatus(metadata);
            if (workflowStatus.isPresent() && workflowStatus.get().isTerminal()) {
                // First terminal workflow status wins (a second is independently an INV-7 break).
                terminalWorkflowStatus.putIfAbsent(workflowId, workflowStatus.get());
                continue;
            }
            var stepStatus = MetadataUtils.getStepStatus(metadata);
            if (stepStatus.isEmpty()) {
                continue;
            }
            String stepName = MetadataUtils.getStepName(metadata);
            if (failingStepName.equals(stepName)
                    && (stepStatus.get() == StepStatus.FAILED || stepStatus.get() == StepStatus.TIMED_OUT)) {
                failingStepTerminallyFailed.put(workflowId, true);
            } else if (afterFailureStepName.equals(stepName)) {
                afterFailureStepBegan.put(workflowId, true);
            }
        }
        for (var entry : terminalWorkflowStatus.entrySet()) {
            String workflowId = entry.getKey();
            WorkflowStatus status = entry.getValue();
            // Facet 1 — the workflow terminated FAILED (the failure propagated to a FAILED terminus): not COMPLETED (a
            // silently-completed step failure), not CANCELLED (INV-7's ctx.cancel path), not TIMED_OUT (a hang the
            // workflow-level timeout caught, not the step failure propagating).
            if (status != WorkflowStatus.FAILED) {
                throw new InvariantViolation(
                        "FailurePropagation",
                        "failing instance '" + workflowId + "' reached terminal workflow status " + status + " — a step "
                                + "that threw an uncaught exception must propagate to a terminal FAILED workflow status, "
                                + "never silently complete (COMPLETED) or take another terminal path");
            }
            // Facet 2 — the failing step is recorded terminally-failed (the failure did not silently vanish).
            if (!failingStepTerminallyFailed.getOrDefault(workflowId, false)) {
                throw new InvariantViolation(
                        "FailurePropagation",
                        "failing instance '" + workflowId + "' reached terminal FAILED but its failing step '"
                                + failingStepName + "' has no terminal FAILURE record (FAILED/TIMED_OUT) — the failure "
                                + "must be recorded on the step, not silently vanish");
            }
            // Facet 3 — no step after the failing one began (jointly with INV-7: termination is final, no new work).
            if (afterFailureStepBegan.getOrDefault(workflowId, false)) {
                throw new InvariantViolation(
                        "FailurePropagation",
                        "failing instance '" + workflowId + "' recorded a step '" + afterFailureStepName + "' after its "
                                + "failing step propagated — no step after the failing one may begin (the failure ends "
                                + "the workflow)");
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-17: StatusHookFiresOncePerStatus (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-17 — Status-hook fires at most once per status (the lifecycle-hook analogue of INV-6 {@code EffectAtMostOnce}/
     * F-0): a registered workflow status-change hook (a {@code @WorkflowStatusChangedHandler} / the test
     * {@code .registerWorkflowStatusChangeListener(status, listener)} registration) fires <strong>at most once</strong>
     * for a given {@code (workflowId, status)} across the instance's whole lifetime — and is <strong>NOT re-fired on a
     * crash + replay</strong> that re-runs the body and re-drives the engine's event evolution. A status hook is a user
     * side effect (it runs application code on a status transition), so — exactly like a step's {@code execute} action
     * under INV-6 — replaying the committed status events must not re-invoke it.
     * <p>
     * This is asserted <strong>per {@code (workflowId, status)}</strong> against the crash-surviving
     * {@link StatusHookFires} fire-counter (the hook analogue of {@link io.axoniq.framework.workflow.simulation.workflow.CountingEffects}),
     * scoped to the hook workflow's instances (those whose id starts with {@code hookIdPrefix}; other instances are not
     * constrained and are skipped). It is content-based (a fire COUNT per status, never an append-order between an
     * instance's own events — F-2 robust) and crash-stable: the counter is kept across {@code crashAndRecover()} (a real
     * external hook effect the engine cannot roll back), so a replay that re-fired the hook would push a status's count
     * above one and trip this check.
     * <p>
     * <strong>Empirical result (the F-0 analogue facet, which this method enforces): HOLDS.</strong> No registered
     * status's hook is observed to fire more than once for an instance, including across (repeated) crash + replay: the
     * recovered engine does not re-invoke the listeners while it re-evolves the already-committed STARTED/COMPLETED
     * status events. A status firing &gt;1 time would be the high-value re-fire finding (a lifecycle-hook F-0 analogue) —
     * it throws {@link InvariantViolation} and would be triaged per the POC rules; it is not observed.
     * <p>
     * <strong>The terminal COMPLETED hook's at-least-once facet — finding F-4, now FIXED — is enforced by
     * {@link #assertCompletedHookFiredExactlyOnce}.</strong> Previously the terminal COMPLETED hook was fired
     * at-most-once but NOT reliably at-least-once: on the happy completion path it could be <em>dropped</em> (fired zero
     * times). The STARTED hook always fired reliably exactly once (the start path drains the task queue via
     * {@code awaitStateChange} before the body proceeds), and the {@code ctx.fail}/{@code ctx.cancel}/timeout terminal
     * paths likewise {@code awaitStateChange} on the terminal status before the body returns; but the COMPLETED happy
     * path returned after only awaiting the event <em>commit</em> and then {@code finishWorkflow} cleared the per-instance
     * task queue, so if the live evolution of the committed COMPLETED workflow-status event had not yet been consumed,
     * its {@code setStatus(COMPLETED)} → listener {@code notify} was cleared and never ran (a lost lifecycle-hook fire).
     * <strong>F-4 is now fixed</strong>: the engine's happy completion path now also
     * {@code awaitStateChange(s -> s.workflowStatus().isTerminal())}s on the terminal status before {@code finishWorkflow}
     * (symmetric with the awaited fail/cancel/timeout paths and STARTED), so the COMPLETED evolution and its hook run
     * before the queue is cleared. The COMPLETED hook therefore now fires reliably exactly once — enforced by
     * {@link #assertCompletedHookFiredExactlyOnce} — and, per this method, is still never re-fired on replay.
     *
     * @param fires        the crash-surviving status-hook fire counter the registered listeners record into.
     * @param workflowId   the workflow instance (must start with the hook workflow's prefix).
     * @param statuses     the statuses whose registered hooks are checked for the at-most-once guarantee.
     */
    public static void assertStatusHookFiresOncePerStatus(StatusHookFires fires, String workflowId,
                                                          WorkflowStatus... statuses) {
        for (WorkflowStatus status : statuses) {
            int count = fires.count(workflowId, status);
            if (count > 1) {
                throw new InvariantViolation(
                        "StatusHookFiresOncePerStatus",
                        "status-change hook for '" + workflowId + "/" + status + "' fired " + count + " times — a "
                                + "registered hook must fire at most once per status and must NOT be re-fired on "
                                + "crash/replay (the lifecycle-hook analogue of INV-6/F-0: replaying the committed "
                                + "status events must not re-invoke the listener)");
            }
        }
    }

    /**
     * Asserts the STARTED status hook fired <strong>exactly once</strong> for the instance — the reliable at-least-once
     * facet of INV-17. The start path drains the per-instance task queue ({@code awaitStateChange}) before the body
     * proceeds, so the live evolution of the committed STARTED workflow-status event always runs and the STARTED
     * listener always {@code notify}s exactly once (and, per {@link #assertStatusHookFiresOncePerStatus}, never re-fires
     * on replay).
     *
     * @param fires      the crash-surviving status-hook fire counter.
     * @param workflowId the workflow instance.
     */
    public static void assertStartedHookFiredExactlyOnce(StatusHookFires fires, String workflowId) {
        int count = fires.count(workflowId, WorkflowStatus.STARTED);
        if (count != 1) {
            throw new InvariantViolation(
                    "StatusHookFiresOncePerStatus",
                    "STARTED status-change hook for '" + workflowId + "' fired " + count + " times — it must fire "
                            + "exactly once (the start path awaits the STARTED evolution before proceeding, so the hook "
                            + "is neither dropped nor re-fired)");
        }
    }

    /**
     * Asserts the terminal COMPLETED status hook fired <strong>exactly once</strong> for the instance — the reliable
     * at-least-once facet of INV-17 for the happy completion path, now enforced after the <strong>F-4 fix</strong>.
     * Mirrors {@link #assertStartedHookFiredExactlyOnce}: the engine's happy completion path now
     * {@code awaitStateChange(s -> s.workflowStatus().isTerminal())}s on the terminal status before
     * {@code finishWorkflow} runs {@code taskQueue.clear()} (symmetric with the awaited
     * {@code ctx.fail}/{@code ctx.cancel}/timeout paths), so the live evolution of the committed COMPLETED
     * workflow-status event — its {@code setStatus(COMPLETED)} → listener {@code notify} — always runs <em>before</em>
     * the queue is cleared. The COMPLETED hook therefore fires reliably exactly once: it is no longer dropped
     * (at-least-once now holds), and — per {@link #assertStatusHookFiresOncePerStatus} — never re-fired on replay.
     *
     * @param fires      the crash-surviving status-hook fire counter.
     * @param workflowId the workflow instance.
     */
    public static void assertCompletedHookFiredExactlyOnce(StatusHookFires fires, String workflowId) {
        int count = fires.count(workflowId, WorkflowStatus.COMPLETED);
        if (count != 1) {
            throw new InvariantViolation(
                    "StatusHookFiresOncePerStatus",
                    "COMPLETED status-change hook for '" + workflowId + "' fired " + count + " times — it must fire "
                            + "exactly once now that F-4 is fixed (the happy completion path awaits the terminal state "
                            + "change before finishWorkflow, so the COMPLETED evolution and its hook run before the queue "
                            + "is cleared — the hook is neither dropped nor re-fired)");
        }
    }

    /**
     * Drop detector / re-fire guard for the terminal COMPLETED hook, retained after the <strong>F-4 fix</strong>.
     * INV-17's no-re-fire facet always held (the COMPLETED hook is fired <strong>at most once</strong>); the
     * at-least-once direction was the F-4 gap — the happy completion path could <em>drop</em> it (fire zero times).
     * <strong>F-4 is now fixed</strong>: the engine's happy completion path {@code awaitStateChange}s on the terminal
     * status before {@code finishWorkflow} clears the per-instance task queue (symmetric with the awaited
     * {@code ctx.fail}/{@code ctx.cancel}/timeout paths), so the COMPLETED evolution and its hook run before the queue
     * is cleared and the drop no longer occurs. As a result this method now always returns {@code false}; it is retained
     * as the re-fire guard / drop detector for the unit pins (it still throws on a count &gt; 1 — the lifecycle-hook
     * re-fire finding, the F-0 analogue — and reports a drop should one ever reappear). The enforced at-least-once
     * assertion is now {@link #assertCompletedHookFiredExactlyOnce}.
     *
     * @param fires      the crash-surviving status-hook fire counter.
     * @param workflowId the workflow instance.
     * @return {@code true} iff the COMPLETED hook was observed to have been dropped (fired zero times) for this instance;
     *         post-fix this is always {@code false}.
     */
    public static boolean documentTerminalHookMayBeDropped(StatusHookFires fires, String workflowId) {
        int count = fires.count(workflowId, WorkflowStatus.COMPLETED);
        if (count > 1) {
            throw new InvariantViolation(
                    "StatusHookFiresOncePerStatus",
                    "COMPLETED status-change hook for '" + workflowId + "' fired " + count + " times — the at-most-once "
                            + "/ no-re-fire guarantee must hold even for the terminal hook (a count above one would be "
                            + "the lifecycle-hook re-fire finding, the F-0 analogue)");
        }
        return count == 0;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-18: DriftGuardPausesCleanly (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-18 — Drift guard pauses cleanly: when replay drift is detected ({@code guardAgainstReplayDrift} throws
     * {@code WorkflowReplayDriftException} — new code runs past a step the recorded state already has terminal, WITHOUT
     * a {@code ctx.migrateVersion}), the engine PAUSES the instance <strong>NON-TERMINALLY</strong> and
     * <strong>CLEANLY</strong>: it appends NO terminal workflow-status event and NO spurious/corrupt step event for that
     * instance (in particular no event for the drifted step the new code was about to publish), and the instance's
     * previously-committed history is left intact — a superset-preserving, recoverable state. The drift-paused instance
     * is exactly the documented INV-5 ({@code EventuallyTerminates}) non-termination carve-out (INVARIANTS.md INV-5
     * "deliberately does NOT hold for the drift-paused state, awaiting redeploy").
     * <p>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log <em>after</em> the divergent
     * replay, scoped to the drifting workflow's instances (those whose id starts with {@code driftIdPrefix}; other
     * instances are not constrained and are skipped). It is <strong>content-based</strong> (presence/absence/counts per
     * {@code workflowId}), <strong>not</strong> the global-append order between an instance's own events — F-2-robust
     * (the engine publishes durably-async; this never asserts the order of an instance's own events). For each drifting
     * instance whose pre-drift snapshot is given, the check requires:
     * <ul>
     *   <li><strong>no terminal workflow status</strong>: the instance recorded NO terminal workflow-status event
     *       ({@code COMPLETED}/{@code FAILED}/{@code CANCELLED}/{@code TIMED_OUT}) — it is PAUSED, not failed/cancelled/
     *       completed (a terminal status on a drift-paused instance would be the high-value finding: the engine drove a
     *       drifted instance terminal);</li>
     *   <li><strong>no spurious drifted-step event</strong>: the instance recorded NO step event for the
     *       {@code driftedStepName} the new code was about to publish (the guard fires <em>before</em> the first publish,
     *       so the drifted step must never appear);</li>
     *   <li><strong>committed history intact (superset-preserving)</strong>: every event the instance had in its
     *       pre-drift committed snapshot is still present after the drifted replay (a per-instance multiset
     *       superset/⊇ check — content, not append-order); the drifted replay must not lose or corrupt any committed
     *       event (it shares the durability contract of INV-3 {@code CommittedHistorySurvivesCrash}).</li>
     * </ul>
     * A genuine break throws {@link InvariantViolation}; that would be a high-value finding (a drift-paused instance that
     * was nonetheless driven to a terminal status, a spurious drifted-step event, or a corrupted/truncated history), to
     * be triaged per the POC rules — not silently tolerated.
     *
     * @param committedAfterDrift   the committed workflow event log after the divergent replay (oldest first).
     * @param driftIdPrefix         the id prefix of the drifting workflow's instances (e.g. {@code drift-}); only
     *                              instances whose id starts with it are checked.
     * @param drivePreDriftSnapshot per-{@code workflowId} committed snapshot taken just before the divergent replay (the
     *                              history that must be preserved); a drifting instance not present here is skipped.
     * @param driftedStepName       the new-code step the drift guard fired on (e.g. {@code repackage}); any step event
     *                              for it on a drifting instance is a spurious-event break.
     */
    public static void assertDriftGuardPausesCleanly(List<EventMessage> committedAfterDrift,
                                                     String driftIdPrefix,
                                                     Map<String, List<EventMessage>> drivePreDriftSnapshot,
                                                     String driftedStepName) {
        var afterByWorkflow = perWorkflowMultisets(committedAfterDrift);
        // Per-instance content: which terminal workflow status (if any) did it record, and did the drifted step appear?
        var terminalWorkflowStatus = new LinkedHashMap<String, WorkflowStatus>();
        var driftedStepBegan = new LinkedHashMap<String, Boolean>();
        for (EventMessage event : committedAfterDrift) {
            var metadata = event.metadata();
            String workflowId = MetadataUtils.getWorkflowId(metadata);
            if (!workflowId.startsWith(driftIdPrefix)) {
                continue; // not a drifting instance — INV-18 does not constrain it.
            }
            var workflowStatus = MetadataUtils.getWorkflowStatus(metadata);
            if (workflowStatus.isPresent() && workflowStatus.get().isTerminal()) {
                terminalWorkflowStatus.putIfAbsent(workflowId, workflowStatus.get());
                continue;
            }
            var stepStatus = MetadataUtils.getStepStatus(metadata);
            if (stepStatus.isPresent() && driftedStepName.equals(MetadataUtils.getStepName(metadata))) {
                driftedStepBegan.put(workflowId, true);
            }
        }
        for (var entry : drivePreDriftSnapshot.entrySet()) {
            String workflowId = entry.getKey();
            if (!workflowId.startsWith(driftIdPrefix)) {
                continue;
            }
            // Facet 1 — the drift-paused instance recorded NO terminal workflow status (it is paused, not failed/
            // cancelled/completed). A terminal status here = the engine drove a drifted instance terminal (the finding).
            WorkflowStatus terminal = terminalWorkflowStatus.get(workflowId);
            if (terminal != null) {
                throw new InvariantViolation(
                        "DriftGuardPausesCleanly",
                        "drift-paused instance '" + workflowId + "' recorded a terminal workflow status " + terminal
                                + " — a replay-drift-paused instance must stay NON-TERMINAL (the documented INV-5 "
                                + "carve-out, awaiting redeploy); a terminal status means the engine drove a drifted "
                                + "instance terminal instead of pausing it cleanly");
            }
            // Facet 2 — no spurious event for the drifted step the new code was about to publish (the guard fires before
            // the first publish, so the step must never appear).
            if (driftedStepBegan.getOrDefault(workflowId, false)) {
                throw new InvariantViolation(
                        "DriftGuardPausesCleanly",
                        "drift-paused instance '" + workflowId + "' recorded a step event for the drifted step '"
                                + driftedStepName + "' — the drift guard fires BEFORE the first publish, so the new "
                                + "code's drifted step must never be appended (a spurious/corrupt step event)");
            }
            // Facet 3 — committed history intact: every pre-drift committed event is still present (per-instance multiset
            // superset/⊇ — content, not append-order; F-2-robust, the INV-3 durability contract).
            List<String> before = new ArrayList<>(perWorkflowMultisets(entry.getValue()).getOrDefault(workflowId,
                                                                                                       List.of()));
            List<String> after = new ArrayList<>(afterByWorkflow.getOrDefault(workflowId, List.of()));
            // Remove each pre-drift event from the after-multiset; any leftover means a committed event was lost.
            List<String> remaining = new ArrayList<>(after);
            for (String beforeEvent : before) {
                if (!remaining.remove(beforeEvent)) {
                    throw new InvariantViolation(
                            "DriftGuardPausesCleanly",
                            "drift-paused instance '" + workflowId + "' lost a committed event across the divergent "
                                    + "replay: '" + beforeEvent + "' was in its pre-drift committed history but is "
                                    + "absent afterwards — the drift-paused state must preserve committed history "
                                    + "intact (the INV-3 durability contract), not truncate/corrupt it (before="
                                    + before + " after=" + after + ")");
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-20: VersioningEdges (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-20 — Versioning edges: the engine's versioning machinery is sound at the edges INV-11
     * ({@code VersionRoutingSound}) and INV-12 ({@code MigrateVersionContract}) do not cover. Across crashes/replays,
     * for a workflow registered at <strong>three</strong> coexisting versions whose body performs multiple in-body
     * {@code ctx.migrateVersion} migrations under distinct {@code changeId}s and attempts a downgrade:
     * <ul>
     *   <li><strong>a downgrade migration is rejected and never recorded</strong>: a
     *       {@code ctx.migrateVersion(changeId, vLower)} whose {@code vLower} is not strictly greater than the
     *       instance's current recorded version throws {@code IllegalArgumentException} ({@code VersionDelegate.java:117-122})
     *       and records no migration marker — so the instance's committed log holds no marker for the downgrade
     *       {@code changeId} and no recorded version below its started version;</li>
     *   <li><strong>multiple distinct {@code changeId}s each record at most once and monotonic non-decreasing</strong>
     *       (extends INV-12 across {@code changeId}s): each {@code (workflowId, changeId)} has at most one committed
     *       migration marker, the recorded versions scanned in committed (append) order are non-decreasing
     *       (first-writer-wins, never downgrades), and each is a valid semver;</li>
     *   <li><strong>deeper multi-version routing</strong> (extends INV-11 with depth): a fresh start spawns at the
     *       highest registered version (the {@code STARTED} event carries it) and no committed event is ever stamped with
     *       a version below the started one (a stamped downgrade). Unlike INV-11 (a non-migrating instance carries
     *       exactly one version), this instance migrates, so its events legitimately carry the started version plus the
     *       forward-bumped versions. An instance recorded (post-migration) at a version no longer registered, recovered
     *       under a reduced registry, routes via the 4/5-pass lookup
     *       ({@code WorkflowConfigurationRegistry.resolveDefinitionForReplay}) to the closest registered sibling
     *       {@code <=} its recorded state — never 0 (stranded, no body), never 2 (two definitions handling one instance);
     *       observed by the recovered instance still progressing/completing cleanly under that one routed definition.</li>
     * </ul>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log, scoped to the versioning-edges
     * workflow's instances (those whose id starts with {@code edgesIdPrefix}; other instances are not constrained and are
     * skipped). The observables are each event's {@code MessageType.version()} (the resolved definition version) and the
     * migration-step events ({@code MetadataUtils.isVersionMigrationStep}, carrying the {@code versionChangeId} + {@code version}
     * keys). Taken per {@code workflowId} (never pooled): the single global log interleaves independent instances
     * non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11), so another instance's marker/version landing here
     * is irrelevant. The replay-stability facet is enforced jointly with INV-4 ({@code DeterministicReplay}): the version
     * stamps and migration markers live on the instance's committed subsequence, a pure function of its history, so a
     * replay resolves the same versions and re-reaches each migration as a no-op. A genuine break — a recorded downgrade
     * marker, a marker recorded twice, a recorded version moving backwards, an instance routed to 0 or 2 versions, or a
     * fresh spawn not at the highest version — throws {@link InvariantViolation}; that would be a high-value versioning
     * finding to triage per the POC rules, not silently tolerated.
     *
     * @param committedLog          the committed workflow event log (oldest first).
     * @param edgesIdPrefix         the id prefix of the versioning-edges workflow's instances (e.g. {@code vedge-});
     *                              only instances whose id starts with it are checked.
     * @param highestRegisteredVersion the highest registered semver version (what a fresh start must spawn at, e.g.
     *                              {@code 2.0.0}); used only when {@code requireHighestForFresh} is {@code true}.
     * @param downgradeChangeId     the {@code changeId} of the attempted downgrade migration; no committed marker may
     *                              carry it (the downgrade must have been rejected, never recorded).
     * @param requireHighestForFresh when {@code true}, every started instance must resolve to exactly
     *                              {@code highestRegisteredVersion} (a fresh spawn at the highest version); when
     *                              {@code false} (an instance recovered under a registry whose highest version was
     *                              dropped), only the exactly-one-version-per-instance facet is enforced (the instance
     *                              keeps its pinned recorded version; the 4/5-pass lookup having routed it to a runnable
     *                              closest sibling is observed by the instance still progressing, not by a changed stamp).
     * @param expectedStartedWorkflowIds versioned ids the caller knows it started; each must be present in the log with a
     *                              resolvable version (else routed to 0). Empty for the per-step always-on call.
     */
    public static void assertVersioningEdges(List<EventMessage> committedLog,
                                             String edgesIdPrefix,
                                             String highestRegisteredVersion,
                                             String downgradeChangeId,
                                             boolean requireHighestForFresh,
                                             Set<String> expectedStartedWorkflowIds) {
        // Per-instance: did it record a workflow-status STARTED; the distinct resolved versions its events carry; the
        // started version (first STARTED event's version); the ordered list of recorded migration versions (append
        // order); the migration markers per (workflowId, changeId); and whether the downgrade changeId was recorded.
        var startedByWorkflow = new LinkedHashMap<String, Boolean>();
        var versionsByWorkflow = new LinkedHashMap<String, Set<String>>();
        var startedVersionByWorkflow = new LinkedHashMap<String, String>();
        var orderedMigrationVersionsByWorkflow = new LinkedHashMap<String, List<String>>();
        var migrationCountByKey = new LinkedHashMap<String, Integer>();
        for (EventMessage event : committedLog) {
            var metadata = event.metadata();
            String workflowId = MetadataUtils.getWorkflowId(metadata);
            if (!workflowId.startsWith(edgesIdPrefix)) {
                continue; // not a versioning-edges instance — INV-20 does not constrain it.
            }
            versionsByWorkflow.computeIfAbsent(workflowId, k -> new java.util.LinkedHashSet<>())
                              .add(event.type().version());
            var workflowStatus = MetadataUtils.getWorkflowStatus(metadata);
            if (workflowStatus.isPresent() && workflowStatus.get() == WorkflowStatus.STARTED) {
                startedByWorkflow.put(workflowId, true);
                startedVersionByWorkflow.putIfAbsent(workflowId, event.type().version());
            }
            if (!MetadataUtils.isVersionMigrationStep(metadata)) {
                continue;
            }
            String changeId = MetadataUtils.getVersionChangeId(metadata).orElse(null);
            String recordedVersion = MetadataUtils.getVersion(metadata).orElse(null);
            if (changeId == null || recordedVersion == null) {
                continue; // not a well-formed migration marker.
            }
            // Facet — downgrade rejected: NO migration marker may carry the downgrade changeId. The engine throws on a
            // downgrade BEFORE appending any task, so a marker for this changeId means the engine wrongly recorded a
            // downgrade — a finding.
            if (downgradeChangeId.equals(changeId)) {
                throw new InvariantViolation(
                        "VersioningEdges",
                        "instance '" + workflowId + "' recorded a migration marker for the downgrade changeId '"
                                + downgradeChangeId + "' (version '" + recordedVersion + "') — a downgrade migration must "
                                + "be REJECTED (IllegalArgumentException), never recorded");
            }
            // Each recorded version must be a valid semver — the primitive validates before recording.
            try {
                Version.validate(recordedVersion);
            } catch (IllegalArgumentException e) {
                throw new InvariantViolation(
                        "VersioningEdges",
                        "migration record for '" + workflowId + "/" + changeId + "' carries an invalid recorded version '"
                                + recordedVersion + "' — migrateVersion must record a valid semver");
            }
            String key = workflowId + "/" + changeId;
            int count = migrationCountByKey.merge(key, 1, Integer::sum);
            orderedMigrationVersionsByWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(recordedVersion);
            // Facet — recorded at most once per (workflowId, changeId): a second marker means replay re-applied it.
            if (count > 1) {
                throw new InvariantViolation(
                        "VersioningEdges",
                        "migration for '" + key + "' was recorded " + count + " times — the recorded migration version "
                                + "must be written at most once per changeId; a replay must re-reach the call as a "
                                + "no-op, never re-apply it");
            }
        }
        // Facet — never 0: a start the caller issued must have produced an instance carrying a resolvable version.
        for (String expected : expectedStartedWorkflowIds) {
            if (versionsByWorkflow.getOrDefault(expected, Set.of()).isEmpty()) {
                throw new InvariantViolation(
                        "VersioningEdges",
                        "expected versioned start '" + expected + "' produced no committed instance carrying a "
                                + "resolvable version — the start event routed to 0 definitions");
            }
        }
        // Facet — monotonic non-decreasing recorded versions (never downgrades) within one instance.
        for (var entry : orderedMigrationVersionsByWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            List<String> ordered = entry.getValue();
            for (int i = 1; i < ordered.size(); i++) {
                if (Version.of(ordered.get(i)).compareTo(Version.of(ordered.get(i - 1))) < 0) {
                    throw new InvariantViolation(
                            "VersioningEdges",
                            "instance '" + workflowId + "' recorded migration version '" + ordered.get(i) + "' after '"
                                    + ordered.get(i - 1) + "' — recorded versions must be monotonic non-decreasing "
                                    + "(first-writer-wins, never downgrades); a downgrade must be rejected, not recorded");
                }
            }
            // No recorded version may drop below the instance's started version (also a downgrade).
            String startedVersion = startedVersionByWorkflow.get(workflowId);
            if (startedVersion != null) {
                for (String recorded : ordered) {
                    if (Version.of(recorded).compareTo(Version.of(startedVersion)) < 0) {
                        throw new InvariantViolation(
                                "VersioningEdges",
                                "instance '" + workflowId + "' recorded migration version '" + recorded + "' below its "
                                        + "started version '" + startedVersion + "' — a downgrade was recorded; it must "
                                        + "have been rejected");
                    }
                }
            }
        }
        // Facet — fresh spawn at the highest registered version, and no event stamped below the started version. Unlike
        // INV-11 (a NON-migrating instance carries exactly one version), this workflow MIGRATES, so its events
        // legitimately carry the started version plus the forward-bumped versions; the routing facet here pins the
        // SPAWN (the STARTED event's version) at the highest registered version, and that no event ever carries a
        // version below the started one (a stamped downgrade). "Never 0" is the presence check above; "never 2" (two
        // definitions concurrently handling one instance) is observed in the scenario by the recovered instance still
        // completing cleanly under exactly the closest sibling the 4/5-pass lookup routed to.
        for (var entry : versionsByWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            if (!startedByWorkflow.getOrDefault(workflowId, false)) {
                continue; // instance never recorded a workflow-status STARTED — nothing to enforce yet.
            }
            String startedVersion = startedVersionByWorkflow.get(workflowId);
            if (requireHighestForFresh && startedVersion != null && !startedVersion.equals(highestRegisteredVersion)) {
                throw new InvariantViolation(
                        "VersioningEdges",
                        "fresh versioned instance '" + workflowId + "' spawned at version '" + startedVersion + "' but a "
                                + "new spawn must use the highest registered version '" + highestRegisteredVersion + "'");
            }
            if (startedVersion != null) {
                for (String carried : entry.getValue()) {
                    if (Version.tryOf(carried).isPresent()
                            && Version.of(carried).compareTo(Version.of(startedVersion)) < 0) {
                        throw new InvariantViolation(
                                "VersioningEdges",
                                "versioned instance '" + workflowId + "' carries a committed event stamped with version '"
                                        + carried + "' below its started version '" + startedVersion + "' — the resolved "
                                        + "version must only move forward (a stamped downgrade is the break)");
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-21: RetryTimingAndExhaustionEdges (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Expected shape of one retrying step, for {@link #assertRetryTimingAndExhaustionEdges}: the configured
     * {@code maxRetries} bound, the exact number of {@code RETRYING} (actual retry-decision) records the step should
     * record, and whether successive {@code RETRYING} backoff gaps must be strictly increasing (linear/exponential) or
     * constant (fixed). The {@code retryWhile}-bounded step carries {@code expectedRetryingRecords < maxRetries} so the
     * predicate-stops-early facet is non-vacuous.
     *
     * @param maxRetries              the configured {@code RetryPolicy.maxRetries(n)} bound (the INV-8 ceiling).
     * @param expectedRetryingRecords the exact number of {@code RETRYING} records the step must record (the actual number
     *                                of retry decisions); {@code <= maxRetries}, and for the {@code retryWhile} step
     *                                strictly {@code < maxRetries}.
     * @param backoffIncreasing       {@code true} for a strategy whose successive backoff gaps grow (linear/exponential),
     *                                {@code false} for a constant gap (fixed) or no backoff.
     */
    public record RetryStepSpec(int maxRetries, int expectedRetryingRecords, boolean backoffIncreasing) {

    }

    /**
     * INV-21 — Retry timing and exhaustion edges (the edges INV-8 {@code RetryBound} and INV-9 {@code TimeoutsFire} did
     * not cover): for a step under {@code RetryPolicy} with a {@code BackoffStrategy}, the retry schedule reconstructed
     * from the committed {@code RETRYING} timestamps follows the strategy (constant gaps for {@code fixed}, increasing for
     * {@code linear}/{@code exponential}) and survives crash/replay; a {@code retryWhile(predicate)} stops retrying when
     * the predicate says so (strictly fewer attempt records than {@code maxRetries + 1}); and every retrying step
     * ultimately reaches a <em>terminal</em> step record (it resolves — COMPLETED or FAILED — never hangs in
     * {@code RETRYING}/{@code STARTED} forever).
     * <p>
     * Asserted <strong>per {@code (workflowId, stepName)}</strong> against the committed event log, content-based (so it
     * is robust to the F-2 intra-instance global-append-order non-determinism — it counts records and reads their
     * committed timestamps, never relying on the absolute global index), for each step whose expected shape is supplied in
     * {@code specsByStep} (steps with no entry are not constrained and are skipped). For each such
     * {@code (workflowId, stepName)} four facets are checked:
     * <ul>
     *   <li><strong>within the INV-8 bound</strong>: the {@code RETRYING} record count is {@code <= maxRetries} (so the
     *       attempt records — one {@code STARTED} + one {@code RETRY_STARTED} per retry — stay
     *       {@code <= maxRetries + 1}, the INV-8 ceiling this invariant builds on);</li>
     *   <li><strong>exact retry-decision count</strong>: the {@code RETRYING} record count equals the spec's
     *       {@code expectedRetryingRecords} — the engine recorded exactly the retries the strategy/predicate dictated,
     *       neither dropping nor inventing one across crash/replay (so the assertion is not trivially self-satisfied);</li>
     *   <li><strong>backoff timing reconstructed from the recorded timestamps</strong>: scanning the step's own
     *       committed {@code STARTED} + {@code RETRYING} + {@code RETRY_STARTED} records in append order, their
     *       timestamps are non-decreasing (the schedule is monotonic in committed time — the engine schedules each retry
     *       from the previously-recorded {@code RETRYING} timestamp, not from in-memory state, so the schedule is a pure
     *       function of the committed log and survives crash/replay); when {@code backoffIncreasing} is set, the backoff
     *       gaps — each {@code RETRYING} to the {@code RETRY_STARTED} that follows it — are non-decreasing (a
     *       {@code linear}/{@code exponential} schedule's growing delay is
     *       visible in the recorded timing). (Timestamps come from the in-memory event store's per-event timestamp, the
     *       same source INV-9 reads; absolute deltas are not asserted because the store stamps from the Axon
     *       {@code GenericEventMessage} static clock — the D5 residual — so only the ordering/monotonicity is cleanly
     *       observable content-based.)</li>
     *   <li><strong>resolves to a terminal step record</strong>: the step has at least one committed terminal record
     *       ({@code COMPLETED}/{@code FAILED}/{@code TIMED_OUT}/{@code CANCELLED}) — a retrying step does not hang
     *       indefinitely in {@code RETRYING}.</li>
     * </ul>
     * Taken per {@code (workflowId, stepName)} (never pooled): the single global log interleaves independent instances
     * non-deterministically (the F-2 surface, ARCHITECTURE.md §8/§11), so another instance's {@code RETRYING} record
     * landing here is irrelevant; one step recording more retries than the bound, the wrong number of retries, an
     * out-of-order schedule, a shrinking {@code linear}/{@code exponential} gap, or never resolving is the break. A
     * genuine break throws {@link InvariantViolation}; that would be a new finding (the engine recorded a retry schedule
     * that does not match the configured strategy/predicate, or a retrying step that hangs), to be triaged per the POC
     * rules — not silently tolerated.
     *
     * @param committedLog the committed workflow event log (oldest first).
     * @param idPrefix     the id prefix of the retry-edges workflow's instances (e.g. {@code retryedge-}); only instances
     *                     whose id starts with it are checked.
     * @param specsByStep  the expected shape per retrying step name (e.g. {@code fixedBackoffCall -> RetryStepSpec(2, 2,
     *                     false)}); steps absent from the map are skipped.
     */
    public static void assertRetryTimingAndExhaustionEdges(List<EventMessage> committedLog,
                                                           String idPrefix,
                                                           Map<String, RetryStepSpec> specsByStep) {
        // Per (workflowId, stepName): ordered list of (status, timestamp) for the step's STARTED/RETRYING attempt records
        // (in committed/append order) plus whether a terminal record was seen.
        record Attempt(StepStatus status, java.time.Instant timestamp) {

        }
        var attemptsByKey = new LinkedHashMap<String, List<Attempt>>();
        var terminalSeenByKey = new HashSet<String>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(idPrefix)) {
                continue; // not a retry-edges instance — INV-21 does not constrain it.
            }
            var stepStatus = MetadataUtils.getStepStatus(event.metadata());
            if (stepStatus.isEmpty()) {
                continue; // workflow-status event — not a step record.
            }
            String stepName = MetadataUtils.getStepName(event.metadata());
            if (!specsByStep.containsKey(stepName)) {
                continue; // step not under an INV-21 retry spec — skip.
            }
            String key = workflowId + "/" + stepName;
            if (stepStatus.get().isTerminal()) {
                terminalSeenByKey.add(key);
            } else {
                attemptsByKey.computeIfAbsent(key, k -> new ArrayList<>())
                             .add(new Attempt(stepStatus.get(), event.timestamp()));
            }
        }
        for (var entry : attemptsByKey.entrySet()) {
            String key = entry.getKey();
            String stepName = key.substring(key.indexOf('/') + 1);
            RetryStepSpec spec = specsByStep.get(stepName);
            List<Attempt> attempts = entry.getValue();
            long retrying = attempts.stream().filter(a -> a.status() == StepStatus.RETRYING).count();

            // Facet 1 — within the INV-8 bound: at most maxRetries RETRYING records (so attempts <= maxRetries + 1).
            if (retrying > spec.maxRetries()) {
                throw new InvariantViolation(
                        "RetryTimingAndExhaustionEdges",
                        "step '" + key + "' recorded " + retrying + " RETRYING records but its policy allows at most "
                                + "maxRetries = " + spec.maxRetries() + " (attempt records would exceed maxRetries+1) — "
                                + "the engine recorded more retries than the policy permits");
            }
            // Facet 2 — exact retry-decision count (strategy/predicate-dictated; non-vacuous).
            if (retrying != spec.expectedRetryingRecords()) {
                throw new InvariantViolation(
                        "RetryTimingAndExhaustionEdges",
                        "step '" + key + "' recorded " + retrying + " RETRYING records but the configured "
                                + "strategy/predicate dictates exactly " + spec.expectedRetryingRecords() + " retry "
                                + "decision(s) — the engine recorded the wrong number of retries");
            }
            // Facet 3 — backoff schedule reconstructed from recorded timestamps: non-decreasing in committed time, and
            // (for linear/exponential) non-decreasing backoff gaps, each RETRYING to the RETRY_STARTED that follows it.
            for (int i = 1; i < attempts.size(); i++) {
                if (attempts.get(i).timestamp().isBefore(attempts.get(i - 1).timestamp())) {
                    throw new InvariantViolation(
                            "RetryTimingAndExhaustionEdges",
                            "step '" + key + "' recorded attempt " + i + " at " + attempts.get(i).timestamp()
                                    + " before the previous attempt at " + attempts.get(i - 1).timestamp() + " — the "
                                    + "retry schedule (reconstructed from the recorded RETRYING timestamps) is not "
                                    + "monotonic in committed time");
                }
            }
            if (spec.backoffIncreasing()) {
                var backoffGaps = new ArrayList<java.time.Duration>();
                for (int i = 1; i < attempts.size(); i++) {
                    if (attempts.get(i - 1).status() == StepStatus.RETRYING
                            && attempts.get(i).status() == StepStatus.RETRY_STARTED) {
                        backoffGaps.add(java.time.Duration.between(attempts.get(i - 1).timestamp(),
                                                                   attempts.get(i).timestamp()));
                    }
                }
                // The committed timestamps come from the in-memory event store's static WALL clock (the Phase-3 D5
                // residual), NOT from the injected virtual backoff scheduler — so adjacent gaps reflect wall-clock
                // processing jitter (microseconds), not the configured linear/exponential delay. Asserting strict gap
                // monotonicity on them is therefore both meaningless and flaky (a sub-millisecond inversion under CPU
                // contention is just scheduling noise, not a real schedule regression). We flag only a GROSS inversion
                // — a shrink larger than this jitter tolerance — which still catches a genuinely reversed schedule (the
                // hand-built pin shrinks by >> the tolerance) while tolerating real-log wall-clock noise.
                var jitterTolerance = java.time.Duration.ofMillis(250);
                for (int i = 1; i < backoffGaps.size(); i++) {
                    var gap = backoffGaps.get(i);
                    var prevGap = backoffGaps.get(i - 1);
                    if (gap.compareTo(prevGap.minus(jitterTolerance)) < 0) {
                        throw new InvariantViolation(
                                "RetryTimingAndExhaustionEdges",
                                "step '" + key + "' has a shrinking backoff gap (" + prevGap + " then " + gap + ", a "
                                        + "drop beyond the " + jitterTolerance + " wall-clock jitter tolerance) but a "
                                        + "linear/exponential strategy must produce non-decreasing gaps between each "
                                        + "RETRYING and the RETRY_STARTED that follows it");
                    }
                }
            }
            // Facet 4 — resolves to a terminal step record (never hangs in RETRYING/STARTED).
            if (!terminalSeenByKey.contains(key)) {
                throw new InvariantViolation(
                        "RetryTimingAndExhaustionEdges",
                        "step '" + key + "' recorded " + attempts.size() + " attempt record(s) but no terminal step "
                                + "record — a retrying step must ultimately resolve (COMPLETED/FAILED/TIMED_OUT), never "
                                + "hang in RETRYING");
            }
        }
    }

    /**
     * INV-21 ({@code onRetry} no-re-fire facet — the headline F-0 analogue): a {@code RetryPolicy.onRetry(...)} handler
     * side effect fires <strong>exactly once per actual retry decision</strong> and is <strong>not</strong> re-invoked
     * when a crash + replay re-reaches the already-recorded {@code RETRYING} steps. This is the retry-handler twin of
     * INV-6 ({@code EffectAtMostOnce}/F-0, {@code execute} actions) and INV-17 ({@code StatusHookFiresOncePerStatus},
     * status hooks): a retry handler is a user side effect, and the engine resumes a {@code RETRYING} step from its
     * persisted state without re-running {@code onRetry} ({@code RetryableExecuteDelegate.handleAttemptFailure} calls
     * {@code onRetry} only on the live failure path, never on the crash-recovery resume path; the {@code RetryHandler}
     * Javadoc states it is "Not called during replay").
     * <p>
     * Asserted <strong>per {@code (workflowId, stepName)}</strong>: the observed {@code onRetry} fire count (from the
     * crash-surviving {@code OnRetryFires} counter, kept across the simulated crash exactly like {@code CountingEffects})
     * must equal {@code expectedRetries} — the number of {@code RETRYING} records the engine actually committed for that
     * step. A count <em>greater</em> than the actual retries is the break (the F-0-analogue re-fire: {@code onRetry} ran
     * again on replay); a count <em>less</em> than the actual retries is also flagged (the handler was dropped, which
     * would be a separate finding). A genuine break throws {@link InvariantViolation}; an {@code onRetry} re-firing on
     * replay would be a real finding (the F-0-analogue for retry handlers), to be triaged per the POC rules — not silently
     * tolerated.
     *
     * @param workflowId      the workflow instance.
     * @param stepName        the retrying step whose {@code onRetry} fire count is checked.
     * @param observedFires   how many times the step's {@code onRetry} handler actually fired (crash-surviving counter).
     * @param expectedRetries the number of {@code RETRYING} records the engine committed for the step (the actual number
     *                        of retry decisions) — the count {@code onRetry} must have fired exactly.
     */
    public static void assertOnRetryFiredOncePerRetry(String workflowId, String stepName,
                                                      int observedFires, int expectedRetries) {
        if (observedFires > expectedRetries) {
            throw new InvariantViolation(
                    "RetryTimingAndExhaustionEdges",
                    "onRetry handler for '" + workflowId + "/" + stepName + "' fired " + observedFires + " times but the "
                            + "engine committed only " + expectedRetries + " RETRYING record(s) — onRetry RE-FIRED on "
                            + "replay (the F-0 analogue for retry handlers: a retry-handler side effect must run once per "
                            + "actual retry decision and NOT be re-invoked when a crash/replay re-reaches the recorded "
                            + "RETRYING steps)");
        }
        if (observedFires < expectedRetries) {
            throw new InvariantViolation(
                    "RetryTimingAndExhaustionEdges",
                    "onRetry handler for '" + workflowId + "/" + stepName + "' fired " + observedFires + " times but the "
                            + "engine committed " + expectedRetries + " RETRYING record(s) — onRetry fired fewer times "
                            + "than there were actual retry decisions (a dropped retry-handler fire)");
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-22: EventNameCustomizationSound (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-22 — Event-name customization is sound: a workflow registered with a custom {@code eventNameCustomizer}
     * records its step/status events under the CUSTOMIZED wire names, those names are stable across crash/replay (a pure
     * function of history), and the engine still routes/replays correctly under customization.
     * <p>
     * Asserted <strong>per {@code workflowId}</strong> against the committed event log, scoped to the custom-named
     * workflow's instances (those whose id starts with {@code namedIdPrefix}; other instances are not constrained and
     * are skipped). The observable is each committed event's {@code MessageType.qualifiedName()} — the customized wire
     * name the engine stamped on it ({@code EventMessageUtils} calls {@code customizer.getEventName(...)} and builds the
     * {@code GenericEventMessage}'s {@code MessageType} from the resulting {@code QualifiedName}). For a
     * {@link CustomNamedWorkflow} registered with a {@code DefaultEventNameCustomizer} (custom namespace +
     * workflowBaseName + a {@code stepCompleted} status-suffix override), every committed step/status event of the
     * instance must carry the EXPECTED customized name/type (namespace + base + step + suffix per the customizer's
     * rules); a step event is named {@code capitalize(stepName)} + the status suffix (no step {@code baseName} is set), a
     * workflow-status event is named {@code capitalize(workflowBaseName)} + the workflow-status suffix, and every event's
     * namespace is the custom namespace. Three facets are checked:
     * <ul>
     *   <li><strong>no dropped start (never 0)</strong>: every id in {@code expectedStartedWorkflowIds} (the ids the
     *       harness knows it started) must appear in the committed log with at least one event — a start that produced no
     *       instance routed to NONE under customization (a routing failure). (Pass an empty set for the per-step
     *       always-on call, where the instance may not have started yet.)</li>
     *   <li><strong>customized name applied + correct</strong>: for every {@code named-} instance present in the log,
     *       each of its committed step/status events carries EXACTLY the customizer-derived expected
     *       {@code QualifiedName} (namespace + local name). A non-empty but UN-customized name (the default namespace or
     *       a default {@code Completed} suffix where the override dictates {@code Done}), a missing suffix, or any
     *       mismatch is the break — this makes the assertion non-vacuous (it equals derived names, not merely
     *       "non-empty").</li>
     *   <li><strong>reached terminal under customization</strong>: an instance present in {@code expectedStartedWorkflowIds}
     *       must have recorded a terminal workflow status — under customization the engine still completes the workflow
     *       (a never-terminal instance would be a routing/replay regression caught by INV-5, surfaced here too).</li>
     * </ul>
     * The <strong>stable-across-replay</strong> facet is enforced jointly with INV-4 ({@code DeterministicReplay}):
     * because each customized name lives on the instance's committed subsequence, and that subsequence is a pure function
     * of the instance's history (the per-{@code workflowId} prefix-stability check in {@code DstSimulation} and the
     * run-level {@link #assertDeterministicReplay}), replaying the same history reproduces the IDENTICAL customized names
     * — a present step yields a cached result and emits nothing, so the recorded names are unchanged. Taken per
     * {@code workflowId} (never pooled): the single global log interleaves independent instances non-deterministically
     * (the F-2 surface, ARCHITECTURE.md §8/§11), so another instance's (differently-named) event landing here is
     * irrelevant; one instance carrying a wrong/un-customized name is the break. A genuine break (a customized name NOT
     * applied, a replay producing a DIFFERENT name, or a customized instance failing to route/terminate) throws
     * {@link InvariantViolation}; that would be a high-value finding (the customizer not honoured, or replay diverging
     * under customization), to be triaged per the POC rules — not silently tolerated.
     *
     * @param committedLog               the committed workflow event log (oldest first).
     * @param namedIdPrefix              the id prefix of the custom-named workflow's instances (e.g. {@code named-});
     *                                   only instances whose id starts with it are checked.
     * @param expectedStartedWorkflowIds custom-named ids the caller knows it started; each must be present in the log
     *                                   (else routed to none) and must have reached a terminal status. Empty for the
     *                                   per-step always-on call (no presence required).
     */
    public static void assertEventNameCustomizationSound(List<EventMessage> committedLog,
                                                         String namedIdPrefix,
                                                         Set<String> expectedStartedWorkflowIds) {
        var presentIds = new HashSet<String>();
        var terminalIds = new HashSet<String>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(namedIdPrefix)) {
                continue; // not a custom-named instance — INV-22 does not constrain it.
            }
            presentIds.add(workflowId);
            var workflowStatus = MetadataUtils.getWorkflowStatus(event.metadata());
            if (workflowStatus.isPresent() && workflowStatus.get().isTerminal()) {
                terminalIds.add(workflowId);
            }
            // Facet 2 — the customized name applied to THIS event must equal the customizer-derived expected name.
            String expectedLocalName = expectedLocalNameFor(event);
            if (expectedLocalName == null) {
                continue; // not a step/status event with a name we derive (e.g. a non-customized infra event) — skip.
            }
            var qualifiedName = event.type().qualifiedName();
            if (!CustomNamedWorkflow.NAMESPACE.equals(qualifiedName.namespace())) {
                throw new InvariantViolation(
                        "EventNameCustomizationSound",
                        "custom-named instance '" + workflowId + "' event " + describe(event) + " carries namespace '"
                                + qualifiedName.namespace() + "' but the registered eventNameCustomizer dictates the "
                                + "custom namespace '" + CustomNamedWorkflow.NAMESPACE + "' — the customized name was not "
                                + "applied (an un-customized / default namespace)");
            }
            if (!expectedLocalName.equals(qualifiedName.localName())) {
                throw new InvariantViolation(
                        "EventNameCustomizationSound",
                        "custom-named instance '" + workflowId + "' event " + describe(event) + " carries customized "
                                + "local name '" + qualifiedName.localName() + "' but the registered eventNameCustomizer "
                                + "dictates '" + expectedLocalName + "' (namespace + base + step + status suffix) — the "
                                + "customized name was not applied as the customizer's rules require");
            }
        }
        // Facet 1 — never routed to none: a start the harness issued must have produced a custom-named instance.
        for (String expected : expectedStartedWorkflowIds) {
            if (!presentIds.contains(expected)) {
                throw new InvariantViolation(
                        "EventNameCustomizationSound",
                        "expected custom-named start '" + expected + "' produced no committed instance — the start event "
                                + "routed to NO definition under event-name customization");
            }
            // Facet 3 — reached terminal under customization.
            if (!terminalIds.contains(expected)) {
                throw new InvariantViolation(
                        "EventNameCustomizationSound",
                        "custom-named instance '" + expected + "' never recorded a terminal workflow status — the engine "
                                + "must still route/complete the workflow under event-name customization");
            }
        }
    }

    /**
     * Derives the EXPECTED customized {@code QualifiedName.localName()} of a committed event of the
     * {@link CustomNamedWorkflow}, from the customizer's deterministic rules. Returns {@code null} for an event whose
     * status is neither a recognised step status nor a recognised workflow status (so the caller skips it rather than
     * over-constraining an event the workflow does not produce).
     */
    @org.jspecify.annotations.Nullable
    private static String expectedLocalNameFor(EventMessage event) {
        var stepStatus = MetadataUtils.getStepStatus(event.metadata());
        if (stepStatus.isPresent()) {
            String suffix = stepSuffixFor(stepStatus.get());
            if (suffix == null) {
                return null;
            }
            return CustomNamedWorkflow.expectedStepLocalName(MetadataUtils.getStepName(event.metadata()), suffix);
        }
        var workflowStatus = MetadataUtils.getWorkflowStatus(event.metadata());
        if (workflowStatus.isPresent()) {
            String suffix = workflowSuffixFor(workflowStatus.get());
            if (suffix == null) {
                return null;
            }
            return CustomNamedWorkflow.expectedWorkflowLocalName(suffix);
        }
        return null;
    }

    /**
     * The customizer's step-status suffix for the statuses {@link CustomNamedWorkflow} actually produces (STARTED and
     * the overridden COMPLETED). Other statuses are not exercised by this execute-only workflow and return {@code null}.
     */
    @org.jspecify.annotations.Nullable
    private static String stepSuffixFor(StepStatus status) {
        return switch (status) {
            case STARTED -> CustomNamedWorkflow.STEP_STARTED_SUFFIX;
            case COMPLETED -> CustomNamedWorkflow.STEP_COMPLETED_SUFFIX;
            default -> null;
        };
    }

    /**
     * The customizer's workflow-status suffix for the statuses {@link CustomNamedWorkflow} actually produces (STARTED
     * and COMPLETED). Other statuses are not exercised by this workflow and return {@code null}.
     */
    @org.jspecify.annotations.Nullable
    private static String workflowSuffixFor(WorkflowStatus status) {
        return switch (status) {
            case STARTED -> CustomNamedWorkflow.WORKFLOW_STARTED_SUFFIX;
            case COMPLETED -> CustomNamedWorkflow.WORKFLOW_COMPLETED_SUFFIX;
            default -> null;
        };
    }

    // ----------------------------------------------------------------------------------------------------------------
    // INV-23: EngineSelfProtection (Safety)
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * INV-23 — Engine self-protection: when code abuses the engine at one of its two known self-protection surfaces
     * (a §3.1-forbidden nested primitive, or driving the per-instance task queue past its bound), the engine must not
     * silently CORRUPT or TEAR an instance's committed history; the worst it does is stop making progress (a non-terminal
     * stall the harness's bounded deadline catches) or throw a clean, surfaced exception.
     * <p>
     * This asserts the <strong>no-corruption</strong> safety property at the <em>nested-primitive</em> surface, per
     * {@code workflowId} (scoped to {@code selfprotIdPrefix}; other instances are skipped), content-based so it is robust
     * to the F-2 intra-instance global-append-order non-determinism (ARCHITECTURE.md §8/§11). For every probed instance
     * its committed subsequence must be either:
     * <ul>
     *   <li><strong>(a) well-formed-complete</strong> — it recorded a terminal workflow status, and its step records are
     *       well-formed: at most one terminal record per {@code (workflowId, stepName)} (the INV-2 shape), and no
     *       {@code COMPLETED}/{@code FAILED}/{@code TIMED_OUT}/{@code CANCELLED} step record whose own {@code STARTED} is
     *       absent (no orphan terminal record); <em>or</em></li>
     *   <li><strong>(b) clean non-terminal prefix</strong> — it recorded NO terminal workflow status (the instance is
     *       still LIVE / stuck — e.g. the nested-primitive deadlock under a single-threaded body executor), with the same
     *       no-torn-record shape: no duplicate terminal record, no orphan terminal step record.</li>
     * </ul>
     * A <em>corrupt</em> history — a torn record (a terminal step record with no {@code STARTED}), a duplicate terminal
     * record for one step, or a workflow-status event recorded twice as terminal — is the break ({@link InvariantViolation}).
     * Taken per {@code workflowId} (never pooled): the single global log interleaves independent instances
     * non-deterministically (the F-2 surface), so another instance's events landing here are irrelevant; one probed
     * instance carrying a corrupt record is the break.
     * <p>
     * The complementary <em>task-queue overflow</em> surface — {@code appendTask} throws a clean
     * {@code RuntimeException("Too many tasks to perform workflow instance")} on a full {@code ArrayBlockingQueue<>(1000)}
     * <em>before</em> publishing anything, leaving the committed log untouched — is exercised directly against the real
     * {@code SimpleWorkflowExecution.appendTask} in {@code Inv23EngineSelfProtectionTest} (the harness cannot practically
     * enqueue 1000+ pending tasks at one self-completing instance); this assertion's no-corruption shape is what such an
     * overflow's committed log must still satisfy.
     *
     * @param committedLog       the committed workflow event log (oldest first).
     * @param selfprotIdPrefix   the id prefix of the self-protection probe's instances (e.g. {@code selfprot-}); only
     *                           instances whose id starts with it are checked.
     */
    public static void assertEngineSelfProtection(List<EventMessage> committedLog,
                                                  String selfprotIdPrefix) {
        var byWorkflow = new LinkedHashMap<String, List<EventMessage>>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(selfprotIdPrefix)) {
                continue; // not a self-protection probe instance — INV-23 does not constrain it.
            }
            byWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>()).add(event);
        }
        for (var entry : byWorkflow.entrySet()) {
            String workflowId = entry.getKey();
            List<EventMessage> events = entry.getValue();
            // Per-step bookkeeping: did this step record a STARTED, and how many terminal records did it accrue.
            var startedSteps = new HashSet<String>();
            var terminalStepCounts = new LinkedHashMap<String, Integer>();
            int terminalWorkflowStatusCount = 0;
            for (EventMessage event : events) {
                var stepStatus = MetadataUtils.getStepStatus(event.metadata());
                if (stepStatus.isPresent()) {
                    String stepName = MetadataUtils.getStepName(event.metadata());
                    if (stepStatus.get() == StepStatus.STARTED) {
                        startedSteps.add(stepName);
                    }
                    if (stepStatus.get().isTerminal()) {
                        terminalStepCounts.merge(stepName, 1, Integer::sum);
                    }
                    continue;
                }
                var workflowStatus = MetadataUtils.getWorkflowStatus(event.metadata());
                if (workflowStatus.isPresent() && workflowStatus.get().isTerminal()) {
                    terminalWorkflowStatusCount++;
                }
            }
            // No torn / orphan terminal step record: a terminal step record must have a matching STARTED. (RETRYING and
            // STARTED produce the STARTED; a terminal record with no STARTED at all is a torn/corrupt half-write.)
            for (var stepEntry : terminalStepCounts.entrySet()) {
                if (!startedSteps.contains(stepEntry.getKey())) {
                    throw new InvariantViolation(
                            "EngineSelfProtection",
                            "self-protection probe instance '" + workflowId + "' has a terminal record for step '"
                                    + stepEntry.getKey() + "' with NO committed STARTED — a torn/orphan (corrupt) record; "
                                    + "the engine must fail cleanly or stall, never half-write committed history");
                }
                // No duplicate terminal record for one step (the INV-2 shape; restated here so an overflow/abuse path
                // cannot smuggle in a duplicate terminal that corrupts the instance's history).
                if (stepEntry.getValue() > 1) {
                    throw new InvariantViolation(
                            "EngineSelfProtection",
                            "self-protection probe instance '" + workflowId + "' recorded " + stepEntry.getValue()
                                    + " terminal records for step '" + stepEntry.getKey() + "' — a duplicate-terminal "
                                    + "(corrupt) record; the engine must not corrupt committed history under abuse");
                }
            }
            // At most one terminal workflow status (a second would be a corrupt/duplicate lifecycle terminus).
            if (terminalWorkflowStatusCount > 1) {
                throw new InvariantViolation(
                        "EngineSelfProtection",
                        "self-protection probe instance '" + workflowId + "' recorded " + terminalWorkflowStatusCount
                                + " terminal workflow-status events — a duplicate terminal lifecycle (corrupt); the engine "
                                + "must not corrupt committed history under abuse");
            }
            // Otherwise the instance is either well-formed-complete (a terminal workflow status, clean records) or a clean
            // non-terminal prefix (stuck, e.g. the nested-primitive deadlock) — both acceptable for INV-23. No corruption.
        }
    }

    /**
     * Documents the candidate robustness gap <strong>F-5</strong>: the engine has <em>no up-front guard</em> against the
     * {@code axon-flow-workflow} skill §3.1-forbidden nested primitive (a primitive called from inside another
     * primitive's action lambda) — there is no re-entrancy check on {@code appendTask}. Whether the nested call silently
     * COMPLETES (under the default virtual-thread body executor, where the inner action gets its own thread) or silently
     * DEADLOCKS the per-instance task queue (under a single-threaded body executor, where the inner task can never be
     * consumed) is decided entirely by the body executor's threading model — the engine <strong>never</strong> surfaces
     * the misuse as a clear up-front error. Captured the same way {@link #documentEffectAtMostOnceGap} captures F-0: a
     * passing check that records the observed outcome rather than breaking the build, so a future guard (a re-entrancy
     * check that rejects the nested call cleanly) would flip the documented expectation.
     * <p>
     * Asserts only that the observed outcome was one of the two documented self-protection behaviours — silent-complete
     * or silent-deadlock — and was <strong>not</strong> a clean rejection (which the engine does not do today). If the
     * engine ever DID reject the nested primitive with a clear error (i.e. neither completed nor deadlocked, but failed
     * observably), that would be the gap closing — flagged here so it is noticed, not silently tolerated.
     *
     * @param reachedTerminal whether the nested-primitive instance reached a terminal workflow status (the
     *                        silent-complete outcome under the default executor).
     * @param deadlocked      whether the nested-primitive instance stalled non-terminally with the queue deadlocked (the
     *                        silent-deadlock outcome under a single-threaded executor).
     */
    public static void documentNestedPrimitiveNotGuarded(boolean reachedTerminal, boolean deadlocked) {
        if (!reachedTerminal && !deadlocked) {
            throw new InvariantViolation(
                    "EngineSelfProtection",
                    "the nested-primitive probe neither completed nor deadlocked — the engine appears to have gained an "
                            + "up-front guard that REJECTS the §3.1-forbidden nested primitive (the F-5 candidate gap "
                            + "closing). Update INV-23 + the F-5 finding: the engine now self-protects against nested "
                            + "primitives with a clear error rather than a silent deadlock/silent-success");
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Returns the set of distinct workflow ids present in the committed log, for diagnostics.
     *
     * @param committedLog the committed workflow event log.
     * @return distinct workflow ids.
     */
        public static Set<String> workflowIdsIn(List<EventMessage> committedLog) {
        Set<String> ids = new HashSet<>();
        for (EventMessage event : committedLog) {
            ids.add(MetadataUtils.getWorkflowId(event.metadata()));
        }
        return ids;
    }

        private static String describe(EventMessage event) {
        return describe(event.metadata());
    }

        private static String describe(org.axonframework.messaging.core.Metadata metadata) {
        var workflowId = MetadataUtils.getWorkflowId(metadata);
        var status = MetadataUtils.getStepStatus(metadata).map(Enum::name)
                                  .or(() -> MetadataUtils.getWorkflowStatus(metadata).map(Enum::name))
                                  .orElse("-");
        var stepName = MetadataUtils.getStepStatus(metadata).isPresent()
                ? MetadataUtils.getStepName(metadata)
                : "<workflow>";
        return workflowId + ":" + stepName + ":" + status;
    }

    /**
     * INV-29 ({@code NoForeignStepRecorded}): <em>An instance evolved from a published event never registers the
     * publisher's step as its own.</em>
     * <p>
     * A published event (ADR-019) carries the publisher's step metadata ({@code workflowId}, {@code stepName},
     * {@code stepType=COMPLETED}, {@code stepPrimitive=PUBLISH}) and is broadcast to every instance the engine owns —
     * the instances it starts, the instances whose wait it matches, and every other live instance. Each of them evolves
     * its state from it. The engine's state projection must treat the event as a business event for every instance but
     * the publisher: a publish step name showing up in the step list of any other instance is the break. Asserted on
     * the engine's own reconstructed state per {@code workflowId} (the history read-model's
     * {@code state().workflowStepNames()}), never on the log — the log holds the event exactly once, under the
     * publisher's id, whatever the projection did with it.
     * <p>
     * The publish step names are unique across the chain's bodies ({@link PublishChainWorkflow}), so the check needs no
     * knowledge of who published what: a {@code publishRequest} step belongs to a requester ({@code pubreq-}) and a
     * {@code publishReply} step belongs to a responder ({@code pubresp-}); anywhere else it is foreign.
     *
     * @param stepNamesByWorkflowId the engine's reconstructed step names per {@code workflowId}.
     */
    public static void assertNoForeignStepRecorded(Map<String, List<String>> stepNamesByWorkflowId) {
        Map<String, String> publishStepOwnerPrefix = Map.of(
                PublishChainWorkflow.STEP_PUBLISH_REQUEST, PublishChainWorkflow.REQUESTER_ID_PREFIX,
                PublishChainWorkflow.STEP_PUBLISH_REPLY, PublishChainWorkflow.RESPONDER_ID_PREFIX);
        for (var entry : stepNamesByWorkflowId.entrySet()) {
            String workflowId = entry.getKey();
            for (var publishStep : publishStepOwnerPrefix.entrySet()) {
                if (entry.getValue().contains(publishStep.getKey())
                        && !workflowId.startsWith(publishStep.getValue())) {
                    throw new InvariantViolation(
                            "NoForeignStepRecorded",
                            "An instance evolved from a published event never registers the publisher\'s step as its "
                                    + "own: instance " + workflowId + " holds step \'" + publishStep.getKey()
                                    + "\', which only a " + publishStep.getValue()
                                    + "* instance publishes. Steps: " + entry.getValue());
                }
            }
        }
    }

    /**
     * INV-30 ({@code PublisherObservesOwnPublish}): <em>Once the publisher's segment has processed the published event,
     * the publisher's state holds the step — exactly one durable record, under the event's own type.</em>
     * <p>
     * Three facets, all per publisher {@code workflowId} (instances whose id starts with {@code publisherIdPrefix}):
     * <ul>
     *   <li><strong>one record</strong>: the publish step has at most one COMPLETED record in the committed log
     *       (distinct event identifiers, so a duplicated commit of the same event is not a second publish), and a
     *       publisher whose state holds the step has exactly one;</li>
     *   <li><strong>the event is the event</strong>: that record's {@link QualifiedName} is the business event's own
     *       type ({@code expectedEventType}), never an engine-derived step name, and it carries the
     *       {@code stepPrimitive=PUBLISH} marker;</li>
     *   <li><strong>the publisher sees it</strong>: a publisher whose committed log holds the record and which is
     *       terminal has the step in its reconstructed state — the broadcast reached its own segment (under a single
     *       segment, the engine delivered the event back to its own execution). A non-terminal publisher is not
     *       decided: its state may lawfully lag the log.</li>
     * </ul>
     *
     * @param committedLog          the committed workflow event log (oldest first).
     * @param publisherIdPrefix     id prefix of the publishing workflow's instances.
     * @param publishStepName       the publish step name of that workflow.
     * @param expectedEventType     the qualified name of the published business event.
     * @param stepNamesByWorkflowId the engine's reconstructed step names per {@code workflowId}.
     */
    public static void assertPublisherObservesOwnPublish(List<EventMessage> committedLog,
                                                         String publisherIdPrefix,
                                                         String publishStepName,
                                                         QualifiedName expectedEventType,
                                                         Map<String, List<String>> stepNamesByWorkflowId) {
        var recordsByPublisher = new LinkedHashMap<String, Set<String>>();
        var terminalPublishers = new HashSet<String>();
        for (EventMessage event : committedLog) {
            String workflowId = MetadataUtils.getWorkflowId(event.metadata());
            if (!workflowId.startsWith(publisherIdPrefix)) {
                continue;
            }
            recordsByPublisher.computeIfAbsent(workflowId, k -> new HashSet<>());
            if (MetadataUtils.getWorkflowStatus(event.metadata()).map(WorkflowStatus::isTerminal).orElse(false)) {
                terminalPublishers.add(workflowId);
            }
            var status = MetadataUtils.getStepStatus(event.metadata());
            if (status.isPresent() && status.get() == StepStatus.COMPLETED
                    && publishStepName.equals(MetadataUtils.getStepName(event.metadata()))) {
                recordsByPublisher.get(workflowId).add(event.identifier());
                if (!expectedEventType.equals(event.type().qualifiedName())) {
                    throw new InvariantViolation(
                            "PublisherObservesOwnPublish",
                            "The published event is the event: publisher " + workflowId + " recorded step \'"
                                    + publishStepName + "\' under type " + event.type().qualifiedName()
                                    + " instead of the business event\'s own type " + expectedEventType);
                }
                if (!MetadataUtils.isPublishStep(event.metadata())) {
                    throw new InvariantViolation(
                            "PublisherObservesOwnPublish",
                            "The published event of " + workflowId + " must carry the stepPrimitive=PUBLISH marker "
                                    + "(without it the engine routes it to the publisher\'s segment only). Metadata: "
                                    + event.metadata());
                }
            }
        }
        for (var entry : recordsByPublisher.entrySet()) {
            String workflowId = entry.getKey();
            int records = entry.getValue().size();
            if (records > 1) {
                throw new InvariantViolation(
                        "PublisherObservesOwnPublish",
                        "Exactly one durable record per publish: publisher " + workflowId + " committed " + records
                                + " distinct \'" + publishStepName + "\' events");
            }
            var steps = stepNamesByWorkflowId.getOrDefault(workflowId, List.of());
            if (terminalPublishers.contains(workflowId) && records == 1 && !steps.contains(publishStepName)) {
                throw new InvariantViolation(
                        "PublisherObservesOwnPublish",
                        "Once the publisher\'s segment has processed the published event, the publisher\'s state holds "
                                + "the step: terminal publisher " + workflowId + " committed \'" + publishStepName
                                + "\' but its reconstructed state lacks it. Steps: " + steps);
            }
            if (steps.contains(publishStepName) && records == 0) {
                throw new InvariantViolation(
                        "PublisherObservesOwnPublish",
                        "Publisher " + workflowId + " holds step \'" + publishStepName
                                + "\' in its state without a durable record of the published event");
            }
        }
    }
}
