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
package io.axoniq.framework.workflow.simulation.scenarios;

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.HookRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.StatusHookFires;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Deterministic scenario for INVARIANTS.md INV-17 ({@code StatusHookFiresOncePerStatus}): a registered workflow
 * status-change hook fires <strong>at most once</strong> per status for an instance and is <strong>NOT re-fired on a
 * crash + replay</strong> — the lifecycle-hook analogue of INV-6 ({@code EffectAtMostOnce}/F-0).
 * <p>
 * It drives {@link io.axoniq.framework.workflow.simulation.workflow.HookWorkflow} (runs one recorded step then completes, reaching
 * STARTED then a terminal COMPLETED workflow status) with a counting
 * {@link io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener status-change listener} registered
 * on STARTED and COMPLETED via the {@code .customized(...)} path (see {@link EngineInstance#hookWorkflow}). The
 * listener records each fire into a crash-surviving {@link StatusHookFires} counter (the lifecycle-hook analogue of the
 * {@link CountingEffects} effect counter that makes F-0 observable) — kept across {@code crashAndRecover()} exactly like
 * a real external hook effect the engine cannot roll back, so a replay that re-fired a hook would push a status's count
 * above one.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs {@code doWork} then completes — the committed log gets the workflow
 *       STARTED status, the step's STARTED/COMPLETED, and the terminal COMPLETED workflow status; the STARTED hook fires;</li>
 *   <li>wait for the COMPLETED workflow-status event to commit, then give the COMPLETED hook a bounded window to fire,
 *       and snapshot the per-status fire counts;</li>
 *   <li>crash + recover (drives the real replay path), <strong>twice</strong>, giving each recovery a bounded window to
 *       (incorrectly) re-fire a listener while it re-evolves the committed STARTED/COMPLETED status events;</li>
 *   <li>assert {@link Invariants#assertStatusHookFiresOncePerStatus} holds (no status fired &gt;1, including across the
 *       crashes — the no-re-fire guarantee), the STARTED hook fired exactly once
 *       ({@link Invariants#assertStartedHookFiredExactlyOnce}), and — now that <strong>F-4 is FIXED</strong> — the
 *       terminal COMPLETED hook fired exactly once ({@link Invariants#assertCompletedHookFiredExactlyOnce}). A dropped
 *       COMPLETED hook is now a FAILURE, not a documented observation; {@link Invariants#documentTerminalHookMayBeDropped}
 *       is retained as the drop detector / re-fire guard and post-fix always reports {@code false}.</li>
 * </ol>
 * <strong>INV-17 is DST-only</strong>: lifecycle hooks are outside the leasing/crash-recovery TLA+ model scope (like
 * INV-7..16). The COMPLETED-hook drop was a non-deterministic race on the happy path (the body runs on the
 * virtual-thread executor, the documented determinism residual); <strong>F-4 is now FIXED</strong> — the engine's happy
 * completion path {@code awaitStateChange}s on the terminal status before {@code finishWorkflow} clears the per-instance
 * task queue (symmetric with the awaited fail/cancel/timeout paths), so the COMPLETED hook now fires reliably exactly
 * once and the scenario asserts that fixed count. The at-most-once / no-re-fire facet remains deterministic and stable.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class StatusHookFiresOncePerStatusScenario {

    private StatusHookFiresOncePerStatusScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal           whether the instance recorded a terminal COMPLETED workflow status.
     * @param startedFiresAtTerminal    STARTED-hook fire count once the instance first reached terminal (INV-17 reliable
     *                                  at-least-once facet requires {@code 1}).
     * @param completedFiresAtTerminal  COMPLETED-hook fire count once the instance first reached terminal — now exactly
     *                                  {@code 1} (F-4 FIXED: the happy completion path awaits the terminal state change
     *                                  before {@code finishWorkflow}, so the COMPLETED hook is no longer dropped).
     * @param startedFiresAfterCrash    STARTED-hook fire count after a crash + replay (INV-17 no-re-fire requires it
     *                                  unchanged at {@code 1}).
     * @param completedFiresAfterCrash  COMPLETED-hook fire count after a crash + replay — now exactly {@code 1}
     *                                  (no-re-fire requires it unchanged, never increased; F-4 FIXED).
     * @param completedHookDropped      whether the COMPLETED hook was observed dropped (fired zero times); now always
     *                                  {@code false} (retained for observability — F-4 FIXED).
     */
    public record Outcome(boolean reachedTerminal, int startedFiresAtTerminal, int completedFiresAtTerminal,
                          int startedFiresAfterCrash, int completedFiresAfterCrash, boolean completedHookDropped) {

    }

    /**
     * Runs the scenario against a fresh world (driving {@link io.axoniq.framework.workflow.simulation.workflow.HookWorkflow}) and
     * returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var effects = new CountingEffects();
        var fires = new StatusHookFires();
        // The listener closure captures `fires`; the registration is held by the world and reused on every recovery, so
        // the counter survives crashes the same way CountingEffects does.
        var registration = EngineInstance.hookWorkflow(effects, fires);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "hook-" + orderId;

            // 1. Start the workflow: doWork runs, then the body returns -> the engine emits STARTED then terminal
            // COMPLETED. The STARTED hook fires on the (awaited) STARTED evolution.
            world.engine().publish(new HookRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "instance to reach a terminal COMPLETED workflow status",
                                () -> isCompleted(world.committedLog(), workflowId));

            // 2. The COMPLETED hook fires asynchronously relative to the COMPLETED commit (the F-2 intra-instance
            // window); give it a bounded window to fire, then snapshot the per-status fire counts.
            Polling.await(Duration.ofSeconds(2),
                          () -> fires.count(workflowId, WorkflowStatus.COMPLETED) >= 1);
            int startedAtTerminal = fires.count(workflowId, WorkflowStatus.STARTED);
            int completedAtTerminal = fires.count(workflowId, WorkflowStatus.COMPLETED);
            // INV-17 must hold at the terminal point: no status fired more than once; STARTED fired exactly once; and —
            // now that F-4 is FIXED — the terminal COMPLETED hook fired exactly once (the happy completion path awaits
            // the terminal state change before finishWorkflow, so the COMPLETED evolution and its hook run before the
            // per-instance task queue is cleared — the hook is no longer dropped).
            Invariants.assertStatusHookFiresOncePerStatus(fires, workflowId,
                                                          WorkflowStatus.STARTED, WorkflowStatus.COMPLETED);
            Invariants.assertStartedHookFiredExactlyOnce(fires, workflowId);
            Invariants.assertCompletedHookFiredExactlyOnce(fires, workflowId);

            // 3. Crash + replay TWICE (drives the real recovery path): each recovery re-evolves the committed
            // STARTED/COMPLETED status events. The no-re-fire guarantee requires no listener fires again.
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2), () -> hookFiredAgain(fires, workflowId,
                                                                      startedAtTerminal, completedAtTerminal));
            Invariants.assertStatusHookFiresOncePerStatus(fires, workflowId,
                                                          WorkflowStatus.STARTED, WorkflowStatus.COMPLETED);
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2), () -> hookFiredAgain(fires, workflowId,
                                                                      startedAtTerminal, completedAtTerminal));
            Invariants.assertStatusHookFiresOncePerStatus(fires, workflowId,
                                                          WorkflowStatus.STARTED, WorkflowStatus.COMPLETED);
            Invariants.assertStartedHookFiredExactlyOnce(fires, workflowId);
            // F-4 FIXED: the COMPLETED hook fired exactly once at terminal and is not re-fired across the crashes.
            Invariants.assertCompletedHookFiredExactlyOnce(fires, workflowId);

            int startedAfterCrash = fires.count(workflowId, WorkflowStatus.STARTED);
            int completedAfterCrash = fires.count(workflowId, WorkflowStatus.COMPLETED);
            // F-4 FIXED: the COMPLETED hook is no longer dropped, so this drop detector / re-fire guard now always
            // returns false; assert that (the gap is closed) — a dropped COMPLETED hook is now a FAILURE, and the call
            // also re-asserts no re-fire.
            boolean dropped = Invariants.documentTerminalHookMayBeDropped(fires, workflowId);

            return new Outcome(true, startedAtTerminal, completedAtTerminal,
                               startedAfterCrash, completedAfterCrash, dropped);
        }
    }

    private static boolean isCompleted(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata())
                                        .map(s -> s == WorkflowStatus.COMPLETED).orElse(false));
    }

    private static boolean hookFiredAgain(StatusHookFires fires, String workflowId,
                                          int startedBaseline, int completedBaseline) {
        return fires.count(workflowId, WorkflowStatus.STARTED) > startedBaseline
                || fires.count(workflowId, WorkflowStatus.COMPLETED) > completedBaseline;
    }
}
