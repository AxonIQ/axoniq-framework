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

import io.axoniq.framework.workflow.runtime.test.fakes.SameThreadExecutorService;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.NestedPrimitiveWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.NestedPrimitiveRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;

/**
 * Deterministic scenario for INVARIANTS.md INV-23 ({@code EngineSelfProtection}): PROBE the engine's self-protection at
 * the <em>nested-primitive</em> failure surface (the {@code axon-flow-workflow} skill §3.1-forbidden pattern — a
 * primitive called from inside another primitive's action lambda), end-to-end, and observe whether the engine
 * detects/rejects it, silently completes, or silently DEADLOCKS — asserting in every case that the committed history is
 * never CORRUPTED/TORN.
 * <p>
 * Drives {@link NestedPrimitiveWorkflow} (whose outer {@code execute} action illegally calls {@code ctx.awaitExecute})
 * twice, under the two body-executor threading models that decide the outcome:
 * <ol>
 *   <li><strong>Default virtual-thread executor</strong> (the engine's production default {@code WORKFLOW_ENGINE_EXECUTOR}):
 *       the inner action gets its own thread, the outer action's {@code awaitStateChange} consumes the inner's queued
 *       tasks, so both steps complete and the instance reaches a terminal COMPLETED status — the engine SILENTLY
 *       TOLERATES the nested primitive (no detection, no rejection). No corruption.</li>
 *   <li><strong>Injected single-threaded executor</strong> ({@link SameThreadExecutorService}, wired test-side via the
 *       additive {@code SimulationWorld}/{@code EngineInstance} body-executor override — no production change): the inner
 *       {@code appendTask} can never be consumed because the only consumer thread is the outer action blocked on it, so
 *       the per-instance task queue DEADLOCKS — the instance reaches NO terminal status (only the workflow STARTED event
 *       is committed) and is observed as stuck within a SHORT wall-clock window. No corruption (nothing further is
 *       appended). This is the skill's documented "blocks forever" case, and the candidate robustness gap <strong>F-5</strong>:
 *       the engine has no up-front guard against the nested primitive — it surfaces only as a silent deadlock here (or a
 *       silent success in (1)), never as a clear up-front error.</li>
 * </ol>
 * The single-threaded run is <strong>bounded</strong>: the start event is published from a daemon thread (so the
 * caller is never the one that blocks), and the scenario waits only a SHORT window (≈1s) for the deadlocked instance to
 * be observed as non-terminal, then returns — the deadlocked engine threads are daemon/virtual and are dropped on
 * {@code world.close()}, so the test finishes fast and never hangs the suite (mirroring {@code DstSmokeTest}'s
 * anti-hang self-check). The complementary <em>task-queue overflow</em> surface is exercised directly against the real
 * {@code SimpleWorkflowExecution.appendTask} in {@code Inv23EngineSelfProtectionTest} (the harness cannot practically
 * enqueue 1000+ pending tasks at one self-completing instance).
 * <p>
 * INV-23 is DST-only (the Phase-2 TLA+ model is scoped to leasing + crash-recovery and has no notion of the per-instance
 * task queue, the body executor, the {@code appendTask} re-entrancy/bound, or nested primitive calls — see INVARIANTS.md
 * INV-23 "Checked by").
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class EngineSelfProtectionScenario {

    /**
     * The id prefix the nested-primitive probe's instances carry (matching {@link EngineInstance#nestedPrimitiveWorkflow}).
     */
    public static final String ID_PREFIX = "selfprot-";

    /**
     * Short observation window for the deadlock case — long enough for the engine to commit the workflow STARTED and (if
     * it were going to) make further progress, short enough that the bounded test finishes fast. The deadlock is
     * observed as the absence of further progress within this window, not by waiting on it.
     */
    private static final Duration OBSERVATION_WINDOW = Duration.ofMillis(1000);

    private EngineSelfProtectionScenario() {
    }

    /**
     * Result of running the nested-primitive probe under one body-executor model.
     *
     * @param reachedTerminal whether the instance reached a terminal workflow status (silent-complete — the default
     *                        virtual-thread executor outcome).
     * @param deadlocked      whether the instance stalled non-terminally with only its STARTED committed and no further
     *                        progress within the observation window (silent-deadlock — the single-threaded executor
     *                        outcome).
     * @param outerStepRan    whether the outer step's effect ran (it completes only in the silent-complete case).
     * @param innerStepRan    whether the inner (nested) step's effect ran (it completes only in the silent-complete case;
     *                        it never runs in the deadlock case because its task is never consumed).
     * @param committedEvents number of committed events for the probe instance.
     */
    public record Outcome(boolean reachedTerminal, boolean deadlocked, boolean outerStepRan, boolean innerStepRan,
                          int committedEvents) {

    }

    /**
     * Runs the nested-primitive probe under the engine's DEFAULT virtual-thread body executor. The OBSERVED outcome is
     * <strong>non-deterministic</strong>: most of the time the nested primitive silently COMPLETES (the inner action
     * runs on its own thread, the outer action's {@code awaitStateChange} consumes the inner's queued tasks, the
     * instance reaches a terminal COMPLETED status), but under thread-scheduling contention it can also STALL
     * non-terminally (the inner-vs-outer queue-consumption race lands the other way) — neither outcome is a clean
     * up-front rejection, and neither corrupts the committed history. The probe must be robust to BOTH; the caller
     * asserts only the two things that always hold here: no corruption, and no clean rejection (F-5).
     * <p>
     * Bounded: publishes on the caller thread (the default executor runs the body on its own threads, so the caller
     * does not block), then waits only a SHORT window for a terminal status. If the instance has not terminated by the
     * window, that is the (tolerated) stall outcome — the window EXPIRY is the observation, not a hang.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome (either {@code reachedTerminal} or a non-terminal stall — both non-rejection,
     *         no-corruption).
     */
        public static Outcome runDefaultExecutor(long seed, String orderId) {
        var effects = new CountingEffects();
        String workflowId = ID_PREFIX + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.nestedPrimitiveWorkflow(effects))) {
            world.engine().publish(new NestedPrimitiveRequestedEvent(orderId));
            // Bounded: wait a SHORT window for the instance to terminate; if it has not, that is the (tolerated) stall —
            // the engine never cleanly rejects the nested primitive either way. Window expiry is the observation, not a
            // hang (the body runs on the default executor's own threads, dropped on world.close()).
            Polling.await(Duration.ofSeconds(3),
                          () -> hasTerminalWorkflowStatus(world.committedLog(), workflowId));
            // INV-23 no-corruption safety property must hold regardless of which (non-rejection) outcome occurred.
            Invariants.assertEngineSelfProtection(world.committedLog(), ID_PREFIX);
            return observe(world, workflowId, effects);
        }
    }

    /**
     * Runs the nested-primitive probe under an injected SINGLE-THREADED body executor: the nested primitive DEADLOCKS
     * the per-instance task queue, so the instance stalls non-terminally (only its STARTED is committed) — observed as
     * stuck within a SHORT window. No corruption.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome (expected: {@code deadlocked == true}, {@code reachedTerminal == false}).
     */
        public static Outcome runSingleThreadedExecutor(long seed, String orderId) {
        var effects = new CountingEffects();
        String workflowId = ID_PREFIX + orderId;
        var sameThread = new SameThreadExecutorService();
        try (var world = new SimulationWorld(seed, EngineInstance.nestedPrimitiveWorkflow(effects), sameThread)) {
            // Publish from a DAEMON thread: under the same-thread executor the publish call drives the body inline and
            // would itself block on the deadlocked queue, so the caller must not be the one that blocks. The daemon
            // thread is dropped on world.close()/JVM exit — it never hangs the suite.
            Thread publisher = new Thread(() -> {
                try {
                    world.engine().publish(new NestedPrimitiveRequestedEvent(orderId));
                } catch (RuntimeException ignored) {
                    // A late interrupt during world teardown is expected and harmless for the probe.
                }
            }, "selfprot-nested-primitive-publisher");
            publisher.setDaemon(true);
            publisher.start();

            // Wait (briefly) for the workflow STARTED to land, then confirm no further progress within the short window:
            // the deadlock is the ABSENCE of progress, observed within OBSERVATION_WINDOW, not a wait ON it.
            Polling.await(OBSERVATION_WINDOW, () -> hasWorkflowStarted(world.committedLog(), workflowId));
            // A small extra settle to give any (non-existent) progress a chance — then observe.
            Polling.await(Duration.ofMillis(300), () -> hasTerminalWorkflowStatus(world.committedLog(), workflowId));

            // INV-23 no-corruption safety property must hold for the stuck instance too (clean non-terminal prefix).
            Invariants.assertEngineSelfProtection(world.committedLog(), ID_PREFIX);
            return observe(world, workflowId, effects);
        }
    }

        private static Outcome observe(SimulationWorld world, String workflowId,
                                  CountingEffects effects) {
        var log = world.committedLog();
        boolean reachedTerminal = hasTerminalWorkflowStatus(log, workflowId);
        int outerEffects = effects.count(workflowId, NestedPrimitiveWorkflow.STEP_OUTER);
        int innerEffects = effects.count(workflowId, NestedPrimitiveWorkflow.STEP_INNER);
        int committedEvents = (int) log.stream()
                                       .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                       .count();
        // Deadlocked iff: never reached terminal, and the only progress is the workflow STARTED (the outer step's
        // own STARTED never lands because its action is enqueued behind the deadlock — see NestedPrimitiveWorkflow).
        boolean startedCommitted = hasWorkflowStarted(log, workflowId);
        boolean deadlocked = startedCommitted && !reachedTerminal && innerEffects == 0
                && !hasTerminalStep(log, workflowId, NestedPrimitiveWorkflow.STEP_OUTER);
        return new Outcome(reachedTerminal, deadlocked, outerEffects > 0, innerEffects > 0, committedEvents);
    }

    private static boolean hasWorkflowStarted(java.util.List<EventMessage> committedLog,
                                              String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).isPresent());
    }

    private static boolean hasTerminalWorkflowStatus(java.util.List<EventMessage> committedLog,
                                                    String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    private static boolean hasTerminalStep(java.util.List<EventMessage> committedLog,
                                          String workflowId, String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }
}
