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
import io.axoniq.framework.workflow.simulation.workflow.ReducerWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ReducerNullEdgeRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ReducerRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ReducerThrowingModifierRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Deterministic scenario for INVARIANTS.md INV-19 ({@code PayloadReducerSemantics}): a workflow whose steps exercise all
 * three payload reducers finishes with an engine-reconstructed payload that reflects <strong>exactly</strong> each
 * reducer's documented merge — a {@code global_only} result discarded, a {@code combine} result merged, a
 * {@code local_only} {@code modifyPayload} replace overwriting the whole payload, and a {@code parameterPayloadReducer}
 * step seeing the global payload — and a crash + replay rebuilds the <strong>identical</strong> payload.
 * <p>
 * It drives {@link ReducerWorkflow} (registered as a single definition via {@link EngineInstance#reducerWorkflow}). A
 * fresh start runs a combine seed, a {@code local_only} replace that drops the seed, a combine that must appear, a
 * {@code global_only} step whose result must be discarded, and a {@code parameterPayloadReducer(combine)} step that must
 * SEE the combined global payload.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs to COMPLETED, recording each reducer's documented outcome in the
 *       engine's reconstructed payload (combine keys present, the {@code global_only} result key absent, the prior
 *       {@code local_only}-dropped key absent, the parameter-side combine view {@code true});</li>
 *   <li>read the engine's reconstructed final payload; {@link Invariants#assertPayloadReducerSemantics} must already
 *       hold (the payload equals the documented reducer fold of the committed log) and demonstrate each reducer's
 *       outcome;</li>
 *   <li>crash + recover (drives the real replay path), then assert the reconstructed payload is byte-for-byte unchanged
 *       — replay applied every reducer identically, no semantics drift.</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class PayloadReducerSemanticsScenario {

    private PayloadReducerSemanticsScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal    whether the instance reached a terminal (COMPLETED) workflow status.
     * @param payloadBeforeCrash the engine's reconstructed final payload at the terminal point (the history read-model's
     *                           {@code state().payload()}). INV-19 requires it to reflect each reducer's documented
     *                           outcome.
     * @param payloadAfterCrash  the engine's reconstructed final payload after a crash + replay. INV-19's
     *                           replay-stability facet requires it to equal {@code payloadBeforeCrash} (replay applies
     *                           every reducer identically).
     */
    public record Outcome(boolean reachedTerminal, Map<String, Object> payloadBeforeCrash,
                          Map<String, Object> payloadAfterCrash) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link ReducerWorkflow} and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.reducerWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "reducer-" + orderId;

            // 1. Start the workflow: a fresh start runs the body (combine seed, local_only replace, combine,
            // global_only, parameterPayloadReducer view) and completes on its own (execute/modifyPayload-only).
            world.engine().publish(new ReducerRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "reducer instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Read the engine's reconstructed final payload at the terminal point. INV-19 must already hold (the
            // payload equals the documented reducer fold) and demonstrate each reducer's documented outcome.
            Map<String, Object> payloadBefore = reconstructedPayload(world, workflowId);
            Invariants.assertPayloadReducerSemantics(world.committedLog(), "reducer-",
                                                     world.engine().reconstructedPayloads("reducer-"));

            // 3. IN-SCOPE INV-19 (replay-stability): a crash + replay must rebuild the SAME payload — every reducer is
            // applied identically across the recovery, no semantics drift.
            world.crashAndRecover();
            // Give replay a bounded window to (potentially) rebuild a different payload before reading it back.
            Polling.await(Duration.ofSeconds(2),
                          () -> !reconstructedPayload(world, workflowId).equals(payloadBefore));
            Map<String, Object> payloadAfter = reconstructedPayload(world, workflowId);
            Invariants.assertPayloadReducerSemantics(world.committedLog(), "reducer-",
                                                     world.engine().reconstructedPayloads("reducer-"));

            return new Outcome(true, payloadBefore, payloadAfter);
        }
    }

    /**
     * Result of the null-value edge run (edge (a)) — a CANDIDATE-FINDING characterization (F-6, POC-TLA-DST.adoc): a
     * {@code combine} step whose result map carries a {@code null} value <strong>wedges</strong> the instance.
     *
     * @param reachedTerminal       whether the null-edge instance reached a terminal workflow status within the window.
     *                              OBSERVED: {@code false} — the instance is left non-terminal (a liveness stall).
     * @param nullCombineCommitted  whether the {@code combine} step's COMPLETED event committed (so the {@code null}
     *                              value DID reach the reducer). OBSERVED: {@code true} — the combine step records.
     * @param workflowCompleted     whether a {@code <workflow>:COMPLETED} status committed. OBSERVED: {@code false} — the
     *                              workflow-completion path threw before recording terminal status.
     * @param payloadReadThrew      whether reading the engine's reconstructed payload for the instance throws (the
     *                              {@code state().payload()} {@code Map.copyOf} rejects the {@code null} value). OBSERVED:
     *                              {@code true}.
     * @param payloadReadError      the simple class name of the exception reading the payload threw (OBSERVED:
     *                              {@code NullPointerException}), or {@code null} if the read did not throw.
     */
    public record NullEdgeOutcome(boolean reachedTerminal, boolean nullCombineCommitted, boolean workflowCompleted,
                                  boolean payloadReadThrew,
                                  @org.jspecify.annotations.Nullable String payloadReadError) {

    }

    /**
     * Runs the null-value edge (edge (a)) and <strong>characterizes the engine's ACTUAL handling</strong> of a
     * {@code null} value in a {@code combine} step's result — a candidate finding (F-6, see POC-TLA-DST.adoc), NOT a
     * passing assertion. It drives {@link ReducerWorkflow#nullEdge} (a {@code combine} step whose result map carries a
     * {@code null} value under {@link ReducerWorkflow#KEY_NULL}).
     * <p>
     * OBSERVED behaviour: the combine reducer ({@code new HashMap<>(global); putAll(local)}) accepts the {@code null}
     * value and merges it into the payload, but the very next read of the workflow payload —
     * {@code EventSourcedWorkflowState.payload()} doing {@code Map.copyOf(payload)} (runtime, line ~176) — throws
     * {@link NullPointerException} because {@code Map.copyOf} rejects null values. That NPE surfaces at workflow
     * completion ({@code completedWorkflow(...).workflowPayload()}, {@code SimpleWorkflowExecution#executeWorkflow}) and
     * lands in the {@code default} branch of {@code handleWorkflowException}, which deliberately does NOT drive the
     * workflow to a terminal status ("we agreed not to drive the workflow to terminal state on any other exception") —
     * so the instance is left <strong>non-terminal / stuck</strong> (a liveness stall) with the NPE merely logged. The
     * engine's read-model {@code state().payload()} read throws the same NPE, so the reconstructed payload cannot even be
     * inspected. This is characterized and FLAGGED, not patched (per the POC's test/docs-only rule).
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single null-edge instance.
     * @return the observed null-edge outcome.
     */
        public static NullEdgeOutcome runNullEdge(long seed, String orderId) {
        var effects = new CountingEffects();
        var registrations = List.of(EngineInstance.reducerWorkflow(effects),
                                    EngineInstance.reducerNullEdgeWorkflow(effects));
        try (var world = new SimulationWorld(seed, registrations)) {
            String workflowId = "reducer-" + orderId;

            world.engine().publish(new ReducerNullEdgeRequestedEvent(orderId));
            // Wait for the combine step to commit (the null reaches the reducer), then give the completion path a bounded
            // window in which it would (but does not) record a terminal status.
            Polling.await(Duration.ofSeconds(5),
                          () -> hasCommittedStep(world.committedLog(), workflowId, ReducerWorkflow.STEP_NULL_COMBINE));
            Polling.await(Duration.ofSeconds(2), () -> isTerminal(world.committedLog(), workflowId));

            boolean nullCombineCommitted =
                    hasCommittedStep(world.committedLog(), workflowId, ReducerWorkflow.STEP_NULL_COMBINE);
            boolean reachedTerminal = isTerminal(world.committedLog(), workflowId);
            boolean workflowCompleted = isWorkflowCompleted(world.committedLog(), workflowId);

            // Reading the engine's reconstructed payload for this instance itself throws (the Map.copyOf null-value NPE).
            boolean payloadReadThrew = false;
            String payloadReadError = null;
            try {
                world.engine().reconstructedPayloads("reducer-");
            } catch (RuntimeException e) {
                payloadReadThrew = true;
                payloadReadError = e.getClass().getSimpleName();
            }

            return new NullEdgeOutcome(reachedTerminal, nullCombineCommitted, workflowCompleted, payloadReadThrew,
                                       payloadReadError);
        }
    }

    /**
     * Result of the throwing-modifier edge run — a CANDIDATE-FINDING characterization (F-6, S-4 generalization;
     * POC-TLA-DST.adoc): a {@code modifyPayload} modifier lambda that throws a plain {@code RuntimeException} between
     * primitives <strong>wedges</strong> the instance, the SAME {@code handleWorkflowException} {@code default}-branch
     * sink as F-6's null-payload trigger, with a wider trigger family.
     *
     * @param modifierRanBeforeCrash whether the modifier lambda ran at least once before the crash (its
     *                               {@code CountingEffects} record bumped). OBSERVED: {@code true} — the throw genuinely
     *                               reached the user lambda on the workflow thread.
     * @param reachedTerminalBeforeCrash whether the instance reached a terminal workflow status before the crash.
     *                                   OBSERVED: {@code false} — wedged non-terminal (the {@code default} branch
     *                                   records no terminal status).
     * @param workflowCompleted      whether a {@code <workflow>:COMPLETED} status committed. OBSERVED: {@code false} —
     *                               the body threw before completion.
     * @param committedStepRecords   the number of committed STEP events for the instance (the {@code modifyPayload}
     *                               COMPLETED among them). OBSERVED: {@code 0} — the modifier threw BEFORE
     *                               {@code PayloadDelegate} builds/publishes its COMPLETED, so only the
     *                               {@code <workflow>:STARTED} status event is committed (no step record).
     * @param anyTerminalStatusEver  whether ANY terminal workflow status (COMPLETED/FAILED/CANCELLED/TIMED_OUT) ever
     *                               committed for the instance, across the crash + replay. OBSERVED: {@code false} — the
     *                               exception is only logged, never turned into FAILED/TIMED_OUT/CANCELLED.
     * @param reachedTerminalAfterRecover whether the instance reached a terminal status after the crash + recover.
     *                                    OBSERVED: {@code false} — STILL wedged: recovery does not rescue it (the wedged
     *                                    instance was removed from the in-memory repository when its body threw, so the
     *                                    orphaned {@code <workflow>:STARTED} is left non-terminal forever).
     */
    public record ThrowingModifierOutcome(boolean modifierRanBeforeCrash, boolean reachedTerminalBeforeCrash,
                                          boolean workflowCompleted, int committedStepRecords,
                                          boolean anyTerminalStatusEver, boolean reachedTerminalAfterRecover) {

    }

    /**
     * Runs the throwing-modifier edge (the S-4 generalization of edge (a)/finding F-6) and
     * <strong>characterizes the engine's ACTUAL handling</strong> of a plain {@code RuntimeException} thrown by a
     * {@code modifyPayload} modifier lambda between primitives — a candidate finding (F-6 broadened, see
     * POC-TLA-DST.adoc), NOT a passing assertion. It drives {@link ReducerWorkflow#throwingModifier} (a
     * {@code modifyPayload} step whose modifier lambda throws {@link ReducerWorkflow#THROWING_MODIFIER_BOOM}).
     * <p>
     * OBSERVED behaviour: the modifier lambda runs on the workflow thread inside {@code PayloadDelegate.modifyPayload}'s
     * {@code appendTask} task ({@code payloadModification.apply(...)}, {@code PayloadDelegate.java:86}); since it throws
     * <em>before</em> the COMPLETED event is built/published, no step record is committed — only the
     * {@code <workflow>:STARTED} status event. The exception propagates up out of {@code modifyPayload} → the workflow
     * body → {@code executeWorkflow} → the {@code try/catch (Throwable)} ({@code SimpleWorkflowExecution.java:160-164}) →
     * {@code handleWorkflowException}'s {@code default} branch ({@code SimpleWorkflowExecution.java:311-323}), which
     * deliberately records NO terminal status ("we agreed not to drive the workflow to terminal state on any other
     * exception") — so the instance is left <strong>non-terminal / stuck</strong> (a liveness stall) with the exception
     * merely logged. Then {@code finishWorkflow} ({@code SimpleWorkflowExecution.java:333-341}) runs the termination
     * handler that removes the wedged execution from the in-memory repository ({@code WorkflowEngine.java:194}), so a
     * subsequent {@code crashAndRecover()} does NOT rescue it: the orphaned {@code <workflow>:STARTED} carries no terminal
     * status, recovery does not re-drive it, and the instance is left non-terminal forever. This is the SAME
     * {@code default}-branch wedge as {@link #runNullEdge} (F-6), with a wider trigger family — characterized and FLAGGED,
     * not patched (per the POC's test/docs-only rule). The wedge is observed within a SHORT bounded window (the wedge is
     * the ABSENCE of a terminal status; the run never hangs).
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single throwing-modifier instance.
     * @return the observed throwing-modifier-edge outcome.
     */
        public static ThrowingModifierOutcome runThrowingModifierEdge(long seed, String orderId) {
        var effects = new CountingEffects();
        var registrations = List.of(EngineInstance.reducerWorkflow(effects),
                                    EngineInstance.reducerThrowingModifierWorkflow(effects));
        try (var world = new SimulationWorld(seed, registrations)) {
            String workflowId = "reducer-" + orderId;

            world.engine().publish(new ReducerThrowingModifierRequestedEvent(orderId));
            // Wait for the modifier lambda to actually run (the throw reaches the user code on the workflow thread),
            // then give the completion path a SHORT bounded window in which it would (but does not) record a terminal
            // status. The wedge is the ABSENCE of a terminal status, so a short deadline suffices and never hangs.
            Polling.await(Duration.ofSeconds(5),
                          () -> effects.count(workflowId, ReducerWorkflow.STEP_THROWING_MODIFIER) >= 1);
            Polling.await(Duration.ofSeconds(2), () -> isTerminal(world.committedLog(), workflowId));

            boolean modifierRanBeforeCrash =
                    effects.count(workflowId, ReducerWorkflow.STEP_THROWING_MODIFIER) >= 1;
            boolean reachedTerminalBeforeCrash = isTerminal(world.committedLog(), workflowId);
            boolean workflowCompleted = isWorkflowCompleted(world.committedLog(), workflowId);
            int committedStepRecords = stepRecordCount(world.committedLog(), workflowId);

            // Recovery: a crash + recover does NOT rescue the wedged instance — it was removed from the in-memory
            // repository when its body threw, so the orphaned <workflow>:STARTED is left non-terminal. Give recovery a
            // bounded window in which it would (but does not) drive the instance to any terminal status.
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2), () -> isTerminal(world.committedLog(), workflowId));

            boolean reachedTerminalAfterRecover = isTerminal(world.committedLog(), workflowId);
            boolean anyTerminalStatusEver = reachedTerminalBeforeCrash || reachedTerminalAfterRecover;

            return new ThrowingModifierOutcome(modifierRanBeforeCrash, reachedTerminalBeforeCrash, workflowCompleted,
                                               committedStepRecords, anyTerminalStatusEver,
                                               reachedTerminalAfterRecover);
        }
    }

    private static int stepRecordCount(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getStepStatus(e.metadata()).isPresent())
                                 .count();
    }

    private static boolean hasCommittedStep(List<EventMessage> committedLog, String workflowId,
                                            String stepName) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata())));
    }

    private static boolean isWorkflowCompleted(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata())
                                        .map(s -> s == WorkflowStatus.COMPLETED)
                                        .orElse(false));
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * The engine's own reconstructed final payload for {@code workflowId} (the history read-model's
     * {@code state().payload()}), or an empty map if it has not been projected yet.
     */
        private static Map<String, Object> reconstructedPayload(SimulationWorld world,
                                                            String workflowId) {
        return world.engine().reconstructedPayloads("reducer-").getOrDefault(workflowId, Map.of());
    }
}
