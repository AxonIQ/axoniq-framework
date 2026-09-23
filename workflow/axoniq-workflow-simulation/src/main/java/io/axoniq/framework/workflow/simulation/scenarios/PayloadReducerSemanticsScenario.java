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
import org.jspecify.annotations.Nullable;

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
     * Result of the throwing-modifier edge run: a {@code modifyPayload} modifier lambda that throws a plain
     * {@code RuntimeException} between primitives. The exception is not recoverable under the default
     * {@link io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy}, so it ends
     * the workflow {@code FAILED}.
     *
     * @param modifierRanBeforeCrash      whether the modifier lambda ran at least once before the crash
     * @param terminalBeforeCrash         the terminal workflow status recorded before the crash, or {@code null}
     * @param committedStepRecords        the number of committed step events for the instance ({@code 0}: the modifier
     *                                    throws before {@code PayloadDelegate} publishes its COMPLETED)
     * @param terminalRecordsAfterRecover the number of committed terminal workflow status records after a crash and
     *                                    recover ({@code 1}: nothing re-publishes the terminal)
     * @param modifierRunsAfterRecover    how often the modifier lambda ran across the whole run ({@code 1}: the failed
     *                                    instance is not re-driven)
     * @param liveAfterRecover            whether the instance is live in the recovered engine ({@code false})
     */
    public record ThrowingModifierOutcome(boolean modifierRanBeforeCrash, @Nullable WorkflowStatus terminalBeforeCrash,
                                          int committedStepRecords, int terminalRecordsAfterRecover,
                                          int modifierRunsAfterRecover, boolean liveAfterRecover) {

    }

    /**
     * Runs the throwing-modifier edge: drives {@link ReducerWorkflow#throwingModifier} (a {@code modifyPayload} step
     * whose modifier lambda throws {@link ReducerWorkflow#THROWING_MODIFIER_BOOM}), waits for the terminal status,
     * then crashes and recovers the engine and observes that nothing re-drives or re-publishes.
     *
     * @param seed    seed for the world's deterministic id source
     * @param orderId business key for the single throwing-modifier instance
     * @return the observed throwing-modifier-edge outcome
     */
    public static ThrowingModifierOutcome runThrowingModifierEdge(long seed, String orderId) {
        var effects = new CountingEffects();
        var registrations = List.of(EngineInstance.reducerWorkflow(effects),
                                    EngineInstance.reducerThrowingModifierWorkflow(effects));
        try (var world = new SimulationWorld(seed, registrations)) {
            String workflowId = "reducer-" + orderId;

            world.engine().publish(new ReducerThrowingModifierRequestedEvent(orderId));
            Polling.await(Duration.ofSeconds(5),
                          () -> effects.count(workflowId, ReducerWorkflow.STEP_THROWING_MODIFIER) >= 1);
            Polling.await(Duration.ofSeconds(5), () -> isTerminal(world.committedLog(), workflowId));

            boolean modifierRanBeforeCrash =
                    effects.count(workflowId, ReducerWorkflow.STEP_THROWING_MODIFIER) >= 1;
            WorkflowStatus terminalBeforeCrash = terminalStatus(world.committedLog(), workflowId);
            int committedStepRecords = stepRecordCount(world.committedLog(), workflowId);

            // A crash + recover after the terminal: a bounded window in which a re-drive or a second terminal shows.
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2),
                          () -> terminalRecordCount(world.committedLog(), workflowId) > 1
                                  || effects.count(workflowId, ReducerWorkflow.STEP_THROWING_MODIFIER) > 1);

            return new ThrowingModifierOutcome(modifierRanBeforeCrash, terminalBeforeCrash, committedStepRecords,
                                               terminalRecordCount(world.committedLog(), workflowId),
                                               effects.count(workflowId, ReducerWorkflow.STEP_THROWING_MODIFIER),
                                               world.engine().liveWorkflowIds().contains(workflowId));
        }
    }

    @Nullable
    private static WorkflowStatus terminalStatus(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .map(e -> MetadataUtils.getWorkflowStatus(e.metadata()).orElse(null))
                           .filter(status -> status != null && status.isTerminal())
                           .findFirst()
                           .orElse(null);
    }

    private static int terminalRecordCount(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                                           .map(WorkflowStatus::isTerminal).orElse(false))
                                 .count();
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
