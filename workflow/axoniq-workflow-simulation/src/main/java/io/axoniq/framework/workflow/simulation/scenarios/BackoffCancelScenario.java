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

import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.BackoffCancelRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

import static io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow.BACKOFF_DELAY;
import static io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow.MODE_EXTERNAL;
import static io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow.MODE_IN_BODY_CANCEL;
import static io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow.STEP_AWAIT_DECISION;
import static io.axoniq.framework.workflow.simulation.workflow.BackoffCancelWorkflow.STEP_FLAKY;

/**
 * Deterministic scenario driving <strong>cancellation during a retry backoff window</strong> — the gap left by
 * {@code RetryableExecuteDelegate.scheduleRetryAttempt}: during a backoff the only registered running future is the
 * backoff-launch future, which has no cancellation-to-publish wiring (its {@code .exceptionally} is chained on the
 * <em>upstream</em> {@code runAsync} stage, so a {@code completeExceptionally} on the registered future runs nothing)
 * and whose scheduled launch task is never unscheduled. Two drives, one root:
 * <ul>
 *   <li>{@link #runInBodyCancel(long, String)} — the body issues {@code ctx.cancel(...)} while its flaky step sits in
 *       the backoff window. {@code TerminateDelegate.terminate} → {@code cancelAllRunningSteps} completes the backoff
 *       future exceptionally, then blocks in {@code awaitStateChange(allTerminal)} — nothing ever drives the RETRYING
 *       step terminal, so the CANCELLED workflow record does not appear until the backoff elapses; when it does, the
 *       "cancelled" step's next attempt still RUNS its side effect and completes, and only then does the workflow
 *       record CANCELLED.</li>
 *   <li>{@link #runExternalCancel(long, String)} — the scenario cancels the retrying step via
 *       {@code cancelRunningStep(...)} (the same surface the external step-cancellation scenario pins as working for a
 *       parked wait step). The cancel is silently swallowed: no CANCELLED step record is ever published, the step stays
 *       durably RETRYING, and when the backoff elapses the retry runs its side effect and records COMPLETED as if no
 *       cancel had ever been requested.</li>
 * </ul>
 * Scenario-pinned only (registration is scenario-only, not in {@link SimulationWorld} defaults): the wait needs the
 * scenario's signal and the backoff needs an explicit virtual-time advance.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class BackoffCancelScenario {

    /**
     * Bounded absence window used for the "nothing may appear" checks — mirrors the parked-subscription scenario's
     * absence-window idiom.
     */
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(2);

    /**
     * Deadline for the positive waits.
     */
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    private BackoffCancelScenario() {
    }

    /**
     * The observables of the in-body-cancel drive.
     *
     * @param cancelledDuringBackoff whether a {@code <workflow>:CANCELLED} record appeared while the flaky step was
     *                               still waiting out its backoff (expected {@code false} — the cancel is held
     *                               hostage).
     * @param effectsBeforeAdvance   flaky-action effect count observed after the cancel was requested but before the
     *                               backoff elapsed (expected 1 — only the first, failed attempt).
     * @param effectsAfterAdvance    flaky-action effect count after the backoff elapsed (expected 2 — the doomed
     *                               retry attempt ran its side effect despite the cancel).
     * @param flakyTerminalStatus    the flaky step's terminal record after settling (expected COMPLETED — the
     *                               "cancelled" step finished normally).
     * @param workflowCancelled      whether the workflow finally recorded CANCELLED once the step went terminal.
     */
    public record InBodyOutcome(boolean cancelledDuringBackoff,
                                long effectsBeforeAdvance,
                                long effectsAfterAdvance,
                                StepStatus flakyTerminalStatus,
                                boolean workflowCancelled) {

    }

    /**
     * The observables of the external-cancel drive.
     *
     * @param stepCancelRecorded   whether the externally-cancelled step ever committed a CANCELLED record (expected
     *                             {@code false} — the cancel is swallowed).
     * @param effectsBeforeAdvance flaky-action effect count after the external cancel, before the backoff elapsed
     *                             (expected 1).
     * @param effectsAfterAdvance  flaky-action effect count after the backoff elapsed (expected 2 — the retry ran
     *                             anyway).
     * @param flakyTerminalStatus  the flaky step's terminal record (expected COMPLETED, as if never cancelled).
     * @param workflowCompleted    whether the workflow reached its normal terminal COMPLETED.
     */
    public record ExternalOutcome(boolean stepCancelRecorded,
                                  long effectsBeforeAdvance,
                                  long effectsAfterAdvance,
                                  StepStatus flakyTerminalStatus,
                                  boolean workflowCompleted) {

    }

    /**
     * Runs the in-body {@code ctx.cancel(...)}-during-backoff drive on a fresh world.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static InBodyOutcome runInBodyCancel(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.backoffCancelWorkflow(effects))) {
            String workflowId = "backoffcancel-" + orderId;

            world.engine().publish(new BackoffCancelRequestedEvent(orderId, MODE_IN_BODY_CANCEL));
            awaitParkedInBackoff(world, workflowId);

            // Release the wait: the body proceeds straight into ctx.cancel(...) while the backoff is pending.
            world.engine().publish(new CorrelatedSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the wait step to complete so the body reaches ctx.cancel",
                                () -> hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_DECISION,
                                                    StepStatus.COMPLETED));

            // Bounded absence: the cancel must NOT take effect while the flaky step waits out its backoff.
            boolean cancelledDuringBackoff = Polling.await(ABSENCE_WINDOW,
                                                           () -> hasWorkflowStatus(world.committedLog(), workflowId,
                                                                                   WorkflowStatus.CANCELLED));
            long effectsBefore = effects.count(workflowId, STEP_FLAKY);

            // Fire the backoff: the "cancelled" step's retry attempt runs anyway, and only then can the cancel land.
            world.advanceTime(BACKOFF_DELAY.plusSeconds(1));
            Polling.awaitOrFail(DEADLINE, "the flaky step to reach a terminal record after the backoff fired",
                                () -> terminalStepStatus(world.committedLog(), workflowId, STEP_FLAKY) != null);
            Polling.awaitOrFail(DEADLINE, "the workflow to finally record CANCELLED",
                                () -> hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.CANCELLED));

            return new InBodyOutcome(
                    cancelledDuringBackoff,
                    effectsBefore,
                    effects.count(workflowId, STEP_FLAKY),
                    terminalStepStatus(world.committedLog(), workflowId, STEP_FLAKY),
                    hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.CANCELLED));
        }
    }

    /**
     * Runs the external {@code cancelRunningStep(...)}-during-backoff drive on a fresh world.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static ExternalOutcome runExternalCancel(long seed, String orderId) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.backoffCancelWorkflow(effects))) {
            String workflowId = "backoffcancel-" + orderId;

            world.engine().publish(new BackoffCancelRequestedEvent(orderId, MODE_EXTERNAL));
            awaitParkedInBackoff(world, workflowId);

            // THE EXTERNAL CANCEL — same surface ExternalStepCancellationScenario pins as working for a wait step.
            world.engine().cancelRunningStepOf(workflowId, STEP_FLAKY,
                                               new StepCancellationException("cancelled externally during backoff"));

            // Bounded absence: no CANCELLED record for the step may appear (the cancel is swallowed).
            boolean stepCancelRecorded = Polling.await(ABSENCE_WINDOW,
                                                       () -> hasStepStatus(world.committedLog(), workflowId,
                                                                           STEP_FLAKY, StepStatus.CANCELLED));
            long effectsBefore = effects.count(workflowId, STEP_FLAKY);

            // Fire the backoff: the cancelled step's retry runs anyway and completes as if never cancelled.
            world.advanceTime(BACKOFF_DELAY.plusSeconds(1));
            Polling.awaitOrFail(DEADLINE, "the flaky step to reach a terminal record after the backoff fired",
                                () -> terminalStepStatus(world.committedLog(), workflowId, STEP_FLAKY) != null);

            // Release the wait so the world settles terminally.
            world.engine().publish(new CorrelatedSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the workflow to reach terminal COMPLETED",
                                () -> hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.COMPLETED));

            return new ExternalOutcome(
                    stepCancelRecorded,
                    effectsBefore,
                    effects.count(workflowId, STEP_FLAKY),
                    terminalStepStatus(world.committedLog(), workflowId, STEP_FLAKY),
                    hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.COMPLETED));
        }
    }

    /**
     * Waits until the instance is parked in the shape both drives need: the flaky step durably RETRYING (attempt 1
     * failed, backoff scheduled), the wait step STARTED, and the backoff launch pending on the virtual scheduler.
     */
    private static void awaitParkedInBackoff(SimulationWorld world, String workflowId) {
        Polling.awaitOrFail(DEADLINE, "the flaky step to commit RETRYING (attempt 1 failed, backoff scheduled)",
                            () -> hasStepStatus(world.committedLog(), workflowId, STEP_FLAKY, StepStatus.RETRYING));
        Polling.awaitOrFail(DEADLINE, "the wait step to park (STARTED)",
                            () -> hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_DECISION,
                                                StepStatus.STARTED));
        Polling.awaitOrFail(DEADLINE, "the backoff launch to be pending on the virtual scheduler",
                            () -> world.scheduler().pendingTasks() > 0);
    }

    // ---- log readers (per (workflowId, stepName), content-based) ----

    private static boolean hasStepStatus(List<EventMessage> committedLog, String workflowId,
                                         String stepName, StepStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s == status).orElse(false));
    }

    private static StepStatus terminalStepStatus(List<EventMessage> committedLog, String workflowId,
                                                 String stepName) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                                   && stepName.equals(MetadataUtils.getStepName(e.metadata())))
                           .map(e -> MetadataUtils.getStepStatus(e.metadata()).orElse(null))
                           .filter(s -> s != null && s.isTerminal())
                           .findFirst()
                           .orElse(null);
    }

    private static boolean hasWorkflowStatus(List<EventMessage> committedLog, String workflowId,
                                             WorkflowStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s == status).orElse(false));
    }
}
