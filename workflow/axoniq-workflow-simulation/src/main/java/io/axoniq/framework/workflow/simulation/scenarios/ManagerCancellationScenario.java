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

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CorrelatedSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ExternalCancelRequestedEvent;
import org.axonframework.common.FutureUtils;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.simulation.harness.EngineInstance.MANAGER_READ_TIMEOUT;
import static io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow.STEP_AWAIT_APPROVAL;
import static io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow.STEP_COMPENSATE;

/**
 * Cancellation requested from outside the workflow through the {@link WorkflowManager}, on live, historic and
 * ambiguous targets.
 * <p>
 * The manager is the outside-in twin of {@code WorkflowExecution.cancelRunningStep(...)}, which
 * {@link ExternalStepCancellationScenario} pins directly. This scenario drives the same
 * {@code ExternalCancelCompensationWorkflow} — a body parked on an approval wait that compensates when the wait is
 * cancelled — through every manager cancellation operation, and adds the two targets the manager introduces: an id
 * that is only historic, and a query that matches more than one instance.
 * <p>
 * Oracles, per drive:
 * <ul>
 *   <li>{@link #cancelStep}: the wait step commits CANCELLED, the body compensates to COMPLETED exactly once, the
 *   request's last answer is {@code true}.</li>
 *   <li>{@link #cancelAllSteps}: same records; the request reports one cancelled step.</li>
 *   <li>{@link #cancelWorkflow}: the workflow commits CANCELLED; INV-7 {@code TerminalIsFinal} holds on the log, so
 *   whatever the woken body attempts after the terminal record, nothing more is recorded.</li>
 *   <li>{@link #cancelTerminalTarget}: on a completed id the three requests answer {@code false}, {@code 0} and a
 *   normal completion, and INV-35 {@code ManagerCancelTargetsLiveOnly} holds: no event is appended for it.</li>
 *   <li>{@link #nonUnique}: with two instances of one definition, {@code findOne(...).singleState()} fails with
 *   {@code NonUniqueWorkflowInstanceMatchException}, {@code findMany(...).size()} is 2, and the batch step cancel
 *   reaches both.</li>
 * </ul>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ManagerCancellationScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(30);

    private ManagerCancellationScenario() {
    }

    /**
     * Outcome of a cancellation drive on one live instance.
     *
     * @param lastStepAnswer     the last {@code requestStepCancellation} answer, or {@code null} when not driven.
     * @param lastAllStepsAnswer the last {@code requestCancellationOfAllSteps} answer, or {@code null} when not driven.
     * @param awaitCancelled     whether the wait step committed CANCELLED.
     * @param compensateCompleted whether the compensation step committed COMPLETED.
     * @param terminalStatus     the terminal workflow status the log folds to, or {@code null} when none.
     * @param compensateEffects  how many times the compensation side effect ran.
     * @param terminalIsFinal    whether INV-7 holds on the final log.
     */
    public record CancelOutcome(@Nullable Boolean lastStepAnswer,
                                @Nullable Integer lastAllStepsAnswer,
                                boolean awaitCancelled,
                                boolean compensateCompleted,
                                @Nullable WorkflowStatus terminalStatus,
                                int compensateEffects,
                                boolean terminalIsFinal) {

    }

    /**
     * Outcome of the three requests against an id that is only historic.
     *
     * @param stepAnswer       the {@code requestStepCancellation} answer.
     * @param allStepsAnswer   the {@code requestCancellationOfAllSteps} answer.
     * @param workflowAnswered whether {@code requestWorkflowCancellation} completed normally.
     * @param batchStepAnswer  the {@code findMany(...).requestStepCancellation} answer.
     * @param batchAllStepsAnswer the {@code findMany(...).requestCancellationOfAllSteps} answer.
     * @param appendedEvents   events appended for the id after the requests (INV-35 requires 0).
     * @param stateStatus      the manager's answer for the id after the requests.
     */
    public record TerminalTargetOutcome(boolean stepAnswer,
                                        int allStepsAnswer,
                                        boolean workflowAnswered,
                                        boolean batchStepAnswer,
                                        int batchAllStepsAnswer,
                                        int appendedEvents,
                                        @Nullable WorkflowStatus stateStatus) {

    }

    /**
     * Outcome of the ambiguous-query drive on two instances.
     *
     * @param singleStateFailure the class of the failure {@code singleState()} completed with, or {@code null}.
     * @param manySize           the size {@code findMany} reported.
     * @param batchStepAnswer    the batch {@code requestStepCancellation} answer.
     * @param cancelledInstances how many of the two instances committed a CANCELLED wait step.
     * @param compensatedInstances how many of the two committed a COMPLETED compensation step.
     */
    public record NonUniqueOutcome(@Nullable Class<? extends Throwable> singleStateFailure,
                                   int manySize,
                                   boolean batchStepAnswer,
                                   int cancelledInstances,
                                   int compensatedInstances) {

    }

    /**
     * Cancels the parked wait step of one instance through {@code findOne(byWorkflowId).requestStepCancellation}.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the single instance.
     * @return the observed outcome.
     */
    public static CancelOutcome cancelStep(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "extcancel-" + orderId;
        try (var world = parkedWorld(seed, effects, orderId)) {
            var one = world.engine().workflowManager().findOne(WorkflowStateQuery.byWorkflowId(workflowId));
            var answer = new Boolean[1];
            Polling.awaitOrFail(DEADLINE, "the wait step to commit CANCELLED via the manager", () -> {
                answer[0] = bounded(one.requestStepCancellation(STEP_AWAIT_APPROVAL, cause()));
                return hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED);
            });
            awaitCompensated(world, workflowId);
            return outcome(world, effects, workflowId, answer[0], null);
        }
    }

    /**
     * Cancels every running step of one instance through {@code requestCancellationOfAllSteps}.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the single instance.
     * @return the observed outcome.
     */
    public static CancelOutcome cancelAllSteps(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "extcancel-" + orderId;
        try (var world = parkedWorld(seed, effects, orderId)) {
            var one = world.engine().workflowManager().findOne(WorkflowStateQuery.byWorkflowId(workflowId));
            var answer = new Integer[1];
            Polling.awaitOrFail(DEADLINE, "the wait step to commit CANCELLED via cancel-all-steps", () -> {
                answer[0] = bounded(one.requestCancellationOfAllSteps(cause()));
                return hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED);
            });
            awaitCompensated(world, workflowId);
            return outcome(world, effects, workflowId, null, answer[0]);
        }
    }

    /**
     * Cancels the workflow itself through {@code requestWorkflowCancellation} while it is parked.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the single instance.
     * @return the observed outcome.
     */
    public static CancelOutcome cancelWorkflow(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "extcancel-" + orderId;
        try (var world = parkedWorld(seed, effects, orderId)) {
            var one = world.engine().workflowManager().findOne(WorkflowStateQuery.byWorkflowId(workflowId));
            bounded(one.requestWorkflowCancellation(null));
            Polling.awaitOrFail(DEADLINE, "the workflow to commit a terminal CANCELLED record and leave the live set",
                                () -> hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.CANCELLED)
                                        && !world.engine().liveWorkflowIds().contains(workflowId));
            // Give a woken body that tries to compensate after the terminal record the chance to try, so the log
            // read below judges what the engine accepted, not how fast the read was.
            Polling.await(Duration.ofSeconds(2), () -> false);
            return outcome(world, effects, workflowId, null, null);
        }
    }

    /**
     * Runs one instance to completion, then issues the three cancellation requests against its historic id.
     *
     * @param seed    seed for the deterministic id sources.
     * @param orderId business key of the single instance.
     * @return the observed outcome.
     */
    public static TerminalTargetOutcome cancelTerminalTarget(long seed, String orderId) {
        var effects = new CountingEffects();
        var workflowId = "extcancel-" + orderId;
        var query = WorkflowStateQuery.byWorkflowId(workflowId);
        try (var world = parkedWorld(seed, effects, orderId)) {
            world.engine().publish(new CorrelatedSignalEvent(orderId));
            Polling.awaitOrFail(DEADLINE, "the body to complete and leave the live set",
                                () -> hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.COMPLETED)
                                        && !world.engine().liveWorkflowIds().contains(workflowId));
            Polling.awaitOrFail(DEADLINE, "the manager to answer the historic id",
                                () -> world.engine().managerSingleState(query) != null);

            var logBefore = world.committedLog();
            var liveBefore = new HashSet<>(world.engine().liveWorkflowIds());
            var one = world.engine().workflowManager().findOne(query);
            boolean stepAnswer = bounded(one.requestStepCancellation(STEP_AWAIT_APPROVAL, cause()));
            int allAnswer = bounded(one.requestCancellationOfAllSteps(cause()));
            bounded(one.requestWorkflowCancellation(null));
            // The batch operations resolve their targets from the live repository only, so on a historic id they
            // must find nothing to do: false, 0, normal completion — and never a failed future.
            var many = world.engine().workflowManager().findMany(query);
            boolean batchStepAnswer = bounded(many.requestStepCancellation(STEP_AWAIT_APPROVAL, cause()));
            int batchAllAnswer = bounded(many.requestCancellationOfAllSteps(cause()));
            bounded(many.requestWorkflowCancellation(null));
            Polling.await(Duration.ofSeconds(2), () -> false);
            var logAfter = world.committedLog();
            Invariants.assertManagerCancelTargetsLiveOnly(logBefore, logAfter, liveBefore);
            var state = world.engine().managerSingleState(query);
            return new TerminalTargetOutcome(stepAnswer, allAnswer, true, batchStepAnswer, batchAllAnswer,
                                             countFor(logAfter, workflowId) - countFor(logBefore, workflowId),
                                             state == null ? null : state.workflowStatus());
        }
    }

    /**
     * Starts two instances of one definition and queries by definition identity.
     *
     * @param seed seed for the deterministic id sources.
     * @return the observed outcome.
     */
    public static NonUniqueOutcome nonUnique(long seed) {
        var effects = new CountingEffects();
        var ids = List.of("extcancel-N1", "extcancel-N2");
        try (var world = new SimulationWorld(seed, EngineInstance.externalCancelWorkflow(effects))) {
            world.engine().publish(new ExternalCancelRequestedEvent("N1"));
            world.engine().publish(new ExternalCancelRequestedEvent("N2"));
            Polling.awaitOrFail(DEADLINE, "both approval waits to park", () -> ids.stream().allMatch(
                    id -> hasStepStatus(world.committedLog(), id, STEP_AWAIT_APPROVAL, StepStatus.STARTED)));
            var byDefinition = WorkflowStateQuery.byWorkflowDefinitionId(VersionedType.of(
                    ExternalCancelCompensationWorkflow.WORKFLOW_NAME, Version.DEFAULT_VERSION));
            var manager = world.engine().workflowManager();

            Class<? extends Throwable> failure = null;
            try {
                bounded(manager.findOne(byDefinition).singleState());
            } catch (RuntimeException e) {
                failure = e.getClass();
            }
            int manySize = world.engine().managerSize(byDefinition);
            var many = manager.findMany(byDefinition);
            var answer = new boolean[1];
            Polling.awaitOrFail(DEADLINE, "both wait steps to commit CANCELLED via the batch request", () -> {
                answer[0] = bounded(many.requestStepCancellation(STEP_AWAIT_APPROVAL, cause()));
                return ids.stream().allMatch(
                        id -> hasStepStatus(world.committedLog(), id, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED));
            });
            for (String id : ids) {
                awaitCompensated(world, id);
            }
            int cancelled = (int) ids.stream().filter(
                    id -> hasStepStatus(world.committedLog(), id, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED)).count();
            int compensated = (int) ids.stream().filter(
                    id -> hasStepStatus(world.committedLog(), id, STEP_COMPENSATE, StepStatus.COMPLETED)).count();
            return new NonUniqueOutcome(failure, manySize, answer[0], cancelled, compensated);
        }
    }

    // ---- drive helpers ----

    private static SimulationWorld parkedWorld(long seed, CountingEffects effects, String orderId) {
        var world = new SimulationWorld(seed, EngineInstance.externalCancelWorkflow(effects));
        var workflowId = "extcancel-" + orderId;
        world.engine().publish(new ExternalCancelRequestedEvent(orderId));
        Polling.awaitOrFail(DEADLINE, "the approval wait to park",
                            () -> hasStepStatus(world.committedLog(), workflowId, STEP_AWAIT_APPROVAL,
                                                StepStatus.STARTED));
        Polling.awaitOrFail(DEADLINE, "the wait-timeout to be scheduled on the virtual scheduler",
                            () -> world.scheduler().pendingTasks() > 0);
        return world;
    }

    private static void awaitCompensated(SimulationWorld world, String workflowId) {
        Polling.awaitOrFail(DEADLINE, "the compensation step to COMPLETE and the workflow to reach terminal COMPLETED",
                            () -> hasStepStatus(world.committedLog(), workflowId, STEP_COMPENSATE,
                                                StepStatus.COMPLETED)
                                    && hasWorkflowStatus(world.committedLog(), workflowId, WorkflowStatus.COMPLETED));
    }

    private static CancelOutcome outcome(SimulationWorld world, CountingEffects effects, String workflowId,
                                         @Nullable Boolean stepAnswer, @Nullable Integer allStepsAnswer) {
        var log = world.committedLog();
        boolean terminalIsFinal;
        try {
            Invariants.assertTerminalIsFinal(log);
            terminalIsFinal = true;
        } catch (RuntimeException violation) {
            terminalIsFinal = false;
        }
        return new CancelOutcome(stepAnswer, allStepsAnswer,
                                 hasStepStatus(log, workflowId, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED),
                                 hasStepStatus(log, workflowId, STEP_COMPENSATE, StepStatus.COMPLETED),
                                 terminalStatus(log, workflowId),
                                 effects.count(workflowId, STEP_COMPENSATE),
                                 terminalIsFinal);
    }

    private static StepCancellationException cause() {
        return new StepCancellationException("cancelled through the WorkflowManager");
    }

    private static <T> T bounded(CompletableFuture<T> future) {
        return FutureUtils.joinAndUnwrap(future.orTimeout(MANAGER_READ_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
    }

    // ---- log readers (per (workflowId, stepName), content-based) ----

    private static boolean hasStepStatus(List<EventMessage> committedLog, String workflowId, String stepName,
                                         StepStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(s -> s == status).orElse(false));
    }

    private static boolean hasWorkflowStatus(List<EventMessage> committedLog, String workflowId,
                                             WorkflowStatus status) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s == status).orElse(false));
    }

    @Nullable
    private static WorkflowStatus terminalStatus(List<EventMessage> committedLog, String workflowId) {
        WorkflowStatus terminal = null;
        for (EventMessage e : committedLog) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))) {
                var status = MetadataUtils.getWorkflowStatus(e.metadata());
                if (status.isPresent() && status.get().isTerminal()) {
                    terminal = status.get();
                }
            }
        }
        return terminal;
    }

    private static int countFor(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .count();
    }

    /**
     * The ids this scenario's two-instance drive uses, for callers that want to cross-check the log.
     *
     * @return the two workflow ids.
     */
    public static Set<String> nonUniqueIds() {
        return Set.of("extcancel-N1", "extcancel-N2");
    }
}
