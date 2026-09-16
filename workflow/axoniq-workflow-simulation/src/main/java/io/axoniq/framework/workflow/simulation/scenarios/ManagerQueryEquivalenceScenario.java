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
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static io.axoniq.framework.workflow.simulation.harness.EngineInstance.MANAGER_READ_TIMEOUT;
import static io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow.STEP_AWAIT_APPROVAL;
import static io.axoniq.framework.workflow.simulation.workflow.ExternalCancelCompensationWorkflow.STEP_COMPENSATE;

/**
 * Differential oracle for {@code WorkflowStateQuery}: the ids the {@code WorkflowManager} answers for each named
 * restriction equal the ids a fold of the committed log selects for the same restriction.
 * <p>
 * Three instances of one definition are driven into three distinct shapes: {@code A} is cancelled through the manager
 * and compensates (wait step CANCELLED, compensation COMPLETED, workflow COMPLETED); {@code B} is signalled and
 * completes (wait step COMPLETED, workflow COMPLETED); {@code C} stays parked (wait step STARTED, workflow STARTED,
 * live). With the world settled, every query in {@link #queries()} is put to the manager and to
 * {@link Invariants#foldLog(List)}; the two id sets must match, and each manager result must satisfy INV-34
 * ({@code ManagerOneStatePerId}). The criteria not derivable from the log (payload values) are checked against the
 * start payload the drive itself supplied.
 * <p>
 * Two probes ride along. {@code detachedSnapshotUnchanged}: the detached state of {@code B} taken while parked is
 * compared with itself after {@code B} completed — the API promises a copy safe to retain. {@code stepOrder}: the
 * detached {@code workflowStepNames()} of {@code A} next to the history state's own order and the log's commit order,
 * because the detached state sorts by step timestamp while the other two do not.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class ManagerQueryEquivalenceScenario {

    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final String ID_A = "extcancel-A";
    private static final String ID_B = "extcancel-B";
    private static final String ID_C = "extcancel-C";
    private static final VersionedType DEFINITION = VersionedType.of(
            ExternalCancelCompensationWorkflow.WORKFLOW_NAME, Version.DEFAULT_VERSION);

    private ManagerQueryEquivalenceScenario() {
    }

    /**
     * One query under test, with the log-side selector that must agree with the manager.
     *
     * @param name     a label for the report.
     * @param query    the manager-side query.
     * @param expected the ids the log fold (or the drive's own knowledge) selects.
     */
    public record Probe(String name, WorkflowStateQuery query, Set<String> expected) {

    }

    /**
     * The manager's answer to one probe next to the expected ids.
     *
     * @param name          the probe label.
     * @param expected      the ids the log side selects.
     * @param managerIds    the ids the manager published.
     * @param managerSize   the size the manager reported.
     */
    public record Comparison(String name, Set<String> expected, Set<String> managerIds, int managerSize) {

        /**
         * Whether the manager agreed with the log side.
         *
         * @return {@code true} when the id sets are equal and the size matches.
         */
        public boolean agrees() {
            return expected.equals(managerIds) && managerSize == managerIds.size();
        }
    }

    /**
     * Outcome of the scenario.
     *
     * @param comparisons              one comparison per probe.
     * @param detachedSnapshotUnchanged whether the detached state of {@code B} taken while parked still reads STARTED
     *                                  with its single STARTED step after {@code B} completed.
     * @param detachedStepOrderA       the detached {@code workflowStepNames()} of {@code A}.
     * @param historyStepOrderA        the history state's {@code workflowStepNames()} of {@code A}.
     * @param logStepOrderA            the step names of {@code A} in first-commit order.
     */
    public record Outcome(List<Comparison> comparisons,
                          boolean detachedSnapshotUnchanged,
                          List<String> detachedStepOrderA,
                          List<String> historyStepOrderA,
                          List<String> logStepOrderA) {

        /**
         * The probes on which the manager disagreed with the log.
         *
         * @return the disagreeing comparisons, empty when all agree.
         */
        public List<Comparison> disagreements() {
            return comparisons.stream().filter(c -> !c.agrees()).toList();
        }
    }

    /**
     * Runs the scenario for one seed.
     *
     * @param seed seed for the deterministic id sources.
     * @return the observed outcome.
     */
    public static Outcome run(long seed) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.externalCancelWorkflow(effects))) {
            for (String key : List.of("A", "B", "C")) {
                world.engine().publish(new ExternalCancelRequestedEvent(key));
            }
            Polling.awaitOrFail(DEADLINE, "all three approval waits to park",
                                () -> Set.of(ID_A, ID_B, ID_C).stream().allMatch(
                                        id -> hasStep(world.committedLog(), id, STEP_AWAIT_APPROVAL, StepStatus.STARTED)));
            Polling.awaitOrFail(DEADLINE, "the wait-timeouts to be scheduled",
                                () -> world.scheduler().pendingTasks() > 0);

            var parkedB = world.engine().managerSingleState(WorkflowStateQuery.byWorkflowId(ID_B));
            var parkedBStatus = parkedB == null ? null : parkedB.workflowStatus();
            var parkedBSteps = parkedB == null ? List.<String>of() : List.copyOf(parkedB.workflowStepNames());

            // A: cancelled through the manager and compensated.
            var oneA = world.engine().workflowManager().findOne(WorkflowStateQuery.byWorkflowId(ID_A));
            Polling.awaitOrFail(DEADLINE, "A's wait step to commit CANCELLED", () -> {
                FutureUtils.joinAndUnwrap(oneA.requestStepCancellation(
                        STEP_AWAIT_APPROVAL, new StepCancellationException("cancelled via manager"))
                                              .orTimeout(MANAGER_READ_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                return hasStep(world.committedLog(), ID_A, STEP_AWAIT_APPROVAL, StepStatus.CANCELLED);
            });
            // B: signalled and completed.
            world.engine().publish(new CorrelatedSignalEvent("B"));
            Polling.awaitOrFail(DEADLINE, "A to compensate and B to complete",
                                () -> hasStep(world.committedLog(), ID_A, STEP_COMPENSATE, StepStatus.COMPLETED)
                                        && hasWorkflowStatus(world.committedLog(), ID_A, WorkflowStatus.COMPLETED)
                                        && hasWorkflowStatus(world.committedLog(), ID_B, WorkflowStatus.COMPLETED));
            // Settle: the manager answers A and B from history only once the projector has applied their terminal
            // records, so wait for it to agree with the log on those two before comparing anything else.
            Polling.awaitOrFail(DEADLINE, "the manager to answer A and B as COMPLETED",
                                () -> statusOf(world, ID_A) == WorkflowStatus.COMPLETED
                                        && statusOf(world, ID_B) == WorkflowStatus.COMPLETED);

            var comparisons = new java.util.ArrayList<Comparison>();
            for (Probe probe : queries(world.committedLog())) {
                var states = world.engine().managerStates(probe.query());
                int size = world.engine().managerSize(probe.query());
                Invariants.assertManagerOneStatePerId(states, size);
                var ids = new TreeSet<String>();
                states.forEach(s -> ids.add(s.workflowId()));
                comparisons.add(new Comparison(probe.name(), probe.expected(), ids, size));
            }

            boolean snapshotUnchanged = parkedBStatus == WorkflowStatus.STARTED
                    && parkedB.workflowStatus() == WorkflowStatus.STARTED
                    && parkedB.workflowStepNames().equals(parkedBSteps)
                    && parkedBSteps.equals(List.of(STEP_AWAIT_APPROVAL));

            var detachedA = world.engine().managerSingleState(WorkflowStateQuery.byWorkflowId(ID_A));
            var historyA = FutureUtils.joinAndUnwrap(world.engine().historyRepository().findById(ID_A));
            return new Outcome(List.copyOf(comparisons),
                               snapshotUnchanged,
                               detachedA == null ? List.of() : List.copyOf(detachedA.workflowStepNames()),
                               historyA.map(h -> List.copyOf(h.state().workflowStepNames())).orElse(List.of()),
                               logStepOrder(world.committedLog(), ID_A));
        }
    }

    /**
     * The probes, with their log-side expectation computed from {@link Invariants#foldLog(List)}.
     *
     * @param committedLog the settled committed log.
     * @return the probes in report order.
     */
    public static List<Probe> queries(List<EventMessage> committedLog) {
        var folds = Invariants.foldLog(committedLog);
        var all = new TreeSet<>(folds.keySet());
        return List.of(
                new Probe("all", WorkflowStateQuery.all(), all),
                new Probe("byWorkflowId(A)", WorkflowStateQuery.byWorkflowId(ID_A), Set.of(ID_A)),
                new Probe("byWorkflowId(unknown)", WorkflowStateQuery.byWorkflowId("extcancel-none"), Set.of()),
                new Probe("byWorkflowDefinitionId", WorkflowStateQuery.byWorkflowDefinitionId(DEFINITION), all),
                new Probe("byWorkflowDefinitionId(other version)",
                          WorkflowStateQuery.byWorkflowDefinitionId(
                                  VersionedType.of(ExternalCancelCompensationWorkflow.WORKFLOW_NAME, "9.9.9")),
                          Set.of()),
                new Probe("byWorkflowStatus(STARTED)", WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED),
                          select(folds, f -> f.status == WorkflowStatus.STARTED)),
                new Probe("byWorkflowStatus(COMPLETED)",
                          WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.COMPLETED),
                          select(folds, f -> f.status == WorkflowStatus.COMPLETED)),
                new Probe("byWorkflowStatus(CANCELLED)",
                          WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.CANCELLED),
                          select(folds, f -> f.status == WorkflowStatus.CANCELLED)),
                new Probe("byStep(awaitApproval)", WorkflowStateQuery.byStep(STEP_AWAIT_APPROVAL),
                          select(folds, f -> f.steps.containsKey(STEP_AWAIT_APPROVAL))),
                new Probe("byStep(compensate)", WorkflowStateQuery.byStep(STEP_COMPENSATE),
                          select(folds, f -> f.steps.containsKey(STEP_COMPENSATE))),
                new Probe("byStepStatus(awaitApproval, STARTED)",
                          WorkflowStateQuery.byStepStatus(STEP_AWAIT_APPROVAL, StepStatus.STARTED),
                          select(folds, f -> f.steps.get(STEP_AWAIT_APPROVAL) == StepStatus.STARTED)),
                new Probe("byStepStatus(awaitApproval, COMPLETED)",
                          WorkflowStateQuery.byStepStatus(STEP_AWAIT_APPROVAL, StepStatus.COMPLETED),
                          select(folds, f -> f.steps.get(STEP_AWAIT_APPROVAL) == StepStatus.COMPLETED)),
                new Probe("byStepStatus(awaitApproval, CANCELLED)",
                          WorkflowStateQuery.byStepStatus(STEP_AWAIT_APPROVAL, StepStatus.CANCELLED),
                          select(folds, f -> f.steps.get(STEP_AWAIT_APPROVAL) == StepStatus.CANCELLED)),
                new Probe("byPayloadValue(orderId, B)", WorkflowStateQuery.byPayloadValue("orderId", "B"),
                          Set.of(ID_B)),
                new Probe("byPayloadValue(orderId, missing)",
                          WorkflowStateQuery.byPayloadValue("orderId", "missing"), Set.of()),
                new Probe("byPayloadValue(absent key, null)", WorkflowStateQuery.byPayloadValue("nope", null),
                          Set.of()),
                new Probe("status STARTED and payload orderId C",
                          WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED).payloadValue("orderId", "C"),
                          select(folds, f -> f.status == WorkflowStatus.STARTED).contains(ID_C)
                                  ? Set.of(ID_C) : Set.of()),
                new Probe("status COMPLETED and step compensate",
                          WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.COMPLETED).step(STEP_COMPENSATE),
                          select(folds, f -> f.status == WorkflowStatus.COMPLETED
                                  && f.steps.containsKey(STEP_COMPENSATE))),
                new Probe("byVersion(unknown change, default)",
                          WorkflowStateQuery.byVersion("no-such-change", Version.DEFAULT_VERSION), all),
                new Probe("byVersionMigration(unknown change)",
                          WorkflowStateQuery.byVersionMigration("no-such-change"), Set.of())
        );
    }

    private static Set<String> select(Map<String, Invariants.LogFold> folds,
                                      Predicate<Invariants.LogFold> predicate) {
        var selected = new TreeSet<String>();
        for (var entry : folds.entrySet()) {
            if (predicate.test(entry.getValue())) {
                selected.add(entry.getKey());
            }
        }
        return selected;
    }

    @Nullable
    private static WorkflowStatus statusOf(SimulationWorld world, String workflowId) {
        WorkflowState state = world.engine().managerSingleState(WorkflowStateQuery.byWorkflowId(workflowId));
        return state == null ? null : state.workflowStatus();
    }

    private static List<String> logStepOrder(List<EventMessage> committedLog, String workflowId) {
        var order = new LinkedHashMap<String, Boolean>();
        for (EventMessage e : committedLog) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                    && MetadataUtils.getStepStatus(e.metadata()).isPresent()) {
                order.putIfAbsent(MetadataUtils.getStepName(e.metadata()), Boolean.TRUE);
            }
        }
        return List.copyOf(order.keySet());
    }

    private static boolean hasStep(List<EventMessage> committedLog, String workflowId, String stepName,
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
}
