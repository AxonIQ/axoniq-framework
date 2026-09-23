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

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.LogCapture;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.DriftWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.FlakyBodyWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.RollingDeployWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.ApprovalGrantedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DriftRequestedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.DriftSignalEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.OrderPlacedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PaymentConfirmedEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RollingDeployOrderEvent;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Restarts the engine after each way a workflow body can stop without reaching a terminal status, and checks that the
 * instance is re-driven to its terminal status afterwards.
 * <p>
 * The four arms:
 * <ol>
 *   <li><strong>Graceful shutdown.</strong> An instance parks on its approval wait, the engine shuts down gracefully
 *       (the shutdown interrupts the parked driver), and a new engine starts over the same event store and the same
 *       processor token store, with nothing frozen or pinned. A late approval after the restart must complete it.</li>
 *   <li><strong>Replay drift pause.</strong> An instance parks, a structurally divergent body is deployed and pauses
 *       it on replay drift, then the original body is restored with a graceful restart and the wake completes it.</li>
 *   <li><strong>Recoverable exception pause.</strong> A body throws an I/O failure between its steps once; the
 *       instance pauses, and after a restart it completes without re-running its first step.</li>
 *   <li><strong>Append rejection.</strong> A foreign write for a live instance makes its next append fail its
 *       condition. The rejected execution leaves this node, and the owner does not change; after a graceful restart the
 *       instance is restored from its own history and must complete, with one terminal record.</li>
 *   <li><strong>Interrupted step start.</strong> The engine shuts down while a step's STARTED commit is still pending,
 *       under a body that fails the workflow on any step failure. The interrupted start must pause the workflow, not
 *       fail it, and the instance must complete after the restart.</li>
 * </ol>
 * Every arm reports its landing evidence (the interrupt, the drift warning, the thrown body, the fence, the stalled
 * commit), whether the instance stayed registered, the processor token around the exit, and the outcome after the
 * restart.
 * <p>
 * The processor token is reported, not asserted: a claim restores every non-terminal instance from its own durable
 * history, so recovery does not depend on the token staying below a parked or paused instance, and it does not.
 * <p>
 * Bridge: the recovery facet of INV-3 {@code CommittedHistorySurvivesCrash} and INV-5 {@code EventuallyTerminates};
 * the {@code Holdback} model's shutdown configurations under {@code specs/workflow/formal/tla/sharding/}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class RecoveryAfterNonTerminalExitScenario {

    private static final String EXECUTION_LOGGER =
            "io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowExecution";
    private static final Duration DEADLINE = Duration.ofSeconds(10);
    private static final Duration ABSENCE_WINDOW = Duration.ofSeconds(2);

    private RecoveryAfterNonTerminalExitScenario() {
    }

    /**
     * What one arm observed.
     *
     * @param faultLanded          whether the arm's fault provably fired (see each arm for its evidence)
     * @param liveAfterPause       whether the instance was still registered, and not re-driven, just before the
     *                             restart
     * @param tokenBeforeRestart   the lowest stored processor token position just before the restart, or -1
     * @param tokenAfterShutdown   the lowest stored processor token position after the old engine stopped, or -1
     * @param instanceHeadIndex    the index in the durable log of the instance's latest event before the restart
     * @param restoredAfterRestart whether the restarted engine held the instance live, or it reached a terminal status
     * @param terminalStatus       the instance's terminal status at the end, or {@code null}
     * @param terminalRecords      committed terminal workflow records for the instance (must never exceed 1)
     * @param stepBeforePauseRuns  how often a step that completed before the pause ran its side effect (1: replayed,
     *                             not re-run)
     * @param stepAfterPauseRuns   how often the side effect of a step that only runs after the restart ran (one run
     *                             per attempt the workflow defines)
     */
    public record Outcome(boolean faultLanded,
                          boolean liveAfterPause,
                          long tokenBeforeRestart,
                          long tokenAfterShutdown,
                          int instanceHeadIndex,
                          boolean restoredAfterRestart,
                          @Nullable WorkflowStatus terminalStatus,
                          int terminalRecords,
                          int stepBeforePauseRuns,
                          int stepAfterPauseRuns) {

    }

    /**
     * Graceful shutdown of a parked instance, then a restart and a late wake.
     * <p>
     * Landing evidence: the execution the old engine held was running before the shutdown, and after it reports its
     * driver stopped while its status is still non-terminal.
     *
     * @param seed seed for the world's deterministic id source
     * @return the observed outcome
     */
    public static Outcome gracefulShutdown(long seed) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.rollingDeployWorkflowV1(effects))) {
            String workflowId = "deploy-A";
            world.engine().publish(new RollingDeployOrderEvent("A"));
            awaitStep(world, workflowId, RollingDeployWorkflow.STEP_AWAIT_APPROVAL, StepStatus.STARTED);
            Optional<WorkflowExecution> parked = world.engine().liveExecution(workflowId);
            boolean drivingBeforeShutdown = parked.map(WorkflowExecution::isRunning).orElse(false);
            long tokenBefore = tokenPosition(world);
            int headIndex = headIndex(world, workflowId);

            world.restartGracefully();

            // The driver the old engine held was running and parked before the shutdown, and stopped after it.
            boolean driverStopped = parked.map(execution -> !execution.isRunning()).orElse(false);
            boolean stillNonTerminal = parked.map(execution -> !execution.state().workflowStatus().isTerminal())
                                             .orElse(false);
            long tokenAfter = tokenPosition(world);
            boolean restored = awaitRestored(world, workflowId);
            awaitWaitReRegistered(world);

            world.engine().publish(new ApprovalGrantedEvent("A"));
            Polling.await(DEADLINE, () -> terminalStatus(world.committedLog(), workflowId) != null);
            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new Outcome(drivingBeforeShutdown && driverStopped && stillNonTerminal, parked.isPresent(),
                               tokenBefore, tokenAfter,
                               headIndex, restored, terminalStatus(world.committedLog(), workflowId),
                               terminalRecords(world.committedLog(), workflowId),
                               effects.count(workflowId, RollingDeployWorkflow.STEP_RESERVE),
                               effects.count(workflowId, RollingDeployWorkflow.STEP_FULFILL));
        }
    }

    /**
     * Replay drift pause under a divergent body, then a graceful restart under the original body and the wake.
     * <p>
     * Landing evidence: the engine warns that the instance paused due to replay drift, and the divergent step never
     * ran.
     *
     * @param seed            seed for the world's deterministic id source
     * @param wakeDuringPause {@code true} delivers the wake while the instance is paused and not after the restart,
     *                        {@code false} delivers it after the restart
     * @return the observed outcome
     */
    public static Outcome driftPause(long seed, boolean wakeDuringPause) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.driftWorkflowV1(effects));
             var executionLog = LogCapture.attach(EXECUTION_LOGGER)) {
            String workflowId = "drift-A";
            world.engine().publish(new DriftRequestedEvent("A"));
            awaitStep(world, workflowId, DriftWorkflow.STEP_AWAIT_SIGNAL, StepStatus.STARTED);

            world.crashAndRecoverWith(List.of(EngineInstance.driftWorkflowV2(effects)));
            Polling.await(DEADLINE, () -> executionLog.messages().stream()
                                                      .anyMatch(m -> m.contains("paused due to replay drift")));
            boolean driftLogged = executionLog.messages().stream()
                                              .anyMatch(m -> m.contains("paused due to replay drift"));
            boolean divergentStepSkipped = effects.count(workflowId, DriftWorkflow.STEP_REPACKAGE) == 0;
            Polling.await(ABSENCE_WINDOW, () -> !world.engine().liveWorkflowIds().contains(workflowId));
            boolean liveAfterPause = world.engine().liveWorkflowIds().contains(workflowId);
            if (wakeDuringPause) {
                world.engine().publish(new DriftSignalEvent("A"));
                publishUnrelatedTraffic(world);
            }
            long tokenBefore = tokenPosition(world);
            int headIndex = headIndex(world, workflowId);

            world.restartGracefullyWith(List.of(EngineInstance.driftWorkflowV1(effects)));
            long tokenAfter = tokenPosition(world);
            boolean restored = awaitRestored(world, workflowId);
            awaitWaitReRegistered(world);

            if (!wakeDuringPause) {
                world.engine().publish(new DriftSignalEvent("A"));
            }
            Polling.await(DEADLINE, () -> terminalStatus(world.committedLog(), workflowId) != null);
            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new Outcome(driftLogged && divergentStepSkipped, liveAfterPause, tokenBefore, tokenAfter,
                               headIndex, restored, terminalStatus(world.committedLog(), workflowId),
                               terminalRecords(world.committedLog(), workflowId),
                               effects.count(workflowId, DriftWorkflow.STEP_RESERVE_INVENTORY),
                               effects.count(workflowId, DriftWorkflow.STEP_CHARGE_PAYMENT));
        }
    }

    /**
     * A recoverable exception thrown by the body once, then a restart.
     * <p>
     * Landing evidence: the body reached its throwing check exactly once before the restart, and its second step did
     * not run.
     *
     * @param seed     seed for the world's deterministic id source
     * @param graceful {@code true} restarts gracefully, {@code false} crashes and recovers with the token pinned
     * @return the observed outcome
     */
    public static Outcome recoverableExceptionPause(long seed, boolean graceful) {
        var effects = new CountingEffects();
        var workflow = new FlakyBodyWorkflow(effects);
        var registration = new EngineInstance.WorkflowRegistration(
                FlakyBodyWorkflow.WORKFLOW_NAME, RollingDeployOrderEvent.class, "flaky-", workflow::execute);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "flaky-A";
            world.engine().publish(new RollingDeployOrderEvent("A"));
            Polling.await(DEADLINE, () -> effects.count(workflowId, FlakyBodyWorkflow.BODY_CHECK) >= 1);
            boolean thrownOnce = effects.count(workflowId, FlakyBodyWorkflow.BODY_CHECK) == 1
                    && effects.count(workflowId, FlakyBodyWorkflow.STEP_FULFILL) == 0;
            // Unrelated traffic after the pause: shows whether the paused instance holds the checkpoint back, and
            // that nothing re-drives it before the restart.
            publishUnrelatedTraffic(world);
            boolean liveAfterPause = world.engine().liveWorkflowIds().contains(workflowId)
                    && terminalStatus(world.committedLog(), workflowId) == null
                    && effects.count(workflowId, FlakyBodyWorkflow.BODY_CHECK) == 1;
            long tokenBefore = tokenPosition(world);
            int headIndex = headIndex(world, workflowId);

            if (graceful) {
                world.restartGracefully();
            } else {
                world.crashAndRecover();
            }
            long tokenAfter = tokenPosition(world);
            boolean restored = awaitRestored(world, workflowId);
            Polling.await(DEADLINE, () -> terminalStatus(world.committedLog(), workflowId) != null);
            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new Outcome(thrownOnce, liveAfterPause, tokenBefore, tokenAfter, headIndex, restored,
                               terminalStatus(world.committedLog(), workflowId),
                               terminalRecords(world.committedLog(), workflowId),
                               effects.count(workflowId, FlakyBodyWorkflow.STEP_RESERVE),
                               effects.count(workflowId, FlakyBodyWorkflow.STEP_FULFILL));
        }
    }

    /**
     * An append rejection with the owner unchanged, then a graceful restart and the wake the instance needs. The
     * rejected execution is expected to leave this node; the restart restores it from its own history.
     * <p>
     * Landing evidence: the store reports the instance the foreign write targeted, and the engine warned that the
     * append was rejected.
     *
     * @param seed seed for the world's deterministic id source
     * @return the observed outcome
     */
    public static Outcome appendRejection(long seed) {
        var effects = new CountingEffects();
        try (var world = new SimulationWorld(seed, EngineInstance.orderWorkflow(effects));
             var executionLog = LogCapture.attach(EXECUTION_LOGGER)) {
            String workflowId = "order-A";
            // Fence the wait's STARTED: no side effect is left in doubt, so the only question is recovery.
            world.eventStore().armForeignWriteBeforeCommitOf(OrderWorkflow.STEP_AWAIT_CONFIRMATION, StepStatus.STARTED);
            world.engine().publish(new OrderPlacedEvent("A"));
            Polling.awaitOrFail(DEADLINE, "the fence to consume the step's commit",
                                () -> !world.eventStore().isFenceArmed());
            boolean fenced = world.eventStore().fencedInstance().map(workflowId::equals).orElse(false);
            Polling.await(DEADLINE, () -> executionLog.messages().stream()
                                                      .anyMatch(m -> m.contains(workflowId) && m.contains("rejected")));
            boolean rejectionLogged = executionLog.messages().stream()
                                                  .anyMatch(m -> m.contains(workflowId) && m.contains("rejected"));
            publishUnrelatedTraffic(world);
            boolean liveAfterPause = world.engine().liveWorkflowIds().contains(workflowId)
                    && terminalStatus(world.committedLog(), workflowId) == null;
            long tokenBefore = tokenPosition(world);
            int headIndex = headIndex(world, workflowId);

            world.restartGracefully();
            long tokenAfter = tokenPosition(world);
            boolean restored = awaitRestored(world, workflowId);
            // The rejected wait STARTED is not durable, so the restored body records it now; wake it after that.
            awaitStep(world, workflowId, OrderWorkflow.STEP_AWAIT_CONFIRMATION, StepStatus.STARTED);
            awaitWaitReRegistered(world);
            world.engine().publish(new PaymentConfirmedEvent("A"));
            Polling.await(DEADLINE, () -> {
                world.advanceTime(Duration.ofSeconds(30));
                return terminalStatus(world.committedLog(), workflowId) != null;
            });
            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new Outcome(fenced && rejectionLogged, liveAfterPause, tokenBefore, tokenAfter, headIndex,
                               restored, terminalStatus(world.committedLog(), workflowId),
                               terminalRecords(world.committedLog(), workflowId),
                               effects.count(workflowId, OrderWorkflow.STEP_CHARGE_PAYMENT),
                               effects.count(workflowId, OrderWorkflow.STEP_SHIP_ORDER));
        }
    }

    /**
     * A graceful shutdown while a step's STARTED commit is still pending, then a restart.
     * <p>
     * Landing evidence: the store holds the stalled STARTED commit, the step's first attempt never ran its side
     * effect, and the driver the old engine held stopped with the status still non-terminal.
     * <p>
     * On this branch the engine's shutdown guard already blocks a terminal publish while it stops, so this arm passes
     * whether the interrupted start reaches the body as a cancellation or as an interrupt. It guards the pause, not the
     * exception type.
     *
     * @param seed seed for the world's deterministic id source
     * @return the observed outcome
     */
    public static Outcome interruptedStepStart(long seed) {
        var effects = new CountingEffects();
        var workflow = new FlakyBodyWorkflow(effects);
        var registration = new EngineInstance.WorkflowRegistration(
                FlakyBodyWorkflow.WORKFLOW_NAME, RollingDeployOrderEvent.class, "flaky-",
                workflow::executeFailingOnAnyStepFailure);
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "flaky-A";
            world.eventStore().armStallCommitFor(FlakyBodyWorkflow.STEP_FULFILL, StepStatus.STARTED);
            world.engine().publish(new RollingDeployOrderEvent("A"));
            Polling.await(DEADLINE, () -> world.eventStore().stalledCommits() >= 1);
            Optional<WorkflowExecution> parked = world.engine().liveExecution(workflowId);
            boolean stalled = world.eventStore().stalledCommits() == 1
                    && effects.count(workflowId, FlakyBodyWorkflow.STEP_FULFILL) == 0;
            boolean liveBeforeShutdown = parked.map(WorkflowExecution::isRunning).orElse(false);
            long tokenBefore = tokenPosition(world);
            int headIndex = headIndex(world, workflowId);

            world.restartGracefully();

            boolean driverStopped = parked.map(execution -> !execution.isRunning()).orElse(false);
            long tokenAfter = tokenPosition(world);
            boolean restored = awaitRestored(world, workflowId);
            Polling.await(DEADLINE, () -> terminalStatus(world.committedLog(), workflowId) != null);
            Invariants.assertAtMostOnceRecording(world.committedLog());
            return new Outcome(stalled && driverStopped, liveBeforeShutdown, tokenBefore, tokenAfter, headIndex,
                               restored, terminalStatus(world.committedLog(), workflowId),
                               terminalRecords(world.committedLog(), workflowId),
                               effects.count(workflowId, FlakyBodyWorkflow.STEP_RESERVE),
                               effects.count(workflowId, FlakyBodyWorkflow.STEP_FULFILL));
        }
    }

    private static void publishUnrelatedTraffic(SimulationWorld world) {
        for (int i = 0; i < 5; i++) {
            world.engine().publish(new ApprovalGrantedEvent("unrelated-" + i));
        }
        Polling.await(ABSENCE_WINDOW, () -> false);
    }

    private static boolean awaitRestored(SimulationWorld world, String workflowId) {
        return Polling.await(DEADLINE, () -> world.engine().liveWorkflowIds().contains(workflowId)
                || terminalStatus(world.committedLog(), workflowId) != null);
    }

    /**
     * A wake delivered before the restored body re-registers its wait is lost, so wait for the rescheduled timeout.
     */
    private static void awaitWaitReRegistered(SimulationWorld world) {
        Polling.await(DEADLINE, () -> world.scheduler().pendingTasks() > 0);
    }

    private static void awaitStep(SimulationWorld world, String workflowId, String stepName, StepStatus status) {
        Polling.awaitOrFail(DEADLINE, workflowId + " to record " + stepName + " " + status,
                            () -> world.committedLog().stream().anyMatch(
                                    e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                                            && stepName.equals(MetadataUtils.getStepName(e.metadata()))
                                            && MetadataUtils.getStepStatus(e.metadata())
                                                            .map(status::equals).orElse(false)));
    }

    private static long tokenPosition(SimulationWorld world) {
        TrackingToken token = world.tokenStore().currentToken();
        return token == null ? -1 : token.position().orElse(-1);
    }

    private static int headIndex(SimulationWorld world, String workflowId) {
        List<TaggedEventMessage<?>> log = world.eventStore().committedTaggedEvents();
        int head = -1;
        for (int i = 0; i < log.size(); i++) {
            var metadata = log.get(i).event().metadata();
            if (MetadataUtils.hasWorkflowId().test(metadata) && workflowId.equals(MetadataUtils.getWorkflowId(metadata))) {
                head = i;
            }
        }
        return head;
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

    private static int terminalRecords(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                                           .map(WorkflowStatus::isTerminal).orElse(false))
                                 .count();
    }
}
