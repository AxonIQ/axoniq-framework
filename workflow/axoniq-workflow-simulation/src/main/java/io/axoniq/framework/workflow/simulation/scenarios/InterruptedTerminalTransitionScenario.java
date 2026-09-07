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

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.FailingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.FailRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Interrupts the workflow driver in the window between a terminal event committing and that event being delivered back
 * to the execution that wrote it, and counts the terminal records the instance ends up with.
 * <p>
 * A terminal transition publishes the event and then waits for its own state to turn terminal
 * ({@code SimpleWorkflowExecution.transitionToTerminalState}). That wait is a {@code taskQueue.take()}, so an interrupt
 * ends it — and the transition only logs that, leaving the projected state non-terminal. The terminal primitive then
 * throws, and the handler that catches it re-checks {@code !state().workflowStatus().isTerminal()} and publishes the
 * terminal event a second time.
 * <p>
 * The append condition cannot stop that second append. Both appends come from the same execution on the same chained
 * marker, which the first one advanced; a rejection means a <em>foreign</em> writer, and there is none here. The
 * condition prevents a second writer, not a repeated fact.
 * <p>
 * The window is real but short, so the scenario takes several attempts over fresh worlds and reports the first one that
 * caught it. An attempt that misses the window (the execution already finished, or already evolved to terminal) is not
 * a result either way and is retried.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class InterruptedTerminalTransitionScenario {

    private static final Duration ATTEMPT_BUDGET = Duration.ofSeconds(15);
    private static final Duration SETTLE_WINDOW = Duration.ofSeconds(2);
    private static final Duration INTERRUPT_WINDOW = Duration.ofSeconds(3);

    private InterruptedTerminalTransitionScenario() {
    }

    /**
     * Outcome of the scenario.
     *
     * @param attempts             attempts made before one caught the window.
     * @param caughtWindow         whether any attempt interrupted the driver inside the window.
     * @param terminalRecords      terminal workflow records the instance ended with (1 = the transition survived the
     *                             interrupt, 2 = the durable log holds the same terminal fact twice).
     * @param distinctTerminalTypes distinct terminal event types among those records; 1 means the same fact twice.
     */
    public record Outcome(int attempts,
                          boolean caughtWindow,
                          int terminalRecords,
                          int distinctTerminalTypes) {

    }

    /**
     * Runs up to {@code maxAttempts} attempts, each on a fresh world, and returns the first that caught the window.
     *
     * @param seed        seed for the deterministic id sources.
     * @param orderId     business key of the failing instance.
     * @param maxAttempts how many fresh worlds to try before giving up on the window.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId, int maxAttempts) {
        Outcome missed = new Outcome(maxAttempts, false, 0, 0);
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            var outcome = attempt(seed + attempt, orderId + "-" + attempt, attempt);
            if (outcome.caughtWindow()) {
                return outcome;
            }
            missed = outcome;
        }
        return missed;
    }

        private static Outcome attempt(long seed, String orderId, int attempt) {
        var effects = new CountingEffects();
        var workflowId = "fail-" + orderId;
        try (var world = new SimulationWorld(seed, EngineInstance.failingWorkflowNoRetry(effects))) {
            world.engine().publish(new FailRequestedEvent(orderId));

            // The window opens the moment the terminal event is durable and closes when it is evolved back into the
            // execution's own state. It is short, so the loop spins on it rather than polling at an interval, and
            // interrupts every time it sees the two disagree.
            Polling.awaitOrFail(ATTEMPT_BUDGET, "the failing step to record its outcome",
                                () -> FenceOracles.stepRecords(world.committedLog(), workflowId,
                                                               FailingWorkflow.STEP_FAILING, StepStatus.FAILED) >= 1);

            boolean caught = false;
            var deadline = System.nanoTime() + INTERRUPT_WINDOW.toNanos();
            while (System.nanoTime() < deadline) {
                var live = world.engine().liveExecution(workflowId).orElse(null);
                if (live == null) {
                    break;
                }
                int terminal = terminalRecords(world.committedLog(), workflowId);
                if (terminal >= 2) {
                    break;
                }
                // Only once the first terminal event is durable: interrupting before that lands in the publication's
                // own wait, which stops the terminal event from being recorded at all rather than recording it twice.
                if (terminal >= 1 && !live.state().workflowStatus().isTerminal()) {
                    live.interruptWorkflowDriver();
                    caught = true;
                }
                Thread.onSpinWait();
            }
            if (!caught) {
                return new Outcome(attempt, false, terminalRecords(world.committedLog(), workflowId), 0);
            }
            Polling.await(SETTLE_WINDOW, () -> terminalRecords(world.committedLog(), workflowId) >= 2);
            return new Outcome(attempt,
                               true,
                               terminalRecords(world.committedLog(), workflowId),
                               distinctTerminalTypes(world.committedLog(), workflowId));
        }
    }

    private static int terminalRecords(List<EventMessage> committedLog, String workflowId) {
        return (int) terminalEvents(committedLog, workflowId).count();
    }

    private static int distinctTerminalTypes(List<EventMessage> committedLog, String workflowId) {
        return (int) terminalEvents(committedLog, workflowId)
                .map(event -> event.type().qualifiedName().toString())
                .distinct()
                .count();
    }

        private static java.util.stream.Stream<EventMessage> terminalEvents(List<EventMessage> committedLog,
                                                                        String workflowId) {
        return committedLog.stream()
                           .filter(event -> workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))
                                   && MetadataUtils.getWorkflowStatus(event.metadata())
                                                   .filter(WorkflowStatus::isTerminal).isPresent());
    }
}
