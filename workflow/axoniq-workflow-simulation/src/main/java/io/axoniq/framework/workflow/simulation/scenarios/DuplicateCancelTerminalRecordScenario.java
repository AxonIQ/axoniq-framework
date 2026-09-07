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
import io.axoniq.framework.workflow.simulation.workflow.CancellingWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CancelRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Deterministic scenario settling candidate finding <strong>F-13</strong> ({@code DuplicateCancelTerminalRecord}): the
 * engine's cancel path re-publishes a <strong>duplicate</strong> {@code <workflow>:CANCELLED} terminal record for one
 * {@code workflowId} when the recovered body is re-driven — a duplicate durable <em>workflow-status</em> terminal record
 * (a corruption-class finding), the cancel-path / workflow-terminal analogue of F-7 (which is the {@code modifyPayload}
 * <em>step</em>-terminal duplicate).
 * <p>
 * Verified engine mechanism (confirmed in source on {@code poc/tla_dst}):
 * <ul>
 *   <li>{@code ctx.cancel()} routes to {@code TerminateDelegate.terminate} -&gt; {@code cancelled(...)}
 *       ({@code TerminateDelegate.java:169-191}), which publishes the {@code <workflow>:CANCELLED} event
 *       <strong>directly</strong> via {@code eventSink.publish(...)} ({@code :178}) and then throws
 *       {@code WorkflowCancelledException}. There is <strong>no "already terminal" gate at this publish site</strong>
 *       (unlike {@code AbstractStepExecutor.sendStepEvent}, which refuses to publish once terminal,
 *       {@code AbstractStepExecutor.java:197-203}).</li>
 *   <li>The exception propagates to {@code SimpleWorkflowExecution.handleWorkflowException}'s
 *       {@code WorkflowCancelledException} branch ({@code :255-271}), which publishes a SECOND {@code <workflow>:CANCELLED}
 *       guarded only by the IN-MEMORY {@code if (!this.state().workflowStatus().isTerminal())} ({@code :257}).</li>
 *   <li>So whenever the engine re-drives the cancelled instance's body, it re-reaches {@code ctx.cancel()} and
 *       {@code TerminateDelegate.cancelled} re-publishes a duplicate {@code <workflow>:CANCELLED} — the only durable
 *       protection being the upstream {@code SimpleWorkflowExecution.execute()} short-circuit ({@code :149}) and the
 *       {@code WorkflowEngine.switchToLiveMode} terminal-eviction filter ({@code :158-163}), neither of which is at the
 *       publish site.</li>
 * </ul>
 * The duplicate durable terminal record reaches the committed log <strong>two ways</strong> from this one ungated publish:
 * <ol>
 *   <li><strong>Crash/replay re-drive (intermittent race):</strong> after the terminal {@code <workflow>:CANCELLED}
 *       commits, a crash + replay re-drives the recovered body when its rebuilt in-memory state has not yet reflected the
 *       committed CANCELLED at the {@code switchToLiveMode} filter; the body re-reaches {@code ctx.cancel()} and
 *       re-publishes only a SECOND, identical {@code <workflow>:CANCELLED} (a <em>pure</em> same-terminal duplicate —
 *       no second {@code <workflow>:STARTED}, no duplicate step record, because {@code publishStartWorkflow} only fires
 *       from status {@code NONE} and {@code ExecuteDelegate} gates on {@code !containsStep}). This is an intermittent
 *       crash/replay-state-timing race (~1-in-4 independent crashes on this harness, dropping sharply under heavy build
 *       load) and is exactly the shape that intermittently tripped {@code Inv7TerminalIsFinalTest.cancelledWorkflow_...}.</li>
 *   <li><strong>Crash + start-event redelivery (deterministic):</strong> after the same crash + recover, re-delivering
 *       the start event for the already-terminated business key <strong>restarts</strong> it (finding F-3: terminal
 *       instances are evicted from the in-memory spawn-dedup repo and the dedup never consults the durable log), so the
 *       engine re-runs the body, re-reaches {@code ctx.cancel()}, and {@code TerminateDelegate.cancelled} re-publishes a
 *       second {@code <workflow>:CANCELLED} for the SAME {@code workflowId} — a duplicate durable terminal record, every
 *       run. This scenario uses this <strong>deterministic</strong> trigger so the gap is pinned for a green-every-run
 *       build (no rerun-masking, no reliance on the timing race), through the <strong>shared, unchanged</strong>
 *       {@link SimulationWorld#crashAndRecover()} replay path.</li>
 * </ol>
 * Both triggers share the SAME root cause (the ungated workflow-terminal publish + no "already terminal" guard when the
 * body re-runs) and the SAME corruption (a duplicate durable {@code <workflow>:CANCELLED} for one {@code workflowId});
 * the redelivery trigger is the deterministic one, the crash/replay re-drive is the intermittent one behind the flake.
 * <p>
 * The verdict observable is the per-{@code workflowId} count of committed {@code <workflow>:CANCELLED} terminal records
 * (content-based, F-2-robust — never the global append order): a healthy instance has exactly one; an F-13 instance has
 * two.
 * <p>
 * Coverage-gap note (part of F-13): a duplicate <em>workflow-status</em> terminal record is <strong>not</strong> caught
 * by INV-2 ({@code AtMostOnceRecording}), which counts only <em>step</em>-status terminals (it skips events carrying no
 * step status). Only INV-7 ({@code TerminalIsFinal}) observes a duplicate workflow-status terminal — which is why this
 * finding surfaces through INV-7, not INV-2.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DuplicateCancelTerminalRecordScenario {

    /**
     * Bounded deadline for the instance to first reach its terminal {@code <workflow>:CANCELLED}.
     */
    private static final Duration TERMINAL_DEADLINE = Duration.ofSeconds(10);

    /**
     * Bounded window after the crash/recover + start-event redelivery for the duplicate {@code <workflow>:CANCELLED} to
     * commit before the count is read. Bounded so the scenario never hangs.
     */
    private static final Duration DUPLICATE_WINDOW = Duration.ofSeconds(5);

    private DuplicateCancelTerminalRecordScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param cancelledRecordsBeforeReDrive the per-{@code workflowId} count of committed {@code <workflow>:CANCELLED}
     *                                      terminal records after the instance first cancels and after the crash + recover
     *                                      but BEFORE the body is re-driven — exactly 1 (the single, correct terminal
     *                                      record; INV-7 holds: a crash/replay alone appends nothing after terminal).
     * @param cancelledRecordsAfterReDrive  the per-{@code workflowId} count of committed {@code <workflow>:CANCELLED}
     *                                      terminal records after the recovered body is re-driven (here via the
     *                                      deterministic F-3 start-event redelivery). F-13: this reaches 2 (a duplicate
     *                                      durable workflow-terminal record), because {@code TerminateDelegate.cancelled}
     *                                      re-publishes ungated.
     */
    public record Outcome(int cancelledRecordsBeforeReDrive, int cancelledRecordsAfterReDrive) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link CancellingWorkflow}: drives one instance to its terminal
     * {@code <workflow>:CANCELLED}, crashes + recovers via the <strong>shared, unchanged</strong>
     * {@link SimulationWorld#crashAndRecover()} replay path, then re-delivers the start event (the deterministic F-3
     * restart trigger) so the recovered body is re-driven and re-reaches {@code ctx.cancel()} — observing whether a
     * SECOND, identical {@code <workflow>:CANCELLED} is committed for the same {@code workflowId}.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance (id {@code cancel-<orderId>}).
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.cancellingWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "cancel-" + orderId;

            // 1. Drive the instance to its terminal CANCELLED: reserveInventory runs, then ctx.cancel() publishes the
            // (first, correct) <workflow>:CANCELLED via TerminateDelegate's ungated direct publish.
            world.engine().publish(new CancelRequestedEvent(orderId));
            Polling.awaitOrFail(TERMINAL_DEADLINE, "instance " + workflowId + " to reach terminal CANCELLED",
                                () -> cancelledRecordCount(world.committedLog(), workflowId) >= 1);

            // 2. Crash + recover (the shared replay path, UNCHANGED). A crash/replay ALONE appends nothing after terminal
            // (INV-7 holds for the in-scope crash/replay): the count stays 1 here.
            world.crashAndRecover();
            int beforeReDrive = cancelledRecordCount(world.committedLog(), workflowId);

            // 3. Re-deliver the START event for the already-terminated business key. The engine restarts it (F-3:
            // evicted from the in-memory spawn-dedup repo, dedup never consults the durable log), re-runs the body, and
            // re-reaches ctx.cancel() -> TerminateDelegate.cancelled re-publishes a SECOND, ungated <workflow>:CANCELLED
            // for the SAME workflowId (the F-13 duplicate durable terminal record).
            world.engine().publish(new CancelRequestedEvent(orderId));
            Polling.await(DUPLICATE_WINDOW,
                          () -> cancelledRecordCount(world.committedLog(), workflowId) >= 2);

            int afterReDrive = cancelledRecordCount(world.committedLog(), workflowId);
            return new Outcome(beforeReDrive, afterReDrive);
        }
    }

    /**
     * Counts the committed {@code <workflow>:CANCELLED} terminal workflow-status records within {@code workflowId}'s own
     * events (content-based, per-{@code workflowId}, F-2-robust — never the global append order).
     */
    private static int cancelledRecordCount(List<EventMessage> committedLog, String workflowId) {
        return (int) committedLog.stream()
                                 .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                                 .filter(e -> MetadataUtils.getWorkflowStatus(e.metadata())
                                                           .map(s -> s == WorkflowStatus.CANCELLED).orElse(false))
                                 .count();
    }
}
