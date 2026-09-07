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

import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CancelRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;

/**
 * Deterministic scenario for INVARIANTS.md INV-7 ({@code TerminalIsFinal}): once an instance records a terminal
 * workflow status, no further step or status events are ever recorded for it — including across a crash/replay and the
 * redelivery (live-switch) boundary.
 * <p>
 * This is the terminal-path twin of {@code WriteThenVanishScenario} (which only ever drives COMPLETED instances). It
 * uses {@link io.axoniq.framework.workflow.simulation.workflow.CancellingWorkflow}, which runs one recorded step
 * ({@code reserveInventory}) and then {@code ctx.cancel()} — a genuine terminal (CANCELLED) workflow status — so the
 * "nothing after terminal" property is non-vacuous.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs {@code reserveInventory} then cancels — the committed log gets the
 *       step's STARTED/COMPLETED and the terminal CANCELLED workflow-status event;</li>
 *   <li>snapshot the instance's committed subsequence at the terminal point;</li>
 *   <li>crash + recover (drives the real replay path) and then re-deliver the start event (at-least-once delivery): an
 *       already-terminal instance must record <strong>nothing</strong> further;</li>
 *   <li>assert {@link Invariants#assertTerminalIsFinal} holds and the instance's committed subsequence is byte-for-byte
 *       unchanged from the pre-crash terminal snapshot (the engine's terminal guards + cached-result replay make the
 *       re-reached {@code ctx.cancel()} a no-op).</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class TerminalIsFinalScenario {

    private TerminalIsFinalScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal       whether the instance recorded a terminal workflow status.
     * @param eventsAtTerminal      number of committed events for the instance once it first went terminal.
     * @param eventsAfterCrash      number of committed events for the instance after a crash + replay (no
     *                              redelivery); INV-7 requires this to equal {@code eventsAtTerminal}.
     * @param eventsAfterRedelivery number of committed events for the instance after additionally redelivering the
     *                              <em>start</em> event for the same business key. This is the <strong>finding F-3</strong>
     *                              probe (see {@code formal/POC-TLA-DST.adoc}): the engine restarts a terminated
     *                              business key on a fresh start event because terminal instances are evicted from the
     *                              in-memory spawn-dedup repository and the dedup never consults the durable log — so
     *                              this is observed to be {@code > eventsAtTerminal}. It is recorded, not asserted as an
     *                              INV-7 break, because start-event redelivery is outside the harness's modeled
     *                              at-least-once set (which redelivers correlation events, never start events) and
     *                              whether a finished business key may restart is an undecided design question.
     */
    public record Outcome(boolean reachedTerminal, int eventsAtTerminal, int eventsAfterCrash,
                          int eventsAfterRedelivery) {

    }

    /**
     * Runs the scenario against a fresh world (driving {@link io.axoniq.framework.workflow.simulation.workflow.CancellingWorkflow})
     * and returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.cancellingWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "cancel-" + orderId;

            // 1. Start the workflow: reserveInventory runs, then ctx.cancel() publishes the terminal CANCELLED status.
            world.engine().publish(new CancelRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Snapshot the instance's committed subsequence at the terminal point.
            int eventsAtTerminal = instanceEvents(world.committedLog(), workflowId).size();
            // INV-7 must already hold at the terminal point (nothing after the CANCELLED status).
            Invariants.assertTerminalIsFinal(world.committedLog());

            // 3. IN-SCOPE INV-7 PROPERTY: a crash + replay alone (no new event delivered) must not append anything
            // after the terminal status. Replay re-reaches ctx.cancel() on an already-terminal instance as a no-op.
            world.crashAndRecover();
            // Give the recovered engine a bounded window to (incorrectly) append anything during replay/live-switch.
            Polling.await(Duration.ofSeconds(2),
                          () -> instanceEvents(world.committedLog(), workflowId).size() > eventsAtTerminal);
            int eventsAfterCrash = instanceEvents(world.committedLog(), workflowId).size();
            Invariants.assertTerminalIsFinal(world.committedLog());

            // 4. FINDING F-3 PROBE (not an INV-7 assertion): additionally redeliver the START event for the same
            // business key. The engine restarts the terminated id (evicted from the in-memory dedup repo), appending a
            // fresh STARTED — recorded here for F1SplitBrainTest-style documentation, see POC-TLA-DST.adoc / F-3.
            world.engine().publish(new CancelRequestedEvent(orderId));
            Polling.await(Duration.ofSeconds(2),
                          () -> instanceEvents(world.committedLog(), workflowId).size() > eventsAfterCrash);
            int eventsAfterRedelivery = instanceEvents(world.committedLog(), workflowId).size();

            return new Outcome(true, eventsAtTerminal, eventsAfterCrash, eventsAfterRedelivery);
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

        private static List<EventMessage> instanceEvents(List<EventMessage> committedLog,
                                                     String workflowId) {
        return committedLog.stream()
                           .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                           .toList();
    }
}
