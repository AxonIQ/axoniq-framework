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
import io.axoniq.framework.workflow.simulation.workflow.CustomNamedWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CustomNamedRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Deterministic scenario for INVARIANTS.md INV-22 ({@code EventNameCustomizationSound}): a workflow registered with a
 * custom {@code eventNameCustomizer} records its step/status events under the CUSTOMIZED wire names, those names are
 * stable across crash/replay (a pure function of history), and the engine still routes/replays correctly under
 * customization.
 * <p>
 * It drives {@link CustomNamedWorkflow} registered with its custom {@link CustomNamedWorkflow#customizer()} (a custom
 * namespace + workflow base name + a {@code stepCompleted("Done")} status-suffix override), via
 * {@link EngineInstance#customNamedWorkflow}. A fresh start must run the (execute-only) body to COMPLETED, recording —
 * under the custom namespace — a customized STARTED + the overridden {@code Done}-suffixed COMPLETED for each step plus
 * the customized workflow STARTED/COMPLETED status events.
 * <p>
 * Steps:
 * <ol>
 *   <li>publish the start event; the body runs to COMPLETED — the committed log gets the customized workflow-status
 *       events and the customized step events;</li>
 *   <li>snapshot the instance's customized wire names (the distinct {@code QualifiedName.fullName()} set);
 *       {@link Invariants#assertEventNameCustomizationSound} must already hold (the customized names applied, the
 *       instance reached terminal);</li>
 *   <li>crash + recover (drives the real replay path), then assert the customized-name set is byte-for-byte unchanged —
 *       replay re-ran the (already-recorded) body and emitted nothing new; the recorded customized names are
 *       reproduced IDENTICALLY (a present step yields a cached result and emits nothing).</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class EventNameCustomizationSoundScenario {

    private EventNameCustomizationSoundScenario() {
    }

    /**
     * Result of running the scenario.
     *
     * @param reachedTerminal      whether the instance reached a terminal (COMPLETED) workflow status under
     *                             customization.
     * @param customizedNamesBeforeCrash the distinct customized wire names ({@code QualifiedName.fullName()}) the
     *                                   instance's committed events carried at the terminal point. INV-22 requires these
     *                                   to be the customizer-derived expected names.
     * @param customizedNamesAfterCrash  the same distinct-name set after a crash + replay. INV-22's
     *                                   stable-across-replay facet requires this to equal
     *                                   {@code customizedNamesBeforeCrash}.
     */
    public record Outcome(boolean reachedTerminal, Set<String> customizedNamesBeforeCrash,
                          Set<String> customizedNamesAfterCrash) {

    }

    /**
     * Runs the scenario against a fresh world registering {@link CustomNamedWorkflow} with its custom customizer and
     * returns what it observed.
     *
     * @param seed    seed for the world's deterministic id source.
     * @param orderId business key for the single instance.
     * @return the observed outcome.
     */
        public static Outcome run(long seed, String orderId) {
        var registration = EngineInstance.customNamedWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration)) {
            String workflowId = "named-" + orderId;

            // 1. Start the workflow: the execute-only body runs to COMPLETED on its own, recording the customized step
            // and workflow-status events.
            world.engine().publish(new CustomNamedRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "custom-named instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));

            // 2. Snapshot the customized wire names at the terminal point. INV-22 must already hold.
            Set<String> namesBefore = customizedNames(world.committedLog(), workflowId);
            Invariants.assertEventNameCustomizationSound(world.committedLog(), "named-", Set.of(workflowId));

            // 3. IN-SCOPE INV-22 (stable across replay): a crash + replay must reproduce the IDENTICAL customized names.
            // Replay re-runs the (already-recorded) body and emits nothing new; the customized-name set is unchanged.
            world.crashAndRecover();
            Polling.await(Duration.ofSeconds(2),
                          () -> !customizedNames(world.committedLog(), workflowId).equals(namesBefore));
            Set<String> namesAfter = customizedNames(world.committedLog(), workflowId);
            Invariants.assertEventNameCustomizationSound(world.committedLog(), "named-", Set.of(workflowId));

            return new Outcome(true, namesBefore, namesAfter);
        }
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }

    /**
     * Returns the distinct customized wire names ({@code QualifiedName.fullName()}, i.e. namespace + local name) carried
     * by the instance's committed events.
     */
        private static Set<String> customizedNames(List<EventMessage> committedLog, String workflowId) {
        Set<String> names = new TreeSet<>();
        for (EventMessage event : committedLog) {
            if (workflowId.equals(MetadataUtils.getWorkflowId(event.metadata()))) {
                names.add(event.type().qualifiedName().fullName());
            }
        }
        return names;
    }
}
