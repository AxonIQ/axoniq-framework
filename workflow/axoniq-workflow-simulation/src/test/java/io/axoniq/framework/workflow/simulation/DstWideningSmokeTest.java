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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.runtime.util.Buggify;
import io.axoniq.framework.workflow.simulation.harness.DstSimulation;
import io.axoniq.framework.workflow.simulation.harness.SimulationConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 — per-PR pins for the widening axes:
 * <ul>
 *   <li><b>BUGGIFY</b>: the smoke workload stays invariant-green with both in-engine fault points active at a real
 *       probability, and the points actually fire (not dead code). Deactivation is pinned in a {@code finally} —
 *       the seam is JVM-global.</li>
 *   <li><b>Swarm</b>: {@code SimulationConfig.swarm(seed)} is a pure function of the seed (same seed, same shape)
 *       and one swarm-shaped world runs invariant-green end to end.</li>
 *   <li><b>DUPLICATED_APPEND</b>: arming the store records a commit twice in the durable recovery log and the
 *       engine's crash+recovery replay stays invariant-green over the duplicated event (replay-idempotence).</li>
 * </ul>
 * The heavy exploration lives in the fuzz-tagged campaigns; these are the fast per-PR existence proofs.
 */
class DstWideningSmokeTest {

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void buggifyActive_smokeStaysGreen_andPointsFire() {
        Buggify.activate(7L, 0.25);
        java.util.Map<String, Integer> fired;
        try {
            new DstSimulation(SimulationConfig.smoke(44L)).run();
        } finally {
            fired = Buggify.deactivate();
        }
        assertThat(fired.values().stream().mapToInt(Integer::intValue).sum())
                .as("the BUGGIFY points actually perturbed scheduling during the run: %s", fired)
                .isPositive();
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void duplicatedAppend_replayStaysIdempotent_acrossCrashRecovery() {
        var registration = io.axoniq.framework.workflow.simulation.harness.EngineInstance.cancellingWorkflow(
                new io.axoniq.framework.workflow.simulation.workflow.CountingEffects());
        try (var world = new io.axoniq.framework.workflow.simulation.harness.SimulationWorld(9L, registration)) {
            String workflowId = "cancel-dup-A";
            // Arm BEFORE the first commit so the duplicated event is in the durable recovery log from the start.
            world.eventStore().armDuplicateNextCommit();
            world.engine().publish(new io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CancelRequestedEvent(
                    "dup-A"));
            io.axoniq.framework.workflow.simulation.harness.Polling.awaitOrFail(
                    java.time.Duration.ofSeconds(10), "instance to reach a terminal workflow status",
                    () -> world.committedLog().stream().anyMatch(e ->
                            workflowId.equals(io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowId(e.metadata()))
                                    && io.axoniq.framework.workflow.runtime.util.MetadataUtils.getWorkflowStatus(e.metadata())
                                                                                    .map(Enum::name)
                                                                                    .filter(n -> n.equals("CANCELLED")
                                                                                            || n.equals("COMPLETED")
                                                                                            || n.equals("FAILED"))
                                                                                    .isPresent()));
            // The fault landed: some event exists twice in the raw durable recovery log (identifier-level dup;
            // the duplicated commit may be the external trigger, which the workflow-filtered render hides).
            var tagged = world.eventStore().committedTaggedEvents();
            long distinctIdentifiers = tagged.stream().map(t -> t.event().identifier()).distinct().count();
            assertThat((long) tagged.size())
                    .as("the duplicated append is present in the durable recovery log")
                    .isGreaterThan(distinctIdentifiers);
            // Crash + recover over the duplicated log: replay must be idempotent (no invariant break, no growth).
            int before = world.committedLog().size();
            world.crashAndRecover();
            io.axoniq.framework.workflow.simulation.invariants.Invariants.assertTerminalIsFinal(world.committedLog());
            assertThat(world.committedLog().size())
                    .as("crash+recovery replay over the duplicated durable event appends nothing")
                    .isEqualTo(before);
        }
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void swarmConfig_isAPureFunctionOfSeed_andRunsGreen() {
        var a = SimulationConfig.swarm(5L);
        var b = SimulationConfig.swarm(5L);
        assertThat(a.workflowCount()).isEqualTo(b.workflowCount());
        assertThat(a.maxSteps()).isEqualTo(b.maxSteps());
        assertThat(a.faultProbability()).isEqualTo(b.faultProbability());
        assertThat(a.faultKinds()).isEqualTo(b.faultKinds());
        assertThat(a.deterministicCarrier()).isEqualTo(b.deterministicCarrier());
        assertThat(a.interleavingSeed()).isEqualTo(b.interleavingSeed());

        var result = new DstSimulation(a).run();
        assertThat(result.committedLog()).isNotEmpty();
    }
}
