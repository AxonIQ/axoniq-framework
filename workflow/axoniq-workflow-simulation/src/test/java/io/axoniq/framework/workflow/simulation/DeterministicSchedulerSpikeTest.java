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

import io.axoniq.framework.workflow.runtime.test.fakes.DeterministicVirtualThreadExecutor;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.CancelRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 spike — pins what the {@link DeterministicVirtualThreadExecutor} single-carrier scheduler DOES and does
 * NOT deliver (measured, not assumed; see the class Javadoc for the mechanism and its honest scope):
 * <ul>
 *   <li>The real engine runs to terminal on ONE carrier thread (no body-vs-body / body-vs-publish parallelism) —
 *       the engine functions correctly without the production executor's parallelism.</li>
 *   <li>Same seed + FIFO carrier: the per-instance committed subsequence is identical across runs (the harness's
 *       determinism contract, now also under single-carrier execution).</li>
 *   <li>Seeded interleaving mode is a REPRODUCIBLE fuzz dimension: the pick order is a pure function of the
 *       interleaving seed whenever the ready sets evolve identically.</li>
 * </ul>
 * GLOBAL (cross-instance) fingerprint equality is measured and printed, not asserted: enqueue timing from Axon's
 * processor threads remains environmental until those threads are also scheduled deterministically — that residual
 * is the documented boundary of this spike (see {@code POC-TLA-DST.adoc} Phase 2).
 */
class DeterministicSchedulerSpikeTest {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(DeterministicSchedulerSpikeTest.class);

    private static final Duration DRIVE_DEADLINE = Duration.ofSeconds(15);

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void engineRunsToTerminal_onSingleDeterministicCarrier() {
        try (var executor = new DeterministicVirtualThreadExecutor()) {
            var fingerprint = runCancellingWorld(7L, "spike-A", executor);
            assertThat(fingerprint).as("the instance committed events on the single carrier").isNotEmpty();
            assertThat(executor.scheduledCount())
                    .as("continuations were actually dispatched by the deterministic carrier (not a fallback pool)")
                    .isPositive();
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void sameSeed_fifoCarrier_perInstanceFingerprintIdentical_globalMeasured() {
        List<List<String>> runs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            try (var executor = new DeterministicVirtualThreadExecutor()) {
                runs.add(runCancellingWorld(42L, "spike-B", executor));
            }
        }
        // Per-instance determinism (the hard contract) — identical committed subsequence on every run.
        for (List<String> run : runs) {
            assertThat(run).as("per-instance committed subsequence is a pure function of the seed")
                           .isEqualTo(runs.get(0));
        }
        // Global measurement for the report: distinct whole-log fingerprints across runs (1 = globally bit-for-bit).
        var distinctGlobal = new HashSet<>(runs).size();
        logger.info("[SPIKE] FIFO carrier, 5 runs, distinct global fingerprints: " + distinctGlobal);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void seededInterleaving_sameInterleavingSeed_reproduces() {
        var first = new ArrayList<List<String>>();
        for (long interleavingSeed : new long[]{1L, 1L, 2L, 3L}) {
            try (var executor = new DeterministicVirtualThreadExecutor(interleavingSeed)) {
                first.add(runCancellingWorld(42L, "spike-C", executor));
            }
        }
        // Same world seed + same interleaving seed => identical per-instance subsequence.
        assertThat(first.get(1)).as("interleaving seed 1 reproduces its own schedule's outcome")
                                .isEqualTo(first.get(0));
        // Different interleaving seeds may differ globally (that's the fuzz dimension) but the per-instance
        // INVARIANT-relevant content must still match the FIFO outcome for this single-instance workload.
        assertThat(first.get(2)).isEqualTo(first.get(0));
        assertThat(first.get(3)).isEqualTo(first.get(0));
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void fullLoop_smokeConfig_carrierOn_perInstanceIdenticalAndGlobalMeasured() {
        var config = io.axoniq.framework.workflow.simulation.harness.SimulationConfig.smoke(41L)
                                                                           .withDeterministicCarrier(null);
        var results = new ArrayList<io.axoniq.framework.workflow.simulation.harness.SimulationResult>();
        long t0 = System.nanoTime();
        for (int i = 0; i < 3; i++) {
            results.add(new io.axoniq.framework.workflow.simulation.harness.DstSimulation(config).run());
        }
        long carrierMillis = (System.nanoTime() - t0) / 1_000_000 / 3;
        // The harness's determinism contract must hold under the carrier: identical per-instance fingerprints.
        assertThat(results.get(1).logFingerprint()).isEqualTo(results.get(0).logFingerprint());
        assertThat(results.get(2).logFingerprint()).isEqualTo(results.get(0).logFingerprint());
        // Global (whole-log order) measurement for the report.
        var globals = new HashSet<List<String>>();
        for (var r : results) {
            globals.add(r.committedLog().stream().map(DeterministicSchedulerSpikeTest::render).toList());
        }
        // Baseline timing for the throughput comparison.
        long t1 = System.nanoTime();
        var baseline = new io.axoniq.framework.workflow.simulation.harness.DstSimulation(
                io.axoniq.framework.workflow.simulation.harness.SimulationConfig.smoke(41L)).run();
        long baselineMillis = (System.nanoTime() - t1) / 1_000_000;
        assertThat(baseline.logFingerprint())
                .as("carrier does not change the per-instance committed content vs the production executor")
                .isEqualTo(results.get(0).logFingerprint());
        logger.info("[SPIKE] full smoke loop x3, carrier on: distinct GLOBAL fingerprints=" + globals.size()
                                   + ", avg run=" + carrierMillis + "ms; baseline run=" + baselineMillis + "ms");
    }

        private static List<String> runCancellingWorld(long seed, String orderId,
                                                   ExecutorService executor) {
        var registration = EngineInstance.cancellingWorkflow(new CountingEffects());
        try (var world = new SimulationWorld(seed, registration, executor)) {
            String workflowId = "cancel-" + orderId;
            world.engine().publish(new CancelRequestedEvent(orderId));
            Polling.awaitOrFail(DRIVE_DEADLINE, "instance to reach a terminal workflow status",
                                () -> isTerminal(world.committedLog(), workflowId));
            return world.committedLog().stream()
                        .filter(e -> workflowId.equals(MetadataUtils.getWorkflowId(e.metadata())))
                        .map(DeterministicSchedulerSpikeTest::render)
                        .toList();
        }
    }

        private static String render(EventMessage e) {
        return e.type().qualifiedName() + "|" + MetadataUtils.getStepName(e.metadata())
                + "|" + MetadataUtils.getWorkflowStatus(e.metadata()).map(Enum::name).orElse("-");
    }

    private static boolean isTerminal(List<EventMessage> committedLog, String workflowId) {
        return committedLog.stream().anyMatch(e ->
                workflowId.equals(MetadataUtils.getWorkflowId(e.metadata()))
                        && MetadataUtils.getWorkflowStatus(e.metadata()).map(s -> s.isTerminal()).orElse(false));
    }
}
