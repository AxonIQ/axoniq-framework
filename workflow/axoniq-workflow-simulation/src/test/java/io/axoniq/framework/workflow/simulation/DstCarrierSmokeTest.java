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

import io.axoniq.framework.workflow.simulation.harness.DstSimulation;
import io.axoniq.framework.workflow.simulation.harness.SimulationConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 — the per-PR carrier gate: the full smoke workload under the Phase-2 deterministic FIFO carrier plus two
 * seeded interleavings. Invariants are asserted inside the loop after every step; this test additionally pins:
 * <ul>
 *   <li>the per-instance fingerprint under the carrier equals the production executor's for the same seed
 *       (single-carrier execution is content-neutral), and</li>
 *   <li>virtual-time liveness accounting is populated ({@code SimulationResult.virtualElapsed()} — liveness is
 *       inspectable in simulated time, not only wall-clock).</li>
 * </ul>
 */
class DstCarrierSmokeTest {

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void smokeWorkload_fifoCarrier_contentNeutral_andTwoInterleavingsGreen() {
        var baseline = new DstSimulation(SimulationConfig.smoke(43L)).run();
        var fifo = new DstSimulation(SimulationConfig.smoke(43L).withDeterministicCarrier(null)).run();
        assertThat(fifo.logFingerprint())
                .as("single-carrier FIFO execution must not change the per-instance committed content")
                .isEqualTo(baseline.logFingerprint());
        assertThat(fifo.virtualElapsed())
                .as("virtual-time liveness accounting is populated under the carrier")
                .isPositive();

        for (long interleavingSeed : new long[]{1L, 2L}) {
            var result = new DstSimulation(SimulationConfig.smoke(43L).withDeterministicCarrier(interleavingSeed))
                    .run();
            assertThat(result.logFingerprint())
                    .as("seeded interleaving %d keeps the per-instance committed content", interleavingSeed)
                    .isEqualTo(baseline.logFingerprint());
        }
    }
}
