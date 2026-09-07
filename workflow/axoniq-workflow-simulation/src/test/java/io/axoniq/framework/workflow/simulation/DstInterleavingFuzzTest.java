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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 3 — the interleaving fuzz axis: the smoke workload driven under the Phase-2 deterministic carrier with a
 * DIFFERENT seeded pick order per exploration step. Every invariant is asserted after every simulation step by the
 * loop itself (exactly like {@link DstFuzzTest}); what this axis adds is coverage over body-vs-publish scheduling
 * orders that the production virtual-thread executor leaves to the OS scheduler and the plain fuzz never varies
 * on purpose.
 * <p>
 * {@code @Tag("fuzz")} — excluded from the per-PR gate, explored by the nightly. {@code -Ddst.seeds=<N>} sets how
 * many interleaving seeds to explore (default 10; the WORLD seed is fixed so the fault/timing dimension stays
 * constant while ONLY the schedule varies — orthogonal axes, one variable at a time); {@code -Ddst.startSeed=<S>}
 * offsets the range so a large sweep chunks into non-overlapping runs, each under the eval-license wall-clock
 * (the {@link DstFuzzTest} chunking precedent).
 */
@Tag("fuzz")
class DstInterleavingFuzzTest {

    private static final Logger logger = LoggerFactory.getLogger(DstInterleavingFuzzTest.class);

    @Test
    void smokeWorkload_underSeededInterleavings_holdsEveryInvariant() {
        long start = startSeed();
        int schedules = seedCount();
        for (long interleavingSeed = start; interleavingSeed < start + schedules; interleavingSeed++) {
            var config = SimulationConfig.smoke(41L).withDeterministicCarrier(interleavingSeed);
            var result = new DstSimulation(config).run();
            logger.info("interleaving seed {} green; virtualElapsed={}, events={}",
                        interleavingSeed, result.virtualElapsed(), result.committedLog().size());
        }
    }

    private static int seedCount() {
        var prop = System.getProperty("dst.seeds");
        if (prop == null || prop.isBlank() || "${dst.seeds}".equals(prop)) {
            return 10;
        }
        return Integer.parseInt(prop.trim());
    }

    private static long startSeed() {
        var prop = System.getProperty("dst.startSeed");
        if (prop == null || prop.isBlank() || "${dst.startSeed}".equals(prop)) {
            return 0L;
        }
        return Long.parseLong(prop.trim());
    }
}
