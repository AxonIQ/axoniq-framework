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
import io.axoniq.framework.workflow.simulation.harness.SimulationResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The long deterministic fuzz, gated behind the {@code "fuzz"} JUnit tag so it does not run in the per-PR build (the
 * module's surefire config excludes the {@code fuzz} tag via the overridable {@code dst.excludedGroups} property). The
 * nightly job runs it with {@code -Ddst.excludedGroups= -Dtest=DstFuzzTest -Ddst.seeds=<N>} to explore thousands of
 * seeds across the full fault set (including write-then-vanish).
 * <p>
 * The single-seed reproduction entry point lives in {@link DstReproduceTest} (deliberately un-tagged so it runs
 * without clearing the fuzz exclusion); the enriched diagnostic the harness prints on a break points there.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Tag("fuzz")
class DstFuzzTest {

    private static final Logger logger = LoggerFactory.getLogger(DstFuzzTest.class);

    /**
     * Number of consecutive seeds the fuzz explores, overridable with {@code -Ddst.seeds=<N>} (default 25 so a manual
     * {@code -DexcludedGroups=} run is still quick; the nightly job passes a large value).
     */
    private static int seedCount() {
        var prop = System.getProperty("dst.seeds");
        if (prop == null || prop.isBlank() || "${dst.seeds}".equals(prop)) {
            return 25;
        }
        return Integer.parseInt(prop.trim());
    }

    /**
     * First seed of the explored range, overridable with {@code -Ddst.startSeed=<N>} (default 0). Lets a large scaled
     * campaign be run as non-overlapping chunks — e.g. {@code -Ddst.startSeed=1000 -Ddst.seeds=1000} explores seeds
     * 1000..1999 — each chunk under the 15-minute eval-license wall-clock so no single run is killed mid-sweep.
     */
    private static long startSeed() {
        var prop = System.getProperty("dst.startSeed");
        if (prop == null || prop.isBlank() || "${dst.startSeed}".equals(prop)) {
            return 0L;
        }
        return Long.parseLong(prop.trim());
    }

    @Test
    void fuzzManySeeds() {
        int seeds = seedCount();
        long start = startSeed();
        logger.info("DST fuzz: exploring {} seeds from {} with the full fault set (incl. write-then-vanish)",
                    seeds, start);
        int f0Count = 0;
        for (long seed = start; seed < start + seeds; seed++) {
            SimulationResult result = new DstSimulation(SimulationConfig.fuzz(seed)).run();
            assertThat(result.committedLog()).as("seed %d produced a non-empty log", seed).isNotEmpty();
            if (result.observedF0Duplicate()) {
                f0Count++;
            }
        }
        logger.info("DST fuzz: {} seeds from {} passed all invariants; {} reproduced the F-0 effect duplication",
                    seeds, start, f0Count);
    }
}
