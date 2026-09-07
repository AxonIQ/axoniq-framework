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
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Single-seed reproduction entry point — the command an engineer runs after a fuzz failure (the enriched diagnostic the
 * harness prints on any break points here):
 * <pre>{@code ./mvnw -pl workflow/axoniq-workflow-simulation -am test -Dtest=DstReproduceTest -Ddst.seed=<seed>}</pre>
 * <p>
 * It runs the given seed (from {@code -Ddst.seed}, default 0) twice and asserts the run is bit-for-bit reproducible:
 * identical per-instance committed log (INV-4 {@code DeterministicReplay}, run granularity) and identical effect
 * counts. Deliberately <strong>not</strong> {@code @Tag("fuzz")} so the reproduce command runs directly without having
 * to clear the module's fuzz exclusion; it doubles as a fast per-PR same-seed-reproducibility check on seed 0.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class DstReproduceTest {

    private static final Logger logger = LoggerFactory.getLogger(DstReproduceTest.class);

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void reproducesSeed() {
        var prop = System.getProperty("dst.seed");
        long seed = (prop == null || prop.isBlank() || "${dst.seed}".equals(prop)) ? 0L : Long.parseLong(prop.trim());
        logger.info("DST reproduce: running seed {} twice", seed);

        SimulationResult first = new DstSimulation(SimulationConfig.smoke(seed)).run();
        SimulationResult second = new DstSimulation(SimulationConfig.smoke(seed)).run();

        // DeterministicReplay (INV-4), run granularity: the per-instance committed subsequences must be identical.
        Invariants.assertDeterministicReplay(first.committedLog(), second.committedLog());
        assertThat(second.logFingerprint())
                .as("seed %d must reproduce the identical per-instance committed log", seed)
                .isEqualTo(first.logFingerprint());
        assertThat(second.effectCounts())
                .as("seed %d must reproduce the identical effect counts", seed)
                .isEqualTo(first.effectCounts());
        logger.info("DST reproduce: seed {} reproduced identically ({} instances)",
                    seed, first.logFingerprint().size());
    }
}
