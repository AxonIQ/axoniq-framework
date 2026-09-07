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
 * Phase 4 — the swarm campaign: N seed-shaped configurations ({@link SimulationConfig#swarm(long)} — instance
 * count, step budget, fault probability, fault subset, carrier mode all derived from the seed), each run
 * invariant-green end to end. Where {@link DstFuzzTest} explores seeds over a fixed shape, swarm explores the
 * SHAPE space itself. {@code @Tag("fuzz")}; {@code -Ddst.seeds=<N>} (default 10), {@code -Ddst.startSeed=<S>}.
 */
@Tag("fuzz")
class DstSwarmFuzzTest {

    private static final Logger logger = LoggerFactory.getLogger(DstSwarmFuzzTest.class);

    @Test
    void swarmShapes_holdEveryInvariant() {
        long start = longProp("dst.startSeed", 0L);
        long count = longProp("dst.seeds", 10L);
        for (long seed = start; seed < start + count; seed++) {
            var config = SimulationConfig.swarm(seed);
            var result = new DstSimulation(config).run();
            logger.info("swarm seed {} green: shape(count={}, steps={}, p={}, kinds={}, carrier={}/{}), events={}",
                        seed, config.workflowCount(), config.maxSteps(),
                        String.format("%.2f", config.faultProbability()), config.faultKinds().length,
                        config.deterministicCarrier(), config.interleavingSeed(),
                        result.committedLog().size());
        }
    }

    private static long longProp(String name, long defaultValue) {
        var prop = System.getProperty(name);
        if (prop == null || prop.isBlank() || ("${" + name + "}").equals(prop)) {
            return defaultValue;
        }
        return Long.parseLong(prop.trim());
    }
}
