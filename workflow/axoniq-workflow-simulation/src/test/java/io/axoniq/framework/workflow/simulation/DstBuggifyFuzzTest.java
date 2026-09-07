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
import io.axoniq.framework.workflow.simulation.harness.SimulationResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The BUGGIFY-amplified fuzz campaign: {@link DstFuzzTest}-shaped seed exploration with both in-engine BUGGIFY
 * points ({@code engine.live-switch} — the replay-to-live boundary — and {@code execution.append-task} — the
 * enqueue-vs-drain window) active around every world. The two points bias exactly the F-7/F-13/F-20
 * corruption-class surface (duplicate durable terminal records) and the live-switch race, so this campaign visits
 * those windows far more often per seed than the plain fuzz.
 * <p>
 * BUGGIFY state is JVM-global, so it is activated per world and deactivated in a {@code finally} (the
 * {@code DstWideningSmokeTest} pin's contract). {@code @Tag("fuzz")}; {@code -Ddst.seeds=<N>} (default 10),
 * {@code -Ddst.startSeed=<S>} for chunking (the {@link DstFuzzTest} precedent).
 */
@Tag("fuzz")
class DstBuggifyFuzzTest {

    private static final Logger logger = LoggerFactory.getLogger(DstBuggifyFuzzTest.class);

    @Test
    void fuzzManySeeds_withBuggifyActive_holdsEveryInvariant() {
        long start = longProp("dst.startSeed", 0L);
        long count = longProp("dst.seeds", 10L);
        for (long seed = start; seed < start + count; seed++) {
            Buggify.activate(seed, 0.25);
            SimulationResult result;
            java.util.Map<String, Integer> fired;
            try {
                result = new DstSimulation(SimulationConfig.fuzz(seed)).run();
            } finally {
                fired = Buggify.deactivate();
            }
            assertThat(result.committedLog()).as("seed %d produced a non-empty log", seed).isNotEmpty();
            logger.info("buggify seed {} green: fired={}, events={}, virtualElapsed={}",
                        seed, fired, result.committedLog().size(), result.virtualElapsed());
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
