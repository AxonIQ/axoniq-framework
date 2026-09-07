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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The heaviest deterministic campaign: the <em>chaos</em> fuzz. It is a sibling of {@link DstFuzzTest} (so the per-PR
 * build excludes it via the same {@code "fuzz"} JUnit tag and the {@code dst.excludedGroups} exclusion) but drives
 * {@link SimulationConfig#chaos(long)} — more instances, more steps, a higher fault probability, and the full
 * {@link SimulationConfig#CHAOS_FAULTS} set: every existing fault plus the five chaos faults (duplicate-completed,
 * event-store latency jitter, flapping restart, clock skew, partial batch).
 * <p>
 * For each seed it runs the chaos config, which asserts every invariant after every step (INV-1..6 plus the DST-only
 * INV-7..19; a genuine break throws an enriched {@code InvariantViolation} with the seed + reproduce command), drives
 * every instance to a terminal status by the horizon (otherwise the harness aborts as a liveness finding), and proves
 * the per-instance committed log is non-empty and the full mixed workload (19 instances) was driven. The nightly /
 * manual job runs it with {@code -Ddst.excludedGroups= -Dtest=DstChaosFuzzTest -Ddst.seeds=<N>}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Tag("fuzz")
class DstChaosFuzzTest {

    private static final Logger logger = LoggerFactory.getLogger(DstChaosFuzzTest.class);

    /**
     * Number of consecutive seeds the chaos fuzz explores, overridable with {@code -Ddst.seeds=<N>} (default 25 so a
     * manual {@code -Ddst.excludedGroups=} run is still quick; the nightly job passes a large value).
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
    void chaosFuzzManySeeds() {
        int seeds = seedCount();
        long start = startSeed();
        logger.info("DST CHAOS fuzz: exploring {} seeds from {} with the full chaos fault set (incl. duplicate-completed,"
                            + " event-store latency jitter, flapping restart, clock skew, partial batch)", seeds, start);
        int f0Count = 0;
        for (long seed = start; seed < start + seeds; seed++) {
            SimulationResult result = new DstSimulation(SimulationConfig.chaos(seed)).run();
            // Reaching here means the chaos run asserted every invariant after every step and drove every instance
            // (the 10 OrderWorkflow + the 7 fixed singletons) to a terminal status; a genuine break would have thrown.
            assertThat(result.committedLog())
                    .as("chaos seed %d must produce a non-empty committed log", seed)
                    .isNotEmpty();
            // 10 OrderWorkflow + 1 VersionedOrderWorkflow (INV-11) + 1 MigratingOrderWorkflow (INV-12) + 1
            // PayloadOrderWorkflow (INV-13) + 1 CombinatorWorkflow (INV-14) + 2 CorrelatedWaitWorkflow (INV-15) + 1
            // ReducerWorkflow (INV-19) + 1 VersioningEdgesWorkflow (INV-20) + 1 CustomNamedWorkflow (INV-22) + the 4
            // P-series production-realism singletons (saga[retry-comp] + subscription + rollingDeploy[spawns at v2] +
            // counterLoop) = 23 instances the chaos config drives via SimulationWorld#defaultRegistrations().
            assertThat(Invariants.workflowIdsIn(result.committedLog()))
                    .as("chaos seed %d drives the full mixed workload (23 instances)", seed)
                    .hasSize(23);
            if (result.observedF0Duplicate()) {
                f0Count++;
            }
        }
        logger.info("DST CHAOS fuzz: {} seeds from {} passed all invariants; {} reproduced the documented F-0 effect "
                            + "duplication (write-then-vanish window)", seeds, start, f0Count);
    }
}
