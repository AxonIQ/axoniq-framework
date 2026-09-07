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
import io.axoniq.framework.workflow.runtime.util.Buggify;
import io.axoniq.framework.workflow.simulation.scenarios.LoopAndStormScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The strict F-20 probe (promoted from the exploration attempt): the interleaving probe
 * ({@link F20InterleavingProbeTest}) run with BUGGIFY active simultaneously — the carrier pins the body-side
 * schedule per interleaving seed while the two in-engine BUGGIFY points ({@code engine.live-switch},
 * {@code execution.append-task}) bias the processor-side windows the carrier cannot yet schedule. Where the
 * plain-carrier probe yields 0 duplicates on every schedule (body-side interleaving alone cannot reopen the F-20
 * window), carrier+BUGGIFY reopens it on EVERY schedule: 44–50 duplicate {@code retryDelay} TIMED_OUT terminals per
 * run (50 = {@code LoopingPollWorkflow.MAX_SPINS} — every loop re-entry re-published the terminal, the F-20
 * unbounded-amplification face made saturated and drivable).
 * <p>
 * <strong>The strict pin:</strong> at promotion time (5 JVM runs × 2 passes × 8 schedules = 80 observations,
 * buggify seed == interleaving seed, p = 0.25) the duplicate count was ≥2 in ALL 80 observations (min 12, never 0,
 * never the fixed-engine exactly-1); schedule 0 hit the saturated 50 in 10/10 passes. The EXACT count is not
 * run-stable (12..50 — the documented enqueue-timing residual: Axon's processor threads are still environmental),
 * but the duplication's PRESENCE is 100% reproducible, which is what the pin asserts: {@code >= 2} for the promoted
 * seeds in BOTH passes, plus the F-19/F-20 gap-alphabet contract ({@code != 1}) on every schedule. When the engine
 * fix lands (F-19 blocking sleep + F-20 durable-gated publish) every schedule must yield exactly 1 — this probe is
 * the acceptance test that flips.
 */
class F20BuggifyInterleavingProbeTest {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(F20BuggifyInterleavingProbeTest.class);

    /** The promoted schedules: duplicates present (≥2) in every observed pass; schedule 0 saturated in all of them. */
    private static final long[] PROMOTED_SEEDS = {0L, 4L, 7L};

    @Test
    @Timeout(value = 480, unit = TimeUnit.SECONDS)
    void seededSchedules_withBuggify_probeReproducibleDuplicates() {
        Map<Long, Integer> firstPass = runPass("p1");
        Map<Long, Integer> secondPass = runPass("p2");
        logger.info("[PROBE] F-20+BUGGIFY pass1: " + firstPass);
        logger.info("[PROBE] F-20+BUGGIFY pass2: " + secondPass);
        var reproducibleDuplicates = new LinkedHashMap<Long, Integer>();
        firstPass.forEach((seed, count) -> {
            if (count >= 2 && secondPass.getOrDefault(seed, 0).equals(count)) {
                reproducibleDuplicates.put(seed, count);
            }
        });
        logger.info("[PROBE] F-20+BUGGIFY reproducible >=2 duplicates (same seeds, same count, twice): "
                                   + reproducibleDuplicates);
        // The strict pin: the promoted schedules reproduce >=2 duplicates in BOTH passes. A fixed engine yields
        // exactly 1 everywhere, which also fails the gap-alphabet assertions below — flip both together then.
        for (long seed : PROMOTED_SEEDS) {
            assertThat(firstPass.get(seed))
                    .as("promoted schedule %d reproduces the F-20 duplicate (pass 1)", seed)
                    .isGreaterThanOrEqualTo(2);
            assertThat(secondPass.get(seed))
                    .as("promoted schedule %d reproduces the F-20 duplicate (pass 2)", seed)
                    .isGreaterThanOrEqualTo(2);
        }
        // Gap alphabet contract (the F20InterleavingProbeTest pin, now under BUGGIFY): never exactly 1.
        firstPass.forEach((seed, count) ->
                assertThat(count)
                        .as("pass1 interleaving seed %d must sit in the F-19/F-20 gap alphabet (0 or >=2), got %d",
                            seed, count)
                        .isNotEqualTo(1));
        secondPass.forEach((seed, count) ->
                assertThat(count)
                        .as("pass2 interleaving seed %d must sit in the F-19/F-20 gap alphabet (0 or >=2), got %d",
                            seed, count)
                        .isNotEqualTo(1));
    }

    private static Map<Long, Integer> runPass(String passLabel) {
        Map<Long, Integer> countBySchedule = new LinkedHashMap<>();
        for (long interleavingSeed = 0; interleavingSeed < 8; interleavingSeed++) {
            Buggify.activate(interleavingSeed, 0.25);
            try (var executor = new DeterministicVirtualThreadExecutor(interleavingSeed)) {
                var outcome = LoopAndStormScenario.reusedNamesLiveLock(
                        0L, "f20bg-" + passLabel + "-" + interleavingSeed, executor);
                countBySchedule.put(interleavingSeed, outcome.retryDelayTerminalRecords());
            } finally {
                Buggify.deactivate();
            }
        }
        return countBySchedule;
    }
}
