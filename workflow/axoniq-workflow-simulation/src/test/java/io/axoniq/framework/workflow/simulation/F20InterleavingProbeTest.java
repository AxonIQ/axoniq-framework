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
import io.axoniq.framework.workflow.simulation.scenarios.LoopAndStormScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2 — the F-20 interleaving probe: drives the reused-names live-lock (the F-19/F-20 composite) under the
 * seeded-interleaving carrier across a bounded set of interleaving seeds and records the retryDelay terminal count
 * per schedule.
 * <p>
 * What this pins TODAY (an exploration substrate, not yet the strict F-20 acceptance):
 * <ul>
 *   <li>The probe RUNS under the carrier for every explored schedule — the interleaving dimension is drivable.</li>
 *   <li>Every schedule yields 0: the F-19 non-blocking sleep never terminates, and the F-20 duplicate mode is closed
 *       (the terminal transition discards queued publishes; see the {@code LoopAndStormTest} pin).</li>
 * </ul>
 * The observed spread is printed for the report. When an engine fix lands (F-19 blocking sleep + F-20 durable-gated
 * publish) every schedule must yield exactly 1 — flip the alphabet assertion then. Residual honesty: enqueue timing
 * from Axon's processor threads is still environmental, so a given interleaving seed's count is not yet guaranteed
 * stable run-to-run; making it so requires scheduling the processor side too (the documented Phase-2 boundary and
 * the natural next increment).
 */
class F20InterleavingProbeTest {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(F20InterleavingProbeTest.class);

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void seededSchedules_driveTheReusedNamesProbe_withinTheGapAlphabet() {
        Map<Long, Integer> countBySchedule = new LinkedHashMap<>();
        for (long interleavingSeed = 0; interleavingSeed < 8; interleavingSeed++) {
            try (var executor = new DeterministicVirtualThreadExecutor(interleavingSeed)) {
                var outcome = LoopAndStormScenario.reusedNamesLiveLock(0L, "f20-" + interleavingSeed, executor);
                countBySchedule.put(interleavingSeed, outcome.retryDelayTerminalRecords());
            }
        }
        logger.info("[SPIKE] F-20 probe, retryDelay terminals by interleaving seed: " + countBySchedule);
        // Every schedule yields exactly 0: the non-blocking sleep (F-19, open) never terminates before the spinning
        // body exhausts, and the >=2 duplicate mode (F-20) is unreachable since the terminal transition discards the
        // queued timed-out publishes before it publishes (SimpleWorkflowExecution.transitionToTerminalState). The
        // ungated in-memory publish gate in AbstractStepExecutor survives, so F-20 stays open on FOLLOW-UPS. A fixed
        // F-19 yields exactly 1; flip then.
        countBySchedule.forEach((seed, count) ->
                assertThat(count)
                        .as("interleaving seed %d: the reused-name sleep records no terminal (F-19 open, F-20 mode "
                                    + "closed by the terminal-transition queue discard), got %d", seed, count)
                        .isEqualTo(0));
    }
}
