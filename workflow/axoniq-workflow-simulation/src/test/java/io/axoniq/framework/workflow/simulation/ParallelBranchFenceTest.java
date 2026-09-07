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

import io.axoniq.framework.workflow.simulation.scenarios.ParallelBranchFenceScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fences one branch of an instance running parallel steps and checks no sibling records past the fence. See
 * {@link ParallelBranchFenceScenario}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class ParallelBranchFenceTest {

    private static final Logger logger = LoggerFactory.getLogger(ParallelBranchFenceTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aFencedBranchStopsItsSiblingsWithoutADuplicateRecord() {
        var outcome = ParallelBranchFenceScenario.run(3L, "fenced-branch");
        logger.info("Parallel branch fence: {}", outcome);

        assertThat(outcome.fencedInstance())
                .as("the fence must have written for the instance, or this run proves nothing")
                .isEqualTo("comb-fenced-branch");
        assertThat(outcome.maxTerminalRecordsForAStep())
                .as("no step of a fenced instance holds two terminal records")
                .isLessThanOrEqualTo(1);
        assertThat(outcome.terminalRecordsWhileFenced())
                .as("a fenced execution publishes nothing terminal")
                .isZero();
        assertThat(outcome.terminalAfterClaim())
                .as("the next claim restores the instance and it finishes")
                .isTrue();
        assertThat(outcome.maxStartedRecords())
                .as("the instance never holds two STARTED workflow records")
                .isLessThanOrEqualTo(1);
        assertThat(outcome.maxTerminalRecordsAtEnd())
                .as("no step ends with two terminal records")
                .isLessThanOrEqualTo(1);
        assertThat(outcome.terminalRecordsAtEnd())
                .as("the instance ends with exactly one terminal workflow record")
                .isEqualTo(1);
    }
}
