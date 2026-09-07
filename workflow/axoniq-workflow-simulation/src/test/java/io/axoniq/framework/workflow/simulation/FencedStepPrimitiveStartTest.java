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

import io.axoniq.framework.workflow.simulation.scenarios.FencedStepPrimitiveStartScenario;
import io.axoniq.framework.workflow.simulation.scenarios.FencedStepPrimitiveStartScenario.Outcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code STARTED} gate holds for a retryable {@code execute}, a {@code waitForEvent} and a {@code sleep}. See
 * {@link FencedStepPrimitiveStartScenario}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class FencedStepPrimitiveStartTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedStepPrimitiveStartTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aRetryableExecuteWhoseStartedWasRejectedDoesNothing() {
        assertGateHeld(FencedStepPrimitiveStartScenario.retryableExecute(16L, "r6a"), "retry-r6a");
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aWaitForEventWhoseStartedWasRejectedDoesNothing() {
        assertGateHeld(FencedStepPrimitiveStartScenario.waitForEvent(17L, "r6b"), "timeout-r6b");
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aSleepWhoseStartedWasRejectedDoesNothing() {
        assertGateHeld(FencedStepPrimitiveStartScenario.sleep(18L, "r6c"), "order-r6c");
    }

    private static void assertGateHeld(Outcome outcome, String workflowId) {
        logger.info("Fenced step primitive start: {}", outcome);

        assertThat(outcome.fencedInstance())
                .as("%s: the fence must have written for the instance, or this run proves nothing",
                    outcome.primitive())
                .isEqualTo(workflowId);
        assertThat(outcome.rejections())
                .as("%s: the step's own STARTED must have been rejected", outcome.primitive())
                .isGreaterThanOrEqualTo(1);
        assertThat(outcome.startedRecords())
                .as("%s: a rejected append leaves no STARTED record", outcome.primitive())
                .isZero();
        assertThat(outcome.terminalStepRecords())
                .as("%s: the step never reaches a terminal record on the fenced node", outcome.primitive())
                .isZero();
        assertThat(outcome.effectRuns())
                .as("%s: the step's own work never happens", outcome.primitive())
                .isZero();
        assertThat(outcome.terminalRecords())
                .as("%s: a fenced execution publishes nothing terminal", outcome.primitive())
                .isZero();
    }
}
