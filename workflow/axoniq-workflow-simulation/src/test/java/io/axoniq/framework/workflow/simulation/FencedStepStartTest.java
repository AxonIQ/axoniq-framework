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

import io.axoniq.framework.workflow.simulation.scenarios.FencedStepStartScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins "run a step's action only once the store accepted its STARTED": a step whose own {@code STARTED} append is
 * rejected must not run its action, and the next claim must still run it. See {@link FencedStepStartScenario}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class FencedStepStartTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedStepStartTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aStepWhoseStartedWasRejectedNeverRunsItsAction() {
        var outcome = FencedStepStartScenario.run(1L, "fenced-start");
        logger.info("Fenced step start: {}", outcome);

        assertThat(outcome.fencedInstance())
                .as("the fence must have written for the instance, or this run proves nothing")
                .isEqualTo("order-fenced-start");
        assertThat(outcome.rejectionsObserved())
                .as("the fenced execution must have logged its rejection")
                .isGreaterThanOrEqualTo(1);

        assertThat(outcome.effectRunsWhileFenced())
                .as("the store rejected this execution's STARTED, so its action must not have run")
                .isZero();
        assertThat(outcome.startedRecordsWhileFenced())
                .as("a rejected append leaves no record")
                .isZero();
        assertThat(outcome.terminalRecordsWhileFenced())
                .as("a fenced execution publishes nothing terminal")
                .isZero();

        assertThat(outcome.effectRunsAfterRestore())
                .as("the next claim owns the step, so its action runs exactly once")
                .isEqualTo(1);
        assertThat(outcome.completedRecordsAfterRestore())
                .as("the restored execution records the step once")
                .isEqualTo(1);
    }
}
