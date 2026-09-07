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

import io.axoniq.framework.workflow.simulation.scenarios.FencedRetryBackoffScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A retry that fires on a node that lost the instance records nothing — and runs its action anyway.
 * <p>
 * The record half is the fence working. The action half is an expected-gap pin: a retry attempt does not go through
 * the {@code STARTED} gate, because the step is already present in the state, so nothing makes the store confirm the
 * attempt is this execution's before the action runs. This flips the day a retry attempt has to pass an accepted
 * append of its own first.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class FencedRetryBackoffTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedRetryBackoffTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aFencedRetryRecordsNothingButStillRunsItsAction() {
        var outcome = FencedRetryBackoffScenario.run(11L, "r1");
        logger.info("Fenced retry backoff: {}", outcome);

        assertThat(outcome.rejections())
                .as("the retry's append must have been rejected, or this run proves nothing")
                .isGreaterThanOrEqualTo(1);
        assertThat(outcome.recordsAfterFence())
                .as("nothing more is accepted for the instance after the foreign write")
                .isEqualTo(outcome.recordsBeforeFence());
        assertThat(outcome.terminalRecords())
                .as("a fenced execution publishes nothing terminal")
                .isZero();
        assertThat(outcome.effectsAfterFence())
                .as("the gap: a retry attempt runs its action without an accepted append of its own")
                .isGreaterThan(outcome.effectsBeforeFence());
    }
}
