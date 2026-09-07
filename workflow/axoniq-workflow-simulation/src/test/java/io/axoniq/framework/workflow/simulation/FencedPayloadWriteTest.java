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

import io.axoniq.framework.workflow.simulation.scenarios.FencedPayloadWriteScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A rejected {@code modifyPayload} write never leaks into a later accepted event. See
 * {@link FencedPayloadWriteScenario}.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class FencedPayloadWriteTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedPayloadWriteTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aRejectedPayloadWriteNeverLeaksIntoTheRestoredPayload() {
        var outcome = FencedPayloadWriteScenario.run(14L, "r4");
        logger.info("Fenced payload write: {}", outcome);

        assertThat(outcome.fencedInstance())
                .as("the fence must have written for the instance, or this run proves nothing")
                .isEqualTo("payload-r4");
        assertThat(outcome.rejections())
                .as("the payload write must have been rejected")
                .isGreaterThanOrEqualTo(1);
        assertThat(outcome.recordsAfterFence())
                .as("nothing more is accepted for the instance while it is fenced")
                .isEqualTo(outcome.recordsBeforeFence());
        assertThat(outcome.payloadFoldConsistent())
                .as("after the claim, the reconstructed payload equals the fold of the committed log")
                .isTrue();
    }
}
