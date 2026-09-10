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

import io.axoniq.framework.workflow.simulation.scenarios.FencedPublishScenario;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stale writer's publish is fenced by the DCB append condition like every other engine append.
 *
 * @author Stefan Dragisic
 */
class FencedPublishTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedPublishTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aFencedPublishRecordsNothingAndStartsNobody() {
        var outcome = FencedPublishScenario.run(29L, "f1");
        logger.info("Fenced publish: {}", outcome);
        assertThat(outcome.rejections())
                .as("the publish append must have been rejected, or this run proves nothing")
                .isGreaterThanOrEqualTo(1);
        assertThat(outcome.publishRecords()).as("a fenced publish leaves no published event").isZero();
        assertThat(outcome.responderStarts()).as("nothing was published, so nothing was started by it").isZero();
        assertThat(outcome.recordsAfterFence())
                .as("nothing more is accepted from the fenced execution")
                .isEqualTo(outcome.recordsBeforeFence());
        assertThat(outcome.terminalRecords()).as("a fenced execution publishes nothing terminal").isZero();
    }
}
