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

import io.axoniq.framework.workflow.simulation.scenarios.PublishAppendFailureScenario;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The publish append against a misbehaving event store: a commit that hangs past the resolution timeout and a commit
 * that fails outright. In both cases nothing is durable while the fault stands, and a restart publishes exactly once.
 * The pre-restart reaction to a failed commit is logged from the outcome and compared with the same fault on a
 * {@code modifyPayload} step, so a surprise there is attributed to the engine, not to the primitive.
 *
 * @author Stefan Dragisic
 */
class PublishAppendFailureTest {

    private static final Logger logger = LoggerFactory.getLogger(PublishAppendFailureTest.class);

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void aPublishCommitThatHangsHitsTheResolutionTimeoutAndTheRestartPublishesOnce() {
        var outcome = PublishAppendFailureScenario.appendStallsPastResolutionTimeout(37L, "s1");
        logger.info("Publish append stalled: {}", outcome);
        assertThat(outcome.faultLanded()).as("the stall must have consumed the commit, or this run proves nothing").isTrue();
        assertThat(outcome.timeoutLogged())
                .as("the engine reports the publication resolution timeout and stops the runtime for recovery")
                .isTrue();
        assertThat(outcome.recordsBeforeRestart()).as("a hung commit leaves nothing durable").isZero();
        assertThat(outcome.publisherStatus()).as("no terminal status is invented for a hung append").isNull();
        assertThat(outcome.recordsAfterRestart()).as("the restart publishes exactly once").isEqualTo(1);
        assertThat(outcome.allTerminal()).isTrue();
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void aPublishCommitThatFailsLeavesNothingDurableAndTheRestartPublishesOnce() {
        var outcome = PublishAppendFailureScenario.appendFails(41L, "s2");
        logger.info("Publish append failed: {}", outcome);
        assertThat(outcome.faultLanded()).isTrue();
        assertThat(outcome.recordsBeforeRestart()).as("a failed commit leaves nothing durable").isZero();
        assertThat(outcome.recordsAfterRestart()).as("the restart publishes exactly once").isEqualTo(1);
        assertThat(outcome.allTerminal()).isTrue();
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void aFailedCommitOnModifyPayloadBehavesLikeAFailedPublishCommit() {
        var publish = PublishAppendFailureScenario.appendFails(43L, "s3");
        var payload = PublishAppendFailureScenario.modifyPayloadAppendFails(43L, "s3");
        logger.info("Failed commit, publish: {} / modifyPayload: {}", publish, payload);
        assertThat(payload.recordsBeforeRestart()).isZero();
        assertThat(payload.recordsAfterRestart()).as("the restart records the step exactly once").isEqualTo(1);
        assertThat(payload.allTerminal()).isTrue();
        assertThat(publish.publisherStatus())
                .as("the engine's pre-restart reaction to a failed append is the same for publish and modifyPayload")
                .isEqualTo(payload.publisherStatus());
        assertThat(publish.publisherLive()).isEqualTo(payload.publisherLive());
    }
}
