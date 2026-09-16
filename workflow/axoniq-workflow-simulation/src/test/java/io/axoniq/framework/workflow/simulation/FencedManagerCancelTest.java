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

import io.axoniq.framework.workflow.simulation.scenarios.FencedManagerCancelScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cancellation requested through the {@code WorkflowManager} on a node that lost the instance records nothing. The
 * manager twin of {@link FencedExternalCancelTest}; see {@link FencedManagerCancelScenario}. Also pins candidate
 * finding F-43: the request's {@code true} is not tied to the durable record it claims.
 *
 * @author Stefan Dragisic
 */
class FencedManagerCancelTest {

    private static final Logger logger = LoggerFactory.getLogger(FencedManagerCancelTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aManagerCancelOnAFencedNodeRecordsNothing() {
        var outcome = FencedManagerCancelScenario.run(15L, "r6");
        logger.info("Fenced manager cancel: {}", outcome);

        assertThat(outcome.rejections())
                .as("the cancel's append must have been rejected, or this run proves nothing")
                .isGreaterThanOrEqualTo(1);
        assertThat(outcome.recordsAfterFence())
                .as("nothing more is accepted for the instance after the foreign write")
                .isEqualTo(outcome.recordsBeforeFence());
        assertThat(outcome.cancelledRecords())
                .as("no CANCELLED step record from a node another writer has overtaken")
                .isZero();
        assertThat(outcome.terminalRecords())
                .as("no terminal workflow record from a fenced node")
                .isZero();
        // THE GAP (F-43): the API documents true as "a terminal step cancellation was recorded", but the answer is
        // decided by the in-memory step cancellation (WorkflowLifecycleControlDelegate#cancelStep) before the store
        // has accepted or rejected the CANCELLED append. On a fenced node nothing is recorded and the caller is still
        // told true. Expected-gap pin: flips when the answer is tied to an accepted append.
        assertThat(outcome.lastAnswer())
                .as("expected-gap pin: the fenced node answers true for a cancellation it never recorded")
                .isTrue();
    }
}
