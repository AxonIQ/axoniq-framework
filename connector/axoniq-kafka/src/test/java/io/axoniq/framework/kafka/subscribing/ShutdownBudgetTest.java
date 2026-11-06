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

package io.axoniq.framework.kafka.subscribing;

import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link ShutdownBudget} matches the time limits that the public documentation of the
 * subscribing Kafka event source states.
 *
 * @author Allard Buijze
 */
class ShutdownBudgetTest {

    @Test
    void stoppingTakesAtMostTheRevocationTimeoutPlusFifteenSeconds() {
        // given
        Duration revocationTimeout = Duration.ofSeconds(7);

        // when
        Duration stopTimeout = ShutdownBudget.stopTimeout(revocationTimeout);

        // then
        assertThat(stopTimeout).isEqualTo(revocationTimeout.plusSeconds(15));
    }

    @Test
    void defaultDeadLetterMaxBlockIsTenSecondsAndFitsTheCommitTimeout() {
        // when / then
        assertThat(ShutdownBudget.DEAD_LETTER_MAX_BLOCK).isEqualTo(Duration.ofSeconds(10));
        assertThat(ShutdownBudget.DEAD_LETTER_MAX_BLOCK).isLessThanOrEqualTo(ShutdownBudget.COMMIT_TIMEOUT);
    }

    @Test
    void deadLetterAcknowledgementIsAwaitedForTwentySeconds() {
        // when / then
        assertThat(ShutdownBudget.DEAD_LETTER_ACK_TIMEOUT).isEqualTo(Duration.ofSeconds(20));
    }
}
