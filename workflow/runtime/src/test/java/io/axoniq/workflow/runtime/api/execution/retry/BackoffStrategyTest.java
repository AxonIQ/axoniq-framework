/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api.execution.retry;

import io.axoniq.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffStrategyTest {

    @Test
    void testNone() {
        BackoffStrategy strategy = BackoffStrategy.NONE;
        assertThat(strategy.delay(1)).isEqualTo(Duration.ZERO);
        assertThat(strategy.delay(10)).isEqualTo(Duration.ZERO);
    }

    @Test
    void testFixed() {
        Duration delay = Duration.ofSeconds(5);
        BackoffStrategy strategy = BackoffStrategy.fixed(delay);
        assertThat(strategy.delay(1)).isEqualTo(delay);
        assertThat(strategy.delay(10)).isEqualTo(delay);
    }

    @Test
    void testLinear() {
        Duration baseDelay = Duration.ofSeconds(2);
        BackoffStrategy strategy = BackoffStrategy.linear(baseDelay);
        assertThat(strategy.delay(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(strategy.delay(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(strategy.delay(10)).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    void testExponential() {
        Duration base = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(10);
        BackoffStrategy strategy = BackoffStrategy.exponential(base, max);

        assertThat(strategy.delay(1)).isEqualTo(Duration.ofSeconds(1)); // 1 * 2^0 = 1
        assertThat(strategy.delay(2)).isEqualTo(Duration.ofSeconds(2)); // 1 * 2^1 = 2
        assertThat(strategy.delay(3)).isEqualTo(Duration.ofSeconds(4)); // 1 * 2^2 = 4
        assertThat(strategy.delay(4)).isEqualTo(Duration.ofSeconds(8)); // 1 * 2^3 = 8
        assertThat(strategy.delay(5)).isEqualTo(Duration.ofSeconds(10)); // min(1 * 2^4 = 16, 10) = 10
        assertThat(strategy.delay(10)).isEqualTo(Duration.ofSeconds(10));
    }
}
