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
package io.axoniq.framework.workflow.runtime.api.execution.retry;

import io.axoniq.framework.workflow.runtime.api.execution.context.retry.BackoffStrategy;
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

    /**
     * Regression test for the exponential-backoff overflow bug (issue #222). With the old
     * {@code 1L << (attempt - 1)} the shift overflowed at large attempt counts, producing a negative
     * or wrapped factor and therefore a negative/huge delay. The result must always stay clamped to
     * {@code max}: never negative and never greater than {@code max}.
     */
    @Test
    void testExponentialClampsAtLargeAttemptCountsWithoutOverflow() {
        Duration base = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(10);
        BackoffStrategy strategy = BackoffStrategy.exponential(base, max);

        for (int attempt : new int[]{63, 64, 65, 100, 1000, Integer.MAX_VALUE - 1, Integer.MAX_VALUE}) {
            Duration delay = strategy.delay(attempt);
            assertThat(delay)
                    .as("delay for attempt %d must not be negative", attempt)
                    .isGreaterThanOrEqualTo(Duration.ZERO);
            assertThat(delay)
                    .as("delay for attempt %d must be clamped to max", attempt)
                    .isLessThanOrEqualTo(max);
            assertThat(delay)
                    .as("delay for attempt %d must equal max once the exponential exceeds it", attempt)
                    .isEqualTo(max);
        }
    }

    /**
     * Regression test (issue #222): a large {@code max} that, combined with a large attempt count,
     * would have overflowed the {@code base * 2^(attempt-1)} multiply must still clamp to {@code max}
     * rather than wrapping to a negative or bogus value.
     */
    @Test
    void testExponentialWithLargeMaxClampsWithoutOverflow() {
        Duration base = Duration.ofMillis(1);
        Duration max = Duration.ofDays(365);
        BackoffStrategy strategy = BackoffStrategy.exponential(base, max);

        for (int attempt : new int[]{64, 100, 1000, Integer.MAX_VALUE}) {
            Duration delay = strategy.delay(attempt);
            assertThat(delay)
                    .as("delay for attempt %d must not be negative", attempt)
                    .isGreaterThanOrEqualTo(Duration.ZERO);
            assertThat(delay)
                    .as("delay for attempt %d must not exceed max", attempt)
                    .isLessThanOrEqualTo(max);
        }
    }

    /**
     * Regression test (issue #222): a zero base would make the clamp guard divide by zero; the fix
     * returns {@code max} instead.
     */
    @Test
    void testExponentialWithZeroBaseReturnsMax() {
        Duration max = Duration.ofSeconds(10);
        BackoffStrategy strategy = BackoffStrategy.exponential(Duration.ZERO, max);

        assertThat(strategy.delay(1)).isEqualTo(max);
        assertThat(strategy.delay(5)).isEqualTo(max);
        assertThat(strategy.delay(Integer.MAX_VALUE)).isEqualTo(max);
    }

    /**
     * Regression test (issue #222): a negative base must never yield a negative delay; the fix
     * returns {@code max}.
     */
    @Test
    void testExponentialWithNegativeBaseReturnsMax() {
        Duration max = Duration.ofSeconds(10);
        BackoffStrategy strategy = BackoffStrategy.exponential(Duration.ofSeconds(-1), max);

        assertThat(strategy.delay(1)).isEqualTo(max);
        assertThat(strategy.delay(5)).isEqualTo(max);
        assertThat(strategy.delay(Integer.MAX_VALUE)).isEqualTo(max);
    }
}
