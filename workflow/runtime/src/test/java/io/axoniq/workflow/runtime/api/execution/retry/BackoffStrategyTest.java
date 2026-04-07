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

import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class BackoffStrategyTest {

    @Test
    void testNone() {
        BackoffStrategy strategy = BackoffStrategy.NONE;
        assertEquals(Duration.ZERO, strategy.delay(1));
        assertEquals(Duration.ZERO, strategy.delay(10));
    }

    @Test
    void testFixed() {
        Duration delay = Duration.ofSeconds(5);
        BackoffStrategy strategy = BackoffStrategy.fixed(delay);
        assertEquals(delay, strategy.delay(1));
        assertEquals(delay, strategy.delay(10));
    }

    @Test
    void testLinear() {
        Duration baseDelay = Duration.ofSeconds(2);
        BackoffStrategy strategy = BackoffStrategy.linear(baseDelay);
        assertEquals(Duration.ofSeconds(2), strategy.delay(1));
        assertEquals(Duration.ofSeconds(4), strategy.delay(2));
        assertEquals(Duration.ofSeconds(20), strategy.delay(10));
    }

    @Test
    void testExponential() {
        Duration base = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(10);
        BackoffStrategy strategy = BackoffStrategy.exponential(base, max);
        
        assertEquals(Duration.ofSeconds(1), strategy.delay(1)); // 1 * 2^0 = 1
        assertEquals(Duration.ofSeconds(2), strategy.delay(2)); // 1 * 2^1 = 2
        assertEquals(Duration.ofSeconds(4), strategy.delay(3)); // 1 * 2^2 = 4
        assertEquals(Duration.ofSeconds(8), strategy.delay(4)); // 1 * 2^3 = 8
        assertEquals(Duration.ofSeconds(10), strategy.delay(5)); // min(1 * 2^4 = 16, 10) = 10
        assertEquals(Duration.ofSeconds(10), strategy.delay(10));
    }
}
