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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    @Test
    void testNone() {
        RetryPolicy policy = RetryPolicy.NONE;
        assertEquals(0, policy.maxRetries());
        assertEquals(RetryHandler.NOOP, policy.onRetryHandler());
        assertEquals(BackoffStrategy.NONE, policy.backoffStrategy());
        
        RetryContext context = new RetryContext("step", 0, 0, new RuntimeException());
        assertFalse(policy.shouldRetry(context));
    }

    @Test
    void testMaxRetriesFactory() {
        RetryPolicy policy = RetryPolicy.maxRetries(3);
        assertEquals(3, policy.maxRetries());
        assertEquals(RetryHandler.NOOP, policy.onRetryHandler());
        assertEquals(BackoffStrategy.NONE, policy.backoffStrategy());
        
        RetryContext context = new RetryContext("step", 1, 3, new RuntimeException());
        assertTrue(policy.shouldRetry(context));
        
        RetryContext contextMax = new RetryContext("step", 3, 3, new RuntimeException());
        assertFalse(policy.shouldRetry(contextMax));
    }

    @Test
    void testConstructors() {
        RetryHandler handler = context -> {};
        BackoffStrategy backoff = BackoffStrategy.fixed(Duration.ofSeconds(1));
        
        RetryPolicy p1 = new RetryPolicy(2, handler);
        assertEquals(2, p1.maxRetries());
        assertEquals(handler, p1.onRetryHandler());
        assertEquals(BackoffStrategy.NONE, p1.backoffStrategy());

        RetryPolicy p2 = new RetryPolicy(2, handler, backoff);
        assertEquals(2, p2.maxRetries());
        assertEquals(handler, p2.onRetryHandler());
        assertEquals(backoff, p2.backoffStrategy());
    }

    @Test
    void testWithers() {
        RetryPolicy base = RetryPolicy.maxRetries(5);
        
        RetryHandler handler = context -> {};
        RetryPolicy p1 = base.onRetry(handler);
        assertEquals(5, p1.maxRetries());
        assertEquals(handler, p1.onRetryHandler());
        
        BackoffStrategy backoff = BackoffStrategy.linear(Duration.ofSeconds(1));
        RetryPolicy p2 = p1.withBackoff(backoff);
        assertEquals(backoff, p2.backoffStrategy());
        assertEquals(handler, p2.onRetryHandler());
        
        Predicate<RetryContext> predicate = ctx -> ctx.attempt() < 2;
        RetryPolicy p3 = p2.retryWhile(predicate);
        
        RetryContext ctx1 = new RetryContext("step", 1, 5, new RuntimeException());
        assertTrue(p3.shouldRetry(ctx1));
        
        RetryContext ctx2 = new RetryContext("step", 2, 5, new RuntimeException());
        assertFalse(p3.shouldRetry(ctx2));
    }

    @Test
    void testShouldRetryWithPredicate() {
        RetryPolicy policy = RetryPolicy.maxRetries(5)
                .retryWhile(ctx -> ctx.error() instanceof IllegalArgumentException);
        
        RetryContext ctx1 = new RetryContext("step", 1, 5, new IllegalArgumentException());
        assertTrue(policy.shouldRetry(ctx1));

        RetryContext ctx2 = new RetryContext("step", 1, 5, new RuntimeException());
        assertFalse(policy.shouldRetry(ctx2));

        RetryContext ctx3 = new RetryContext("step", 5, 5, new IllegalArgumentException());
        assertFalse(policy.shouldRetry(ctx3));
    }

    @Test
    void testRetryHandlerNoop() {
        AtomicBoolean called = new AtomicBoolean(false);
        RetryHandler handler = ctx -> called.set(true);
        
        RetryHandler.NOOP.onRetry(new RetryContext("step", 1, 1, new RuntimeException()));
        assertFalse(called.get());
        
        handler.onRetry(new RetryContext("step", 1, 1, new RuntimeException()));
        assertTrue(called.get());
    }
}
