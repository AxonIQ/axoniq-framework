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
package io.axoniq.framework.workflow.dsl.api.retry;

import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @Test
    void testNone() {
        RetryPolicy policy = RetryPolicy.NONE;
        assertThat(policy.maxRetries()).isZero();
        assertThat(policy.onRetryHandler()).isEqualTo(RetryHandler.NOOP);
        assertThat(policy.backoffStrategy()).isEqualTo(BackoffStrategy.NONE);

        RetryContext context = new RetryContext("step", 1, 0, new RuntimeException());
        assertThat(policy.shouldRetry(context)).isFalse();
    }

    @Test
    void testMaxRetriesFactory() {
        RetryPolicy policy = RetryPolicy.maxRetries(3);
        assertThat(policy.maxRetries()).isEqualTo(3);
        assertThat(policy.onRetryHandler()).isEqualTo(RetryHandler.NOOP);
        assertThat(policy.backoffStrategy()).isEqualTo(BackoffStrategy.NONE);

        RetryContext context = new RetryContext("step", 1, 3, new RuntimeException());
        assertThat(policy.shouldRetry(context)).isTrue();

        RetryContext contextMax = new RetryContext("step", 3, 3, new RuntimeException());
        assertThat(policy.shouldRetry(contextMax)).isTrue();

        RetryContext contextExceeded = new RetryContext("step", 4, 3, new RuntimeException());
        assertThat(policy.shouldRetry(contextExceeded)).isFalse();
    }

    @Test
    void testMaxRetriesOneShouldAllowOneRetry() {
        RetryPolicy policy = RetryPolicy.maxRetries(1);

        RetryContext firstFailure = new RetryContext("step", 1, 1, new RuntimeException());
        assertThat(policy.shouldRetry(firstFailure)).isTrue();

        RetryContext afterRetry = new RetryContext("step", 2, 1, new RuntimeException());
        assertThat(policy.shouldRetry(afterRetry)).isFalse();
    }

    @Test
    void testConstructors() {
        RetryHandler handler = context -> {
        };
        BackoffStrategy backoff = BackoffStrategy.fixed(Duration.ofSeconds(1));

        RetryPolicy p1 = new RetryPolicy(2, handler);
        assertThat(p1.maxRetries()).isEqualTo(2);
        assertThat(p1.onRetryHandler()).isEqualTo(handler);
        assertThat(p1.backoffStrategy()).isEqualTo(BackoffStrategy.NONE);

        RetryPolicy p2 = new RetryPolicy(2, handler, backoff);
        assertThat(p2.maxRetries()).isEqualTo(2);
        assertThat(p2.onRetryHandler()).isEqualTo(handler);
        assertThat(p2.backoffStrategy()).isEqualTo(backoff);
    }

    @Test
    void testWithers() {
        RetryPolicy base = RetryPolicy.maxRetries(5);

        RetryHandler handler = context -> {
        };
        RetryPolicy p1 = base.onRetry(handler);
        assertThat(p1.maxRetries()).isEqualTo(5);
        assertThat(p1.onRetryHandler()).isEqualTo(handler);

        BackoffStrategy backoff = BackoffStrategy.linear(Duration.ofSeconds(1));
        RetryPolicy p2 = p1.withBackoff(backoff);
        assertThat(p2.backoffStrategy()).isEqualTo(backoff);
        assertThat(p2.onRetryHandler()).isEqualTo(handler);

        Predicate<RetryContext> predicate = ctx -> ctx.attempt() < 2;
        RetryPolicy p3 = p2.retryWhile(predicate);

        RetryContext ctx1 = new RetryContext("step", 1, 5, new RuntimeException());
        assertThat(p3.shouldRetry(ctx1)).isTrue();

        RetryContext ctx2 = new RetryContext("step", 2, 5, new RuntimeException());
        assertThat(p3.shouldRetry(ctx2)).isFalse();
    }

    @Test
    void testShouldRetryWithPredicate() {
        RetryPolicy policy = RetryPolicy.maxRetries(5)
                                        .retryWhile(ctx -> ctx.error() instanceof IllegalArgumentException);

        RetryContext ctx1 = new RetryContext("step", 1, 5, new IllegalArgumentException());
        assertThat(policy.shouldRetry(ctx1)).isTrue();

        RetryContext ctx2 = new RetryContext("step", 1, 5, new RuntimeException());
        assertThat(policy.shouldRetry(ctx2)).isFalse();

        RetryContext ctx3 = new RetryContext("step", 6, 5, new IllegalArgumentException());
        assertThat(policy.shouldRetry(ctx3)).isFalse();
    }

    @Test
    void testRetryHandlerNoop() {
        AtomicBoolean called = new AtomicBoolean(false);
        RetryHandler handler = ctx -> called.set(true);

        RetryHandler.NOOP.onRetry(new RetryContext("step", 1, 1, new RuntimeException()));
        assertThat(called.get()).isFalse();

        handler.onRetry(new RetryContext("step", 1, 1, new RuntimeException()));
        assertThat(called.get()).isTrue();
    }
}
