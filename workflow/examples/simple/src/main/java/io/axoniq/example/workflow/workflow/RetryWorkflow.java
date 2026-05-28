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
package io.axoniq.example.workflow.workflow;

import io.axoniq.workflow.dsl.base.BaseWorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryContext;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.example.workflow.fixture.SleepUtils.sleepQuietly;

/**
 * Workflow exercising all retry scenarios:
 * <ol>
 *   <li>Normal step (no retry)</li>
 *   <li>Retry then succeed</li>
 *   <li>Retry with handler callback</li>
 *   <li>Cancel during retry</li>
 *   <li>Timeout during retry</li>
 *   <li>Retry exhaustion</li>
 *   <li>Retry with backoff</li>
 *   <li>Retry until predicate stops</li>
 * </ol>
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class RetryWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(RetryWorkflow.class);

    private final AtomicInteger retryThenSucceedAttempts = new AtomicInteger(0);
    private final AtomicInteger retryWithHandlerAttempts = new AtomicInteger(0);
    private final AtomicInteger cancelDuringRetryAttempts = new AtomicInteger(0);
    private final AtomicInteger timeoutDuringRetryAttempts = new AtomicInteger(0);
    private final AtomicInteger retryExhaustionAttempts = new AtomicInteger(0);
    private final AtomicInteger retryWithBackoffAttempts = new AtomicInteger(0);
    private final AtomicInteger retryWhileStopAttempts = new AtomicInteger(0);
    private final CopyOnWriteArrayList<RetryContext> handlerCalls = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Instant> backoffTimestamps = new CopyOnWriteArrayList<>();

    public List<RetryContext> getHandlerCalls() {
        return handlerCalls;
    }

    public List<Instant> getBackoffTimestamps() {
        return backoffTimestamps;
    }

    public void execute(@Nonnull BaseWorkflowContext ctx) {
        logger.info("Retry workflow started for {}", ctx.workflowPayload());

        // Step 1: Normal step — succeeds immediately, no retry policy
        ctx.awaitExecute(
                "normalStep",
                Map.of(),
                (c, p) -> {
                    logger.info("normalStep: executing");
                    return Map.of("normalStep", "done");
                }
        );

        // Step 2: Retry then succeed — fails 2 times, succeeds on attempt 3
        ctx.awaitExecute(
                "retryThenSucceed",
                Map.of(),
                (c, p) -> {
                    int attempt = retryThenSucceedAttempts.incrementAndGet();
                    logger.info("retryThenSucceed: attempt {}", attempt);
                    if (attempt <= 2) {
                        throw new RuntimeException("retryThenSucceed failure on attempt " + attempt);
                    }
                    return Map.of("retryThenSucceed", "done");
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(3))
        );

        //ctx.execute(...).retry(RetryPolicy.maxRetries(3)) -> make it composable, execution properties

        // Step 3: Retry with handler — fails 1 time, handler is called, succeeds on attempt 2
        ctx.awaitExecute(
                "retryWithHandler",
                Map.of(),
                (c, p) -> {
                    int attempt = retryWithHandlerAttempts.incrementAndGet();
                    logger.info("retryWithHandler: attempt {}", attempt);
                    if (attempt <= 1) {
                        throw new RuntimeException("retryWithHandler failure on attempt " + attempt);
                    }
                    return Map.of("retryWithHandler", "done");
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(2).onRetry(handlerCalls::add))
        );

        // Step 4: Cancel during retry — fails attempt 1 (triggers retry), throws StepCancellationException on attempt 2
        WorkflowStepResult r4 = ctx.execute(
                "cancelDuringRetry",
                Map.of(),
                (c, p) -> {
                    int attempt = cancelDuringRetryAttempts.incrementAndGet();
                    logger.info("cancelDuringRetry: attempt {}", attempt);
                    if (attempt == 1) {
                        throw new RuntimeException("cancelDuringRetry failure on attempt 1");
                    }
                    throw new StepCancellationException("cancelDuringRetry cancelled on attempt " + attempt);
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(3))
        );
        r4.await();

        // Step 5: Timeout during retry — each attempt sleeps 500ms; per-attempt timeout 300ms → TIMED_OUT
        WorkflowStepResult r5 = ctx.execute(
                "timeoutDuringRetry",
                Map.of(),
                (c, p) -> {
                    int attempt = timeoutDuringRetryAttempts.incrementAndGet();
                    logger.info("timeoutDuringRetry: attempt {}", attempt);
                    sleepQuietly(500);
                    throw new RuntimeException("timeoutDuringRetry failure on attempt " + attempt);
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(5)).timeout(Duration.ofMillis(300))
        );
        r5.await();

        // Step 6: Retry exhaustion — always throws, retries exhausted after 3 retries
        WorkflowStepResult r6 = ctx.execute(
                "retryExhaustion",
                Map.of(),
                (c, p) -> {
                    int attempt = retryExhaustionAttempts.incrementAndGet();
                    logger.info("retryExhaustion: attempt {}", attempt);
                    throw new RuntimeException("retryExhaustion failure on attempt " + attempt);
                },
                step -> step.retryPolicy(RetryPolicy.maxRetries(3))
        );
        r6.await();

        // Step 7: Retry with backoff — fails 2 times, succeeds on attempt 3 with fixed 200ms backoff
        ctx.awaitExecute(
                "retryWithBackoff",
                Map.of(),
                (c, p) -> {
                    backoffTimestamps.add(Instant.now());
                    int attempt = retryWithBackoffAttempts.incrementAndGet();
                    logger.info("retryWithBackoff: attempt {}", attempt);
                    if (attempt <= 2) {
                        throw new RuntimeException("retryWithBackoff failure on attempt " + attempt);
                    }
                    return Map.of("retryWithBackoff", "done");
                },
                step -> step.retryPolicy(
                        RetryPolicy.maxRetries(3)
                                   .withBackoff(BackoffStrategy.fixed(Duration.ofMillis(200)))
                )
        );

        // Step 8: retryWhile — always fails, predicate stops retrying after attempt 2 → FAILED
        WorkflowStepResult r8 = ctx.execute(
                "retryWhileStop",
                Map.of(),
                (c, p) -> {
                    int attempt = retryWhileStopAttempts.incrementAndGet();
                    throw new RuntimeException("retryWhileStop failure on attempt " + attempt);
                },
                step -> step.retryPolicy(RetryPolicy
                                                 .maxRetries(5)
                                                 .retryWhile(rc -> rc.attempt() < 2))
        );
        r8.await();

        logger.info("Retry workflow completed");
    }
}
