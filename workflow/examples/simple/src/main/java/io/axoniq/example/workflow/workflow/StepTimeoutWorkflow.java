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
package io.axoniq.example.workflow.workflow;

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * Reproduction of AxonIQ/extension-workflow#122 — step-level timeout is not enforced.
 * <p>
 * The workflow exposes two variants keyed off the payload {@code status} field:
 * <ul>
 *   <li>{@code no-retry} — mirrors the reporter's exact setup: one step, timeout {@link #STEP_TIMEOUT},
 *       no retry policy, action busy-waits {@link #STEP_SLEEP_MILLIS}ms and returns normally.</li>
 *   <li>{@code retry} — adds {@link RetryPolicy#maxRetries(int)} with {@link #MAX_RETRIES}.</li>
 * </ul>
 * The action uses a busy-wait (ignoring interrupts) so the step cannot voluntarily honor the timeout.
 * The runtime's {@code orTimeout} scheduler must be what fires.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class StepTimeoutWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(StepTimeoutWorkflow.class);

    public static final Duration STEP_TIMEOUT = Duration.ofMillis(200);
    public static final long STEP_SLEEP_MILLIS = 1500L;
    public static final int MAX_RETRIES = 5;

    private final AtomicInteger attempts = new AtomicInteger(0);
    private volatile WorkflowStepResult stepResult;

    public int attempts() {
        return attempts.get();
    }

    public WorkflowStepResult stepResult() {
        return stepResult;
    }

    public void execute(@Nonnull SimpleWorkflowContext ctx) {
        logger.info("StepTimeoutWorkflow started for {}", ctx.workflowPayload());

        boolean useRetry = "retry".equals(ctx.workflowPayload().get("status"));

        if (useRetry) {
            stepResult = ctx.execute("slowStep", Map.of(), this::slowAction,
                                     STEP_TIMEOUT, defaults(), RetryPolicy.maxRetries(MAX_RETRIES));
        } else {
            stepResult = ctx.execute("slowStep", Map.of(), this::slowAction,
                                     STEP_TIMEOUT, defaults());
        }

        stepResult.await();

        logger.info("StepTimeoutWorkflow completed");
    }

    private Map<String, Object> slowAction(ProcessingContext c, Map<String, Object> p) {
        int attempt = attempts.incrementAndGet();
        long started = System.currentTimeMillis();
        double random = Math.random();
        logger.info("slowStep: attempt {} — busy-waiting for {}ms (random={})",
                    attempt, STEP_SLEEP_MILLIS, random);
        long deadline = started + STEP_SLEEP_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            Thread.interrupted();
        }
        long elapsed = System.currentTimeMillis() - started;
        logger.info("slowStep: attempt {} — returning after {}ms (timeout was {}ms)",
                    attempt, elapsed, STEP_TIMEOUT.toMillis());
        return Map.of("slowStep", "done", "random", random, "elapsedMs", elapsed);
    }
}
