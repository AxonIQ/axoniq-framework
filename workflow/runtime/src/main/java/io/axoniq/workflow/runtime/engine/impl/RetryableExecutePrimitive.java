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
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.RetryContext;
import io.axoniq.workflow.runtime.api.RetryPolicy;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.result.WorkflowStepResults;
import io.axoniq.workflow.runtime.engine.step.StepRetryInfo;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Decorator that adds retry behavior to an {@link ExecuteDelegate}.
 * <p>
 * For calls without a {@link RetryPolicy} (or with {@link RetryPolicy#NONE}),
 * execution is delegated straight through. When a retry policy is active,
 * this primitive provides custom {@link FailureHandler} and {@link TimeoutHandler}
 * callbacks that evaluate the policy and schedule retry attempts.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class RetryableExecutePrimitive extends AbstractStepExecutor implements ExecutePrimitive {

    private static final Logger logger = LoggerFactory.getLogger(RetryableExecutePrimitive.class);

    private final ExecuteDelegate delegate;

    RetryableExecutePrimitive(
            @Nonnull ExecuteDelegate delegate,
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor
    ) {
        super(workflowContext, workflowExecution, parentEventNameCustomizer, clock, unitOfWorkFactory, eventSink,
              executor);
        this.delegate = delegate;
    }

    // ---- No-retry path: straight delegation ----

    @Nonnull
    @Override
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterPayloadReducer,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer
    ) {
        return delegate.execute(stepName, local, action, parameterPayloadReducer, resultPayloadReducer, timeout, eventNameCustomizer);
    }

    // ---- Retry path ----

    @Nonnull
    @Override
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterPayloadReducer,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy
    ) {
        if (retryPolicy == RetryPolicy.NONE || retryPolicy.maxRetries() <= 0) {
            return delegate.execute(stepName, local, action, parameterPayloadReducer, resultPayloadReducer, timeout,
                                    eventNameCustomizer);
        }

        // Crash recovery: resume from persisted RETRYING state
        if (workflowExecution.state().containsStep(stepName)) {
            var step = workflowExecution.state().getStep(stepName);
            if (step.status() == StepStatus.RETRYING && step.result() instanceof StepRetryInfo info) {
                Instant retryReadyAt = computeRetryReadyAt(retryPolicy, info.attempt(), step.timestamp());
                scheduleRetryAttempt(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                                     timeout, eventNameCustomizer, retryPolicy,
                                     info.attempt() + 1, retryReadyAt);
                return WorkflowStepResults.stateBased(stepName, workflowExecution);
            }
        }

        // Normal path: first attempt
        return launchWithRetry(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                               timeout, eventNameCustomizer, retryPolicy, 1);
    }

    // ---- Core retry logic ----

    private WorkflowStepResult launchWithRetry(
            @Nonnull String stepName,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterPayloadReducer,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy,
            int attempt
    ) {
        FailureHandler failureHandler = (name, error, enc) ->
                handleAttemptFailure(name, error, false, local, action, parameterPayloadReducer,
                                     resultPayloadReducer, timeout, enc, retryPolicy, attempt);

        TimeoutHandler timeoutHandler = (name, enc) ->
                handleAttemptFailure(name, null, true, local, action, parameterPayloadReducer,
                                     resultPayloadReducer, timeout, enc, retryPolicy, attempt);

        return delegate.execute(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                               timeout, eventNameCustomizer, failureHandler, timeoutHandler);
    }

    private void handleAttemptFailure(
            @Nonnull String stepName,
            @Nullable Throwable error,
            boolean isTimeout,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterPayloadReducer,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy,
            int attempt
    ) {
        Duration backoffDelay = retryPolicy.backoffStrategy().delay(attempt);
        var retryContext = new RetryContext(stepName, attempt, retryPolicy.maxRetries(), error, backoffDelay);

        if (retryPolicy.shouldRetry(retryContext)) {
            retryPolicy.onRetryHandler().onRetry(retryContext);

            var retryInfo = new StepRetryInfo(attempt, retryPolicy.maxRetries(), error);
            workflowExecution.appendTask(i -> retrying(stepName, retryInfo, eventNameCustomizer));

            Instant retryReadyAt = computeRetryReadyAt(retryPolicy, attempt, clock.instant());
            scheduleRetryAttempt(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                                 timeout, eventNameCustomizer, retryPolicy,
                                 attempt + 1, retryReadyAt);
        } else {
            if (isTimeout) {
                workflowExecution.appendTask(i -> timedOut(stepName, clock.instant(), eventNameCustomizer));
            } else {
                workflowExecution.appendTask(i -> failed(stepName, error, eventNameCustomizer));
            }
        }
    }

    // ---- Backoff computation ----

    @Nullable
    private Instant computeRetryReadyAt(@Nonnull RetryPolicy retryPolicy, int attempt,
                                         @Nonnull Instant attemptStartTime) {
        Duration backoffDelay = retryPolicy.backoffStrategy().delay(attempt);
        return (backoffDelay.isZero() || backoffDelay.isNegative())
                ? null
                : attemptStartTime.plus(backoffDelay);
    }

    // ---- Delayed retry scheduling ----

    private void scheduleRetryAttempt(
            @Nonnull String stepName,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterPayloadReducer,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy,
            int nextAttempt,
            @Nullable Instant retryReadyAt
    ) {
        Duration delay = retryReadyAt == null ? Duration.ZERO
                : Duration.between(Instant.now(clock), retryReadyAt);

        if (delay.isNegative() || delay.isZero()) {
            // No backoff or already elapsed (crash recovery) — launch on next task cycle
            workflowExecution.appendTask(i -> {
                if (!i.state().getStep(stepName).status().isTerminal()) {
                    relaunchWithRetry(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                                      timeout, eventNameCustomizer, retryPolicy, nextAttempt);
                }
            });
        } else {
            // Schedule delayed launch via delayedExecutor (non-blocking, keeps workflow thread responsive)
            var backoffFuture = CompletableFuture.runAsync(
                    () -> workflowExecution.appendTask(i -> {
                        if (!i.state().getStep(stepName).status().isTerminal()) {
                            relaunchWithRetry(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                                              timeout, eventNameCustomizer, retryPolicy, nextAttempt);
                        }
                    }),
                    CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS)
            ).exceptionally(e -> {
                // Cancelled during backoff — cancellation handled by the event flow
                return null;
            });
            workflowExecution.registerRunningStep(stepName, backoffFuture);
        }
    }

    private void relaunchWithRetry(
            @Nonnull String stepName,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterPayloadReducer,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull RetryPolicy retryPolicy,
            int attempt
    ) {
        FailureHandler failureHandler = (name, error, enc) ->
                handleAttemptFailure(name, error, false, local, action, parameterPayloadReducer,
                                     resultPayloadReducer, timeout, enc, retryPolicy, attempt);

        TimeoutHandler timeoutHandler = (name, enc) ->
                handleAttemptFailure(name, null, true, local, action, parameterPayloadReducer,
                                     resultPayloadReducer, timeout, enc, retryPolicy, attempt);

        delegate.execute(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                         timeout, eventNameCustomizer, failureHandler, timeoutHandler);
    }
}
