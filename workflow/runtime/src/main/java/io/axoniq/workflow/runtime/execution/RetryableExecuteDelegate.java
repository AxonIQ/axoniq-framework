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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryContext;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Decorator that adds retry behavior to an {@link ExecuteDelegate}.
 * <p>
 * For calls without a {@link RetryPolicy} (or with {@link RetryPolicy#NONE}), execution is delegated straight through.
 * When a retry policy is active, this primitive provides custom {@link FailureHandler} and {@link TimeoutHandler}
 * callbacks that evaluate the policy and schedule retry attempts.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public class RetryableExecuteDelegate extends AbstractStepExecutor implements ExecutePrimitive {

    private final ExecuteDelegate delegate;

    /**
     * Constructs the delegate.
     * @param delegate executoion delegate.
     * @param workflowContext workflow context.
     * @param workflowExecution workflow execution.
     * @param parentEventNameCustomizer parent event name customizer.
     * @param clock clock for time calculations.
     * @param unitOfWorkFactory unit of work factory for creation of new processing contexts.
     * @param eventSink event sink for event publications.
     * @param executor executor to offload execution tasks from workflow thread.
     */
    @Internal
    public RetryableExecuteDelegate(
            @Nonnull ExecuteDelegate delegate,
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull ExecutorService executor
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
        return delegate.execute(stepName,
                                local,
                                action,
                                parameterPayloadReducer,
                                resultPayloadReducer,
                                timeout,
                                eventNameCustomizer);
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
                return stateBased(stepName, workflowExecution);
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
                    launchWithRetry(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
                                      timeout, eventNameCustomizer, retryPolicy, nextAttempt);
                }
            });
        } else {
            // Schedule delayed launch via delayedExecutor (non-blocking, keeps workflow thread responsive)
            var backoffFuture = CompletableFuture.runAsync(
                    () -> workflowExecution.appendTask(i -> {
                        if (!i.state().getStep(stepName).status().isTerminal()) {
                            launchWithRetry(stepName, local, action, parameterPayloadReducer, resultPayloadReducer,
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

}
