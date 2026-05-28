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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryContext;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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

    @Nonnull
    @Override
    public WorkflowStepResult execute(@Nonnull ExecutePrimitive.ExecuteCommand command) {
        var retryPolicy = command.retryPolicy();
        if (retryPolicy == RetryPolicy.NONE || retryPolicy.maxRetries() <= 0) {
            return delegate.execute(command);
        }

        var stepName = command.stepName();

        // Crash recovery: resume from persisted RETRYING state
        if (workflowExecution.state().containsStep(stepName)) {
            var step = workflowExecution.state().getStep(stepName);
            if (step.status() == StepStatus.RETRYING && step.result() instanceof StepRetryInfo info) {
                Instant retryReadyAt = computeRetryReadyAt(retryPolicy, info.attempt(), step.timestamp());
                scheduleRetryAttempt(command, info.attempt() + 1, retryReadyAt);
                return stateBased(stepName, workflowExecution);
            }
        }

        // Normal path: first attempt
        return launchWithRetry(command, 1);
    }

    // ---- Core retry logic ----

    private WorkflowStepResult launchWithRetry(@Nonnull ExecutePrimitive.ExecuteCommand command, int attempt) {
        FailureHandler failureHandler = (name, error, enc) ->
                handleAttemptFailure(command, name, error, false, enc, attempt);

        TimeoutHandler timeoutHandler = (name, enc) ->
                handleAttemptFailure(command, name, null, true, enc, attempt);

        return delegate.execute(command, failureHandler, timeoutHandler);
    }


    private void handleAttemptFailure(
            @Nonnull ExecutePrimitive.ExecuteCommand command,
            @Nonnull String stepName,
            @Nullable Throwable error,
            boolean isTimeout,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            int attempt
    ) {
        var retryPolicy = command.retryPolicy();
        Duration backoffDelay = retryPolicy.backoffStrategy().delay(attempt);
        var retryContext = new RetryContext(stepName, attempt, retryPolicy.maxRetries(), error, backoffDelay);

        if (retryPolicy.shouldRetry(retryContext)) {
            retryPolicy.onRetryHandler().onRetry(retryContext);

            var retryInfo = new StepRetryInfo(attempt, retryPolicy.maxRetries(), WorkflowError.from(error));
            workflowExecution.appendTask(i -> retrying(stepName, retryInfo, eventNameCustomizer));
            try {
                workflowExecution.awaitStateChange(s -> {
                    var st = s.getStep(stepName);
                    if (st.status().isTerminal()) {
                        return true;
                    }
                    return st.status() == StepStatus.RETRYING
                            && st.result() instanceof StepRetryInfo r
                            && r.attempt() == attempt;
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (workflowExecution.state().getStep(stepName).status().isTerminal()) {
                return;
            }

            Instant retryReadyAt = computeRetryReadyAt(retryPolicy, attempt, clock.instant());
            scheduleRetryAttempt(command, attempt + 1, retryReadyAt);
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
            @Nonnull ExecutePrimitive.ExecuteCommand command,
            int nextAttempt,
            @Nullable Instant retryReadyAt
    ) {
        var stepName = command.stepName();
        Duration delay = retryReadyAt == null ? Duration.ZERO
                : Duration.between(Instant.now(clock), retryReadyAt);

        if (delay.isNegative() || delay.isZero()) {
            // No backoff or already elapsed (crash recovery) — launch on next task cycle
            workflowExecution.appendTask(i -> {
                if (!i.state().getStep(stepName).status().isTerminal()) {
                    launchWithRetry(command, nextAttempt);
                }
            });
        } else {
            // Schedule delayed launch via delayedExecutor (non-blocking, keeps workflow thread responsive)
            var backoffFuture = CompletableFuture.runAsync(
                    () -> workflowExecution.appendTask(i -> {
                        if (!i.state().getStep(stepName).status().isTerminal()) {
                            launchWithRetry(command, nextAttempt);
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
