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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Execute delegate implementing {@link ExecutePrimitive}.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 1.0.0
 */
@Internal
public class ExecuteDelegate extends AbstractStepExecutor implements ExecutePrimitive {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteDelegate.class);

    /**
     * Constructs the delegate.
     *
     * @param context                   workflow context.
     * @param workflowExecution         workflow state.
     * @param parentEventNameCustomizer event name customizer.
     * @param clock                     clock for time calculations.
     * @param unitOfWorkFactory         unit of work factory for creation of new processing contexts.
     * @param eventSink                 event sink for event publications.
     * @param executor                  executor to offload execution tasks from workflow thread.
     */
    @Internal
    public ExecuteDelegate(@Nonnull WorkflowContext context,
                           @Nonnull WorkflowExecution workflowExecution,
                           @Nonnull EventNameCustomizer parentEventNameCustomizer,
                           @Nonnull Clock clock,
                           @Nonnull UnitOfWorkFactory unitOfWorkFactory,
                           @Nonnull EventSink eventSink,
                           @Nonnull Executor executor
    ) {
        super(context, workflowExecution, parentEventNameCustomizer, clock, unitOfWorkFactory, eventSink, executor);
    }

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
        return execute(stepName, local, action, parameterPayloadReducer, resultPayloadReducer, timeout,
                       eventNameCustomizer,
                       // default failure handler — publish FAILED
                       (name, error, enc) ->
                               workflowExecution.appendTask(i -> failed(name, error, enc)),
                       // default timeout handler — publish TIMED_OUT
                       (name, enc) ->
                               workflowExecution.appendTask(i -> timedOut(name, clock.instant(), enc))
        );
    }

    @Nonnull
    WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nullable Map<String, Object> local,
            @Nonnull PayloadProcessor action,
            @Nonnull PayloadReducer parameterPayloadReducer,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer,
            @Nonnull FailureHandler failureHandler,
            @Nonnull TimeoutHandler timeoutHandler
    ) {
        logger.trace("Execute {} called from thread {}", stepName, Thread.currentThread());

        acceptAllPendingTasksForStep(stepName);

        if (!workflowExecution.state().containsStep(stepName)) {
            workflowExecution.appendTask(i ->
                                                 started(stepName, sanitize(local), eventNameCustomizer)
            );
            try {
                workflowExecution.awaitStateChange(s -> s.containsStep(stepName)
                        && s.getStep(stepName).status() == StepStatus.STARTED);
            } catch (InterruptedException e) {
                return WorkflowStepResults.failed(stepName, e);
            }
        }

        // FIXME -> consider to use QOS (at least once/at most once)
        var step = workflowExecution.state().getStep(stepName);
        if (step.status() == StepStatus.STARTED || step.status() == StepStatus.RETRYING) {
            var actualStartTime = step.timestamp();
            var remainingTimeout = Duration.between(Instant.now(clock),
                                                    actualStartTime.plus(timeout));
            // FIXME - This is where we capture our current consistency marker

            var result = unitOfWorkFactory
                    .create(stepName,
                            customize -> customize.workScheduler(executor)) // FIXME -> define a new thread pool for execution customer code
                    .executeWithResult(processingContext -> {
                        var procContext = ProcessingContextUtils.copyResources(workflowExecution.state()
                                                                                                .getStep(stepName)
                                                                                                .context(),
                                                                               processingContext);
                        var payload = parameterPayloadReducer.apply(workflowContext.workflowPayload(), local);
                        try {
                            return CompletableFuture.completedFuture(action.apply(procContext, payload));
                        } catch (StepCancellationException | WorkflowCancelledException | WorkflowFailedException t) {
                            // Framework control-flow signals must keep their original type
                            // — runtime dispatch downstream (e.g. isCancellation) relies on it.
                            throw t;
                        } catch (Throwable t) {
                            throw WorkflowError.from(t).toThrowable();
                        }
                    });

            workflowExecution.registerRunningStep(stepName, result);

            if (remainingTimeout.isNegative()) {
                workflowExecution.appendTask(i -> {
                    // TODO - Do one last check on the state to make sure we didn't have any concurrent state changes
                    // FIXME - This is where we should publish using an append condition
                    timeoutHandler.onTimeout(stepName, eventNameCustomizer);
                });
            } else {
                result
                        .orTimeout(remainingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                        .whenComplete((r, e) -> {
                            workflowExecution.removeRunningStep(stepName);
                            if (r != null) {
                                workflowExecution.appendTask(i -> {
                                    // FIXME - This is where we should publish using an append condition
                                    final String resultMappingName;
                                    if (PayloadReducer.isDefault(resultPayloadReducer)) {
                                        resultMappingName = PayloadReducer.name(resultPayloadReducer);
                                    } else {
                                        resultMappingName = null;
                                    }
                                    completed(stepName, r, resultMappingName, eventNameCustomizer);
                                });
                            } else {
                                if (e instanceof TimeoutException || e.getCause() instanceof TimeoutException) {
                                    // FIXME - This is where we should publish using an append condition
                                    workflowExecution.appendTask(
                                            i -> timeoutHandler.onTimeout(stepName, eventNameCustomizer));
                                } else if (isCancellation(e)) {
                                    var terminationCause = unwrapCancellation(e);
                                    // FIXME - This is where we should publish using an append condition
                                    workflowExecution.appendTask(i -> {
                                        cancelled(stepName, terminationCause, eventNameCustomizer);
                                    });
                                } else {
                                    // FIXME - This is where we should publish using an append condition
                                    Throwable failure = e instanceof CompletionException && e.getCause() != null
                                            ? e.getCause() : e;
                                    workflowExecution.appendTask(
                                            i -> failureHandler.onFailure(stepName, failure, eventNameCustomizer));
                                }
                            }
                        });
            }
        }

        return stateBased(stepName, workflowExecution);
    }
}
