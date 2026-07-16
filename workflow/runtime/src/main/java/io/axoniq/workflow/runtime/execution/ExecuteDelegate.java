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
import io.axoniq.workflow.runtime.api.execution.state.StepIndeterminateException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
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
    private final ExecuteStepActionResolver actionResolver;

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
                           @Nonnull Executor executor,
                           @Nonnull WorkflowScheduler timeoutScheduler,
                           @Nonnull ExecuteStepActionResolver actionResolver
    ) {
        super(context,
              workflowExecution,
              parentEventNameCustomizer,
              clock,
              unitOfWorkFactory,
              eventSink,
              executor,
              timeoutScheduler);
        this.actionResolver = actionResolver;
    }

    @Nonnull
    @Override
    public WorkflowStepResult execute(@Nonnull ExecutePrimitive.ExecuteCommand command) {
        return execute(command,
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
            @Nonnull ExecutePrimitive.ExecuteCommand command,
            @Nonnull FailureHandler failureHandler,
            @Nonnull TimeoutHandler timeoutHandler
    ) {
        var stepName = command.stepName();
        var local = command.local();
        var parameterPayloadReducer = command.parameterPayloadReducer();
        var resultPayloadReducer = command.resultPayloadReducer();
        var timeout = command.timeout();
        var eventNameCustomizer = command.eventNameCustomizer();
        logger.trace("Execute {} called from thread {}", stepName, Thread.currentThread());

        // AT-MOST-ONCE: snapshot the step state BEFORE this run publishes STARTED. A step already present-and-STARTED
        // here can only be a prior incarnation's in-flight attempt rebuilt from the durable log (a fresh run has not
        // published STARTED yet at this point), so its external effect may already have run. To keep effects
        // at-most-once we must NOT re-run the action; instead route the interrupted attempt through the regular error
        // flow via the passed-in failure handler (no retry policy -> step FAILED with StepIndeterminateException; retry
        // policy -> RETRYING + next attempt). Live retry attempts reach this method with status RETRYING, never STARTED,
        // so they are unaffected and still execute.
        boolean resumedInFlight = workflowExecution.state().containsStep(stepName)
                && workflowExecution.state().getStep(stepName).status() == StepStatus.STARTED;

        workflowExecution.recordStepReference(stepName);

        acceptAllPendingTasksForStep(stepName);

        if (resumedInFlight) {
            failureHandler.onFailure(stepName, new StepIndeterminateException(stepName), eventNameCustomizer);
            return stateBased(stepName, workflowExecution);
        }

        if (!workflowExecution.state().containsStep(stepName)) {
            workflowExecution.guardAgainstReplayDrift(stepName);
            workflowExecution.appendTask(i ->
                                                 started(stepName, sanitize(local), eventNameCustomizer)
            );
            try {
                workflowExecution.awaitStateChange(s -> s.containsStep(stepName)
                        && s.getStep(stepName).status() == StepStatus.STARTED);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return WorkflowStepResults.canceled(stepName);
            }
        }

        // FIXME -> consider to use QOS (at least once/at most once)
        var step = workflowExecution.state().getStep(stepName);
        if (step.status() == StepStatus.STARTED || step.status() == StepStatus.RETRYING) {
            var actualStartTime = step.timestamp();
            var timeoutDeadline = actualStartTime.plus(timeout);
            var remainingTimeout = Duration.between(clock.instant(), timeoutDeadline);
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
                            var action = actionResolver.resolve(workflowContext, workflowExecution, command);
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
                var timeoutTask = timeoutScheduler.schedule(
                        timeoutDeadline,
                        () -> {
                            if (!result.isDone()) {
                                result.completeExceptionally(new TimeoutException(
                                        "Step '" + stepName + "' timed out"));
                            }
                        }
                );
                result.whenComplete((r, e) -> {
                            timeoutTask.cancel();
                            workflowExecution.removeRunningStep(stepName);
                            if (r != null) {
                                workflowExecution.appendTask(i -> {
                                    // FIXME - This is where we should publish using an append condition
                                    completed(stepName, r, resultPayloadReducer.name(), eventNameCustomizer);
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
