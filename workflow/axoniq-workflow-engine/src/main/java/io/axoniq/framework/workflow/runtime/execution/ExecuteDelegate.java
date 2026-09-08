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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepIndeterminateException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowError;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.util.FutureResolver;
import io.axoniq.framework.workflow.runtime.util.ProcessingContextUtils;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Execute delegate implementing {@link ExecutePrimitive}.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
public class ExecuteDelegate extends AbstractStepExecutor implements ExecutePrimitive {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteDelegate.class);
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;
    private final ExecuteStepActionResolver actionResolver;
    /**
     * Steps whose attempt currently in flight was started by this execution: the store accepted this execution's
     * {@code STARTED} or {@code RETRY_STARTED} append for it. Ownership is per attempt and is dropped when the attempt
     * resolves, so every retry attempt has to earn it again.
     */
    private final Set<String> ownStartedSteps = ConcurrentHashMap.newKeySet();

    /**
     * Constructs the delegate.
     *
     * @param context                   workflow context
     * @param workflowExecution         workflow state
     * @param runningSteps              running step registry
     * @param reachedSteps              reached steps tracker
     * @param parentEventNameCustomizer event name customizer
     * @param clock                     clock for time calculations
     * @param unitOfWorkFactory         unit of work factory for creation of new processing contexts
     * @param executor                  executor to offload execution tasks from workflow thread
     * @param timeoutScheduler          scheduler for workflow step timeouts
     * @param actionResolver            resolver for execute step actions
     */
    @Internal
    public ExecuteDelegate(WorkflowContext context,
                           WorkflowExecution workflowExecution,
                           RunningSteps runningSteps,
                           ReachedSteps reachedSteps,
                           EventNameCustomizer parentEventNameCustomizer,
                           Clock clock,
                           UnitOfWorkFactory unitOfWorkFactory,
                           Executor executor,
                           WorkflowScheduler timeoutScheduler,
                           ExecuteStepActionResolver actionResolver
    ) {
        super(context,
              workflowExecution,
              runningSteps,
              reachedSteps,
              parentEventNameCustomizer,
              clock,
              timeoutScheduler);
        this.unitOfWorkFactory = unitOfWorkFactory;
        this.executor = executor;
        this.actionResolver = actionResolver;
    }

    @Override
    public WorkflowStepResult execute(ExecutePrimitive.ExecuteCommand command) {
        return execute(command,
                // default failure handler — publish FAILED
                       (name, error, enc) ->
                               workflowExecution.appendTask(i -> failed(name, error, enc)),
                // default timeout handler — publish TIMED_OUT
                       (name, enc) ->
                               workflowExecution.appendTask(i -> timedOut(name, clock.instant(), enc))
        );
    }

    WorkflowStepResult execute(
            ExecutePrimitive.ExecuteCommand command,
            FailureHandler failureHandler,
            TimeoutHandler timeoutHandler
    ) {
        var stepName = command.stepName();
        var local = command.local();
        var parameterPayloadReducer = command.parameterPayloadReducer();
        var resultPayloadReducer = command.resultPayloadReducer();
        var timeout = command.timeout();
        var eventNameCustomizer = command.eventNameCustomizer();
        logger.trace("Execute {} called from thread {}", stepName, Thread.currentThread());

        reachedSteps.record(stepName);

        acceptAllPendingTasksForStep(stepName);

        // AT-MOST-ONCE: read the step state once every pending task is applied, and before this run publishes its
        // own start record. A step present with an in-flight attempt (STARTED or RETRY_STARTED) that this execution
        // did not start belongs to another run: either a prior incarnation's attempt rebuilt from the durable log, or
        // the run of whichever execution owns the instance now. Both may already have performed the step's external
        // effect, so this run must NOT execute the action; instead route the attempt through the regular error flow
        // via the passed-in failure handler (no retry policy -> step FAILED with StepIndeterminateException; retry
        // policy -> RETRYING + next attempt). A live retry attempt reaches this method with status RETRYING and earns
        // ownership below by getting its own RETRY_STARTED accepted.
        var currentState = workflowExecution.state();
        boolean resumedInFlight = (WorkflowStateUtils.isStepStatus(currentState, stepName, StepStatus.STARTED)
                || WorkflowStateUtils.isStepStatus(currentState, stepName, StepStatus.RETRY_STARTED))
                && !ownStartedSteps.contains(stepName);

        if (resumedInFlight) {
            failureHandler.onFailure(stepName, new StepIndeterminateException(stepName), eventNameCustomizer);
            return stateBased(stepName, eventNameCustomizer, workflowExecution);
        }

        if (!workflowExecution.state().containsStep(stepName)) {
            reachedSteps.assertNoReplayDrift(workflowExecution.workflowId(), workflowExecution.state(), stepName);
            if (!tryStartStep(stepName,
                              () -> started(stepName, sanitize(local), eventNameCustomizer),
                              StepStatus.STARTED)) {
                return WorkflowStepResults.canceled(stepName);
            }
        } else {
            var existing = workflowExecution.state().getStep(stepName);
            if (existing.status() == StepStatus.RETRYING && existing.result() instanceof StepRetryInfo previous) {
                // A retry attempt passes the same gate as the first one: the action runs only after the store
                // accepted this execution's own RETRY_STARTED for the attempt. A node that lost the instance during
                // the backoff is rejected here and never runs the action.
                var attempt = new StepRetryInfo(previous.attempt() + 1, previous.maxRetries(), previous.error());
                if (!tryStartStep(stepName,
                                  () -> retryStarted(stepName, attempt, eventNameCustomizer),
                                  StepStatus.RETRY_STARTED)) {
                    return WorkflowStepResults.canceled(stepName);
                }
            }
        }

        var step = workflowExecution.state().getStep(stepName);
        if (step.status() == StepStatus.STARTED || step.status() == StepStatus.RETRY_STARTED) {
            var actualStartTime = step.timestamp();
            var timeoutDeadline = actualStartTime.plus(timeout);
            var remainingTimeout = Duration.between(clock.instant(), timeoutDeadline);

            var result = unitOfWorkFactory
                    .create(stepName,
                            customize -> customize.workScheduler(executor))
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

            runningSteps.register(stepName, result);

            if (remainingTimeout.isNegative()) {
                workflowExecution.appendTask(i -> {
                    timeoutHandler.onTimeout(stepName, eventNameCustomizer);
                });
            } else {
                var timeoutTask = timeoutScheduler.schedule(timeoutDeadline);
                timeoutTask.completion().thenRun(() -> {
                    if (!result.isDone()) {
                        result.completeExceptionally(new TimeoutException(
                                "Step '" + stepName + "' timed out"));
                    }
                });
                result.whenComplete((r, e) -> {
                    timeoutTask.cancel();
                    runningSteps.remove(stepName);
                    // Ownership is per attempt: a following retry attempt must get its own RETRY_STARTED accepted.
                    ownStartedSteps.remove(stepName);
                    if (e == null) {
                        // Normal completion — a null action result sanitizes to an empty map in completed(),
                        // so a null-returning action COMPLETES rather than wedging on a null-e dereference.
                        workflowExecution.appendTask(i -> {
                            completed(stepName, r, resultPayloadReducer.name(), eventNameCustomizer);
                        });
                    } else {
                        if (e instanceof TimeoutException || e.getCause() instanceof TimeoutException) {
                            workflowExecution.appendTask(
                                    i -> timeoutHandler.onTimeout(stepName, eventNameCustomizer));
                        } else if (isCancellation(e)) {
                            var terminationCause = unwrapCancellation(e);
                            workflowExecution.appendTask(i -> {
                                cancelled(stepName, terminationCause, eventNameCustomizer);
                            });
                        } else {
                            var cause = unwrapCompletionException(e);
                            if (cause instanceof StepInterruptedException) {
                                // A whole-workflow terminal transition interrupts a running step only to unblock the
                                // workflow body. It has no corresponding durable step-terminal event.
                            } else {
                                workflowExecution.appendTask(
                                        i -> failureHandler.onFailure(stepName, cause, eventNameCustomizer));
                            }
                        }
                    }
                });
            }
        }

        return stateBased(stepName, eventNameCustomizer, workflowExecution);
    }

    /**
     * Publishes this execution's start record for one attempt of the given step ({@code STARTED} for the first
     * attempt, {@code RETRY_STARTED} for a retry) and waits until the step is present with the expected status. That
     * state change carries no writer identity: the event may have been recorded by another execution of the same
     * workflow instance and delivered here over this execution's own event stream. The store accepting this
     * execution's own append is therefore the only proof that this execution took the attempt. The append is then
     * resolved through the workflow's bounded future-resolution policy before the action is allowed to run.
     *
     * @param stepName name of the step to start
     * @param publish  publishes this execution's start record for the attempt
     * @param expected the step status the start record evolves the step into
     * @return {@code true} when the store accepted this execution's append, so this execution owns the attempt and may
     * run its action. {@code false} when the append was rejected, when the step reached the expected status before
     * this execution's append ran, or when the wait was interrupted (the interrupt flag is restored)
     */
    private boolean tryStartStep(String stepName, Supplier<CompletableFuture<Void>> publish, StepStatus expected) {
        var ownStarted = new AtomicReference<CompletableFuture<Void>>();
        workflowExecution.appendTask(i -> ownStarted.set(publish.get()));
        try {
            workflowExecution.awaitStateChange(WorkflowStateUtils.stepStatus(stepName, expected));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        var ownAppend = ownStarted.get();
        if (ownAppend == null) {
            return false;
        }
        var acceptedAppend = ownAppend.handle((result, failure) -> failure == null);
        FutureResolver.resolve(workflowExecution.processingContext(), acceptedAppend);
        boolean accepted = acceptedAppend.getNow(false);
        if (accepted) {
            ownStartedSteps.add(stepName);
        } else {
            logger.info("The {} event of step '{}' of workflow '{}' is not this execution's. Leaving the step "
                                + "to the execution that recorded it.",
                        expected, stepName, workflowExecution.workflowId());
        }
        return accepted;
    }
}
