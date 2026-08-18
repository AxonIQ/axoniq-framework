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
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.util.WorkflowStateUtils;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.util.EventMessageUtils.*;

/**
 * Abstract class for step executor implementations.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public abstract class AbstractStepExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractStepExecutor.class);
    protected final WorkflowContext workflowContext;
    protected final WorkflowExecution workflowExecution;
    protected final RunningSteps runningSteps;
    protected final ReachedSteps reachedSteps;
    protected final Clock clock;
    protected final EventNameCustomizer parentEventNameCustomizer;
    protected final UnitOfWorkFactory unitOfWorkFactory;
    protected final EventSink eventSink;
    protected final Executor executor;
    protected final WorkflowScheduler timeoutScheduler;

    /**
     * Constructs the abstract step executor.
     *
     * @param workflowContext           workflow context
     * @param workflowExecution         workflow execution
     * @param runningSteps              running step registry
     * @param reachedSteps              reached steps tracker
     * @param parentEventNameCustomizer parent event name customizer
     * @param clock                     clock for time calculations
     * @param unitOfWorkFactory         unit of work factory for creation of new processing contexts
     * @param eventSink                 event sink for event publications
     * @param executor                  executor to offload execution tasks from workflow thread
     * @param timeoutScheduler          timeout scheduler
     */
    @Internal
    public AbstractStepExecutor(
            WorkflowContext workflowContext,
            WorkflowExecution workflowExecution,
            RunningSteps runningSteps,
            ReachedSteps reachedSteps,
            EventNameCustomizer parentEventNameCustomizer,
            Clock clock,
            UnitOfWorkFactory unitOfWorkFactory,
            EventSink eventSink,
            Executor executor,
            WorkflowScheduler timeoutScheduler
    ) {
        this.clock = Objects.requireNonNull(clock, "Clock is mandatory");
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow state is mandatory");
        this.runningSteps = Objects.requireNonNull(runningSteps, "Running steps are mandatory");
        this.reachedSteps = Objects.requireNonNull(reachedSteps, "Reached steps tracker is mandatory");
        this.parentEventNameCustomizer = Objects.requireNonNull(parentEventNameCustomizer,
                                                                "Event name customizer is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UoW Factory state is mandatory");
        this.eventSink = Objects.requireNonNull(eventSink, "Event sink is mandatory");
        this.executor = executor;
        this.timeoutScheduler = Objects.requireNonNull(timeoutScheduler, "Timeout scheduler is mandatory");
    }

    protected void acceptAllPendingTasksForStep(String stepName) {
        while ((!workflowExecution.state().containsStep(stepName) && workflowExecution.hasTasks())
                || !workflowExecution.isRunning()) {
            var poll = workflowExecution.getNextTask();
            if (poll != null) {
                poll.accept(this.workflowExecution);
            }
        }
    }

    protected Context getContext(String stepName) {
        return workflowExecution.state().containsStep(stepName)
                ? workflowExecution.state().getStep(stepName).context()
                : workflowExecution.processingContext();
    }

    protected CompletableFuture<Void> started(String stepName, Map<String, @Nullable Object> payload,
                                              EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, startedStep(workflowContext, stepName, sanitize(payload),
                                                   merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> startedWaitForEvent(String stepName,
                                                          Map<String, @Nullable Object> payload,
                                                          EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, startedWaitForEventStep(workflowContext, stepName, sanitize(payload),
                                                               merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> completed(String stepName, Map<String, @Nullable Object> payload,
                                                EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, completedStep(workflowContext, stepName, sanitize(payload),
                                                     null,
                                                     merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> completed(String stepName, Map<String, @Nullable Object> payload,
                                                @Nullable String payloadReducerName,
                                                EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, completedStep(workflowContext, stepName, sanitize(payload),
                                                     payloadReducerName,
                                                     merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> completedWaitForEvent(String stepName,
                                                            Map<String, @Nullable Object> payload,
                                                            @Nullable String payloadReducerName,
                                                            EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, completedWaitForEventStep(workflowContext,
                                                                 stepName,
                                                                 sanitize(payload),
                                                                 payloadReducerName,
                                                                 merge(parentEventNameCustomizer,
                                                                       eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> cancelled(String stepName,
                                                EventNameCustomizer eventNameCustomizer) {
        return cancelled(stepName, null, eventNameCustomizer);
    }

    protected CompletableFuture<Void> cancelled(String stepName,
                                                @Nullable Throwable cause,
                                                EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, cancelledStep(workflowContext, stepName, cause,
                                                     merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    /**
     * Registers the given future as the step's running phase while the step is parked (a wait timeout window, a retry
     * backoff window). When the future is completed exceptionally with a cancellation cause, the step is recorded
     * CANCELLED through the guarded publish path and the cleanup hook runs; any other completion only removes the
     * running-step registration, leaving the durable step state untouched.
     *
     * @param stepName            name of the parked step
     * @param parkedPhase         future representing the parked phase
     * @param eventNameCustomizer event name customizer of the step
     * @param onCancelled         cleanup to run when the parked phase is cancelled
     */
    protected void registerParkedStep(String stepName,
                                      CompletableFuture<?> parkedPhase,
                                      EventNameCustomizer eventNameCustomizer,
                                      Runnable onCancelled) {
        registerParkedStep(stepName, parkedPhase, () -> {
        }, eventNameCustomizer, onCancelled);
    }

    /**
     * Registers a parked step and owns the lifetime of its timer.
     *
     * @param stepName            name of the parked step
     * @param parkedPhase         future representing the parked phase
     * @param cancelTimer         cancels the timer associated with the parked phase
     * @param eventNameCustomizer event name customizer of the step
     * @param onCancelled         cleanup to run when the parked phase is cancelled
     */
    protected void registerParkedStep(String stepName,
                                      CompletableFuture<?> parkedPhase,
                                      Runnable cancelTimer,
                                      EventNameCustomizer eventNameCustomizer,
                                      Runnable onCancelled) {
        // Register before observing completion. If a timer has already completed, whenComplete removes this exact
        // registration immediately instead of leaving a completed future permanently marked as running.
        runningSteps.register(stepName, parkedPhase);
        parkedPhase.whenComplete((result, e) -> {
            runningSteps.remove(stepName);
            cancelTimer.run();
            if (e != null && isCancellation(e)) {
                onCancelled.run();
                var terminationCause = unwrapCancellation(e);
                workflowExecution.appendTask(i -> {
                    if (!WorkflowStateUtils.isStepTerminal(i.state(), stepName)) {
                        cancelled(stepName, terminationCause, eventNameCustomizer);
                    }
                });
            }
        });
    }

    protected CompletableFuture<Void> cancelledWaitForEvent(String stepName,
                                                            @Nullable Throwable cause,
                                                            EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, cancelledWaitForEventStep(workflowContext, stepName, cause,
                                                                 merge(parentEventNameCustomizer,
                                                                       eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> failed(String stepName, Throwable ex,
                                             EventNameCustomizer eventNameCustomizer) {
        logger.error("Step '{}' failed in workflow '{}'", stepName, workflowExecution.workflowId(), ex);
        return sendStepEvent(stepName, failStep(workflowContext, stepName, ex,
                                                merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> retrying(String stepName,
                                               StepRetryInfo retryInfo,
                                               EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, retryingStep(workflowContext, stepName, retryInfo,
                                                    merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> timedOut(String stepName,
                                               EventNameCustomizer eventNameCustomizer) {
        return timedOut(stepName, Instant.now(clock), eventNameCustomizer);
    }

    protected CompletableFuture<Void> timedOut(String stepName, Instant timeoutTimestamp,
                                               EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, timeoutStep(workflowContext, stepName, timeoutTimestamp,
                                                   merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    protected CompletableFuture<Void> timedOutWaitForEvent(String stepName,
                                                           EventNameCustomizer eventNameCustomizer) {
        return timedOutWaitForEvent(stepName, Instant.now(clock), eventNameCustomizer);
    }

    protected CompletableFuture<Void> timedOutWaitForEvent(String stepName,
                                                           Instant timeoutTimestamp,
                                                           EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, timeoutWaitForEventStep(workflowContext, stepName, timeoutTimestamp,
                                                               merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    private CompletableFuture<Void> sendStepEvent(String stepName,
                                                  EventMessage eventMessage,
                                                  Context context) {
        if (workflowContext.workflowStatus().isTerminal()) {
            logger.debug("Skipping step event {} — workflow is in terminal state {}", eventMessage.type(),
                         workflowContext.workflowStatus());
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Workflow is in terminal state " + workflowContext.workflowStatus()
                            + ", cannot publish step event " + eventMessage.type()));
        }
        if (WorkflowStateUtils.isStepTerminal(workflowExecution.state(), stepName)) {
            logger.debug("Skipping step event {} — step '{}' is already in terminal state {}", eventMessage.type(),
                         stepName, workflowExecution.state().getStep(stepName).status());
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Step '" + stepName + "' is in terminal state " + workflowExecution.state().getStep(stepName)
                                                                                       .status()
                            + ", cannot publish step event " + eventMessage.type()));
        }
        return appendEvent(eventMessage, context);
    }

    /**
     * Appends the given {@code eventMessage} under its append condition, deriving the append's unit of work from the
     * given {@code context}. Use this from a primitive that publishes an event without the step guards of
     * {@code sendStepEvent}, so every append still shares one condition and one fencing path.
     *
     * @param eventMessage the event to append.
     * @param context      the context the append's unit of work derives its resources from.
     * @return a future completing once the event is appended.
     */
    protected CompletableFuture<Void> appendEvent(EventMessage eventMessage, Context context) {
        return WorkflowAppendConditions.append(eventSink,
                                               unitOfWorkFactory,
                                               executor,
                                               context,
                                               eventMessage,
                                               workflowExecution);
    }

    protected Map<String, @Nullable Object> sanitize(@Nullable Map<String, @Nullable Object> payload) {
        if (payload == null) {
            return new LinkedHashMap<>();
        }
        return payload;
    }

    protected Map<String, @Nullable Object> eventMessagePayload(EventMessage eventMessage) {
        return sanitize(eventMessage.payloadAs(new TypeReference<>() {
                        })
        );
    }

    /**
     * Determines whether an exceptional step completion represents cancellation rather than failure or interruption.
     *
     * @param error completion error to classify
     * @return {@code true} when the error represents step or workflow cancellation
     */
    public static boolean isCancellation(Throwable error) {
        var cause = unwrapCompletionException(error);
        return cause instanceof StepCancellationException
                || cause instanceof WorkflowCancelledException
                || cause instanceof WorkflowFailedException;
    }

    protected static Throwable unwrapCancellation(Throwable e) {
        return unwrapCompletionException(e);
    }

    protected static Throwable unwrapCompletionException(Throwable e) {
        return FutureUtils.unwrap(e);
    }

    /**
     * Creates a durable result handle for a step while retaining its event-name customizer for later cancellation.
     *
     * @param stepName            logical name of the step
     * @param eventNameCustomizer customizer originally supplied for the step primitive
     * @param workflowExecution   execution providing state access and cancellation delegation
     * @return state-backed step result handle
     */
    public static WorkflowStepResult stateBased(String stepName,
                                                EventNameCustomizer eventNameCustomizer,
                                                WorkflowExecution workflowExecution) {
        return new StateBasedWorkflowStepResult(stepName, () -> {
            workflowExecution.awaitStateChange(s -> true);
            return null;
        }, cause -> workflowExecution.workflowContext().cancelStep(PrimitiveCommands.cancelStep(
                stepName, cause, eventNameCustomizer
        )), workflowExecution);
    }
}
