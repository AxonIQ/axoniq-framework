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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.StepRetryInfo;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
import java.util.concurrent.CompletionException;
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
    protected final Clock clock;
    protected final EventNameCustomizer parentEventNameCustomizer;
    protected final UnitOfWorkFactory unitOfWorkFactory;
    protected final EventSink eventSink;
    protected final Executor executor;

    /**
     * Constructs the abstract step executor.
     *
     * @param workflowContext           workflow context.
     * @param workflowExecution         workflow execution.
     * @param parentEventNameCustomizer parent event name customizer.
     * @param clock                     clock for time calculations.
     * @param unitOfWorkFactory         unit of work factory for creation of new processing contexts.
     * @param eventSink                 event sink for event publications.
     * @param executor                  executor to offload execution tasks from workflow thread.
     */
    @Internal
    public AbstractStepExecutor(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowExecution workflowExecution,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor
    ) {
        this.clock = Objects.requireNonNull(clock, "Clock is mandatory");
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow state is mandatory");
        this.parentEventNameCustomizer = Objects.requireNonNull(parentEventNameCustomizer,
                                                                "Event name customizer is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UoW Factory state is mandatory");
        this.eventSink = Objects.requireNonNull(eventSink, "Event sink is mandatory");
        this.executor = executor;
    }

    protected void acceptAllPendingTasksForStep(@Nonnull String stepName) {
        while ((!workflowExecution.state().containsStep(stepName) && !workflowExecution.hasTasks())
                || !workflowExecution.isExecutable()) {
            var poll = workflowExecution.getNextTask();
            if (poll != null) {
                poll.accept(this.workflowExecution);
            }
        }
    }

    @Nonnull
    protected Context getContext(@Nonnull String stepName) {
        return workflowExecution.state().containsStep(stepName)
                ? workflowExecution.state().getStep(stepName).context()
                : workflowExecution.processingContext();
    }

    @Nonnull
    protected CompletableFuture<Void> started(@Nonnull String stepName, @Nonnull Map<String, Object> payload,
                                              @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, startedStep(workflowContext, stepName, sanitize(payload),
                                                   merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> startedWaitForEvent(@Nonnull String stepName,
                                                          @Nonnull Map<String, Object> payload,
                                                          @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, startedWaitForEventStep(workflowContext, stepName, sanitize(payload),
                                                               merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> completed(@Nonnull String stepName, @Nonnull Map<String, Object> payload,
                                                @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, completedStep(workflowContext, stepName, sanitize(payload),
                                                     null,
                                                     merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> completed(@Nonnull String stepName, @Nonnull Map<String, Object> payload,
                                                @Nullable String payloadReducerName,
                                                @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, completedStep(workflowContext, stepName, sanitize(payload),
                                                     payloadReducerName,
                                                     merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> completedWaitForEvent(@Nonnull String stepName,
                                                            @Nonnull Map<String, Object> payload,
                                                            @Nullable String payloadReducerName,
                                                            @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, completedWaitForEventStep(workflowContext,
                                                                 stepName,
                                                                 sanitize(payload),
                                                                 payloadReducerName,
                                                                 merge(parentEventNameCustomizer,
                                                                       eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> cancelled(@Nonnull String stepName,
                                                @Nonnull EventNameCustomizer eventNameCustomizer) {
        return cancelled(stepName, null, eventNameCustomizer);
    }

    @Nonnull
    protected CompletableFuture<Void> cancelled(@Nonnull String stepName,
                                                @Nullable Throwable cause,
                                                @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, cancelledStep(workflowContext, stepName, cause,
                                                     merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> cancelledWaitForEvent(@Nonnull String stepName,
                                                            @Nullable Throwable cause,
                                                            @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, cancelledWaitForEventStep(workflowContext, stepName, cause,
                                                                 merge(parentEventNameCustomizer,
                                                                       eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> failed(@Nonnull String stepName, @Nonnull Throwable ex,
                                             @Nonnull EventNameCustomizer eventNameCustomizer) {
        logger.error("Step '{}' failed in workflow '{}'", stepName, workflowExecution.workflowId(), ex);
        return sendStepEvent(stepName, failStep(workflowContext, stepName, ex,
                                                merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> failedWaitForEvent(@Nonnull String stepName,
                                                         @Nonnull Throwable ex,
                                                         @Nonnull EventNameCustomizer eventNameCustomizer) {
        logger.error("Wait-for-event step '{}' failed in workflow '{}'", stepName, workflowExecution.workflowId(), ex);
        return sendStepEvent(stepName, failWaitForEventStep(workflowContext, stepName, ex,
                                                            merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> retrying(@Nonnull String stepName,
                                               @Nonnull StepRetryInfo retryInfo,
                                               @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, retryingStep(workflowContext, stepName, retryInfo,
                                                    merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> timedOut(@Nonnull String stepName,
                                               @Nonnull EventNameCustomizer eventNameCustomizer) {
        return timedOut(stepName, Instant.now(clock), eventNameCustomizer);
    }

    @Nonnull
    protected CompletableFuture<Void> timedOut(@Nonnull String stepName, @Nonnull Instant timeoutTimestamp,
                                               @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, timeoutStep(workflowContext, stepName, timeoutTimestamp,
                                                   merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> timedOutWaitForEvent(@Nonnull String stepName,
                                                           @Nonnull EventNameCustomizer eventNameCustomizer) {
        return timedOutWaitForEvent(stepName, Instant.now(clock), eventNameCustomizer);
    }

    @Nonnull
    protected CompletableFuture<Void> timedOutWaitForEvent(@Nonnull String stepName,
                                                           @Nonnull Instant timeoutTimestamp,
                                                           @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(stepName, timeoutWaitForEventStep(workflowContext, stepName, timeoutTimestamp,
                                                               merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    private CompletableFuture<Void> sendStepEvent(@Nonnull String stepName,
                                                  @Nonnull EventMessage eventMessage,
                                                  @Nonnull Context context) {
        if (workflowContext.workflowStatus().isTerminal()) {
            logger.debug("Skipping step event {} — workflow is in terminal state {}", eventMessage.type(),
                         workflowContext.workflowStatus());
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Workflow is in terminal state " + workflowContext.workflowStatus()
                            + ", cannot publish step event " + eventMessage.type()));
        }
        if (workflowExecution.state().containsStep(stepName) && workflowExecution.state().getStep(stepName).status()
                                                                                 .isTerminal()) {
            logger.debug("Skipping step event {} — step '{}' is already in terminal state {}", eventMessage.type(),
                         stepName, workflowExecution.state().getStep(stepName).status());
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Step '" + stepName + "' is in terminal state " + workflowExecution.state().getStep(stepName)
                                                                                       .status()
                            + ", cannot publish step event " + eventMessage.type()));
        }
        logger.trace("Appending event {}", eventMessage.type());
        return ProcessingContextUtils
                .executeWithResult(
                        workflowExecution.workflowId(),
                        unitOfWorkFactory,
                        executor,
                        context,
                        ctx -> {
                            logger.trace("Thread: {}, ProcessingContext {}", Thread.currentThread(), ctx);
                            return eventSink.publish(ctx, eventMessage);
                        }
                );
    }

    @Nonnull
    protected Map<String, Object> sanitize(@Nullable Map<String, Object> payload) {
        if (payload == null) {
            return new LinkedHashMap<>();
        }
        return payload;
    }

    @Nonnull
    protected Map<String, Object> eventMessagePayload(@Nonnull EventMessage eventMessage) {
        return sanitize(eventMessage.payloadAs(new TypeReference<>() {
                        })
        );
    }

    public static boolean isCancellation(@Nonnull Throwable e) {
        var cause = e instanceof CompletionException ? e.getCause() : e;
        return cause instanceof StepCancellationException
                || cause instanceof WorkflowCancelledException
                || cause instanceof WorkflowFailedException;
    }

    @Nonnull
    protected static Throwable unwrapCancellation(@Nonnull Throwable e) {
        return e instanceof CompletionException ? e.getCause() : e;
    }

    public static WorkflowStepResult stateBased(@Nonnull String stepName, WorkflowExecution workflowExecution) {
        return new StateBasedWorkflowStepResult(stepName, () -> {
            workflowExecution.awaitStateChange(s -> true);
            return null;
        }, workflowExecution);
    }

}
