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
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
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

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.*;

public abstract class AbstractStepExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractStepExecutor.class);
    protected final WorkflowContext workflowContext;
    protected final WorkflowState workflowState;
    protected final Clock clock;
    protected final EventNameCustomizer parentEventNameCustomizer;
    protected final UnitOfWorkFactory unitOfWorkFactory;
    protected final EventSink eventSink;
    protected final Executor executor;

    public AbstractStepExecutor(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowState workflowState,
            @Nonnull EventNameCustomizer parentEventNameCustomizer,
            @Nonnull Clock clock,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull EventSink eventSink,
            @Nonnull Executor executor
    ) {
        this.clock = Objects.requireNonNull(clock, "Clock is mandatory");
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.workflowState = Objects.requireNonNull(workflowState, "Workflow state is mandatory");
        this.parentEventNameCustomizer = Objects.requireNonNull(parentEventNameCustomizer,
                                                                "Event name customizer is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UoW Factory state is mandatory");
        this.eventSink = Objects.requireNonNull(eventSink, "Event sink is mandatory");
        this.executor = executor;
    }

    protected void acceptAllPendingTasksForStep(@Nonnull String stepName) {
        while ((!workflowState.containsStep(stepName) && !workflowState.hasTasks()) || !workflowState.isExecutable()) {
            var poll = workflowState.getNextTask();
            if (poll != null) { // FIXME forever?
                poll.accept(this.workflowState);
            }
        }
    }

    @Nonnull
    protected Context getContext(@Nonnull String stepName) {
        return workflowState.containsStep(stepName)
                ? workflowState.getStep(stepName).context()
                : workflowState.processingContext();
    }

    @Nonnull
    protected CompletableFuture<Void> started(@Nonnull String stepName, @Nonnull Map<String, Object> payload,
                                              @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(startedStep(workflowContext, stepName, sanitize(payload),
                                         merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> completed(@Nonnull String stepName, @Nonnull Map<String, Object> payload,
                                                @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(completedStep(workflowContext, stepName, sanitize(payload),
                                           merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> cancelled(@Nonnull String stepName,
                                                @Nonnull EventNameCustomizer eventNameCustomizer) {
        return sendStepEvent(cancelledStep(workflowContext, stepName,
                                           merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    protected CompletableFuture<Void> failed(@Nonnull String stepName, @Nonnull Throwable ex,
                                             @Nonnull EventNameCustomizer eventNameCustomizer) {
        LoggerFactory.getLogger(AbstractStepExecutor.class).error("Error", ex);
        return sendStepEvent(failStep(workflowContext, stepName, ex,
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
        return sendStepEvent(timeoutStep(workflowContext, stepName, timeoutTimestamp,
                                         merge(parentEventNameCustomizer, eventNameCustomizer)
        ), getContext(stepName));
    }

    @Nonnull
    private CompletableFuture<Void> sendStepEvent(@Nonnull EventMessage eventMessage, @Nonnull Context context) {
        logger.trace("Appending event {}", eventMessage.type());
        return ProcessingContextUtils.executeWithResult(
                null,
                unitOfWorkFactory,
                executor,
                context,
                ctx -> eventSink.publish(ctx, eventMessage)
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
                        }, workflowContext.processingContext().component(Converter.class))
        );
    }
}
