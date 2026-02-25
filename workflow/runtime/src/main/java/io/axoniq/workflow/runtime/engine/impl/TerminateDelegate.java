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
import io.axoniq.workflow.runtime.api.TerminatePrimitive;
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.util.ProcessingContextUtils;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;

import java.util.Objects;
import java.util.concurrent.Executor;

import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.cancelledWorkflow;
import static io.axoniq.workflow.runtime.engine.util.EventMessageUtils.failedWorkflow;

/**
 * Delegate that owns the full workflow termination flow: cancel futures, send event, apply state, throw.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class TerminateDelegate implements TerminatePrimitive {

    private final WorkflowContext workflowContext;
    private final WorkflowState workflowState;
    private final EventSink eventSink;
    private final String workflowName;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final Executor executor;

    public TerminateDelegate(
            @Nonnull WorkflowContext workflowContext,
            @Nonnull WorkflowState workflowState,
            @Nonnull EventSink eventSink,
            @Nonnull String workflowName,
            @Nonnull UnitOfWorkFactory unitOfWorkFactory,
            @Nonnull Executor executor
    ) {
        this.workflowContext = Objects.requireNonNull(workflowContext, "Workflow context is mandatory");
        this.workflowState = Objects.requireNonNull(workflowState, "Workflow state is mandatory");
        this.eventSink = Objects.requireNonNull(eventSink, "Event sink is mandatory");
        this.workflowName = Objects.requireNonNull(workflowName, "Workflow name is mandatory");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UnitOfWork factory is mandatory");
        this.executor = Objects.requireNonNull(executor, "Executor is mandatory");
    }

    @Override
    public void terminate(boolean error, @Nullable Throwable cause, @Nonnull EventNameCustomizer eventNameCustomizer) {
        terminate(error, cause, eventNameCustomizer, workflowName);
    }

    public void terminate(boolean error, @Nullable Throwable cause,
                          @Nonnull EventNameCustomizer eventNameCustomizer,
                          @Nonnull String workflowNameOverride) {
        terminate(error, cause, eventNameCustomizer, workflowNameOverride, null);
    }

    public void terminate(boolean error, @Nullable Throwable cause,
                          @Nonnull EventNameCustomizer eventNameCustomizer,
                          @Nonnull String workflowNameOverride,
                          @Nullable WorkflowConfiguration<?> configuration) {

        workflowState.cancelAllRunningSteps(cause);

        if (error) {
            failed(cause, eventNameCustomizer, workflowNameOverride, configuration);
        } else {
            cancelled(cause, eventNameCustomizer, workflowNameOverride, configuration);
        }
    }

    protected void failed(@Nullable Throwable cause, @Nonnull EventNameCustomizer eventNameCustomizer) {
        failed(cause, eventNameCustomizer, workflowName);
    }

    protected void failed(@Nullable Throwable cause, @Nonnull EventNameCustomizer eventNameCustomizer,
                          @Nonnull String workflowNameOverride) {
        failed(cause, eventNameCustomizer, workflowNameOverride, null);
    }

    protected void failed(@Nullable Throwable cause, @Nonnull EventNameCustomizer eventNameCustomizer,
                          @Nonnull String workflowNameOverride,
                          @Nullable WorkflowConfiguration<?> configuration) {
        var exception = cause instanceof Exception ? (Exception) cause : new RuntimeException(cause);

        ProcessingContextUtils.executeWithResult(
                null,
                unitOfWorkFactory,
                executor,
                workflowContext.processingContext(),
                ctx -> eventSink.publish(ctx, failedWorkflow(workflowContext, workflowNameOverride, exception, eventNameCustomizer))
        ).join(); // FIXME join

        workflowState.applyStateChange(
                failedWorkflow(workflowContext, workflowNameOverride, exception, eventNameCustomizer),
                workflowContext.processingContext()
        );

        if (configuration != null) {
            var listener = configuration.workflowStatusChangeListeners().get(WorkflowStatus.FAILED);
            if (listener != null) {
                listener.onWorkflowStatus(WorkflowStatus.FAILED, workflowContext);
            }
        }

        if (cause != null) {
            throw new WorkflowFailedException(cause);
        }
        throw new WorkflowFailedException("Workflow terminated with error");
    }

    protected void cancelled(@Nullable Throwable cause, @Nonnull EventNameCustomizer eventNameCustomizer) {
        cancelled(cause, eventNameCustomizer, workflowName);
    }

    protected void cancelled(@Nullable Throwable cause, @Nonnull EventNameCustomizer eventNameCustomizer,
                             @Nonnull String workflowNameOverride) {
        cancelled(cause, eventNameCustomizer, workflowNameOverride, null);
    }

    protected void cancelled(@Nullable Throwable cause, @Nonnull EventNameCustomizer eventNameCustomizer,
                             @Nonnull String workflowNameOverride,
                             @Nullable WorkflowConfiguration<?> configuration) {
        ProcessingContextUtils.executeWithResult(
                null,
                unitOfWorkFactory,
                executor,
                workflowContext.processingContext(),
                ctx -> eventSink.publish(ctx, cancelledWorkflow(workflowContext, workflowNameOverride, cause, eventNameCustomizer))
        ).join(); // FIXME join

        workflowState.applyStateChange(
                cancelledWorkflow(workflowContext, workflowNameOverride, cause, eventNameCustomizer),
                workflowContext.processingContext()
        );

        if (configuration != null) {
            var listener = configuration.workflowStatusChangeListeners().get(WorkflowStatus.CANCELLED);
            if (listener != null) {
                listener.onWorkflowStatus(WorkflowStatus.CANCELLED, workflowContext);
            }
        }

        if (cause != null) {
            throw new WorkflowCancelledException(cause);
        }
        throw new WorkflowCancelledException("Workflow cancelled");
    }
}
