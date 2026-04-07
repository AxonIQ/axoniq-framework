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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Base class for DSL implementations.
 * <p>Implementors of DSLs have to provide their version of a {@link WorkflowContext} class and
 * are intended to subclass this class and delegate their calls to the methods available in the
 * {@link WorkflowContext}.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public abstract class AbstractDSLWorkflowContext implements WorkflowContext {

    private final WorkflowContext delegate;
    private final SimpleWorkflowExecution workflowExecution;

    /**
     * Constructs a new DSL context.
     *
     * @param workflowId            workflow id.
     * @param payload               initial payload.
     * @param processingContext     processing context of the incoming event.
     * @param workflowConfiguration workflow configuration.
     */
    public AbstractDSLWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        this.workflowExecution = new SimpleWorkflowExecution(
                Objects.requireNonNull(workflowId, "Workflow id must not be null"),
                Objects.requireNonNull(payload, "Initial workflow payload must not be null"),
                Objects.requireNonNull(processingContext, "Processing context must not be null"),
                Objects.requireNonNull(workflowConfiguration, "Workflow configuration must not be null"),
                this
        );
        this.delegate = workflowExecution.workflowContext();
    }

    @Nonnull
    @Override
    public WorkflowStepResult waitFor(@Nonnull String stepName,
                                      @Nonnull EventCondition eventCondition,
                                      @Nonnull PayloadReducer resultPayloadReducer,
                                      @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        return delegate.waitFor(stepName, eventCondition, resultPayloadReducer, timeout, eventNameCustomizer);
    }

    @Nonnull
    @Override
    public WorkflowStepResult execute(@Nonnull String stepName, @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterPayloadReducer,
                                      @Nonnull PayloadReducer resultPayloadReducer, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        return delegate.execute(stepName,
                                local,
                                action,
                                parameterPayloadReducer,
                                resultPayloadReducer,
                                timeout,
                                eventNameCustomizer);
    }

    @Nonnull
    @Override
    public WorkflowStepResult execute(@Nonnull String stepName, @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterMapping,
                                      @Nonnull PayloadReducer resultMapping, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer,
                                      @Nonnull RetryPolicy retryPolicy) {
        return delegate.execute(stepName,
                                local,
                                action,
                                parameterMapping,
                                resultMapping,
                                timeout,
                                eventNameCustomizer,
                                retryPolicy);
    }

    @Nonnull
    @Override
    public CombinatorWorkflowStepResult allMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return delegate.allMatch(predicate, results);
    }

    @Nonnull
    @Override
    public CombinatorWorkflowStepResult anyMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return delegate.anyMatch(predicate, results);
    }

    @Nonnull
    @Override
    public CombinatorWorkflowStepResult noneMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return delegate.noneMatch(predicate, results);
    }

    @Override
    public void terminate(@Nonnull TerminateCommand command) {
        delegate.terminate(command);
    }


    @Nonnull
    @Override
    public String workflowId() {
        return delegate.workflowId();
    }

    @Nonnull
    @Override
    public Map<String, Object> workflowPayload() {
        return delegate.workflowPayload();
    }

    @Nonnull
    @Override
    public WorkflowStatus workflowStatus() {
        return workflowExecution.state().workflowStatus();
    }

    @Override
    public void modifyPayload(@Nonnull String stepName,
                              @Nonnull PayloadModification payloadModification,
                              @Nonnull EventNameCustomizer eventNameCustomizer) {
        delegate.modifyPayload(stepName, payloadModification, eventNameCustomizer);
    }

    @Nonnull
    @Override
    public List<String> workflowStepNames() {
        return delegate.workflowStepNames();
    }

    @Nonnull
    @Override
    public ProcessingContext processingContext() {
        return workflowExecution.processingContext();
    }

    @Nonnull
    public WorkflowExecution execution() {
        return workflowExecution;
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        workflowExecution.describeTo(descriptor);
    }
}
