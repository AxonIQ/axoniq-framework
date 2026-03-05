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
package io.axoniq.workflow.dsl;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.PayloadModification;
import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.PayloadReducer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.SimpleWorkflowExecution;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for DSL implementations.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public abstract class AbstractDSLWorkflowContext implements WorkflowContext {

    private final WorkflowContext delegate;
    private final SimpleWorkflowExecution workflowExecution;

    /**
     * Constructs new DSL context.
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
    public WorkflowStepResult waitFor(@Nonnull String stepName, @Nonnull EventCondition eventCondition,
                                      @Nonnull Duration timeout, @Nonnull EventNameCustomizer eventNameCustomizer) {
        return delegate.waitFor(stepName, eventCondition, timeout, eventNameCustomizer);
    }

    @Nonnull
    @Override
    public WorkflowStepResult execute(@Nonnull String stepName, @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterMapping,
                                      @Nonnull PayloadReducer resultMapping, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        return delegate.execute(stepName,
                                local,
                                action,
                                parameterMapping,
                                resultMapping,
                                timeout,
                                eventNameCustomizer);
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
    public void applyPayloadModification(@Nonnull PayloadModification payloadModification) {
        delegate.applyPayloadModification(payloadModification);
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
