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
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.WorkflowInstance;
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
 * are intended to subclass this class and delegate their calls to the methods available in the {@link WorkflowContext}.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public abstract class AbstractDSLWorkflowContext implements WorkflowContext {

    private final WorkflowInstance workflowInstance;

    /**
     * Constructs new DSL context.
     *
     * @param workflowId        workflow id.
     * @param payload           initial payload.
     * @param processingContext processing context of the incoming event.
     * @param parentCustomizer  parent event name customizer.
     */
    public AbstractDSLWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull EventNameCustomizer parentCustomizer
    ) {
        this.workflowInstance = new WorkflowInstance(
                Objects.requireNonNull(workflowId, "Workflow id must not be null"),
                Objects.requireNonNull(payload, "Initial workflow payload must not be null"),
                Objects.requireNonNull(processingContext, "Processing context must not be null"),
                Objects.requireNonNull(parentCustomizer, "Parent EventNameCustomizer must not be null")
        );
    }

    @Nonnull
    @Override
    public WorkflowStepResult waitFor(@Nonnull String stepName, @Nonnull EventCondition eventCondition,
                                      @Nonnull Duration timeout, @Nonnull EventNameCustomizer eventNameCustomizer) {
        return workflowInstance.waitFor(stepName, eventCondition, timeout, eventNameCustomizer);
    }

    @Nonnull
    @Override
    public WorkflowStepResult execute(@Nonnull String stepName, @Nullable Map<String, Object> local,
                                      @Nonnull PayloadProcessor action, @Nonnull PayloadReducer parameterMapping,
                                      @Nonnull PayloadReducer resultMapping, @Nonnull Duration timeout,
                                      @Nonnull EventNameCustomizer eventNameCustomizer) {
        return workflowInstance.execute(stepName,
                                        local,
                                        action,
                                        parameterMapping,
                                        resultMapping,
                                        timeout,
                                        eventNameCustomizer);
    }

    @Nonnull
    @Override
    public WorkflowStepResult all(WorkflowStepResult... results) {
        return workflowInstance.all(results);
    }

    @Nonnull
    @Override
    public WorkflowStepResult anyMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                       WorkflowStepResult... results) {
        return workflowInstance.anyMatch(predicate, results);
    }

    @Nonnull
    @Override
    public WorkflowStepResult noneMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                        WorkflowStepResult... results) {
        return workflowInstance.noneMatch(predicate, results);
    }

    @Override
    public void terminate(@Nonnull TerminateCommand command) {
        workflowInstance.terminate(command);
    }


    @Nonnull
    @Override
    public String workflowId() {
        return workflowInstance.workflowId();
    }

    @Nonnull
    @Override
    public Map<String, Object> workflowPayload() {
        return workflowInstance.workflowPayload();
    }

    @Nonnull
    @Override
    public WorkflowStatus workflowStatus() {
        return workflowInstance.workflowStatus();
    }

    @Override
    public void applyPayloadModification(@Nonnull PayloadModification payloadModification) {
        workflowInstance.applyPayloadModification(payloadModification);
    }

    @Nonnull
    @Override
    public List<String> workflowStepNames() {
        return workflowInstance.workflowStepNames();
    }

    @Nonnull
    @Override
    public ProcessingContext processingContext() {
        return workflowInstance.processingContext();
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        workflowInstance.describeTo(descriptor);
    }

    @Nonnull
    public WorkflowInstance getWorkflowInstance() {
        return workflowInstance;
    }
}
