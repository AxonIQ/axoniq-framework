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

import io.axoniq.workflow.runtime.api.execution.context.CancelStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.CancelWorkflowDefinition;
import io.axoniq.workflow.dsl.api.WorkflowDSL;
import io.axoniq.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.ExecuteStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.FailWorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.PayloadPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.PayloadStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WaitForPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WaitForStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

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
public abstract class AbstractDSLWorkflowContext implements WorkflowContext, WorkflowDSL {

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

    @Override
    @Nonnull
    public WorkflowStepResult execute(@Nonnull ExecuteStepDefinition stepDefinition) {
        return this.execute(
                new PrimitiveCommands.WorkflowStepResultExecuteCommand(
                        stepDefinition.primitiveMetadata().stepName(),
                        stepDefinition.inputPayload(),
                        stepDefinition.action(),
                        stepDefinition.payloadMapping().parameterPayloadReducer(),
                        stepDefinition.payloadMapping().resultPayloadReducer(),
                        stepDefinition.timing().timeout(),
                        stepDefinition.primitiveMetadata().eventNameCustomizer(),
                        stepDefinition.retryPolicy()
                )
        );
    }

    @Override
    @Nonnull
    public Map<String, Object> awaitExecute(@Nonnull ExecuteStepDefinition stepDefinition) {
        return resolveStepPayload(this.execute(stepDefinition));
    }

    @Override
    @Nonnull
    public WorkflowStepResult waitForEvent(@Nonnull WaitForStepDefinition stepDefinition) {
        return this.waitForEvent(
                PrimitiveCommands.waitForEvent(
                        stepDefinition.primitiveMetadata().stepName(),
                        stepDefinition.eventCondition(),
                        stepDefinition.payloadMapping().resultPayloadReducer(),
                        stepDefinition.timing().timeout(),
                        stepDefinition.primitiveMetadata().eventNameCustomizer()
                )
        );
    }

    @Override
    @Nonnull
    public Map<String, Object> awaitEvent(@Nonnull WaitForStepDefinition stepDefinition) {
        return resolveStepPayload(this.waitForEvent(stepDefinition));
    }

    @Override
    @Nonnull
    public WorkflowStepResult modifyPayload(@Nonnull PayloadStepDefinition stepDefinition) {
        return this.modifyPayload(
                PrimitiveCommands.modifyPayload(
                        stepDefinition.primitiveMetadata().stepName(),
                        stepDefinition.modification(),
                        stepDefinition.primitiveMetadata().eventNameCustomizer()
                )
        );
    }

    @Override
    public void awaitModifyPayload(@Nonnull PayloadStepDefinition stepDefinition) {
        awaitStepCompletion(this.modifyPayload(stepDefinition));
    }


    @Override
    @Nonnull
    public WorkflowStepResult waitForEvent(@Nonnull WaitForPrimitive.WaitForCommand command) {
        return this.delegate.waitForEvent(command);
    }

    @Nonnull
    @Override
    public WorkflowStepResult execute(@Nonnull ExecutePrimitive.ExecuteCommand command) {
        return delegate.execute(command);
    }

    @Override
    @Nonnull
    public WorkflowStepResult modifyPayload(@Nonnull PayloadPrimitive.ModifyPayloadCommand command) {
        return delegate.modifyPayload(command);
    }

    @Override
    public void fail(@Nonnull FailWorkflowDefinition definition) {
        delegate.terminate(
                TerminateCommand.fail(
                        definition.cause(),
                        definition.primitiveMetadata().eventNameCustomizer()
                )
        );
    }

    @Override
    public void cancel(@Nonnull CancelWorkflowDefinition definition) {
        delegate.terminate(TerminateCommand.cancel(definition.cause(),
                                                   definition.primitiveMetadata().eventNameCustomizer()));
    }

    @Override
    public void cancelStep(@Nonnull CancelStepDefinition definition) {
        delegate.terminate(TerminateCommand.cancelledStep(definition.primitiveMetadata().stepName(),
                                                          definition.cause(),
                                                          definition.primitiveMetadata().eventNameCustomizer())
        );
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

    @Nonnull
    private Map<String, Object> resolveStepPayload(@Nonnull WorkflowStepResult result) {
        if (result.success()) {
            return result.resultAs(new TypeReference<Map<String, Object>>() {
            }, processingContext().component(EventConverter.class)).orElse(Map.of());
        }
        throw result.error().orElseThrow();
    }

    private void awaitStepCompletion(@Nonnull WorkflowStepResult result) {
        result.await();
        if (result.error().isPresent()) {
            throw result.error().orElseThrow();
        }
    }
}
