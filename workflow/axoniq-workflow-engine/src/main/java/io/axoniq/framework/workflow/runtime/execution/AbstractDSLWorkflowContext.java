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

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.dsl.api.WorkflowDSL;
import io.axoniq.framework.workflow.runtime.api.execution.context.CancelStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.CancelWorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.ExecuteStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.FailWorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.PayloadPrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.PayloadStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.PublishPrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.PublishStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.VersionPrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.VersionStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WaitForPrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WaitForStepDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowLifecycleControl;
import io.axoniq.framework.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.framework.workflow.runtime.api.execution.state.StepTimedOutException;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;

/**
 * Base class for DSL implementations.
 * <p>Implementors of DSLs have to provide their version of a {@link WorkflowContext} class and
 * are intended to subclass this class and delegate their calls to the methods available in the
 * {@link WorkflowContext}.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
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
            String workflowId,
            Map<String, @Nullable Object> payload,
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
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
    public WorkflowStepResult execute(ExecuteStepDefinition stepDefinition) {
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
    public Map<String, @Nullable Object> awaitExecute(ExecuteStepDefinition stepDefinition) {
        return resolveStepPayload(this.execute(stepDefinition));
    }

    @Override
    public WorkflowStepResult waitForEvent(WaitForStepDefinition stepDefinition) {
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
    public Map<String, @Nullable Object> awaitEvent(WaitForStepDefinition stepDefinition) {
        return resolveStepPayload(this.waitForEvent(stepDefinition));
    }

    @Override
    public WorkflowStepResult modifyPayload(PayloadStepDefinition stepDefinition) {
        return this.modifyPayload(
                PrimitiveCommands.modifyPayload(
                        stepDefinition.primitiveMetadata().stepName(),
                        stepDefinition.modification(),
                        stepDefinition.primitiveMetadata().eventNameCustomizer()
                )
        );
    }

    @Override
    public void awaitModifyPayload(PayloadStepDefinition stepDefinition) {
        awaitStepCompletion(this.modifyPayload(stepDefinition));
    }

    @Override
    public boolean migrateVersion(VersionStepDefinition stepDefinition) {
        var stepName = stepDefinition.primitiveMetadata().stepName();
        var newVersion = stepDefinition.newVersion();
        var result = this.version(
                PrimitiveCommands.version(
                        stepName,
                        newVersion,
                        stepDefinition.primitiveMetadata().eventNameCustomizer()
                )
        );
        awaitStepCompletion(result);

        var state = workflowExecution.state();
        var requested = Version.of(newVersion);
        if (state.hasVersionMigrationStep(stepName)) {
            return Version.of(state.versionFor(stepName)).isGreaterThanOrEqualTo(requested);
        }
        // No step recorded: either same-as-current path (true) or guard-blocked (false).
        // Inspect current workflow version to distinguish.
        return Version.of(state.workflowDefinitionId().version()).isGreaterThanOrEqualTo(requested);
    }


    @Override
    public WorkflowStepResult waitForEvent(WaitForPrimitive.WaitForCommand command) {
        return this.delegate.waitForEvent(command);
    }

    @Override
    public WorkflowStepResult execute(ExecutePrimitive.ExecuteCommand command) {
        return delegate.execute(command);
    }

    @Override
    public WorkflowStepResult modifyPayload(PayloadPrimitive.ModifyPayloadCommand command) {
        return delegate.modifyPayload(command);
    }

    @Override
    public WorkflowStepResult version(VersionPrimitive.VersionCommand command) {
        return delegate.version(command);
    }

    @Override
    public WorkflowStepResult publish(PublishPrimitive.PublishCommand command) {
        return delegate.publish(command);
    }

    @Override
    public WorkflowStepResult publish(PublishStepDefinition stepDefinition) {
        return this.publish(PrimitiveCommands.publish(
                stepDefinition.primitiveMetadata().stepName(),
                stepDefinition.event()
        ));
    }

    @Override
    public void awaitPublish(PublishStepDefinition stepDefinition) {
        awaitStepCompletion(this.publish(stepDefinition));
    }

    @Override
    public void fail(FailWorkflowDefinition definition) {
        delegate.failWorkflow(PrimitiveCommands.failWorkflow(
                definition.cause(),
                definition.primitiveMetadata().eventNameCustomizer()
        ));
    }

    @Override
    public void cancel(CancelWorkflowDefinition definition) {
        delegate.cancelWorkflow(PrimitiveCommands.cancelWorkflow(
                definition.cause(),
                definition.primitiveMetadata().eventNameCustomizer()
        ));
    }

    @Override
    public void cancelStep(CancelStepDefinition definition) {
        delegate.cancelStep(PrimitiveCommands.cancelStep(
                definition.primitiveMetadata().stepName(),
                definition.cause(),
                definition.primitiveMetadata().eventNameCustomizer()
        ));
    }


    @Override
    public CombinatorWorkflowStepResult allMatch(Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return delegate.allMatch(predicate, results);
    }

    @Override
    public CombinatorWorkflowStepResult anyMatch(Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return delegate.anyMatch(predicate, results);
    }

    @Override
    public CombinatorWorkflowStepResult noneMatch(Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return delegate.noneMatch(predicate, results);
    }

    @Override
    public void cancelWorkflow(WorkflowLifecycleControl.CancelWorkflowCommand command) {
        delegate.cancelWorkflow(command);
    }

    @Override
    public void failWorkflow(WorkflowLifecycleControl.FailWorkflowCommand command) {
        delegate.failWorkflow(command);
    }

    @Override
    public boolean cancelStep(WorkflowLifecycleControl.CancelStepCommand command) {
        return delegate.cancelStep(command);
    }


    @Override
    public String workflowId() {
        return delegate.workflowId();
    }

    @Override
    public String workflowVersion() {
        return delegate.workflowVersion();
    }

    @Override
    public Map<String, @Nullable Object> workflowPayload() {
        return delegate.workflowPayload();
    }

    @Override
    public WorkflowStatus workflowStatus() {
        return workflowExecution.state().workflowStatus();
    }

    @Override
    public List<String> workflowStepNames() {
        return delegate.workflowStepNames();
    }

    @Override
    public ProcessingContext processingContext() {
        return workflowExecution.processingContext();
    }

    public WorkflowExecution execution() {
        return workflowExecution;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        workflowExecution.describeTo(descriptor);
    }

    private Map<String, @Nullable Object> resolveStepPayload(WorkflowStepResult result) {
        if (result.success()) {
            return result.resultAs(PAYLOAD_TYPE, processingContext().component(EventConverter.class)).orElse(Map.of());
        }
        if (result.timeout()) {
            throw new StepTimedOutException(
                    "Step '" + result.getStepName() + "' timed out before completing");
        }
        if (result.canceled()) {
            throw new StepCancellationException(
                    "Step '" + result.getStepName() + "' was cancelled before completing");
        }
        throw result.error().orElseThrow();
    }

    private void awaitStepCompletion(WorkflowStepResult result) {
        result.await();
        if (result.timeout()) {
            throw new StepTimedOutException(
                    "Step '" + result.getStepName() + "' timed out before completing");
        }
        if (result.canceled()) {
            throw new StepCancellationException(
                    "Step '" + result.getStepName() + "' was cancelled before completing");
        }
        if (result.error().isPresent()) {
            throw result.error().orElseThrow();
        }
    }
}
