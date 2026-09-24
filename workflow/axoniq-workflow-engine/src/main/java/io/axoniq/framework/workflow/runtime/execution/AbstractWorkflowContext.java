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

import io.axoniq.framework.workflow.dsl.api.CancelStepDefinition;
import io.axoniq.framework.workflow.dsl.api.CancelWorkflowDefinition;
import io.axoniq.framework.workflow.dsl.api.CombinatorWorkflowStepResult;
import io.axoniq.framework.workflow.dsl.api.ExecuteStepDefinition;
import io.axoniq.framework.workflow.dsl.api.FailWorkflowDefinition;
import io.axoniq.framework.workflow.dsl.api.PayloadStepDefinition;
import io.axoniq.framework.workflow.dsl.api.PublishStepDefinition;
import io.axoniq.framework.workflow.dsl.api.StepCancellationException;
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.StepInterruptedException;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.StepTimedOutException;
import io.axoniq.framework.workflow.dsl.api.VersionStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WaitForStepDefinition;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStep;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState.PAYLOAD_TYPE;

/**
 * Base class for author-facing {@link WorkflowContext} implementations.
 *
 * <p>Implementors extend this class and call its constructor with the workflow execution configuration. This class
 * translates step definitions to runtime commands and delegates them to internal {@link WorkflowExecutionOperations},
 * which are not exposed through the author-facing context type. For reading operations, it exposes parts of the runtime
 * state with own methods delegating to the {@link WorkflowExecution#state()}.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public abstract class AbstractWorkflowContext implements WorkflowContext {

    private final WorkflowExecutionOperations workflowExecutionOperations;
    private final SimpleWorkflowExecution workflowExecution;

    /**
     * Constructs a new author-facing workflow context.
     *
     * @param workflowId            workflow id.
     * @param payload               initial payload.
     * @param processingContext     processing context of the incoming event.
     * @param workflowConfiguration workflow configuration.
     */
    public AbstractWorkflowContext(
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
        this.workflowExecutionOperations = workflowExecution.workflowExecutionOperations();
    }

    @Override
    public WorkflowStepResult execute(ExecuteStepDefinition stepDefinition) {
        return workflowExecutionOperations.execute(
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
        return workflowExecutionOperations.waitForEvent(
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
        return workflowExecutionOperations.modifyPayload(
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
    public WorkflowStepResult publish(PublishStepDefinition stepDefinition) {
        return workflowExecutionOperations.publish(PrimitiveCommands.publish(
                stepDefinition.stepName(),
                stepDefinition.event()
        ));
    }

    @Override
    public void awaitPublish(PublishStepDefinition stepDefinition) {
        awaitStepCompletion(this.publish(stepDefinition));
    }

    @Override
    public boolean migrateVersion(VersionStepDefinition stepDefinition) {
        var stepName = stepDefinition.primitiveMetadata().stepName();
        var newVersion = stepDefinition.newVersion();
        var result = workflowExecutionOperations.version(
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
        // Inspect the current workflow version to distinguish.
        return Version.of(state.workflowDefinitionId().version()).isGreaterThanOrEqualTo(requested);
    }

    @Override
    public void fail(FailWorkflowDefinition definition) {
        workflowExecutionOperations.failWorkflow(PrimitiveCommands.failWorkflow(
                definition.cause(),
                definition.primitiveMetadata().eventNameCustomizer()
        ));
    }

    @Override
    public void cancel(CancelWorkflowDefinition definition) {
        workflowExecutionOperations.cancelWorkflow(PrimitiveCommands.cancelWorkflow(
                definition.cause(),
                definition.primitiveMetadata().eventNameCustomizer()
        ));
    }

    @Override
    public void cancelStep(CancelStepDefinition definition) {
        workflowExecutionOperations.cancelStep(PrimitiveCommands.cancelStep(
                definition.primitiveMetadata().stepName(),
                definition.cause(),
                definition.primitiveMetadata().eventNameCustomizer()
        ));
    }

    @Override
    public CombinatorWorkflowStepResult allMatch(Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return workflowExecutionOperations.allMatch(predicate, results);
    }

    @Override
    public CombinatorWorkflowStepResult anyMatch(Predicate<WorkflowStepResult> predicate,
                                                 WorkflowStepResult... results) {
        return workflowExecutionOperations.anyMatch(predicate, results);
    }

    @Override
    public CombinatorWorkflowStepResult noneMatch(Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return workflowExecutionOperations.noneMatch(predicate, results);
    }

    @Override
    public <T> T resolveComponent(Class<T> componentType) {
        return processingContext().component(componentType);
    }

    @Override
    public String workflowId() {
        return workflowExecution.state().workflowId();
    }

    @Override
    public VersionedType workflowDefinitionId() {
        return workflowExecution.state().workflowDefinitionId();
    }

    @Override
    public Map<String, @Nullable Object> workflowPayload() {
        return workflowExecution.state().payload();
    }

    @Override
    public WorkflowStatus workflowStatus() {
        return workflowExecution.state().workflowStatus();
    }

    @Override
    public List<String> workflowStepNames() {
        return workflowExecution.state().workflowStepNames();
    }

    @Override
    @Nullable
    public WorkflowStep getStep(String stepName) {
        return workflowExecution.state().getStep(stepName);
    }

    @Override
    public boolean containsStep(String stepName) {
        return workflowExecution.state().containsStep(stepName);
    }


    /**
     * Internal access to processing context used by implemeters.
     *
     * @return processing context.
     */
    protected ProcessingContext processingContext() {
        return workflowExecution.processingContext();
    }

    /**
     * Internal access to execution used by WorkflowContextAdoptingExecutionFactory
     *
     * @return execution.
     */
    WorkflowExecution execution() {
        return workflowExecution;
    }


    /**
     * Helper to resolve step payload, see {@link #awaitStepCompletion(WorkflowStepResult)}.
     *
     * @param result a payload of the step is successful.
     * @return payload.
     * @throws StepTimedOutException if the step timed out.
     */
    private Map<String, @Nullable Object> resolveStepPayload(WorkflowStepResult result) {
        if (result.success()) {
            return result.resultAs(PAYLOAD_TYPE, processingContext().component(EventConverter.class)).orElse(Map.of());
        }
        if (result.timeout()) {
            throw new StepTimedOutException(
                    "Step '" + result.getStepName() + "' timed out before completing");
        }
        if (result.canceled()) {
            throw cancellationOf(result);
        }
        throw result.error().orElseThrow();
    }

    /**
     * Awaits step completion and throws a corresponding exception, depending on status, see
     * {@link #resolveStepPayload(WorkflowStepResult)}.
     *
     * @param result workflow step result.
     */
    private void awaitStepCompletion(WorkflowStepResult result) {
        result.await();
        if (result.timeout()) {
            throw new StepTimedOutException(
                    "Step '" + result.getStepName() + "' timed out before completing");
        }
        if (result.canceled()) {
            throw cancellationOf(result);
        }
        if (result.error().isPresent()) {
            throw result.error().orElseThrow();
        }
    }

    /**
     * A durably cancelled step is a step outcome the body sees as a {@link StepCancellationException}. A step with no
     * cancellation record never started for this execution, because the driver was interrupted or another execution
     * owns the attempt, so the body sees a {@link StepInterruptedException} and the workflow pauses instead of
     * failing.
     */
    private StepFailedException cancellationOf(WorkflowStepResult result) {
        var stepName = result.getStepName();
        if (WorkflowStateUtils.isStepStatus(workflowExecution.state(), stepName, StepStatus.CANCELLED)) {
            return new StepCancellationException("Step '" + stepName + "' was cancelled before completing");
        }
        return new StepInterruptedException("Step '" + stepName + "' did not start for this execution");
    }
}
