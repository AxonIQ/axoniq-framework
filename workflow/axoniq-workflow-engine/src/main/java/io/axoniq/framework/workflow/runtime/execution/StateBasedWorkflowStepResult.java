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

import io.axoniq.framework.workflow.dsl.api.StepCancellationException;
import io.axoniq.framework.workflow.dsl.api.StepFailedException;
import io.axoniq.framework.workflow.dsl.api.StepInterruptedException;
import io.axoniq.framework.workflow.dsl.api.WorkflowStep;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static io.axoniq.framework.workflow.dsl.api.StepStatus.*;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.PAYLOAD_TYPE;

/**
 * Workflow Step result based on the Workflow State.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class StateBasedWorkflowStepResult implements WorkflowStepResult {

    private final String stepName;
    private final StateChangeTrigger stateChangeTrigger;
    private final Consumer<Throwable> cancellation;
    private final WorkflowExecution workflowExecution;

    /**
     * Creates a result backed by the current workflow execution state.
     *
     * @param stepName           step represented by this result
     * @param stateChangeTrigger operation that waits for the next state change
     * @param cancellation       operation that requests cancellation of this step
     * @param state              workflow execution providing the state
     */
    public StateBasedWorkflowStepResult(String stepName,
                                        StateChangeTrigger stateChangeTrigger,
                                        Consumer<Throwable> cancellation,
                                        WorkflowExecution state) {
        this.stepName = Objects.requireNonNull(stepName, "Step name must not be null");
        this.stateChangeTrigger = Objects.requireNonNull(stateChangeTrigger, "State change trigger must not be null");
        this.cancellation = Objects.requireNonNull(cancellation, "Cancellation operation must not be null");
        this.workflowExecution = Objects.requireNonNull(state, "Workflow execution must not be null");
    }

    @Override
    public String getStepName() {
        return stepName;
    }

    @Override
    public boolean isCompleted() {
        return WorkflowStateUtils.isStepTerminal(workflowExecution.state(), stepName);
    }

    @Override
    public Optional<Map<String, @Nullable Object>> result() {
        return Optional.of(workflowExecution.state().getStep(stepName))
                       .map(WorkflowStep::result)
                       .map(o -> workflowExecution.processingContext().component(EventConverter.class)
                                                  .convert(o, PAYLOAD_TYPE.getType()));
    }

    @Override
    public <T> Optional<T> resultAs(Type type) {
        return resultAs(type, workflowExecution.processingContext().component(EventConverter.class));
    }

    @Override
    public <T> Optional<T> resultAs(Type type, Converter converter) {
        return Optional.of(workflowExecution.state().getStep(stepName))
                       .map(WorkflowStep::result)
                       .map(o -> converter.convert(o, type));
    }

    @Override
    public Optional<StepFailedException> error() {
        return Optional.of(workflowExecution.state().getStep(stepName)).map(step -> {
            var cause = step.error();
            if (cause instanceof StepFailedException) {
                return (StepFailedException) cause;
            } else {
                return new StepFailedException(cause);
            }
        });
    }

    @Override
    public boolean success() {
        await();
        return WorkflowStateUtils.isStepStatus(workflowExecution.state(), stepName, COMPLETED);
    }

    @Override
    public boolean failure() {
        await();
        return WorkflowStateUtils.isStepStatus(workflowExecution.state(), stepName, FAILED);
    }

    @Override
    public boolean canceled() {
        await();
        return WorkflowStateUtils.isStepStatus(workflowExecution.state(), stepName, CANCELLED);
    }

    @Override
    public boolean timeout() {
        await();
        return WorkflowStateUtils.isStepStatus(workflowExecution.state(), stepName, TIMED_OUT);
    }

    @Override
    public void await() {
        // Exits once the step is terminal, or when shutdown, cancellation or a rejected append interrupts the
        // driver thread.
        while (!WorkflowStateUtils.isStepTerminal(workflowExecution.state(), stepName)) {
            try {
                stateChangeTrigger.awaitStateChange();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StepInterruptedException("Step wait interrupted because the workflow reached a terminal state",
                                                   e);
            }
        }
    }

    @Override
    public void cancel() {
        cancellation.accept(new StepCancellationException("Step cancelled"));
    }

    @Override
    public void cancel(String reason) {
        cancellation.accept(new StepCancellationException(reason));
    }

    /**
     * Blocks until the workflow state changes.
     */
    @FunctionalInterface
    public interface StateChangeTrigger {

        /**
         * Blocks the calling thread until the next workflow state change.
         *
         * @throws InterruptedException if the waiting thread is interrupted
         */
        void awaitStateChange() throws InterruptedException;
    }
}
