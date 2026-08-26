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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.workflow.runtime.api.execution.state.StepInterruptedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.util.WorkflowStateUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.api.execution.status.StepStatus.*;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.PAYLOAD_TYPE;

/**
 * Workflow Step result based on the Workflow State.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class StateBasedWorkflowStepResult implements WorkflowStepResult {

    private final String stepName;
    private final Callable<Void> stateChangeTrigger;
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
    public StateBasedWorkflowStepResult(@Nonnull String stepName,
                                        @Nonnull Callable<Void> stateChangeTrigger,
                                        @Nonnull Consumer<Throwable> cancellation,
                                        @Nonnull WorkflowExecution state) {
        this.stepName = Objects.requireNonNull(stepName, "Step name must not be null");
        this.stateChangeTrigger = Objects.requireNonNull(stateChangeTrigger, "State change trigger must not be null");
        this.cancellation = Objects.requireNonNull(cancellation, "Cancellation operation must not be null");
        this.workflowExecution = Objects.requireNonNull(state, "Workflow execution must not be null");
    }

    @Override
    @Nonnull
    public String getStepName() {
        return stepName;
    }

    @Override
    public boolean isCompleted() {
        return WorkflowStateUtils.isStepTerminal(workflowExecution.state(), stepName);
    }

    @Override
    @Nonnull
    public Optional<Map<String, Object>> result() {
        return Optional.of(workflowExecution.state().getStep(stepName))
                       .map(WorkflowStep::result)
                       .map(o -> workflowExecution.processingContext().component(EventConverter.class)
                                                  .convert(o, PAYLOAD_TYPE.getType()));
    }

    @Override
    @Nonnull
    public <T> Optional<T> resultAs(@Nonnull Type type) {
        return resultAs(type, workflowExecution.processingContext().component(EventConverter.class));
    }

    @Override
    @Nonnull
    public <T> Optional<T> resultAs(@Nonnull Type type, @Nonnull Converter converter) {
        return Optional.of(workflowExecution.state().getStep(stepName))
                       .map(WorkflowStep::result)
                       .map(o -> converter.convert(o, type));
    }

    @Override
    @Nonnull
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
        do {
            if (WorkflowStateUtils.isStepTerminal(workflowExecution.state(), stepName)) {
                return;
            }
            try {
                stateChangeTrigger.call();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StepInterruptedException("Step wait interrupted because the workflow reached a terminal state",
                                                   e);
            } catch (Exception e) {
                throw new RuntimeException(e); // FIXME -> replace callable with a better fit.
            }
        } while (true /* FIXME workflow is not suspended */);
    }

    @Override
    public void cancel() {
        cancellation.accept(new StepCancellationException("Step cancelled"));
    }

    @Override
    public void cancel(@Nonnull String reason) {
        cancellation.accept(new StepCancellationException(reason));
    }
}
