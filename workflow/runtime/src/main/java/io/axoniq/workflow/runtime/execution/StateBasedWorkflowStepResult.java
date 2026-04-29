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
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.Optional;
import java.util.concurrent.Callable;

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
    private final WorkflowExecution workflowExecution;

    public StateBasedWorkflowStepResult(String stepName, Callable<Void> stateChangeTrigger, WorkflowExecution state) {
        this.stepName = stepName;
        this.stateChangeTrigger = stateChangeTrigger;
        this.workflowExecution = state;
    }

    @Override
    @Nonnull
    public String getStepName() {
        return stepName;
    }

    @Override
    public boolean isCompleted() {
        return workflowExecution.state().getStep(stepName).status().isTerminal();
    }

    @Override
    @Nonnull
    public <T> Optional<T> result() {
        return Optional.of(workflowExecution.state().getStep(stepName))
                       .map(WorkflowStep::result)
                       .map(o -> workflowExecution.processingContext().component(EventConverter.class)
                                                      .convert(o, PAYLOAD_TYPE.getType()));
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
        return COMPLETED == workflowExecution.state().getStep(stepName).status();
    }

    @Override
    public boolean failure() {
        await();
        return FAILED == workflowExecution.state().getStep(stepName).status();
    }

    @Override
    public boolean canceled() {
        await();
        return CANCELLED == workflowExecution.state().getStep(stepName).status();
    }

    @Override
    public boolean timeout() {
        await();
        return TIMED_OUT == workflowExecution.state().getStep(stepName).status();
    }

    @Override
    public void await() {
        do {
            if (workflowExecution.state().getStep(stepName).status().isTerminal()) {
                return;
            }
            try {
                stateChangeTrigger.call();
            } catch (Exception e) {
                throw new RuntimeException(e); // FIXME -> replace callable with a better fit.
            }
        } while (true /* FIXME workflow is not suspended */);
    }

    @Override
    public void cancel() {
        workflowExecution.cancelRunningStep(stepName, new StepCancellationException("Step cancelled"));
    }

    @Override
    public void cancel(@Nonnull String reason) {
        workflowExecution.cancelRunningStep(stepName, new StepCancellationException(reason));
    }
}
