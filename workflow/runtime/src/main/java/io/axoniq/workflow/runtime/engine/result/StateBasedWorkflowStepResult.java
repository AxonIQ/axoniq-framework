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
package io.axoniq.workflow.runtime.engine.result;

import io.axoniq.workflow.runtime.api.StepCancellationException;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Optional;
import java.util.concurrent.Callable;

import static io.axoniq.workflow.runtime.engine.step.StepStatus.*;

/**
 * Workflow Step result based on the Workflow State.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class StateBasedWorkflowStepResult implements WorkflowStepResult {

    private final String stepName;
    private final Callable<Void> stateChangeTrigger;
    private final WorkflowState workflowState;

    public StateBasedWorkflowStepResult(String stepName, Callable<Void> stateChangeTrigger, WorkflowState state) {
        this.stepName = stepName;
        this.stateChangeTrigger = stateChangeTrigger;
        this.workflowState = state;
    }

    @Override
    @Nonnull
    public String getStepName() {
        return stepName;
    }

    @Override
    public boolean isCompleted() {
        return workflowState.getStep(stepName).status().isTerminal();
    }

    @Override
    @Nonnull
    public <T> Optional<T> result() {
        //noinspection unchecked
        return Optional.of(workflowState.getStep(stepName)).map(step -> (T) step.result());
    }

    @Override
    @Nonnull
    public Optional<StepFailedException> error() {
        return Optional.of(workflowState.getStep(stepName)).map(step -> {
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
        return COMPLETED == workflowState.getStep(stepName).status();
    }

    @Override
    public boolean failure() {
        await();
        return FAILED == workflowState.getStep(stepName).status();
    }

    @Override
    public boolean canceled() {
        await();
        return CANCELLED == workflowState.getStep(stepName).status();
    }

    @Override
    public boolean timeout() {
        await();
        return TIMED_OUT == workflowState.getStep(stepName).status();
    }

    @Override
    public void await() {
        do {
            if (workflowState.getStep(stepName).status().isTerminal()) {
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
        workflowState.cancelRunningStep(stepName, new StepCancellationException("Step cancelled"));
    }

    @Override
    public void cancel(@Nonnull String reason) {
        workflowState.cancelRunningStep(stepName, new StepCancellationException(reason));
    }
}
