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

import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Optional;
import java.util.concurrent.Callable;

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
    public <T> Optional<T> payload() {
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
    public boolean isSuccess() {
        do {
            switch (workflowState.getStep(stepName).status()) {
                case COMPLETED:
                    return true;
                case FAILED, TIMED_OUT, CANCELLED:
                    return false;
            }
            try {
                stateChangeTrigger.call();
            } catch (Exception e) {
                return false;
            }
        } while (true /* FIXME workflow is not suspended */);
    }

    @Override
    public boolean isFailure() {
        do {
            switch (workflowState.getStep(stepName).status()) {
                case FAILED:
                    return true;
                case COMPLETED, TIMED_OUT, CANCELLED:
                    return false;
            }
            try {
                stateChangeTrigger.call();
            } catch (Exception e) {
                return false;
            }
        } while (true /* FIXME workflow is not suspended */);
    }

    @Override
    public boolean isCanceled() {
        do {
            switch (workflowState.getStep(stepName).status()) {
                case CANCELLED:
                    return true;
                case COMPLETED, TIMED_OUT, FAILED:
                    return false;
            }
            try {
                stateChangeTrigger.call();
            } catch (Exception e) {
                return false;
            }
        } while (true /* FIXME workflow is not suspended */);
    }

    @Override
    public boolean isTimeout() {
        do {
            switch (workflowState.getStep(stepName).status()) {
                case TIMED_OUT:
                    return true;
                case COMPLETED, FAILED, CANCELLED:
                    return false;
            }
            try {
                stateChangeTrigger.call();
            } catch (Exception e) {
                return false;
            }
        } while (true /* FIXME workflow is not suspended */);
    }
}
