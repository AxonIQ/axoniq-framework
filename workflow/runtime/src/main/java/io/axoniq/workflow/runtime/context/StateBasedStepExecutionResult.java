package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;

import java.util.Optional;
import java.util.concurrent.Callable;

public class StateBasedStepExecutionResult implements StepExecutionResult {

    private final String stepName;
    private final Callable<Void> stateChangeTrigger;
    private final WorkflowState workflowState;

    public StateBasedStepExecutionResult(String stepName, Callable<Void> stateChangeTrigger, WorkflowState state) {
        this.stepName = stepName;
        this.stateChangeTrigger = stateChangeTrigger;
        this.workflowState = state;
    }

    @Override
    public boolean isCompleted() {
        return switch (workflowState.getStep(stepName).status()) {
            case COMPLETED -> true;
            case FAILED -> true;
            case TIMED_OUT -> true;
            default -> false;
        };
    }

    @Override
    public <T> Optional<T> payload() {
        return Optional.empty();
    }

    @Override
    public Optional<StepFailedException> error() {
        return Optional.empty();
    }

    @Override
    public boolean isSuccess() {
        do {
            switch (workflowState.getStep(stepName).status()) {
                case COMPLETED:
                    return true;
                case FAILED:
                    return false;
                case TIMED_OUT:
                    return false;
            }
            try {
                stateChangeTrigger.call();
            }  catch (Exception e) {
                return false;
            }
        } while (true /* workflow is not suspended */);
    }

    @Override
    public boolean isFailure() {
        return false;
    }

    @Override
    public boolean isCanceled() {
        return false;
    }

    @Override
    public boolean isTimeout() {
        return false;
    }
}
