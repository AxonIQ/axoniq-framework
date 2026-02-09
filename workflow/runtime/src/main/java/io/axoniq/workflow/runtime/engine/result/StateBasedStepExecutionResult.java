package io.axoniq.workflow.runtime.engine.result;

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
  public String getStepName() {
    return stepName;
  }

  @Override
  public boolean isCompleted() {
    return workflowState.getStep(stepName).status().isTerminal();
  }

  @Override
  public <T> Optional<T> payload() {
    //noinspection unchecked
    return Optional.ofNullable(workflowState.getStep(stepName)).map(step -> (T) step.result());
  }

  @Override
  public Optional<StepFailedException> error() {
    return Optional.ofNullable(workflowState.getStep(stepName)).map(step -> {
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
