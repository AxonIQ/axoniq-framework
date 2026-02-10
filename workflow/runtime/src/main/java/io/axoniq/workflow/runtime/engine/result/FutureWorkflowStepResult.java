package io.axoniq.workflow.runtime.engine.result;

import io.axoniq.workflow.runtime.api.primitives.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class FutureWorkflowStepResult implements WorkflowStepResult {

  private final CompletableFuture<WorkflowStepResult> futureResult;

  FutureWorkflowStepResult(CompletableFuture<WorkflowStepResult> futureResult) {
    this.futureResult = futureResult;
  }

  @Override
  public String getStepName() {
    if (isCompleted()) {
      return futureResult.join().getStepName();
    }
    return null;
  }

  @Override
  public boolean isCompleted() {
    return futureResult.isDone();
  }

  @Override
  public <T> Optional<T> payload() {
    if (isCompleted()) {
      return futureResult.join().payload();
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Optional<StepFailedException> error() {
    if (isCompleted()) {
      return futureResult.join().error();
    } else {
      return Optional.empty();
    }
  }

  @Override
  public boolean isSuccess() {
    return futureResult.join().isSuccess();
  }

  @Override
  public boolean isFailure() {
    return futureResult.join().isFailure();
  }

  @Override
  public boolean isCanceled() {
    return futureResult.join().isCanceled();
  }

  @Override
  public boolean isTimeout() {
    return futureResult.join().isTimeout();
  }
}
