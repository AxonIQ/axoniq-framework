package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class FutureStepExecutionResult implements StepExecutionResult {

  private final CompletableFuture<StepExecutionResult> futureResult;

  FutureStepExecutionResult(CompletableFuture<StepExecutionResult> futureResult) {
    this.futureResult = futureResult;
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
