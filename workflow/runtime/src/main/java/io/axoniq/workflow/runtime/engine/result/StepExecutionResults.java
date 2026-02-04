package io.axoniq.workflow.runtime.engine.result;

import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Utility containing {@link StepExecutionResult} factory methods.
 */
public class StepExecutionResults {

  /**
   * Constructs a future result.
   *
   * @param futureResult future of the result.
   * @return step execution result.
   */
  public static StepExecutionResult fromFuture(CompletableFuture<StepExecutionResult> futureResult) {
    return new FutureStepExecutionResult(futureResult);
  }

  /**
   * Constructs completed result.
   *
   * @param payload payload of the result, might be null.
   * @return completed step result.
   */
  public static StepExecutionResult completed(@Nullable Object payload) {
    return new CompletedStepExecutionResult(payload, null, null, false);
  }

  /**
   * Constructs failed result.
   *
   * @param error failure causing error.
   * @return failed result.
   */
  public static StepExecutionResult failed(@Nonnull Throwable error) {
    return new CompletedStepExecutionResult(null, Objects.requireNonNull(error, "Error must be provided"), null, false);
  }

  /**
   * Constructs cancelled result.
   *
   * @return cancelled result.
   */
  public static StepExecutionResult cancelled() {
    return new CompletedStepExecutionResult(null, null, null, true);
  }

  /**
   * Constructs timed out result.
   *
   * @param timeout timeout duration.
   * @return timed out result.
   */
  public static StepExecutionResult timeout(@Nonnull Duration timeout) {
    return new CompletedStepExecutionResult(null, null, Objects.requireNonNull(timeout, "Timeout must be provided"), false);
  }


  public static StepExecutionResult all(StepExecutionResult... results) {
    return new StepExecutionResult() {

      @Override
      public String getStepName() {
        return "all(" + String.join(", ", Arrays.stream(results).map(StepExecutionResult::getStepName).toList()) + ")";
      }

      @Override
      public boolean isCompleted() {
        return Arrays.stream(results).allMatch(StepExecutionResult::isCompleted);
      }

      @Override
      public <T> Optional<T> payload() {
        return Optional.empty();
      }

      @Override
      public Optional<StepFailedException> error() {
        return Arrays.stream(results).filter(StepExecutionResult::isFailure).findFirst().flatMap(StepExecutionResult::error);
      }

      @Override
      public boolean isSuccess() {
        return Arrays.stream(results).allMatch(StepExecutionResult::isSuccess);
      }

      @Override
      public boolean isFailure() {
        return Arrays.stream(results).anyMatch(StepExecutionResult::isFailure);
      }

      @Override
      public boolean isCanceled() {
        return Arrays.stream(results).anyMatch(StepExecutionResult::isCanceled);
      }

      @Override
      public boolean isTimeout() {
        return Arrays.stream(results).anyMatch(StepExecutionResult::isCompleted);
      }
    };
  }


  private StepExecutionResults() {
    // util class
  }
}
