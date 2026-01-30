package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.StepExecutionResult;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
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
  static StepExecutionResult timeout(@Nonnull Duration timeout) {
    return new CompletedStepExecutionResult(null, null, Objects.requireNonNull(timeout, "Timeout must be provided"), false);
  }


  private StepExecutionResults() {
    // util class
  }
}
