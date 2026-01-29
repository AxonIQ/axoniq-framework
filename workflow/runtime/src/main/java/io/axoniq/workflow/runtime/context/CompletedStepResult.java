package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.StepResult;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public class CompletedStepResult implements StepResult {

  private final Object payload;
  private final Throwable error;
  private final Duration timeout;
  private final Boolean cancelled;

  /**
   * Constructs completed result.
   * @param payload payload of the result, might be null.
   * @return completed step result.
   */
  static StepResult completed(@Nullable Object payload) {
    return new CompletedStepResult(payload, null, null, false);
  }

  /**
   * Constructs failed result.
   * @param error failure causing error.
   * @return failed result.
   */
  static StepResult failed(@Nonnull Throwable error) {
    return new CompletedStepResult(null, Objects.requireNonNull(error, "Error must be provided"), null, false);
  }

  /**
   * Constructs cancelled result.
   * @return cancelled result.
   */
  static StepResult cancelled() {
    return new CompletedStepResult(null, null, null, true);
  }

  /**
   * Constructs timed out result.
   * @param timeout timeout duration.
   * @return timed out result.
   */
  static StepResult timeout(@Nonnull Duration timeout) {
    return new CompletedStepResult(null, null, Objects.requireNonNull(timeout, "Timeout must be provided"), false);
  }

  CompletedStepResult(Object payload, Throwable error, Duration timeout, Boolean cancelled) {
    this.payload = payload;
    this.error = error;
    this.timeout = timeout;
    this.cancelled = cancelled;
  }

  @Override
  public boolean isCompleted() {
    return true;
  }

  @Override
  public <T> Optional<T> result() {
    //noinspection unchecked
    return Optional.ofNullable((T) payload);
  }

  @Override
  public Optional<Throwable> error() {
    return Optional.ofNullable(error);
  }

  @Override
  public boolean isSuccess() {
    return !cancelled && timeout == null && error == null;
  }

  @Override
  public boolean isFailure() {
    return error != null;
  }

  @Override
  public boolean isCanceled() {
    return cancelled;
  }

  @Override
  public boolean isTimeout() {
    return timeout != null;
  }
}
