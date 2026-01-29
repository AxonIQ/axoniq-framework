package io.axoniq.workflow.runtime.api.primitives;

/**
 * Represents a typed result of a step execution.
 * @param <T>
 */
public interface StepResult<T> {

  boolean isSuccess();
  boolean isFailure();
  boolean isCanceled();
  boolean isTimeout();

  /**
   * Blocks and delivers result after the step is executed.
   * @return result of step execution.
   */
  T await();
}
