package io.axoniq.workflow.runtime.api.primitives;

import java.util.Optional;

/**
 * Represents a typed result of a step execution.
 */
public interface StepResult {

  boolean isCompleted();

  <T> Optional<T> result();

  Optional<Throwable> error();

  /**
   * Blocks until step execution is finished.
   *
   * @return true, if the operation completed successfully.
   */
  boolean isSuccess();

  /**
   * Blocks until step execution is finished.
   *
   * @return true, if the operation completed with an exception.
   */
  boolean isFailure();

  /**
   * Blocks until step execution is finished.
   *
   * @return true, if the operation was interrupted by external party.
   */
  boolean isCanceled();

  /**
   * Blocks until step execution is finished.
   *
   * @return true, if the operation was interrupted by timeout specified on start.
   */
  boolean isTimeout();

}
