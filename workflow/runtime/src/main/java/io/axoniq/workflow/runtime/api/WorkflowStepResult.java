package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Optional;

/**
 * Represents a typed result of a step execution.
 */
public interface WorkflowStepResult {

  String getStepName();

  /**
   * Check (non-blocking)
   * @return
   */
  boolean isCompleted();

  @Nonnull
  <T> Optional<T> payload();

  @Nonnull
  Optional<StepFailedException> error();

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
