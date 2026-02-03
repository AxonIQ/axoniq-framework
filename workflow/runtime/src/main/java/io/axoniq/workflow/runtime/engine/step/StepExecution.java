package io.axoniq.workflow.runtime.engine.step;

import java.time.Instant;

public record StepExecution(
  String stepName,
  StepStatus status,
  Object result,
  Throwable error,
  Instant timestamp
) {
  public static StepExecution started(String name, Object parameters, Instant timestamp) {
    return new StepExecution(name, StepStatus.STARTED, parameters, null, timestamp);
  }

  public static StepExecution completed(String name, Object result, Instant timestamp) {
    return new StepExecution(name, StepStatus.COMPLETED, result, null, timestamp);
  }

  public static StepExecution failed(String name, Throwable error, Instant timestamp) {
    return new StepExecution(name, StepStatus.FAILED, null, error, timestamp);
  }

  public static StepExecution timedOut(String name, Object payload, Instant timestamp) {
    return new StepExecution(name, StepStatus.TIMED_OUT, payload, null, timestamp);
  }
}
