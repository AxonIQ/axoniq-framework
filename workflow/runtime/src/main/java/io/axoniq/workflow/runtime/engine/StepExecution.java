package io.axoniq.workflow.runtime.engine;

public record StepExecution(
  String stepName,
  StepStatus status,
  Object result,
  Throwable error
) {
  public static StepExecution started(String name, Object started) {
    return new StepExecution(name, StepStatus.STARTED, started, null);
  }

  public static StepExecution completed(String name, Object result) {
    return new StepExecution(name, StepStatus.COMPLETED, result, null);
  }

  public static StepExecution failed(String name, Throwable error) {
    return new StepExecution(name, StepStatus.FAILED, null, error);
  }

  public static StepExecution timedOut(String name, Object timestamp) {
    return new StepExecution(name, StepStatus.TIMED_OUT, timestamp, null);
  }
}
