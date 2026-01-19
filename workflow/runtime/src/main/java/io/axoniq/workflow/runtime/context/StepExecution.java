package io.axoniq.workflow.runtime.context;

public record StepExecution(
  String stepName,
  StepStatus status,
  Object result,
  Throwable error
) {
  public static StepExecution inProgress(String name, Object started) {
    return new StepExecution(name, StepStatus.IN_PROGRESS, started, null);
  }

  public static StepExecution completed(String name, Object result) {
    return new StepExecution(name, StepStatus.COMPLETED, result, null);
  }

  public static StepExecution failed(String name, Throwable error) {
    return new StepExecution(name, StepStatus.FAILED, null, error);
  }
}
