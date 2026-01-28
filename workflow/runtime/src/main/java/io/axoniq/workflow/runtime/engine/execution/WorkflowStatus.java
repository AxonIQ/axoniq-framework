package io.axoniq.workflow.runtime.engine.execution;

public enum WorkflowStatus {
  NONE,
  STARTED,
  COMPLETED,
  FAILED,
  CANCELLED,
  TIMED_OUT;

  public boolean isTerminal() {
    return this == COMPLETED || this == FAILED || this == CANCELLED || this == TIMED_OUT;
  }
}
