package io.axoniq.workflow.runtime.engine.execution;

public enum WorkflowStatus {
  NONE,
  STARTED,
  COMPLETED,
  FAILED,
  CANCELLED,
  TIMED_OUT;

  public boolean isTerminal() {
    return switch (this) {
      case NONE, STARTED -> false;
      case COMPLETED, FAILED, CANCELLED, TIMED_OUT -> true;
    };
  }
}
