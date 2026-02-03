package io.axoniq.workflow.runtime.engine.step;

public enum StepStatus {
  STARTED,
  COMPLETED,
  FAILED,
  TIMED_OUT,
  CANCELLED;

  public boolean isTerminal() {
    return switch (this) {
      case STARTED -> false;
      case COMPLETED, FAILED, CANCELLED, TIMED_OUT -> true;
    };
  }

}
