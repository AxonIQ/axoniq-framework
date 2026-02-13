package io.axoniq.workflow.runtime.api;

public class WorkflowFailedException extends RuntimeException {

  public WorkflowFailedException(String message) {
    super(message);
  }

  public WorkflowFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public WorkflowFailedException(Throwable cause) {
    super(cause);
  }
}
