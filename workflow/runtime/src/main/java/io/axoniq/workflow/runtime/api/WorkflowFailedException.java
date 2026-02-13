package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;

public class WorkflowFailedException extends RuntimeException {

  public WorkflowFailedException(@Nonnull String message) {
    super(message);
  }

  public WorkflowFailedException(@Nonnull String message, Throwable cause) {
    super(message, cause);
  }

  public WorkflowFailedException(@Nonnull Throwable cause) {
    super(cause);
  }
}
