package io.axoniq.workflow.runtime.event;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event(namespace = "io.axoniq.workflow")
public record StepFailed(String stepId, String message, Throwable cause){
  public static String ID = "io.axoniq.workflow.StepFailed#0.1";

  @Override
  public String toString() {
    return "✗ FAILED [" + stepId + "] → " + message;
  }
}
