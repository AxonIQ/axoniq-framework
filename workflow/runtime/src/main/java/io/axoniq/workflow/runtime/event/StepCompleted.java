package io.axoniq.workflow.runtime.event;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event(namespace = "io.axoniq.workflow")
public record StepCompleted(String stepId, Object result) {
  public static String ID = "io.axoniq.workflow.StepCompleted#0.1";

  @Override
  public String toString() {
    if (result == null) {
      return "✓ COMPLETED [" + stepId + "]";
    }
    return "✓ COMPLETED [" + stepId + "] → " + result;
  }
}
