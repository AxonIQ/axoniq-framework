package io.axoniq.workflow.runtime.event;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event(namespace = "io.axoniq.workflow")
public record StepStarted(String stepId) {
  public static String ID = "io.axoniq.workflow.StepStarted#0.1";

  @Override
  public String toString() {
    return "▶ STARTED [" + stepId + "]";
  }
}
