package io.axoniq.workflow.runtime.event;

import org.axonframework.messaging.eventhandling.annotation.Event;

import java.time.Instant;

@Event(namespace = "io.axoniq.workflow")
public record StepTimedOut(String stepId, Instant timestamp) {
  public static String ID = "io.axoniq.workflow.StepTimedOut#0.1";

  @Override
  public String toString() {
    return "⌛ TIMED OUT [" + stepId + "] → timestamp: " + timestamp;
  }
}
