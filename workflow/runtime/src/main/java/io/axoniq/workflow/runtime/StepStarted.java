package io.axoniq.workflow.runtime;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event(namespace = "io.axoniq.workflow")
public record StepStarted(String stepId) {
}
