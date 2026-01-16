package io.axoniq.workflow.runtime;

import org.axonframework.messaging.eventhandling.annotation.Event;

import java.util.Map;

@Event(namespace = "io.axoniq.workflow")
public record StepCompleted(String stepId, Map<String, Object> results) {

}
