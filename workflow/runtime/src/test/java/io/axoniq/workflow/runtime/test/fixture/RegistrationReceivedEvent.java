package io.axoniq.workflow.runtime.test.fixture;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event(namespace = "my.custom", name = "RegistrationReceived")
public record RegistrationReceivedEvent(String id, String email) {
}