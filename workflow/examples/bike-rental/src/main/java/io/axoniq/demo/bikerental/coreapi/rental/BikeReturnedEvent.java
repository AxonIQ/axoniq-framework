package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event
public record BikeReturnedEvent(String bikeId, String location) {}
