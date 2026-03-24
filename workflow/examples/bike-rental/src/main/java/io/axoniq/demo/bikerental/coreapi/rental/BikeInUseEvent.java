package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event
public record BikeInUseEvent(String bikeId, String renter) {

}
