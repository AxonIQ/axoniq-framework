package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event
public record BikeRequestedEvent(String bikeId, String renter, String rentalReference) {

}
