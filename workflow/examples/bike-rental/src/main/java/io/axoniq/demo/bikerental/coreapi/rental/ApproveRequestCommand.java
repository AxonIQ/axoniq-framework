package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.messaging.commandhandling.annotation.Command;
import org.axonframework.modelling.annotation.TargetEntityId;

@Command
public record ApproveRequestCommand(
        @EventTag(key = "Bike") String bikeId,
        String renter
) {

}
