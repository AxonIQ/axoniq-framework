package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.commandhandling.annotation.Command;
import org.axonframework.modelling.annotation.TargetEntityId;

@Command
public record RejectRequestCommand(
        @TargetEntityId
        String bikeId,
        String renter
) {

}
