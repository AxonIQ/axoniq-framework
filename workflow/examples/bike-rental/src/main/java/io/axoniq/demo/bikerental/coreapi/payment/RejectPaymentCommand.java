package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.commandhandling.annotation.Command;
import org.axonframework.modelling.annotation.TargetEntityId;

@Command
public record RejectPaymentCommand(
        @TargetEntityId
        String paymentId
) {

}