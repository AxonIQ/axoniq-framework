package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.commandhandling.annotation.Command;
import org.axonframework.modelling.annotation.TargetEntityId;

@Command
public record ConfirmPaymentCommand(@TargetEntityId String paymentId) {

}