package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event
public record PaymentRejectedEvent(String paymentId, String paymentReference) {

}

