package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event
public record PaymentConfirmedEvent(String paymentId, String paymentReference) {

}