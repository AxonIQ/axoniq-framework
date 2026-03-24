package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.eventhandling.annotation.Event;

@Event
public record PaymentPreparedEvent(String paymentId, int amount, String paymentReference) {

}