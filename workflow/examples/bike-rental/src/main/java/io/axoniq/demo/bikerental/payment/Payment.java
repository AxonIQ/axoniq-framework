/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.demo.bikerental.payment;

import io.axoniq.demo.bikerental.coreapi.payment.ConfirmPaymentCommand;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentConfirmedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentPreparedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentRejectedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.RejectPaymentCommand;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.extension.spring.stereotype.EventSourced;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

@EventSourced(tagKey = "Payment")
public class Payment {

    private boolean closed;
    private String paymentReference;

    @EntityCreator
    public Payment() {
    }

    @CommandHandler
    public void handle(ConfirmPaymentCommand command, EventAppender eventAppender) {
        if (paymentReference == null) {
            throw new IllegalStateException("Payment not prepared yet");
        }
        if (!closed) {
            eventAppender.append(new PaymentConfirmedEvent(command.paymentId(), paymentReference));
        }
    }

    @CommandHandler
    public void handle(RejectPaymentCommand command, EventAppender eventAppender) {
        if (paymentReference == null) {
            throw new IllegalStateException("Payment not prepared yet");
        }
        if (!closed) {
            eventAppender.append(new PaymentRejectedEvent(command.paymentId(), paymentReference));
        }
    }

    @EventSourcingHandler
    public void handle(PaymentPreparedEvent event) {
        this.paymentReference = event.paymentReference();
    }

    @EventSourcingHandler
    protected void on(PaymentConfirmedEvent event) {
        this.closed = true;
    }

    @EventSourcingHandler
    protected void on(PaymentRejectedEvent event) {
        this.closed = true;
    }
}
