package io.axoniq.demo.bikerental.payment;

import io.axoniq.demo.bikerental.coreapi.payment.ConfirmPaymentCommand;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentConfirmedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentPreparedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentRejectedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PreparePaymentCommand;
import io.axoniq.demo.bikerental.coreapi.payment.RejectPaymentCommand;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.extension.spring.stereotype.EventSourced;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

import java.util.UUID;

@EventSourced(type = "Payment")
public class Payment {

    private String id;

    private boolean closed;
    private String paymentReference;

    @EntityCreator
    public Payment() {
    }

    @CommandHandler
    public static void handle(PreparePaymentCommand command, EventAppender eventAppender) {
        String paymentId = UUID.randomUUID().toString();
        eventAppender.append(new PaymentPreparedEvent(paymentId, command.amount(), command.paymentReference()));
    }

    @CommandHandler
    public void handle(ConfirmPaymentCommand command, EventAppender eventAppender) {
        if (!closed) {
            eventAppender.append(new PaymentConfirmedEvent(command.paymentId(), paymentReference));
        }
    }

    @CommandHandler
    public void handle(RejectPaymentCommand command, EventAppender eventAppender) {
        if (!closed) {
            eventAppender.append(new PaymentRejectedEvent(command.paymentId(), paymentReference));
        }
    }

    @EventSourcingHandler
    protected void on(PaymentPreparedEvent event) {
        this.id = event.paymentId();
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
