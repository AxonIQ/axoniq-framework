package io.axoniq.demo.bikerental.payment;

import io.axoniq.demo.bikerental.coreapi.payment.FindAllPaymentsQuery;
import io.axoniq.demo.bikerental.coreapi.payment.GetPaymentId;
import io.axoniq.demo.bikerental.coreapi.payment.GetPaymentStatus;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentConfirmedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentPreparedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentRejectedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentStatus;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.springframework.stereotype.Component;

import static io.axoniq.demo.bikerental.coreapi.payment.PaymentStatus.Status.*;

@Component
public class PaymentStatusProjection {

    private final PaymentStatusRepository paymentStatusRepository;

    public PaymentStatusProjection(PaymentStatusRepository paymentStatusRepository) {
        this.paymentStatusRepository = paymentStatusRepository;
    }

    @QueryHandler(queryName = GetPaymentStatus.QUERY_NAME)
    public PaymentStatus getStatus(String paymentId) {
        return paymentStatusRepository.findById(paymentId).orElse(null);
    }

    @QueryHandler(queryName = GetPaymentId.QUERY_NAME)
    public String getPaymentId(String paymentReference) {
        return paymentStatusRepository.findByReferenceAndStatus(paymentReference, PENDING).map(PaymentStatus::getId)
                                      .orElse(null);
    }

    @QueryHandler(queryName = FindAllPaymentsQuery.QUERY_NAME)
    public Iterable<PaymentStatus> findByStatus(PaymentStatus.Status status) {
        if (status == null) {
            return paymentStatusRepository.findAll();
        }
        return paymentStatusRepository.findAllByStatus(status);
    }
    
    @EventHandler
    public void handle(PaymentPreparedEvent event, QueryUpdateEmitter updateEmitter) {
        paymentStatusRepository.save(new PaymentStatus(event.paymentId(), event.amount(), event.paymentReference()));
        updateEmitter.emit(String.class, event.paymentReference()::equals, event.paymentId());
    }

    @EventHandler
    public void handle(PaymentConfirmedEvent event) {
        paymentStatusRepository.findById(event.paymentId()).ifPresent(s -> s.setStatus(APPROVED));
    }

    @EventHandler
    public void handle(PaymentRejectedEvent event) {
        paymentStatusRepository.findById(event.paymentId()).ifPresent(s -> s.setStatus(REJECTED));
    }
}
