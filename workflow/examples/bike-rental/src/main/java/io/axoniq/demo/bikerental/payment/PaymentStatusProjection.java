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

import io.axoniq.demo.bikerental.coreapi.payment.GetAllPaymentsQuery;
import io.axoniq.demo.bikerental.coreapi.payment.GetPaymentIdQuery;
import io.axoniq.demo.bikerental.coreapi.payment.GetPaymentStatusQuery;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentConfirmedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentPreparedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentRejectedEvent;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentStatus;
import org.axonframework.messaging.core.annotation.Namespace;
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

    @QueryHandler(queryName = "io.axoniq.demo.bikerental.coreapi.payment.getPaymentStatus")
    public PaymentStatus getStatus(GetPaymentStatusQuery q) {
        return paymentStatusRepository.findById(q.paymentId()).orElse(null);
    }

    @QueryHandler(queryName = "io.axoniq.demo.bikerental.coreapi.payment.getPaymentId")
    public String getPaymentId(GetPaymentIdQuery q) {
        return paymentStatusRepository.findByReferenceAndStatus(q.paymentReference(), PENDING)
                                      .map(PaymentStatus::getId)
                                      .orElse(null);
    }

    @QueryHandler(queryName = "io.axoniq.demo.bikerental.coreapi.payment.getAllPayments")
    public Iterable<PaymentStatus> findByStatus(GetAllPaymentsQuery q) {
        if (q.status() != null) {
            return paymentStatusRepository.findAllByStatus(q.status());
        }
        return paymentStatusRepository.findAll();
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
