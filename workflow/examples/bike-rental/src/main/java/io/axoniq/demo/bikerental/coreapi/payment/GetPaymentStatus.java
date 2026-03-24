package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.queryhandling.annotation.Query;

@Query(name = GetPaymentStatus.QUERY_NAME)
public record GetPaymentStatus(
        String paymentId
) {
    public static final String QUERY_NAME = "getPaymentStatus";
}
