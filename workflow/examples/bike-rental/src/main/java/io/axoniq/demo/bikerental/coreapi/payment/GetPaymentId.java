package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.queryhandling.annotation.Query;

@Query(name = GetPaymentId.QUERY_NAME)
public record GetPaymentId(
        String paymentReference
) {

    public static final String QUERY_NAME = "getPaymentId";
}
