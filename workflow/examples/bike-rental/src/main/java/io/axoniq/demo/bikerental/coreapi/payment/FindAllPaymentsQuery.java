package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.queryhandling.annotation.Query;

@Query(name = FindAllPaymentsQuery.QUERY_NAME)
public record FindAllPaymentsQuery(
        PaymentStatus.Status status
) {
    public static final String QUERY_NAME = "getAllPayments";
}
