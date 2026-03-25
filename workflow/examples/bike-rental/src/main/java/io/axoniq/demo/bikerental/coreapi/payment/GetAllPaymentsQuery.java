package io.axoniq.demo.bikerental.coreapi.payment;

import org.axonframework.messaging.core.annotation.Namespace;
import org.axonframework.messaging.queryhandling.annotation.Query;

@Namespace("io.axoniq.demo.bikerental.coreapi.payment")
@Query(name = GetAllPaymentsQuery.QUERY_NAME)
public record GetAllPaymentsQuery(
        PaymentStatus.Status status
) {
    public static final String QUERY_NAME = "getAllPayments";
}
