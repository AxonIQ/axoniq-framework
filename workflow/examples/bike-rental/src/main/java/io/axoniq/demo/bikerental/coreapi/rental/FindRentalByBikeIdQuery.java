package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.queryhandling.annotation.Query;


@Query(name = FindRentalByBikeIdQuery.QUERY_NAME)
public record FindRentalByBikeIdQuery(
        String bikeId
) {
    public static final String QUERY_NAME = "findOne";
}
