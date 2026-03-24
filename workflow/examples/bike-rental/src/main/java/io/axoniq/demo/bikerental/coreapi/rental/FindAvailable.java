package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.queryhandling.annotation.Query;

import static io.axoniq.demo.bikerental.coreapi.rental.FindAvailable.QUERY_NAME;

@Query(name = QUERY_NAME)
public record FindAvailable(
        String bikeType
) {
    public static final String QUERY_NAME = "findAvailable";
}
