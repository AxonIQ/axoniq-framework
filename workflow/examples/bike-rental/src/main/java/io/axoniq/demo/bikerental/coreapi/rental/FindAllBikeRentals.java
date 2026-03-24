package io.axoniq.demo.bikerental.coreapi.rental;

import org.axonframework.messaging.queryhandling.annotation.Query;

@Query(name = FindAllBikeRentals.QUERY_NAME)
public class FindAllBikeRentals {

    public static final String QUERY_NAME = "findAll";
    @SuppressWarnings("InstantiationOfUtilityClass")
    public static final FindAllBikeRentals INSTANCE = new FindAllBikeRentals();

    FindAllBikeRentals() {

    }
}
