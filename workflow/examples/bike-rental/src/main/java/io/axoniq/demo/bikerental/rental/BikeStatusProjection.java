package io.axoniq.demo.bikerental.rental;

import io.axoniq.demo.bikerental.coreapi.rental.BikeStatus;
import io.axoniq.demo.bikerental.coreapi.rental.FindAvailable;
import io.axoniq.demo.bikerental.coreapi.rental.FindRentalByBikeIdQuery;
import io.axoniq.demo.bikerental.coreapi.rental.RentalStatus;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.springframework.stereotype.Component;

@Component
public class BikeStatusProjection {

    private final BikeStatusRepository bikeStatusRepository;

    public BikeStatusProjection(BikeStatusRepository bikeStatusRepository) {
        this.bikeStatusRepository = bikeStatusRepository;
    }

    @QueryHandler(queryName = "io.axoniq.demo.bikerental.coreapi.rental.findAll")
    public Iterable<BikeStatus> findAll() {
        return bikeStatusRepository.findAll();
    }

    @QueryHandler(queryName = "io.axoniq.demo.bikerental.coreapi.rental.findAvailable")
    public Iterable<BikeStatus> findAvailable(FindAvailable q) {
        return bikeStatusRepository.findAllByBikeTypeAndStatus(q.bikeType(), RentalStatus.AVAILABLE);
    }

    @QueryHandler(queryName = "io.axoniq.demo.bikerental.coreapi.rental.findOne")
    public BikeStatus findOne(FindRentalByBikeIdQuery q) {
        return bikeStatusRepository.findById(q.bikeId()).orElse(null);
    }
}
