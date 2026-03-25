package io.axoniq.demo.bikerental.rental;

import io.axoniq.demo.bikerental.coreapi.rental.BikeInUseEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeRegisteredEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeRequestedEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeReturnedEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeStatus;
import io.axoniq.demo.bikerental.coreapi.rental.FindAllBikeRentals;
import io.axoniq.demo.bikerental.coreapi.rental.FindRentalByBikeIdQuery;
import io.axoniq.demo.bikerental.coreapi.rental.RequestRejectedEvent;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.springframework.stereotype.Component;

@Component
public class BikeStatusProjector {

    private final BikeStatusRepository bikeStatusRepository;

    public BikeStatusProjector(BikeStatusRepository bikeStatusRepository) {
        this.bikeStatusRepository = bikeStatusRepository;
    }

    @EventHandler
    public void on(BikeRegisteredEvent event, QueryUpdateEmitter updateEmitter) {
        var bikeStatus = new BikeStatus(event.bikeId(), event.bikeType(), event.location());
        bikeStatusRepository.save(bikeStatus);
        updateEmitter.emit(FindAllBikeRentals.class, q -> true, bikeStatus);
    }

    @EventHandler
    public void on(BikeRequestedEvent event, QueryUpdateEmitter updateEmitter) {
        bikeStatusRepository
                .findById(event.bikeId())
                .map(bs -> {
                    bs.requestedBy(event.renter());
                    return bs;
                })
                .ifPresent(bikeStatus -> {
                    updateEmitter.emit(FindAllBikeRentals.class, q -> true, bikeStatus);
                    updateEmitter.emit(FindRentalByBikeIdQuery.class,
                                       q -> event.bikeId().equals(q.bikeId()),
                                       bikeStatus);
                });
    }

    @EventHandler
    public void on(BikeInUseEvent event, QueryUpdateEmitter updateEmitter) {
        bikeStatusRepository
                .findById(event.bikeId())
                .map(bs -> {
                    bs.rentedBy(event.renter());
                    return bs;
                })
                .ifPresent(bikeStatus -> {
                    updateEmitter.emit(FindAllBikeRentals.class, q -> true, bikeStatus);
                    updateEmitter.emit(FindRentalByBikeIdQuery.class,
                                       q -> event.bikeId().equals(q.bikeId()),
                                       bikeStatus);
                });
    }

    @EventHandler
    public void on(BikeReturnedEvent event, QueryUpdateEmitter updateEmitter) {
        bikeStatusRepository
                .findById(event.bikeId())
                .map(bs -> {
                    bs.returnedAt(event.location());
                    return bs;
                })
                .ifPresent(bikeStatus -> {
                    updateEmitter.emit(FindAllBikeRentals.class, q -> true, bikeStatus);
                    updateEmitter.emit(FindRentalByBikeIdQuery.class,
                                       q -> event.bikeId().equals(q.bikeId()),
                                       bikeStatus);
                });
    }

    @EventHandler
    public void on(RequestRejectedEvent event, QueryUpdateEmitter updateEmitter) {
        bikeStatusRepository
                .findById(event.bikeId())
                .map(bs -> {
                    bs.returnedAt(bs.getLocation());
                    return bs;
                })
                .ifPresent(bikeStatus -> {
                    updateEmitter.emit(FindAllBikeRentals.class, q -> true, bikeStatus);
                    updateEmitter.emit(FindRentalByBikeIdQuery.class,
                                       q -> event.bikeId().equals(q.bikeId()),
                                       bikeStatus);
                });
    }
}
