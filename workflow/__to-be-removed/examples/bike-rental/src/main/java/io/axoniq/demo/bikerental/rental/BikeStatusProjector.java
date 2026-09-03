/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.demo.bikerental.rental;

import io.axoniq.demo.bikerental.coreapi.rental.BikeInUseEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeRegisteredEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeRequestedEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeReturnedEvent;
import io.axoniq.demo.bikerental.coreapi.rental.BikeStatus;
import io.axoniq.demo.bikerental.coreapi.rental.FindAllBikeRentalsQuery;
import io.axoniq.demo.bikerental.coreapi.rental.FindRentalByBikeIdQuery;
import io.axoniq.demo.bikerental.coreapi.rental.RequestRejectedEvent;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.springframework.stereotype.Component;

/**
 * @since 5.4.0
 */
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
        updateEmitter.emit(FindAllBikeRentalsQuery.class, q -> true, bikeStatus);
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
                    updateEmitter.emit(FindAllBikeRentalsQuery.class, q -> true, bikeStatus);
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
                    updateEmitter.emit(FindAllBikeRentalsQuery.class, q -> true, bikeStatus);
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
                    updateEmitter.emit(FindAllBikeRentalsQuery.class, q -> true, bikeStatus);
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
                    updateEmitter.emit(FindAllBikeRentalsQuery.class, q -> true, bikeStatus);
                    updateEmitter.emit(FindRentalByBikeIdQuery.class,
                                       q -> event.bikeId().equals(q.bikeId()),
                                       bikeStatus);
                });
    }
}
