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

import io.axoniq.demo.bikerental.coreapi.rental.BikeStatus;
import io.axoniq.demo.bikerental.coreapi.rental.FindAvailableQuery;
import io.axoniq.demo.bikerental.coreapi.rental.FindRentalByBikeIdQuery;
import io.axoniq.demo.bikerental.coreapi.rental.RentalStatus;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.springframework.stereotype.Component;

/**
 * @since 0.1.0
 */
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
    public Iterable<BikeStatus> findAvailable(FindAvailableQuery q) {
        return bikeStatusRepository.findAllByBikeTypeAndStatus(q.bikeType(), RentalStatus.AVAILABLE);
    }

    @QueryHandler(queryName = "io.axoniq.demo.bikerental.coreapi.rental.findOne")
    public BikeStatus findOne(FindRentalByBikeIdQuery q) {
        return bikeStatusRepository.findById(q.bikeId()).orElse(null);
    }
}
