/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.demo.bikerental.coreapi.rental;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class BikeStatus {

    @Id
    private String bikeId;
    private String bikeType;
    private String location;
    private String renter;
    private RentalStatus status;

    public BikeStatus() {
    }

    public BikeStatus(String bikeId, String bikeType, String location) {
        this.bikeId = bikeId;
        this.bikeType = bikeType;
        this.location = location;
        this.status = RentalStatus.AVAILABLE;
    }

    public String getBikeId() {
        return bikeId;
    }

    public String getLocation() {
        return location;
    }

    public void returnedAt(String location) {
        this.location = location;
        this.status = RentalStatus.AVAILABLE;
        this.renter = null;
    }

    public String getRenter() {
        return renter;
    }

    public void requestedBy(String renter) {
        this.renter = renter;
        this.status = RentalStatus.REQUESTED;
    }

    public void rentedBy(String renter) {
        this.renter = renter;
        this.status = RentalStatus.RENTED;
    }

    public RentalStatus getStatus() {
        return status;
    }

    public String description() {
        switch (status) {
            case RENTED:
                return String.format("Bike %s was rented by %s in %s", bikeId, renter, location);
            case AVAILABLE:
                return String.format("Bike %s is available for rental in %s.", bikeId, location);
            case REQUESTED:
                return String.format("Bike %s is requested by %s in %s", bikeId, renter, location);
            default:
                return "Status unknown";
        }
    }

    public String getBikeType() {
        return bikeType;
    }
}
