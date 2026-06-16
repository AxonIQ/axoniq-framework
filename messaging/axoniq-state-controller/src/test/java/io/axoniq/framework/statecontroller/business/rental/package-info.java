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

/**
 * Bike rental sample re-expressed in the business-first {@code @Decide} / {@code History} API, modelled on the
 * same separated-payment lifecycle as {@code sample.rental}: {@code RentBike} only records the rental intent
 * ({@code BikeRequested}); a downstream payment service then issues an approve or reject command which produces
 * either {@code RequestApproved} or {@code RequestRejected}; the renter eventually returns the bike, producing
 * {@code BikeReturned}.
 * <p>
 * This package is the equivalence proof for the new API: the decision bodies read with {@code history.of(...)}
 * and {@code latestOf(...) instanceof ...} per the ADR worked example, yet every Given-When-Then scenario from
 * {@code sample.rental.BikeRentalsTest} passes unchanged. Same behaviour, same tight DCB boundary, no condition
 * vocabulary.
 */
@NullMarked
package io.axoniq.framework.statecontroller.business.rental;

import org.jspecify.annotations.NullMarked;
