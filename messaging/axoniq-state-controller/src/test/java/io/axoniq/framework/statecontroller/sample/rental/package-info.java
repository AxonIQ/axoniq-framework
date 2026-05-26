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
 * Bike rental sample, modelled on a separated-payment lifecycle: {@code RentBike} only records the rental
 * intent ({@code BikeRequested}); a downstream payment service then issues an approve or reject command which
 * produces either {@code RequestApproved} or {@code RequestRejected}; the renter eventually returns the bike,
 * producing {@code BikeReturned}.
 * <p>
 * Showcases the DCB sweet spot — each decision reads only the events that actually move its outcome:
 * <ul>
 *     <li>{@code rentBike} reads {@code latestOf(BikeRequested, RequestRejected, BikeReturned)} (skips the
 *         {@code RequestApproved} payload entirely);</li>
 *     <li>{@code returnBike} reads {@code latestOf(RequestApproved, BikeReturned)} and pulls the current
 *         renter's {@code userId} straight off the {@code RequestApproved} event.</li>
 * </ul>
 * A rent attempt against an unavailable bike is a pure error response: no events are appended.
 */
@NullMarked
package io.axoniq.framework.statecontroller.sample.rental;

import org.jspecify.annotations.NullMarked;
