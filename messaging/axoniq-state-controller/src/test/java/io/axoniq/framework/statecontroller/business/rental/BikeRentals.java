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

package io.axoniq.framework.statecontroller.business.rental;

import io.axoniq.framework.statecontroller.decisions.Decide;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.history.History;

import static io.axoniq.framework.statecontroller.decisions.Decision.accept;
import static io.axoniq.framework.statecontroller.decisions.Decision.reject;

/**
 * Bike rental decisions in the business-first {@code @Decide} / {@link History} API, per the ADR worked example.
 * Each decision narrows the injected {@link History} to the per-bike scope with {@code history.of("bike", id)},
 * then reads {@link History#latestOf(Class[]) latestOf(...)} and branches with a plain {@code instanceof}
 * pattern — collapsing the lazy-{@code Condition} chain of the original {@code sample.rental.BikeRentals} into a
 * single expression. Same behaviour, same tight DCB boundary, no condition vocabulary.
 * <p>
 * {@link #rentBike(RentBike, History) rentBike} deliberately reads only
 * {@code latestOf(BikeRequested, RequestRejected, BikeReturned)} — {@link RequestApproved} is excluded to keep
 * the consistency boundary narrow — and accepts iff that latest event is <em>not</em> a {@link BikeRequested}
 * (the pending/active state). {@link #returnBike(ReturnBike, History) returnBike} reads
 * {@code latestOf(RequestApproved, BikeReturned)}: the bike is rented iff the latest of the pair is an approval,
 * whose {@code userId} identifies the current renter.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public class BikeRentals {

    @Decide
    public Decision rentBike(RentBike cmd, History history) {
        History bike = history.of("bike", cmd.bikeId());

        // available unless the latest of these three is a pending/active request
        if (bike.latestOf(BikeRequested.class, RequestRejected.class, BikeReturned.class)
                instanceof BikeRequested) {
            return reject("bike is not available for rental");
        }
        return accept(new BikeRequested(cmd.bikeId(), cmd.userId()));
    }

    @Decide
    public Decision returnBike(ReturnBike cmd, History history) {
        History bike = history.of("bike", cmd.bikeId());

        // rented iff the latest of this pair is an approval; its userId is the current renter
        if (!(bike.latestOf(RequestApproved.class, BikeReturned.class) instanceof RequestApproved approved)) {
            return reject("bike is not currently rented");
        }
        if (!approved.userId().equals(cmd.userId())) {
            return reject("bike is rented by another user");
        }
        return accept(new BikeReturned(cmd.bikeId(), cmd.userId()));
    }
}
