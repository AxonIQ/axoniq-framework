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

package io.axoniq.framework.statecontroller.sample.rental;

import io.axoniq.framework.statecontroller.conditions.OptionalCondition;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.decisions.StateController;
import io.axoniq.framework.statecontroller.eventstream.EventStream;

import java.util.Optional;

/**
 * Bike rental decisions, hand-tuned for a tight DCB consistency boundary in the separated-payment lifecycle: a
 * {@link RentBike} command merely records the {@link BikeRequested intent}; a downstream payment service subsequently
 * issues either an approve or reject command, which is what produces a {@link RequestApproved} or
 * {@link RequestRejected} event. A {@link ReturnBike} command produces a {@link BikeReturned} event when the renter
 * matches.
 * <p>
 * <h3>{@link #rentBike(RentBike, DecisionContext) rentBike}</h3>
 * Reads {@code latestOf(BikeRequested, RequestRejected, BikeReturned)} on the per-bike scope and accepts iff that
 * latest event is <em>not</em> a {@link BikeRequested}:
 * <ul>
 *     <li>no events from this set → never used → available</li>
 *     <li>{@link BikeReturned} latest → just returned → available</li>
 *     <li>{@link RequestRejected} latest → previous request canceled (payment failed) → available</li>
 *     <li>{@link BikeRequested} latest → pending payment, or rented (a later
 *         {@link RequestApproved} is intentionally filtered out of this query yet is still implied by the
 *         pairing) → <strong>not</strong> available</li>
 * </ul>
 * On accept, the only event emitted is {@link BikeRequested} — the approval/rejection arrives later, in a
 * separate command. On reject, the decision throws without appending any audit events; an unavailable bike is
 * a pure error response.
 * <p>
 * <h3>{@link #returnBike(ReturnBike, DecisionContext) returnBike}</h3>
 * Reads {@code latestOf(RequestApproved, BikeReturned)} on the per-bike scope. The bike is currently rented
 * <em>iff</em> the latest event from this pair is a {@link RequestApproved}; the approval's {@code userId}
 * identifies the current renter, which must match the command's {@code userId} for the return to succeed.
 * A pending {@link BikeRequested} that hasn't yet been approved is correctly treated as "not rented" — both
 * because it isn't in this query and because no payment has been confirmed.
 */
public class BikeRentals {

    @StateController
    public Decision rentBike(RentBike cmd, DecisionContext ctx) {
        EventStream bike = ctx.scope("bike", cmd.bikeId());
        var available = bike.latestOf(BikeRequested.class,
                                      RequestRejected.class,
                                      BikeReturned.class)
                            .isA(BikeRequested.class)
                            .not();
        if (available.isTrue()) {
            return Decision.emit(new BikeRequested(cmd.bikeId(), cmd.userId()));
        }
        return Decision.reject("bike is not available for rental");
    }

    @StateController
    public Decision returnBike(ReturnBike cmd, DecisionContext ctx) {
        EventStream bike = ctx.scope("bike", cmd.bikeId());
        OptionalCondition<String> currentRenter = bike.latestOf(RequestApproved.class, BikeReturned.class)
                                                      .as(RequestApproved.class)
                                                      .mapPresent(RequestApproved::userId);

        Optional<String> renter = currentRenter.value();
        if (renter.isEmpty()) {
            return Decision.reject("bike is not currently rented");
        }
        if (!renter.get().equals(cmd.userId())) {
            return Decision.reject("bike is rented by another user");
        }
        return Decision.emit(new BikeReturned(cmd.bikeId(), cmd.userId()));
    }
}
