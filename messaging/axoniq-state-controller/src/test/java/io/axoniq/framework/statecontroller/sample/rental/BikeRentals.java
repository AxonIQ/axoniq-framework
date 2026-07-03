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

import io.axoniq.framework.statecontroller.History;
import io.axoniq.framework.statecontroller.Outcome;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;

import static io.axoniq.framework.statecontroller.Outcome.accept;
import static io.axoniq.framework.statecontroller.Outcome.reject;

/**
 * Bike rental decisions, hand-tuned for a tight DCB consistency boundary in the separated-payment lifecycle: a
 * {@link RentBike} command merely records the {@link BikeRequested intent}; a downstream payment service
 * subsequently issues either an approve or reject command, which is what produces a {@link RequestApproved} or
 * {@link RequestRejected} event. A {@link ReturnBike} command produces a {@link BikeReturned} event when the
 * renter matches.
 * <p>
 * <h3>{@link #rentBike(RentBike, History) rentBike}</h3>
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
 * <h3>{@link #returnBike(ReturnBike, History) returnBike}</h3>
 * Reads {@code latestOf(RequestApproved, BikeReturned)} on the per-bike scope. The bike is currently rented
 * <em>iff</em> the latest event from this pair is a {@link RequestApproved}; the approval's {@code userId}
 * identifies the current renter, which must match the command's {@code userId} for the return to succeed.
 * A pending {@link BikeRequested} that hasn't yet been approved is correctly treated as "not rented" — both
 * because it isn't in this query and because no payment has been confirmed.
 */
public class BikeRentals {

    @CommandHandler
    public Outcome rentBike(RentBike cmd, History history) {
        History bike = history.of("bike", cmd.bikeId());

        // available unless the latest of these three is a pending/active request
        if (bike.latestOf(BikeRequested.class, RequestRejected.class, BikeReturned.class).resolve()
                instanceof BikeRequested) {
            return reject("bike is not available for rental");
        }
        return accept(new BikeRequested(cmd.bikeId(), cmd.userId()));
    }

    @CommandHandler
    public Outcome returnBike(ReturnBike cmd, History history) {
        History bike = history.of("bike", cmd.bikeId());

        // rented iff the latest of this pair is an approval; its userId is the current renter
        if (!(bike.latestOf(RequestApproved.class, BikeReturned.class).resolve()
                instanceof RequestApproved approved)) {
            return reject("bike is not currently rented");
        }
        if (!approved.userId().equals(cmd.userId())) {
            return reject("bike is rented by another user");
        }
        return accept(new BikeReturned(cmd.bikeId(), cmd.userId()));
    }
}
