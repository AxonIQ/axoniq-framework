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
import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import io.axoniq.framework.statecontroller.eventstream.EventStream;
import io.axoniq.framework.statecontroller.history.History;

import static io.axoniq.framework.statecontroller.decisions.Decision.accept;
import static io.axoniq.framework.statecontroller.decisions.Decision.reject;

/**
 * The lazy sibling of {@link BikeRentals}: the same bike-rental decisions, expressed through the injected
 * {@link DecisionContext} (the lazy {@link EventStream} surface) rather than the eager {@link History}. Both
 * carry the {@code @Decide} annotation and produce identical events and rejection messages — this class exists to
 * prove the lazy path is behaviourally equivalent to the eager one.
 * <p>
 * Each decision narrows the context to the per-bike scope with {@link DecisionContext#scope(String, Object)
 * scope("bike", id)}, then reads the most recent relevant event with
 * {@link EventStream#resolveLatestOf(Class[]) resolveLatestOf(...)} and branches with a plain {@code instanceof}
 * pattern — the lazy mirror of the eager {@code history.of("bike", id).latestOf(...)} expression. The
 * {@code resolveXxx(...)} shortcut forces the scope's read inline, so the decision body reads like the eager one
 * while staying on the lazy {@link DecisionContext} surface.
 * <p>
 * {@link #rentBike(RentBike, DecisionContext) rentBike} deliberately reads only
 * {@code resolveLatestOf(BikeRequested, RequestRejected, BikeReturned)} — {@link RequestApproved} is excluded to
 * keep the consistency boundary narrow — and accepts iff that latest event is <em>not</em> a {@link BikeRequested}
 * (the pending/active state). {@link #returnBike(ReturnBike, DecisionContext) returnBike} reads
 * {@code resolveLatestOf(RequestApproved, BikeReturned)}: the bike is rented iff the latest of the pair is an
 * approval, whose {@code userId} identifies the current renter.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
public class BikeRentalsViaContext {

    @Decide
    public Decision rentBike(RentBike cmd, DecisionContext ctx) {
        EventStream bike = ctx.scope("bike", cmd.bikeId());

        // available unless the latest of these three is a pending/active request
        if (bike.resolveLatestOf(BikeRequested.class, RequestRejected.class, BikeReturned.class)
                instanceof BikeRequested) {
            return reject("bike is not available for rental");
        }
        return accept(new BikeRequested(cmd.bikeId(), cmd.userId()));
    }

    @Decide
    public Decision returnBike(ReturnBike cmd, DecisionContext ctx) {
        EventStream bike = ctx.scope("bike", cmd.bikeId());

        // rented iff the latest of this pair is an approval; its userId is the current renter
        if (!(bike.resolveLatestOf(RequestApproved.class, BikeReturned.class) instanceof RequestApproved approved)) {
            return reject("bike is not currently rented");
        }
        if (!approved.userId().equals(cmd.userId())) {
            return reject("bike is rented by another user");
        }
        return accept(new BikeReturned(cmd.bikeId(), cmd.userId()));
    }
}
