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

package io.axoniq.framework.springcloud.query;

import org.axonframework.common.AxonException;

/**
 * Raised when the members able to answer a subscription query changed while it was active.
 * <p>
 * A subscription query is answered by every member advertising its name, because an update is emitted on whichever
 * member's state changed and only reaches subscriptions that member holds a registration for. When a member starts
 * advertising the name after a subscription began, that subscription cannot be repaired by subscribing to the new
 * member: the updates it emitted between advertising the name and being subscribed to are already gone, and no
 * amount of catching up recovers them.
 * <p>
 * Failing is therefore the honest outcome. A subscriber that acts on this by establishing the subscription query
 * again gets a fresh initial result and a complete stream of updates from that point, where one that carried on
 * would silently be missing whatever it missed.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class SubscriptionQueryMembersChangedException extends AxonException {

    private static final long serialVersionUID = 6153055395184270281L;

    /**
     * Constructs a {@code SubscriptionQueryMembersChangedException} with the given {@code message}.
     *
     * @param message the description of how the members able to answer the query changed
     */
    public SubscriptionQueryMembersChangedException(String message) {
        super(message);
    }
}
