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

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;

/**
 * Renders an {@link EventMessage} as a single-line description for diagnostic exception messages.
 * Shared by the transformer chain and its transformations so failure diagnostics identify an event
 * identically wherever they are raised.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class EventDescriptions {

    private EventDescriptions() {
    }

    /**
     * Describes the given event for a diagnostic message as {@code type=..., identifier=...}, plus
     * {@code , position=...} when the entry context carries a {@link TrackingToken}. The position is
     * omitted on entity-load / DCB-source paths where the storage engine attaches no token.
     *
     * @param message      the event to describe
     * @param entryContext the read-stream entry's {@link Context}, possibly carrying a tracking token
     * @return a single-line description of the event's identity and stream position
     */
    static String describe(EventMessage message, Context entryContext) {
        StringBuilder description = new StringBuilder()
                .append("type=").append(message.type())
                .append(", identifier=").append(message.identifier());
        TrackingToken.fromContext(entryContext)
                     .ifPresent(token -> description.append(", position=").append(token));
        return description.toString();
    }
}
