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

import org.axonframework.messaging.core.MessageType;

import static java.util.Objects.requireNonNull;

/**
 * One event a split produces in place of the matched event, given by its {@code type} and {@code payload}.
 *
 * @param type    the event's identity
 * @param payload the event's payload, whose runtime type drives downstream conversion
 * @author Laura Devriendt
 * @since 5.2.1
 */
public record TransformedEvent(MessageType type, Object payload) {

    public TransformedEvent {
        requireNonNull(type, "type may not be null");
        requireNonNull(payload, "payload may not be null");
    }

    /**
     * Creates a produced event with the given identity and payload.
     *
     * @param type    the event's identity
     * @param payload the event's payload
     * @return a {@link TransformedEvent} carrying {@code type} and {@code payload}
     */
    public static TransformedEvent of(MessageType type, Object payload) {
        return new TransformedEvent(type, payload);
    }
}
