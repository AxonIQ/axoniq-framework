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

package org.axonframework.messaging.eventhandling;

import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.Message;
import org.axonframework.conversion.Converter;
import org.jspecify.annotations.Nullable;


import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;

/**
 * A {@link Message} wrapping an event, which is represented by its {@link #payload() payload}.
 * <p>
 * An event is a representation of an occurrence of an event (i.e. anything that happened any might be of importance to
 * any other component) in the application. It contains the data relevant for components that need to act based on that
 * event.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public interface EventMessage extends Message {

    /**
     * Returns the identifier of this {@link EventMessage event}.
     * <p>
     * The identifier is used to define the uniqueness of an event. Two events may contain similar (or equal)
     * {@link #payload() payloads} and {@link #timestamp() timestamp}, if the event identifiers are different, they both
     * represent a different occurrence of an Event.
     * <p>
     * If two messages have the same identifier, they both represent the same unique occurrence of an event, even though
     * the resulting view may be different. You may not assume two messages are equal (i.e. interchangeable) if their
     * identifier is equal.
     * <p>
     * For example, an {@code AddressChangeEvent} may occur twice for the same event, because someone moved back to the
     * previous address. In that case, the event payload is equal for both {@code EventMessage} instances, but the event
     * identifier is different for both.
     *
     * @return The identifier of this {@link EventMessage event}.
     */
    @Override
    String identifier();

    /**
     * Returns the timestamp of this {@link EventMessage event}.
     * <p>
     * The timestamp is set to the date and time the event was reported.
     *
     * @return The timestamp of this {@link EventMessage event}.
     */
    Instant timestamp();

    @Override
    EventMessage withMetadata(Map<String, @Nullable String> metadata);

    @Override
    EventMessage andMetadata(Map<String,@Nullable String> metadata);

    @Override
        default EventMessage withConvertedPayload(Class<?> type, Converter converter) {
        return withConvertedPayload((Type) type, converter);
    }

    @Override
        default EventMessage withConvertedPayload(TypeReference<?> type, Converter converter) {
        return withConvertedPayload(type.getType(), converter);
    }

    @Override
    EventMessage withConvertedPayload(Type type, Converter converter);
}
