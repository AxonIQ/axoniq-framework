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

package org.axonframework.messaging.eventhandling.conversion;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.conversion.MessageConverter;

import java.lang.reflect.Type;

/**
 * A converter specific for {@link EventMessage EventMessages}, acting on the {@link EventMessage#payload() payload}.
 * <p>
 * This interface serves the purpose of enforcing use of the right type of converter. Implementation of this interface
 * typically delegate operations to a {@link MessageConverter} instance, unless the serialized format of events and other messages differ.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public interface EventConverter extends Converter {

    /**
     * Converts the given {@code event's} {@link EventMessage#payload() payload} into a payload of the given
     * {@code targetType}.
     *
     * @param event      The {@code EventMessage} to convert the {@link EventMessage#payload() payload} for.
     * @param targetType The type to convert the {@link EventMessage#payload() payload} into.
     * @param <E>        The type of {@code EventMessage} to convert the payload for.
     * @param <T>        The target data type.
     * @return A converted version of the given {@code EventMessage's} {@link EventMessage#payload() payload} into the
     * given {@code targetType}.
     */
    @Nullable
    default <E extends EventMessage, T> T convertPayload(E event, Class<T> targetType) {
        return convertPayload(event, (Type) targetType);
    }

    /**
     * Converts the given {@code event's} {@link EventMessage#payload() payload} into a payload of the given
     * {@code targetType}.
     *
     * @param event      The {@code EventMessage} to convert the {@link EventMessage#payload() payload} for.
     * @param targetType The type to convert the {@link EventMessage#payload() payload} into.
     * @param <E>        The type of {@code EventMessage} to convert the payload for.
     * @param <T>        The target data type.
     * @return A converted version of the given {@code EventMessage's} {@link EventMessage#payload() payload} into the
     * given {@code targetType}.
     */
    @Nullable
    <E extends EventMessage, T> T convertPayload(E event, Type targetType);

    /**
     * Converts the given {@code event's} {@link EventMessage#payload() payload} to the given {@code targetType},
     * returning a new {@code EventMessage} with the converted payload.
     *
     * @param event      The {@code EventMessage} to convert the {@link EventMessage#payload() payload} for.
     * @param targetType The type to convert the {@link EventMessage#payload() payload} into.
     * @param <E>        The type of {@code EventMessage} to convert and return.
     * @param <T>        The target data type.
     * @return A new {@code EventMessage} containing the converted version of the given {@code event's}
     * {@link EventMessage#payload() payload} into the given {@code targetType}.
     */
    default <E extends EventMessage, T> E convertEvent(E event, Class<T> targetType) {
        return convertEvent(event, (Type) targetType);
    }

    /**
     * Converts the given {@code event's} {@link EventMessage#payload() payload} to the given {@code targetType},
     * returning a new {@code EventMessage} with the converted payload.
     *
     * @param event      The {@code EventMessage} to convert the {@link EventMessage#payload() payload} for.
     * @param targetType The type to convert the {@link EventMessage#payload() payload} into.
     * @param <E>        The type of {@code EventMessage} to convert and return.
     * @return A new {@code EventMessage} containing the converted version of the given {@code event's}
     * {@link EventMessage#payload() payload} into the given {@code targetType}.
     */
    <E extends EventMessage> E convertEvent(E event, Type targetType);
}
