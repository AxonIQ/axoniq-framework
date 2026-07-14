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

import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;

import io.axoniq.framework.messaging.transformation.TransformationContext;

import java.lang.reflect.Type;

import static java.util.Objects.requireNonNull;

/**
 * The input type a transformation declares for its payload mapper, resolving a stored payload to that type.
 *
 * @param <T>      the declared input payload type
 * @param type     the declared type, preserving any generic parameters
 * @param rawClass the raw class of {@code type}
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
record DeclaredInputType<T>(Type type, Class<T> rawClass) {

    /**
     * Creates a declared input type from the given {@link TypeReference}.
     *
     * @param <T>       the declared input payload type
     * @param inputType the reference to the declared type
     * @return a {@link DeclaredInputType} for {@code inputType}
     */
    static <T> DeclaredInputType<T> of(TypeReference<T> inputType) {
        requireNonNull(inputType, "inputType may not be null");
        return new DeclaredInputType<>(inputType.getType(), inputType.getTypeAsClass());
    }

    /**
     * Returns the message's payload typed as {@code T}, converting it via the {@link MessageConverter} when the stored
     * payload is not already an instance of {@link #rawClass}.
     *
     * @param message the message whose payload to resolve
     * @param context the per-message {@link TransformationContext}, supplying the converter
     * @return the payload typed as {@code T}
     * @throws IllegalStateException if the converter resolves the stored payload to {@code null}
     */
    T resolvePayload(EventMessage message, TransformationContext context) {
        Object payload = message.payload();
        if (rawClass.isInstance(payload)) {
            return rawClass.cast(payload);
        }
        T converted = context.converter().convertPayload(message, type);
        if (converted == null) {
            throw new IllegalStateException("""
                    MessageConverter resolved the stored payload to null for declared input type %s. \
                    Input event: %s. \
                    The stored payload is missing or malformed.""".formatted(
                    type.getTypeName(),
                    EventDescriptions.describe(message, context.entryContext())));
        }
        return converted;
    }
}
