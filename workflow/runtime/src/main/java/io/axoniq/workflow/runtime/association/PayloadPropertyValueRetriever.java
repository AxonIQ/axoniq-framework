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
package io.axoniq.workflow.runtime.association;

import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Map;
import java.util.Objects;

/**
 * Retrieves the value based on a payload property.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class PayloadPropertyValueRetriever implements ValueRetriever {

    /**
     * Map type reference for easy access.
     */
    public static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final String payloadPropertyName;

    /**
     * Creates a value retriever based on the given payload property.
     *
     * @param payloadPropertyName property to read from the message payload.
     * @return value retriever.
     */
    public static ValueRetriever payloadProperty(@Nonnull String payloadPropertyName) {
        return new PayloadPropertyValueRetriever(payloadPropertyName);
    }

    /**
     * Constructs a new instance reading a specified payload property.
     *
     * @param payloadPropertyName property to read from the message payload.
     */
    @Internal
    public PayloadPropertyValueRetriever(@Nonnull String payloadPropertyName) {
        this.payloadPropertyName = Objects.requireNonNull(payloadPropertyName,
                                                          "Payload property name must not be null");
    }

    @Override
    public Object apply(@Nonnull EventMessage eventMessage, @Nonnull Converter converter) {
        Map<String, Object> payloadMap = eventMessage.payloadAs(PAYLOAD_TYPE.getType(), converter);
        return Objects.requireNonNull(payloadMap).get(payloadPropertyName);
    }
}
