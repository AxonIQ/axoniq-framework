/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import com.fasterxml.jackson.core.type.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * Retrieves the value based on a payload property.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class PayloadPropertyValueRetriever implements ValueRetriever {

    /**
     * Map type reference for easy access.
     */
    public static final TypeReference<Map<String, @Nullable Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };
    /**
     * Qualifier for the association schema.
     */
    public static final String QUALIFIER = "payload";

    private final String payloadPropertyName;

    /**
     * Constructs a new instance reading a specified payload property.
     *
     * @param payloadPropertyName property to read from the message payload.
     */
    @Internal
    public PayloadPropertyValueRetriever(String payloadPropertyName) {
        this.payloadPropertyName = Objects.requireNonNull(payloadPropertyName,
                                                          "Payload property name must not be null");
    }

    /**
     * Creates a value retriever based on the given payload property.
     *
     * @param payloadPropertyName property to read from the message payload.
     * @return value retriever.
     */
    public static ValueRetriever payloadProperty(String payloadPropertyName) {
        return new PayloadPropertyValueRetriever(payloadPropertyName);
    }

    @Override
    public Object apply(EventMessage eventMessage, ProcessingContext processingContext) {
        Map<String, @Nullable Object> payloadMap = eventMessage.payloadAs(Map.class);
        return Objects.requireNonNull(payloadMap).get(payloadPropertyName);
    }

    @Override
    public String qualifier() {
        return QUALIFIER;
    }

    @Override
    public String path() {
        return payloadPropertyName;
    }
}
