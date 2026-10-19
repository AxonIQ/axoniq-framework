/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;

/**
 * Retrieves the value based on a metadata property.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class MetadataPropertyValueRetriever implements ValueRetriever {

    /**
     * Qualifier for the association schema.
     */
    public static final String QUALIFIER = "metadata";

    private final String metadataPropertyName;

    /**
     * Constructs a new instance reading a specified metadata property.
     *
     * @param metadataPropertyName property to read from the message metadata.
     */
    @Internal
    public MetadataPropertyValueRetriever(String metadataPropertyName) {
        this.metadataPropertyName = Objects.requireNonNull(metadataPropertyName,
                                                           "Metadata property name must not be null");
    }

    /**
     * Creates a value retriever based on the given metadata property.
     *
     * @param metadataPropertyName property to read from the message metadata.
     * @return value retriever.
     */
    public static ValueRetriever metadataProperty(String metadataPropertyName) {
        return new MetadataPropertyValueRetriever(metadataPropertyName);
    }

    @Override
    public Object apply(EventMessage eventMessage, ProcessingContext processingContext) {
        return eventMessage.metadata().get(metadataPropertyName);
    }

    @Override
    public String qualifier() {
        return QUALIFIER;
    }

    @Override
    public String path() {
        return metadataPropertyName;
    }
}
