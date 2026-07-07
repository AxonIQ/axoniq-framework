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
package io.axoniq.workflow.runtime.association;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;

/**
 * Retrieves the value based on a metadata property.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class MetadataPropertyValueRetriever implements ValueRetriever {

    public static final String QUALIFIER = "metadata";

    private final String metadataPropertyName;

    /**
     * Creates a value retriever based on the given metadata property.
     *
     * @param metadataPropertyName property to read from the message metadata.
     * @return value retriever.
     */
    public static ValueRetriever metadataProperty(@Nonnull String metadataPropertyName) {
        return new MetadataPropertyValueRetriever(metadataPropertyName);
    }

    /**
     * Constructs a new instance reading a specified metadata property.
     *
     * @param metadataPropertyName property to read from the message metadata.
     */
    @Internal
    public MetadataPropertyValueRetriever(@Nonnull String metadataPropertyName) {
        this.metadataPropertyName = Objects.requireNonNull(metadataPropertyName,
                                                           "Metadata property name must not be null");
    }

    @Override
    public Object apply(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext) {
        return eventMessage.metadata().get(metadataPropertyName);
    }

    @Override
    public @Nonnull String qualifier() {
        return QUALIFIER;
    }

    @Override
    public @Nonnull String path() {
        return metadataPropertyName;
    }
}
