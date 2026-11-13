/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.jspecify.annotations.Nullable;

/**
 * Specification for terminating a single step with cancellation.
 *
 * @param primitiveMetadata metadata containing name of the step to cancel
 * @param cause             optional cancellation cause
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record CancelStepDefinition(
        PrimitiveMetadata primitiveMetadata,
        @Nullable Throwable cause
) {

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     *
     * @param primitiveMetadata metadata containing the step to cancel
     * @return copied step definition with updated metadata
     */
    public CancelStepDefinition primitiveMetadata(PrimitiveMetadata primitiveMetadata) {
        return new CancelStepDefinition(primitiveMetadata, cause);
    }

    /**
     * Returns a copy of this definition with the provided event name customizer.
     *
     * @param eventNameCustomizer customizer for published event names
     * @return copied step definition with updated event naming
     */
    public CancelStepDefinition eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
        return primitiveMetadata(primitiveMetadata.eventNameCustomizer(eventNameCustomizer));
    }

    /**
     * Returns a copy of this definition with the provided cancellation cause.
     *
     * @param cause optional cancellation cause
     * @return copied step definition with updated cancellation cause
     */
    public CancelStepDefinition cause(@Nullable Throwable cause) {
        return new CancelStepDefinition(primitiveMetadata, cause);
    }
}
