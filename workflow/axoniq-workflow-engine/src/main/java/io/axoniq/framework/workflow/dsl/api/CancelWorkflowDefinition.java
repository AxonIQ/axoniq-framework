/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.jspecify.annotations.Nullable;

/**
 * Specification for terminating a workflow with cancellation.
 *
 * @param primitiveMetadata metadata of the primitive
 * @param cause             optional cancellation cause
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record CancelWorkflowDefinition(
        PrimitiveMetadata primitiveMetadata,
        @Nullable Throwable cause
) {

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     *
     * @param primitiveMetadata metadata of the primitive
     * @return copied workflow definition with updated metadata
     */
    public CancelWorkflowDefinition primitiveMetadata(PrimitiveMetadata primitiveMetadata) {
        return new CancelWorkflowDefinition(primitiveMetadata, cause);
    }

    /**
     * Returns a copy of this definition with the provided event name customizer.
     *
     * @param eventNameCustomizer customizer for published event names
     * @return copied workflow definition with updated event naming
     */
    public CancelWorkflowDefinition eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
        return primitiveMetadata(primitiveMetadata.eventNameCustomizer(eventNameCustomizer));
    }

    /**
     * Returns a copy of this definition with the provided cancellation cause.
     *
     * @param cause optional cancellation cause
     * @return copied workflow definition with updated cancellation cause
     */
    public CancelWorkflowDefinition cause(@Nullable Throwable cause) {
        return new CancelWorkflowDefinition(primitiveMetadata, cause);
    }
}
