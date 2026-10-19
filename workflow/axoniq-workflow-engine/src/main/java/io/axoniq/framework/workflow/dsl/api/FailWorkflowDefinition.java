/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

/**
 * Specification for terminating a workflow with failure.
 *
 * @param primitiveMetadata metadata of the primitive
 * @param cause             failure cause
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record FailWorkflowDefinition(
        PrimitiveMetadata primitiveMetadata,
        Throwable cause
) {

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     *
     * @param primitiveMetadata metadata of the primitive
     * @return copied workflow definition with updated metadata
     */
    public FailWorkflowDefinition primitiveMetadata(PrimitiveMetadata primitiveMetadata) {
        return new FailWorkflowDefinition(primitiveMetadata, cause);
    }

    /**
     * Returns a copy of this definition with the provided event name customizer.
     *
     * @param eventNameCustomizer customizer for published event names
     * @return copied workflow definition with updated event naming
     */
    public FailWorkflowDefinition eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
        return primitiveMetadata(primitiveMetadata.eventNameCustomizer(eventNameCustomizer));
    }

    /**
     * Returns a copy of this definition with the provided failure cause.
     *
     * @param cause failure cause
     * @return copied workflow definition with updated failure cause
     */
    public FailWorkflowDefinition cause(Throwable cause) {
        return new FailWorkflowDefinition(primitiveMetadata, cause);
    }
}
