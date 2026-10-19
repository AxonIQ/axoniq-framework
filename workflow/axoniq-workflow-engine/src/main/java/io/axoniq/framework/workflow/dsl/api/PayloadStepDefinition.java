/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

/**
 * Specification for modifying of payload within a workflow.
 *
 * @param primitiveMetadata metadata of the primitive
 * @param modification      payload modification
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record PayloadStepDefinition(
        PrimitiveMetadata primitiveMetadata,
        PayloadModification modification
) {

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     *
     * @param primitiveMetadata metadata of the primitive
     * @return copied step definition with updated metadata
     */
    public PayloadStepDefinition primitiveMetadata(PrimitiveMetadata primitiveMetadata) {
        return new PayloadStepDefinition(primitiveMetadata, modification);
    }

    /**
     * Returns a copy of this definition with the provided step name.
     *
     * @param stepName logical step name
     * @return copied step definition with updated step name
     */
    public PayloadStepDefinition stepName(String stepName) {
        return primitiveMetadata(primitiveMetadata.stepName(stepName));
    }

    /**
     * Returns a copy of this definition with the provided event name customizer.
     *
     * @param eventNameCustomizer customizer for published event names
     * @return copied step definition with updated event naming
     */
    public PayloadStepDefinition eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
        return primitiveMetadata(primitiveMetadata.eventNameCustomizer(eventNameCustomizer));
    }

    /**
     * Returns a copy of this definition with the provided payload modification.
     *
     * @param modification payload modification
     * @return copied step definition with updated payload modification
     */
    public PayloadStepDefinition modification(PayloadModification modification) {
        return new PayloadStepDefinition(primitiveMetadata, modification);
    }
}
