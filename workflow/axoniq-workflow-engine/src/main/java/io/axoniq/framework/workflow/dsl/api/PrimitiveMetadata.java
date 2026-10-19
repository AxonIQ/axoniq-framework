/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;


/**
 * Reusable metadata shared by primitive specs.
 *
 * @param stepName            step name.
 * @param eventNameCustomizer event name customizer.
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record PrimitiveMetadata(
        String stepName,
        EventNameCustomizer eventNameCustomizer
) {

    /**
     * Returns a copy of this metadata with the provided step name.
     *
     * @param stepName logical step name
     * @return copied metadata with updated step name
     */
    public PrimitiveMetadata stepName(String stepName) {
        return new PrimitiveMetadata(stepName, eventNameCustomizer);
    }

    /**
     * Returns a copy of this metadata with the provided event name customizer.
     *
     * @param eventNameCustomizer customizer for published event names
     * @return copied metadata with updated event naming
     */
    public PrimitiveMetadata eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
        return new PrimitiveMetadata(stepName, eventNameCustomizer);
    }
}
