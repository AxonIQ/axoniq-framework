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
package io.axoniq.workflow.runtime.api.execution.context;


/**
 * Reusable metadata shared by primitive specs.
 *
 * @param stepName            step name.
 * @param eventNameCustomizer event name customizer.
 * @author Simon Zambrovski
 * @since 1.0.0
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
