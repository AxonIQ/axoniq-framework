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

import org.jspecify.annotations.Nullable;

/**
 * Specification for terminating a single step with cancellation.
 *
 * @param primitiveMetadata metadata containing name of the step to cancel
 * @param cause             optional cancellation cause
 * @author Simon Zambrovski
 * @since 0.2.0
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
