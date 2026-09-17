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
package io.axoniq.framework.workflow.dsl.api;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.PrimitiveMetadata;

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
