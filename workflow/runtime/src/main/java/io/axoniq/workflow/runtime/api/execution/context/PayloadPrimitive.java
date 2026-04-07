/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.api.execution.context;

import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import jakarta.annotation.Nonnull;

/**
 * Primitive modifying the workflow instance payload allowing durable data flow support.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface PayloadPrimitive {

    /**
     * Applies payload modification.
     *
     * @param stepName            name of the step.
     * @param payloadModification payload modification function.
     * @param eventNameCustomizer event name customizer.
     */
    void modifyPayload(
            @Nonnull String stepName,
            @Nonnull PayloadModification payloadModification,
            @Nonnull EventNameCustomizer eventNameCustomizer
    );
}
