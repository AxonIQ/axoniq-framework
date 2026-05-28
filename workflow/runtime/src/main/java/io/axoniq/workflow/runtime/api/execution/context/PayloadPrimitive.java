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

import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
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
     * @param command command object for the primitive.
     * @return durable result handle.
     */
    @Nonnull
    WorkflowStepResult modifyPayload(@Nonnull ModifyPayloadCommand command);

    /**
     * Base payload modification command.
     * @since 1.0.0
     * @author Simon Zambrovski
     */
    interface ModifyPayloadCommand {

        /**
         * Retrieves the step name.
         * @return step name
         */
        @Nonnull
        String stepName();

        /**
         * Retrieves payload modification function.
         * @return modification function
         */
        @Nonnull
        PayloadModification payloadModification();

        /**
         * Retrieves event name customizer for the step.
         * @return event name customizer
         */
        @Nonnull
        EventNameCustomizer eventNameCustomizer();
    }
}
