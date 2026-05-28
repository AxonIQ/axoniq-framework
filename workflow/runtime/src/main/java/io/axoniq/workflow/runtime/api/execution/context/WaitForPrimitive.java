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
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;

import java.time.Duration;

/**
 * Primitive that allows waiting for an event to occur.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface WaitForPrimitive {

    /**
     * Wait for event.
     *
     * @param command command object
     * @return result of command execution
     */
    @Nonnull
    WorkflowStepResult waitForEvent(@Nonnull WaitForCommand command);

    /**
     * Parameter object for the primitive.
     *
     * @author Simon Zambrovski
     * @since 1.0.0
     */
    interface WaitForCommand {

        /**
         * Name of the step.
         *
         * @return step name
         */
        @Nonnull
        String stepName();

        /**
         * Event condition to wait for.
         *
         * @return event condition
         */
        @Nonnull
        EventCondition eventCondition();

        /**
         * Maximum wait timeout
         *
         * @return timeout duration
         */
        @Nonnull
        Duration timeout();

        /**
         * Returns the result payload reducer.
         *
         * @return result reducer
         */
        @Nonnull
        PayloadReducer resultPayloadReducer();

        /**
         * Event name customizer
         *
         * @return event name customizer
         */
        @Nonnull
        EventNameCustomizer eventNameCustomizer();
    }
}
