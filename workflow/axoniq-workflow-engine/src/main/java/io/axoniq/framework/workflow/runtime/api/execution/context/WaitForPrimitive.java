/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.PayloadReducer;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;

import java.time.Duration;

/**
 * Primitive that allows waiting for an event to occur.
 *
 * @author Allard Buijze
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface WaitForPrimitive {

    /**
     * Wait for event.
     *
     * @param command command object
     * @return result of command execution
     */
    WorkflowStepResult waitForEvent(WaitForCommand command);

    /**
     * Parameter object for the primitive.
     *
     * @author Simon Zambrovski
     * @since 5.4.0
     */
    interface WaitForCommand {

        /**
         * Name of the step.
         *
         * @return step name
         */
        String stepName();

        /**
         * Event condition to wait for.
         *
         * @return event condition
         */
        EventCondition eventCondition();

        /**
         * Maximum wait timeout
         *
         * @return timeout duration
         */
        Duration timeout();

        /**
         * Returns the result payload reducer.
         *
         * @return result reducer
         */
        PayloadReducer resultPayloadReducer();

        /**
         * Event name customizer
         *
         * @return event name customizer
         */
        EventNameCustomizer eventNameCustomizer();
    }
}
