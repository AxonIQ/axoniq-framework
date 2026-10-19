/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.PayloadModification;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;

/**
 * Primitive modifying the workflow instance payload allowing durable data flow support.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface PayloadPrimitive {

    /**
     * Applies payload modification.
     *
     * @param command command object for the primitive.
     * @return durable result handle.
     */
    WorkflowStepResult modifyPayload(ModifyPayloadCommand command);

    /**
     * Base payload modification command.
     *
     * @author Simon Zambrovski
     * @since 5.4.0
     */
    interface ModifyPayloadCommand {

        /**
         * Retrieves the step name.
         *
         * @return step name
         */
        String stepName();

        /**
         * Retrieves payload modification function.
         *
         * @return modification function
         */
        PayloadModification payloadModification();

        /**
         * Retrieves event name customizer for the step.
         *
         * @return event name customizer
         */
        EventNameCustomizer eventNameCustomizer();
    }
}
