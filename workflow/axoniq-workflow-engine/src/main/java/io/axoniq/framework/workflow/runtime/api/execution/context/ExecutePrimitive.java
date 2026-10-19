/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.PayloadProcessor;
import io.axoniq.framework.workflow.dsl.api.PayloadReducer;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy.NONE;

/**
 * Primitive for executing actions within a workflow.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface ExecutePrimitive {

    /**
     * Execute primitive.
     *
     * @param command command object for the primitive.
     * @return durable result handle.
     */
    WorkflowStepResult execute(ExecuteCommand command);

    /**
     * Parameter object for the primitive.
     */
    interface ExecuteCommand {

        /**
         * Name of the step.
         *
         * @return name of the step.
         */
        String stepName();

        /**
         * Returns local payload passed to the action.
         *
         * @return payload.
         */
        Map<String, @Nullable Object> local();

        /**
         * Returns action to be executed.
         *
         * @return action.
         */
        PayloadProcessor action();

        /**
         * Returns reducer to be used for the step parameters.
         *
         * @return step parameter reducer for payload.
         */
        PayloadReducer parameterPayloadReducer();

        /**
         * Returns reducer to be used for the step result.
         *
         * @return step result reducer for payload.
         */
        PayloadReducer resultPayloadReducer();

        /**
         * Returns timeout of the action.
         *
         * @return timeout.
         */
        Duration timeout();

        /**
         * Returns event name customizer.
         *
         * @return event name customizer.
         */
        EventNameCustomizer eventNameCustomizer();

        /**
         * Returns retry policy for the action.
         *
         * @return retry policy.
         */
        default RetryPolicy retryPolicy() {
            return NONE;
        }
    }
}
