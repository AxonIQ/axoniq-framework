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

import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;

import java.time.Duration;
import java.util.Map;

import static io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy.NONE;

/**
 * Primitive for executing actions within a workflow.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface ExecutePrimitive {

    /**
     * Execute primitive.
     *
     * @param command command object for the primitive.
     * @return durable result handle.
     */
    @Nonnull
    WorkflowStepResult execute(@Nonnull ExecuteCommand command);

    /**
     * Parameter object for the primitive.
     */
    interface ExecuteCommand {

        /**
         * Name of the step.
         *
         * @return name of the step.
         */
        @Nonnull
        String stepName();

        /**
         * Returns local payload passed to the action.
         *
         * @return payload.
         */
        @Nonnull
        Map<String, Object> local();

        /**
         * Returns action to be executed.
         *
         * @return action.
         */
        @Nonnull
        PayloadProcessor action();

        /**
         * Returns reducer to be used for the step parameters.
         *
         * @return step parameter reducer for payload.
         */
        @Nonnull
        PayloadReducer parameterPayloadReducer();

        /**
         * Returns reducer to be used for the step result.
         *
         * @return step result reducer for payload.
         */
        @Nonnull
        PayloadReducer resultPayloadReducer();

        /**
         * Returns timeout of the action.
         *
         * @return timeout.
         */
        @Nonnull
        Duration timeout();

        /**
         * Returns event name customizer.
         *
         * @return event name customizer.
         */
        @Nonnull
        EventNameCustomizer eventNameCustomizer();

        /**
         * Returns retry policy for the action.
         *
         * @return retry policy.
         */
        @Nonnull
        default RetryPolicy retryPolicy() {
            return NONE;
        }
    }

}
