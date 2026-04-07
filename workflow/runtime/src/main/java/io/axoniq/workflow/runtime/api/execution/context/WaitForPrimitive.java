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

import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;

import java.time.Duration;
import java.util.Objects;

public interface WaitForPrimitive {

    /**
     * Wait for event.
     *
     * @param stepName             name of the workflow step.
     * @param eventCondition       event condition of event to wait for.
     * @param timeout              maximum wait duration until timeout.
     * @param resultPayloadReducer payload reducer controlling how the result payload (message payload) is combined with
     *                             the workflow payload.
     * @param eventNameCustomizer  event name customizer.
     * @return result.
     */
    @Nonnull
    WorkflowStepResult waitFor(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull PayloadReducer resultPayloadReducer,
            @Nonnull Duration timeout,
            @Nonnull EventNameCustomizer eventNameCustomizer
    );

    default <T> T waitFor(@Nonnull WaitForCommand<T> command) {
        Objects.requireNonNull(command, "command must not be null");
        return command.result(
                waitFor(
                        command.stepName(),
                        command.eventCondition(),
                        command.resultPayloadReducer(),
                        command.timeout(),
                        command.eventNameCustomizer()
                )
        );
    }

    /**
     * Parameter object for the primitive.
     *
     * @param <T> type of command result.
     */
    interface WaitForCommand<T> {

        @Nonnull
        String stepName();

        @Nonnull
        EventCondition eventCondition();

        @Nonnull
        Duration timeout();

        /**
         * Returns the result reducer.
         *
         * @return result reducer.
         */
        @Nonnull
        PayloadReducer resultPayloadReducer();

        @Nonnull
        EventNameCustomizer eventNameCustomizer();

        T result(@Nonnull WorkflowStepResult result);
    }
}
