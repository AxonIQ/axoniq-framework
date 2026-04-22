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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;

import java.time.Duration;
import java.util.Objects;

/**
 * Utility containing {@link WorkflowStepResult} factory methods.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class WorkflowStepResults {

    private WorkflowStepResults() {
        // util class
    }
    /**
     * Constructs completed result.
     *
     * @param payload payload of the result, might be null.
     * @return completed step result.
     */
    @Nonnull
    public static WorkflowStepResult completed(@Nonnull String stepName, @Nullable Object payload) {
        return new CompletedWorkflowStepResult(stepName, payload, null, null, false);
    }

    /**
     * Constructs failed result.
     *
     * @param error failure causing error.
     * @return failed result.
     */
    @Nonnull
    public static WorkflowStepResult failed(@Nonnull String stepName, @Nonnull Throwable error) {
        return new CompletedWorkflowStepResult(stepName,
                                               null,
                                               Objects.requireNonNull(error, "Error must be provided"),
                                               null,
                                               false);
    }

    /**
     * Constructs cancelled result.
     *
     * @return cancelled result.
     */
    @Nonnull
    public static WorkflowStepResult canceled(@Nonnull String stepName) {
        return new CompletedWorkflowStepResult(stepName, null, null, null, true);
    }

    /**
     * Constructs timed out result.
     *
     * @param timeout timeout duration.
     * @return timed out result.
     */
    @Nonnull
    public static WorkflowStepResult timeout(@Nonnull String stepName, @Nonnull Duration timeout) {
        return new CompletedWorkflowStepResult(stepName,
                                               null,
                                               null,
                                               Objects.requireNonNull(timeout, "Timeout must be provided"),
                                               false);
    }
}
