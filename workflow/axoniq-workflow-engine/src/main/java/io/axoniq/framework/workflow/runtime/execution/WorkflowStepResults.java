/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStepResult;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Objects;

/**
 * Utility containing {@link WorkflowStepResult} factory methods.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
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
    public static WorkflowStepResult completed(String stepName, @Nullable Object payload,
                                               EventConverter converter) {
        return new CompletedWorkflowStepResult(stepName, payload, null, null, false, converter);
    }

    /**
     * Constructs a completed result that carries no payload — for primitives whose recorded value is read from
     * {@link WorkflowState} rather than from the result handle (e.g. the migration primitive).
     *
     * @return completed step result.
     */
    public static WorkflowStepResult completed(String stepName) {
        return new CompletedWorkflowStepResult(stepName, null, null, null, false, null);
    }

    /**
     * Constructs failed result.
     *
     * @param error failure causing error.
     * @return failed result.
     */
    public static WorkflowStepResult failed(String stepName, Throwable error) {
        return new CompletedWorkflowStepResult(stepName,
                                               null,
                                               Objects.requireNonNull(error, "Error must be provided"),
                                               null,
                                               false,
                                               null);
    }

    /**
     * Constructs cancelled result.
     *
     * @return cancelled result.
     */
    public static WorkflowStepResult canceled(String stepName) {
        return new CompletedWorkflowStepResult(stepName, null, null, null, true, null);
    }

    /**
     * Constructs timed out result.
     *
     * @param timeout timeout duration.
     * @return timed out result.
     */
    public static WorkflowStepResult timeout(String stepName, Duration timeout) {
        return new CompletedWorkflowStepResult(stepName,
                                               null,
                                               null,
                                               Objects.requireNonNull(timeout, "Timeout must be provided"),
                                               false,
                                               null);
    }
}
