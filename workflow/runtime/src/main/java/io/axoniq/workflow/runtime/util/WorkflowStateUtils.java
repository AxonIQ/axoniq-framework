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
package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

import java.util.function.Predicate;

/**
 * Utility functions for inspecting {@link WorkflowState}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public class WorkflowStateUtils {

    private WorkflowStateUtils() {
        // avoid instantiation
    }

    /**
     * Safely inspects a step in state if it exists.
     *
     * @param state     workflow state to inspect
     * @param stepName  name of the step
     * @param predicate condition to test on the step
     * @return {@code true} if the step exists and matches the predicate, {@code false} otherwise
     */
    public static boolean matchesStep(@Nonnull WorkflowState state,
                                      @Nonnull String stepName,
                                      @Nonnull Predicate<WorkflowStep> predicate) {
        var step = state.getStep(stepName);
        return step != null && predicate.test(step);
    }

    /**
     * Checks if a step with the given name exists in the workflow state and has reached a terminal status.
     *
     * @param state    workflow state to inspect
     * @param stepName name of the step
     * @return {@code true} if the step exists and is terminal, {@code false} otherwise
     */
    public static boolean isStepTerminal(@Nonnull WorkflowState state, @Nonnull String stepName) {
        return matchesStep(state, stepName, s -> s.status().isTerminal());
    }

    /**
     * Checks if a step with the given name exists in the workflow state and has not reached a terminal status.
     *
     * @param state    workflow state to inspect
     * @param stepName name of the step
     * @return {@code true} if the step exists and is not terminal, {@code false} otherwise
     */
    public static boolean isStepActive(@Nonnull WorkflowState state, @Nonnull String stepName) {
        return matchesStep(state, stepName, s -> !s.status().isTerminal());
    }

    /**
     * Checks if a step with the given name exists in the workflow state and has the specified status.
     *
     * @param state    workflow state to inspect
     * @param stepName name of the step
     * @param status   expected step status
     * @return {@code true} if the step exists and matches the given status, {@code false} otherwise
     */
    public static boolean isStepStatus(@Nonnull WorkflowState state,
                                       @Nonnull String stepName,
                                       @Nonnull StepStatus status) {
        return matchesStep(state, stepName, s -> s.status() == status);
    }

    /**
     * Creates a predicate that matches when a step in the workflow state has reached a terminal status.
     *
     * @param stepName name of the step
     * @return predicate testing if the step exists and is terminal
     */
    public static Predicate<WorkflowState> stepTerminal(@Nonnull String stepName) {
        return state -> isStepTerminal(state, stepName);
    }

    /**
     * Creates a predicate that matches when a step in the workflow state exists and is not terminal.
     *
     * @param stepName name of the step
     * @return predicate testing if the step exists and is not terminal
     */
    public static Predicate<WorkflowState> stepActive(@Nonnull String stepName) {
        return state -> isStepActive(state, stepName);
    }

    /**
     * Creates a predicate that matches when a step in the workflow state exists and has the specified status.
     *
     * @param stepName name of the step
     * @param status   expected step status
     * @return predicate testing if the step exists and matches the given status
     */
    public static Predicate<WorkflowState> stepStatus(@Nonnull String stepName, @Nonnull StepStatus status) {
        return state -> isStepStatus(state, stepName, status);
    }
}
