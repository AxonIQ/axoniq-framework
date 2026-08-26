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
package io.axoniq.workflow.runtime.api.execution.state;

import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Event sourced state of the workflow execution.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public interface WorkflowState extends DescribableComponent {

    /**
     * Returns the unique workflow execution identifier.
     *
     * @return workflow identifier
     */
    @Nonnull
    String workflowId();

    /**
     * Returns the workflow definition identity, including its current definition version.
     * <p>
     * Use {@link MessageType#version()} to obtain the definition version.
     *
     * @return workflow definition identity
     */
    @Nonnull
    MessageType workflowDefinitionId();

    /**
     * Retrieves a list of step names in the workflow execution.
     *
     * @return list of step names
     */
    List<String> workflowStepNames();

    /**
     * Retrieves a step by name.
     *
     * @param stepName name of the step
     * @return workflow step
     */
    WorkflowStep getStep(@Nonnull String stepName);

    /**
     * Checks if a step with the given name exists in the workflow execution.
     *
     * @param stepName name of the step
     * @return true if the step exists, false otherwise
     */
    boolean containsStep(@Nonnull String stepName);

    /**
     * Checks if a step with the given name exists and has reached a terminal status.
     *
     * @param stepName name of the step
     * @return true if the step exists and is terminal, false otherwise
     */
    default boolean isStepTerminal(@Nonnull String stepName) {
        var step = getStep(stepName);
        return step != null && step.status().isTerminal();
    }

    /**
     * Checks if a step with the given name exists and has not reached a terminal status.
     *
     * @param stepName name of the step
     * @return true if the step exists and is not terminal, false otherwise
     */
    default boolean isStepActive(@Nonnull String stepName) {
        var step = getStep(stepName);
        return step != null && !step.status().isTerminal();
    }

    /**
     * Returns the status of the workflow execution.
     *
     * @return workflow status
     */
    @Nonnull
    WorkflowStatus workflowStatus();

    /**
     * Retrieves the payload of the workflow execution.
     *
     * @return payload of the workflow execution
     */
    @Nonnull
    Map<String, Object> payload();

    /**
     * Returns the effective version for the given {@code changeId}.
     * <p>
     * The effective version is the recorded version-migration string when a migration step has been projected for the
     * change. Otherwise, it is {@link #workflowDefinitionId()}'s version. This fallback is implicit, so workflows
     * that have never executed a {@code ctx.migrateVersion(changeId, newVersion)} call carry no version-migration step
     * in their event history.
     *
     * @param changeId the change identifier to query
     * @return effective version for the change
     */
    @Nonnull
    String versionFor(@Nonnull String changeId);

    /**
     * Returns {@code true} iff a migration step has been projected into state for the given {@code changeId}.
     * <p>
     * Used by the migration primitive to distinguish "no recorded step, using the definition identity's version" from
     * "an explicit migration step at the effective version". Only the former permits a new step to be written for a
     * different version number.
     *
     * @param changeId the change identifier to query
     * @return {@code true} iff a migration step was recorded for this {@code changeId}
     */
    boolean hasVersionMigrationStep(@Nonnull String changeId);

    /**
     * Guards against invoking any primitive when the workflow has already reached a terminal state. Rethrows the
     * original termination cause wrapped in the appropriate exception type.
     */
    void throwTerminalCause();

    /**
     * Returns the step name that reached a terminal state first among the given candidates, determined by event-sourced
     * timestamps. This is a safeguard against a race condition during event-sourcing replay: when multiple steps
     * completed before cancellation took effect, array iteration order would pick an arbitrary winner. The event store
     * timestamps are the source of truth for ordering and are stable across replays.
     *
     * @param stepNames the candidate step names to compare
     * @return the step name with the earliest terminal-state timestamp, or empty if none found
     */
    @Nonnull
    default Optional<String> firstCompletedAmong(@Nonnull Set<String> stepNames) {
        return stepNames.stream()
                        .filter(this::isStepTerminal)
                        .min(Comparator.comparing(name -> getStep(name).timestamp()));
    }

    /**
     * Returns the step names that reached a terminal state among the given candidates, sorted by event-sourced
     * timestamps (earliest first). This is the plural counterpart of {@link #firstCompletedAmong(Set)}.
     *
     * @param stepNames the candidate step names to compare
     * @return step names with terminal states, sorted by the earliest timestamp first
     */
    @Nonnull
    default List<String> sortedCompletedAmong(@Nonnull Set<String> stepNames) {
        return stepNames.stream()
                        .filter(this::isStepTerminal)
                        .sorted(Comparator.comparing(name -> getStep(name).timestamp()))
                        .toList();
    }
}
