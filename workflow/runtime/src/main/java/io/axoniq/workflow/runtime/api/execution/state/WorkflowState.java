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

import java.util.List;
import java.util.Map;

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
     * Returns the stable workflow definition identity.
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
     * Returns the recorded version-migration string for the given {@code changeId}, or
     * {@link #workflowDefinitionVersion()} if no version-migration step has been projected for it. The default value is
     * implicit — workflows that have never executed a {@code ctx.migrateVersion(changeId, newVersion)} call carry no
     * version-migration step in their event history and yet still observe the workflow version via this accessor.
     *
     * @param changeId the change identifier to query
     * @return recorded version, or the workflow definition version if none was projected for {@code changeId}
     */
    @Nonnull
    String currentWorkflowVersion(@Nonnull String changeId);

    /**
     * Returns the workflow's definition version — the version this instance was started under, possibly bumped by
     * intervening {@code ctx.migrateVersion(...)} calls. Tracked from {@code eventMessage.type().version()} on the
     * started event and updated by migration steps whenever the new version is strictly greater than the previous
     * (semver). Defaults to {@link org.axonframework.messaging.core.MessageType#DEFAULT_VERSION} ({@code "0.0.1"}) for
     * legacy event streams without a version on the started event.
     *
     * @return the workflow's definition version
     */
    @Nonnull
    String workflowDefinitionVersion();

    /**
     * Returns {@code true} iff a migration step has been projected into state for the given {@code changeId}.
     * <p>
     * Used by the migration primitive to distinguish "no recorded step, defaulting to current" from "an explicit
     * migration step at the current version" — only the former permits a new step to be written for a different version
     * number.
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
}
