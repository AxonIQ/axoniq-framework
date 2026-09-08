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
package io.axoniq.framework.workflow.runtime.api.execution.state;

import org.jspecify.annotations.Nullable;

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageType;

import java.util.List;
import java.util.Map;

/**
 * Event sourced state of the workflow execution.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface WorkflowState extends DescribableComponent {

    /**
     * Returns the unique workflow execution identifier.
     *
     * @return workflow identifier
     */
    String workflowId();

    /**
     * Returns the workflow definition identity, including its current definition version.
     * <p>
     * Use {@link MessageType#version()} to obtain the definition version.
     *
     * @return workflow definition identity
     */
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
    WorkflowStep getStep(String stepName);

    /**
     * Checks if a step with the given name exists in the workflow execution.
     *
     * @param stepName name of the step
     * @return true if the step exists, false otherwise
     */
    boolean containsStep(String stepName);

    /**
     * Returns the status of the workflow execution.
     *
     * @return workflow status
     */
    WorkflowStatus workflowStatus();

    /**
     * Retrieves the payload of the workflow execution.
     *
     * @return payload of the workflow execution
     */
    Map<String, @Nullable Object> payload();

    /**
     * Returns the effective version for the given {@code changeId}.
     * <p>
     * The effective version is the recorded version-migration string when a migration step has been projected for the
     * change. Otherwise, it is {@link #workflowDefinitionId()}'s version. This fallback is implicit, so workflows that
     * have never executed a {@code ctx.migrateVersion(changeId, newVersion)} call carry no version-migration step in
     * their event history.
     *
     * @param changeId the change identifier to query
     * @return effective version for the change
     */
    String versionFor(String changeId);

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
    boolean hasVersionMigrationStep(String changeId);

    /**
     * Guards against invoking any primitive when the workflow has already reached a terminal state. Rethrows the
     * original termination cause wrapped in the appropriate exception type.
     */
    void throwTerminalCause();
}
