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

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Objects;

/**
 * Stable identity of a workflow definition used for workflow-state rehydration.
 *
 * @param qualifiedName qualified workflow name
 * @param version       workflow definition version
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public record WorkflowDefinitionId(@Nonnull QualifiedName qualifiedName,
                                   @Nonnull String version) {

    /**
     * Derives a workflow definition id from a workflow configuration.
     *
     * @param workflowConfiguration workflow configuration to identify
     * @return workflow definition id
     */
    @Nonnull
    public static WorkflowDefinitionId from(@Nonnull WorkflowConfiguration<?> workflowConfiguration) {
        Objects.requireNonNull(workflowConfiguration, "Workflow configuration must not be null");
        return new WorkflowDefinitionId(
                new QualifiedName(workflowConfiguration.workflowName()),
                workflowConfiguration.workflowVersion()
        );
    }

    public WorkflowDefinitionId {
        Objects.requireNonNull(qualifiedName, "Qualified name must not be null");
        Objects.requireNonNull(version, "Version must not be null");
        if (version.isBlank()) {
            throw new IllegalArgumentException("Version must not be blank");
        }
    }

    @Override
    @Nonnull
    public String toString() {
        return qualifiedName + ":" + version;
    }
}
