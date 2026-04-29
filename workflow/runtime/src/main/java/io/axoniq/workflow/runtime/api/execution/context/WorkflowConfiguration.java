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

import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;

import java.util.Map;

/**
 * Configures definition, context factory and correlation provider.
 *
 * @param <T> workflow context type.
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface WorkflowConfiguration<T extends WorkflowContext> {

    /**
     * Returns the type of the workflow context.
     *
     * @return type of the workflow context.
     */
    Class<T> getWorkflowContextType();

    /**
     * Returns workflow definition.
     *
     * @return workflow definition method expressed using workflow context.
     */
    @Nonnull
    WorkflowDefinition<T> workflowDefinition();

    /**
     * Returns workflow context factory.
     *
     * @return factory for workflow context.
     */
    @Nonnull
    WorkflowContextFactory<T> workflowContextFactory();

    /**
     * Returns workflow state factory.
     *
     * @return factory for workflow state.
     */
    @Nonnull
    WorkflowExecutionFactory workflowExecutionFactory();

    /**
     * Returns workflow id provider.
     *
     * @return provider responsible for creation of workflow id out of initial event message.
     */
    @Nonnull
    WorkflowIdProvider workflowIdProvider();

    /**
     * Returns the workflow name.
     *
     * @return name of the workflow.
     */
    @Nonnull
    default String workflowName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Default event name customizer applied on workflow level.
     *
     * @return default customizer.
     */
    @Nonnull
    EventNameCustomizer eventNameCustomizer();

    /**
     * Returns workflow status change listeners.
     *
     * @return map of workflow status change listeners.
     */
    @Nonnull
    default Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
        return Map.of();
    }
}
