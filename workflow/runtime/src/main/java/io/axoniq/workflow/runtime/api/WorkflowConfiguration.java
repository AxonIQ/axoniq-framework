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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.MessageWorkflowIdProvider;
import jakarta.annotation.Nonnull;

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
    WorkflowStateFactory workflowStateFactory();

    /**
     * Returns workflow id provider.
     *
     * @return provider responsible for creation of workflow id out of initial event message.
     */
    @Nonnull
    default WorkflowIdProvider workflowIdProvider() {
        return new MessageWorkflowIdProvider();
    }

    /**
     * Returns workflow name.
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
    default EventNameCustomizer eventNameCustomizer() {
        return DefaultEventNameCustomizer.Builder.eventName();
    }
}
