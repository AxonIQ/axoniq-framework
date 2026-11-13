/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;

import java.util.Map;

/**
 * Configures definition, context factory and correlation provider.
 *
 * @param <T> workflow context type.
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
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
    WorkflowDefinition<T> workflowDefinition();

    /**
     * Returns workflow context factory.
     *
     * @return factory for workflow context.
     */
    WorkflowContextFactory<T> workflowContextFactory();

    /**
     * Returns workflow state factory.
     *
     * @return factory for workflow state.
     */
    WorkflowExecutionFactory workflowExecutionFactory();

    /**
     * Returns workflow id provider.
     *
     * @return provider responsible for creation of workflow id out of initial event message.
     */
    WorkflowIdProvider workflowIdProvider();

    /**
     * Returns the workflow name.
     *
     * @return name of the workflow.
     */
    default String workflowName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Workflow definition version (semver, e.g. {@code "0.0.2"}). Defaults to {@link Version#DEFAULT_VERSION}
     * ({@code "0.0.1"}).
     *
     * @return workflow definition version.
     */
    default String workflowVersion() {
        return Version.DEFAULT_VERSION;
    }

    /**
     * Default event name customizer applied on workflow level.
     *
     * @return default customizer.
     */
    EventNameCustomizer eventNameCustomizer();

    /**
     * Returns workflow status change listeners.
     *
     * @return map of workflow status change listeners.
     */
    default Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
        return Map.of();
    }

    /**
     * Classifies an exception that escaped the workflow body. A recoverable exception pauses the workflow so a later
     * run can succeed; any other exception fails the workflow.
     *
     * @return the policy deciding whether an unhandled body exception is recoverable, by default
     * {@link RecoverableWorkflowExceptionPolicy#DEFAULT}
     */
    default RecoverableWorkflowExceptionPolicy recoverableExceptionPolicy() {
        return RecoverableWorkflowExceptionPolicy.DEFAULT;
    }
}
