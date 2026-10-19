/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;


/**
 * Provides the cancellation coordinator associated with a running workflow implementation.
 * <p>
 * This internal bridge keeps cancellation out of the {@code WorkflowExecution} contract while an execution repository
 * still stores executions directly. A future user-facing {@code WorkflowManager} will own the external cancellation
 * API; its final design will determine whether it replaces this bridge or exposes the capability through its own API.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
interface WorkflowCancellationProvider {

    /**
     * Returns the cancellation coordinator associated with this workflow execution.
     *
     * @return cancellation coordinator for the workflow execution
     */
    WorkflowCancellation workflowCancellation();
}
