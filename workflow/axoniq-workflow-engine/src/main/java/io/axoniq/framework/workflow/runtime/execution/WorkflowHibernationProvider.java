/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

/**
 * Provides the hibernation support associated with a running workflow implementation.
 * <p>
 * This internal bridge keeps hibernation out of the {@code WorkflowExecution} contract, the way
 * {@link WorkflowCancellationProvider} keeps cancellation out of it. The {@link WorkflowEngineHibernationSupport} uses
 * it to enable hibernation and to request it.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
interface WorkflowHibernationProvider {

    /**
     * Returns the hibernation support associated with this workflow execution.
     *
     * @return hibernation support for the workflow execution
     */
    WorkflowExecutionHibernationSupport workflowHibernation();
}
