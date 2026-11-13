/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import org.axonframework.common.annotation.Internal;

/**
 * Factory to create a workflow execution.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
@FunctionalInterface
public interface WorkflowExecutionFactory {

    /**
     * Creates workflow execution for a given workflow context.
     *
     * @param workflowContext author-facing context to create the workflow execution for
     * @return workflow execution.
     */
    WorkflowExecution create(WorkflowContext workflowContext);
}
