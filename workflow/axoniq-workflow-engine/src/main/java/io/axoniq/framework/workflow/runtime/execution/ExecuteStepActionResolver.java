/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.PayloadProcessor;
import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import org.axonframework.common.annotation.Internal;

/**
 * Resolves the action to invoke for an "execute step".
 * <p>
 * Production defaults to the action carried by the command. Tests can replace this component to provide fake actions
 * without changing workflow definitions.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
@FunctionalInterface
public interface ExecuteStepActionResolver {

    /**
     * Resolve the action for the given execute command.
     *
     * @param workflowExecutionOperations runtime primitive-operation surface
     * @param workflowExecution           workflow execution.
     * @param command                     execute command.
     * @return action to invoke.
     */
    PayloadProcessor resolve(WorkflowExecutionOperations workflowExecutionOperations,
                             WorkflowExecution workflowExecution,
                             ExecutePrimitive.ExecuteCommand command);
}
