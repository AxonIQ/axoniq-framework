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
 * Default action resolver that invokes the action supplied by the workflow definition.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public class DefaultExecuteStepActionResolver implements ExecuteStepActionResolver {

    @Override
    public PayloadProcessor resolve(WorkflowExecutionOperations workflowExecutionOperations,
                                    WorkflowExecution workflowExecution,
                                    ExecutePrimitive.ExecuteCommand command) {
        return command.action();
    }
}
