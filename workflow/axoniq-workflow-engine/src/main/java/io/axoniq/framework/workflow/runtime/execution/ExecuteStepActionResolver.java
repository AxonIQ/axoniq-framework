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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;

import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.payload.PayloadProcessor;
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
     * @param workflowExecution workflow execution.
     * @param command           execute command.
     * @return action to invoke.
     */
    PayloadProcessor resolve(WorkflowExecutionOperations workflowExecutionOperations,
                             WorkflowExecution workflowExecution,
                             ExecutePrimitive.ExecuteCommand command);
}
