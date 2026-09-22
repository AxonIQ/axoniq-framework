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

import io.axoniq.framework.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.payload.PayloadProcessor;
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
    public PayloadProcessor resolve(WorkflowContext workflowContext,
                                    WorkflowExecution workflowExecution,
                                    ExecutePrimitive.ExecuteCommand command) {
        return command.action();
    }
}
