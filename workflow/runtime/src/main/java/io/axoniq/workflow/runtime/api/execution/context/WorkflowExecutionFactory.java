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

import org.axonframework.common.annotation.Internal;

/**
 * Factory to create a workflow execution.
 *
 * @author Simon Zambrovski
 * @since 0.1.0
 */
@Internal
@FunctionalInterface
public interface WorkflowExecutionFactory {

    /**
     * Creates workflow execution for a given workflow context.
     *
     * @param context context to create the workflow execution for.
     * @return workflow execution.
     */
    WorkflowExecution create(WorkflowContext context);
}
