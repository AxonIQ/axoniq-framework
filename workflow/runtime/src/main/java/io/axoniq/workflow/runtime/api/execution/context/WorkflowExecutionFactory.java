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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.api.execution.context;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

/**
 * Factory to create a workflow execution.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
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
    @Nonnull
    WorkflowExecution create(@Nonnull WorkflowContext context);
}
