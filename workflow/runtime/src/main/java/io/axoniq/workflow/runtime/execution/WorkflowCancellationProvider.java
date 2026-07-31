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
package io.axoniq.workflow.runtime.execution;

import jakarta.annotation.Nonnull;

/**
 * Provides the cancellation coordinator associated with a running workflow implementation.
 * <p>
 * This internal bridge keeps cancellation out of the {@code WorkflowExecution} contract while an execution repository
 * still stores executions directly. A future workflow-instance abstraction can replace this bridge with structural
 * ownership of both capabilities.
 *
 * @author Simon Zambrovski
 * @since 0.3.0
 */
interface WorkflowCancellationProvider {

    /**
     * Returns the cancellation coordinator associated with this workflow execution.
     *
     * @return cancellation coordinator for the workflow execution
     */
    @Nonnull
    WorkflowCancellation workflowCancellation();
}
