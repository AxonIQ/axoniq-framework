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

import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;

/**
 * Listener informed on status change of workflow.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@FunctionalInterface
public interface WorkflowStatusChangeListener {

    /**
     * React on workflow status change.
     *
     * @param state   workflow status.
     * @param context workflow context.
     */
    <C extends WorkflowContext> void onWorkflowStatus(@Nonnull WorkflowStatus state, @Nonnull C context);
}
