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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;

/**
 * Listener informed on status change of workflow.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowStatusChangeListener {

    /**
     * React on workflow status change.
     *
     * @param state   workflow status.
     * @param context workflow context.
     */
    void onWorkflowStatus(@Nonnull WorkflowStatus state, @Nonnull WorkflowContext context);
}
