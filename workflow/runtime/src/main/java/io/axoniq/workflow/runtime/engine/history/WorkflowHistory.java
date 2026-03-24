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
package io.axoniq.workflow.runtime.engine.history;

import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import jakarta.annotation.Nonnull;

/**
 * Represents a workflow history of a passed execution.
 *
 * @param workflowId    id of the historic execution.
 * @param state resulting (final) state of the workflow.
 */
public record WorkflowHistory(
        @Nonnull String workflowId,
        @Nonnull WorkflowState state
) {

}
