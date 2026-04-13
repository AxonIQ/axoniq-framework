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

import io.axoniq.workflow.runtime.api.execution.state.AllMatchCombinator;
import io.axoniq.workflow.runtime.api.execution.state.AnyMatchCombinator;
import io.axoniq.workflow.runtime.api.execution.state.NoneMatchCombinator;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.List;
import java.util.Map;

/**
 * Public API facing class to access the execution from workflow definition.
 */
public interface WorkflowContext extends
        ExecutePrimitive,
        WaitForPrimitive,
        TerminatePrimitive,
        PayloadPrimitive,
        AllMatchCombinator,
        NoneMatchCombinator,
        AnyMatchCombinator,
        DescribableComponent {

    /**
     * Retrieves workflow id.
     *
     * @return the unique identifier of the workflow.
     */
    @Nonnull
    String workflowId();

    /**
     * Retrieves the workflow payload.
     *
     * @return the payload of the workflow.
     */
    @Nonnull
    Map<String, Object> workflowPayload();

    /**
     * Retrieves workflow status.
     *
     * @return the status of the workflow.
     */
    @Nonnull
    WorkflowStatus workflowStatus();

    /**
     * Retrieves workflow step names.
     *
     * @return the names of the workflow steps.
     */
    @Nonnull
    List<String> workflowStepNames();

    /**
     * Retrieves processing context.
     *
     * @return the processing context.
     */
    @Nonnull
    ProcessingContext processingContext();
}
