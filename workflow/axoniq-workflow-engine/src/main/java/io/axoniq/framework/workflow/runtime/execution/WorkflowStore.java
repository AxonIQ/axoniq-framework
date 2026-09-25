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

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Narrow adapter around workflow-state repositories used during startup.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface WorkflowStore {

    /**
     * Loads the singleton running-workflows projection.
     *
     * @param processingContext processing context used for sourcing
     * @return current running workflow ids
     */
    CompletableFuture<RunningWorkflows> loadRunningWorkflows(ProcessingContext processingContext);

    /**
     * Loads the event-sourced durable state for a workflow id.
     *
     * @param workflowId        workflow identifier
     * @param processingContext processing context used for sourcing
     * @return durable workflow state
     */
    CompletableFuture<WorkflowState> loadWorkflow(String workflowId,
                                                  ProcessingContext processingContext);

    /**
     * Sources the state of a workflow that may not exist.
     *
     * @param workflowId        identifier of the workflow to source
     * @param processingContext processing context the sourcing runs in
     * @return a future completing with the sourced state, or empty when the store holds no events for the workflow
     */
    CompletableFuture<Optional<WorkflowState>> findWorkflow(String workflowId,
                                                            ProcessingContext processingContext);
}
