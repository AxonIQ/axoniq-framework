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
package io.axoniq.framework.workflow.runtime.api.manager;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;

import java.util.concurrent.CompletableFuture;

/**
 * Represents one workflow instance selected by a query. An instance can represent a live execution or a historic
 * workflow instance, and always provides access to its detached state. The inherited cancellation operations act only
 * when the selected instance is live.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowInstance extends WorkflowInstanceOperator {

    /**
     * Reads the detached workflow state selected for this instance.
     *
     * @return a future completing with a detached workflow state
     */
    CompletableFuture<WorkflowState> state();
}
