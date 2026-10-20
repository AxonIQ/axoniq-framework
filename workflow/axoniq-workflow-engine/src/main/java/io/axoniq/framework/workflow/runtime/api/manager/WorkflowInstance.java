/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
