/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Restores one workflow from its durable state into a new execution, which has not entered memory yet.
 * <p>
 * Every workflow that comes back into memory comes back through a restorer: a claim of a segment restores each running
 * workflow of the segment it does not leave hibernated, see {@link WorkflowEngine#restoreWorkflowsFor}, and a wake
 * restores a hibernated workflow, see {@link WorkflowEngineHibernationSupport}. A restorer only creates the execution:
 * the caller brings it into memory, under the claim that holds its segment.
 * <p>
 * The {@link WorkflowEngine} creates its {@link DefaultWorkflowRestorer}, and hands it to its hibernation support.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@FunctionalInterface
interface WorkflowRestorer {

    /**
     * Restores the given workflow from its durable state into a new execution, which runs on the given
     * {@code executionContext} and resumes appending from the position its durable state was read at.
     *
     * @param workflowId       the identifier of the workflow to restore
     * @param resources        the context whose resources the workflow is read with
     * @param executionContext the context the restored execution runs on
     * @return a future completing with the restored execution, or with an empty optional when the durable state of the
     * workflow is terminal, so there is nothing to restore
     */
    CompletableFuture<Optional<WorkflowExecution>> restore(String workflowId,
                                                           ProcessingContext resources,
                                                           ProcessingContext executionContext);
}
