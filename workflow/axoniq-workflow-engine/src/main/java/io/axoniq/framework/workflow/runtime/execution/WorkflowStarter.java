/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Starts the body of a workflow execution in memory: lets cancellation requests reach it from now on, see
 * {@link LiveWorkflows#markCancellable(WorkflowExecution)}, and runs its body, after which the execution leaves memory,
 * see {@link LiveWorkflows#leave(WorkflowExecution)}.
 * <p>
 * The body of an execution starts once the claim of its segment tells it bodies start
 * {@link SegmentClaim.BodyStart#NOW now}. The {@link WorkflowEngine} starts the bodies of the executions it starts and
 * restores, and its {@link WorkflowEngineHibernationSupport} the body of an execution that woke once its claim caught
 * up. Each starts an execution under the lock of the claim it entered memory under, if any, and only when that claim
 * tells it bodies start now, so the activation of the claim, which starts every execution of the claim that does not
 * run yet, and a start by the engine or a wake never both start one execution.
 * <p>
 * The engine creates one, from its {@link LiveWorkflows}, and hands it to its hibernation support.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class WorkflowStarter {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowStarter.class);

    private final LiveWorkflows liveWorkflows;

    /**
     * Creates the starter of the bodies of the executions in memory of the given live workflows.
     *
     * @param liveWorkflows the live workflows the started executions are in memory in, and leave once they ended
     */
    WorkflowStarter(LiveWorkflows liveWorkflows) {
        this.liveWorkflows = Objects.requireNonNull(liveWorkflows, "The LiveWorkflows must not be null.");
    }

    /**
     * Starts the body of the given execution in memory, which takes cancellation requests from now on, and leaves
     * memory once its body ended. Marking an execution that takes cancellation requests already changes nothing.
     *
     * @param execution the execution in memory whose body starts
     */
    void start(WorkflowExecution execution) {
        liveWorkflows.markCancellable(execution);
        execution.workflowExecutionOperations()
                 .processingContext()
                 .whenComplete(context -> {
                     try {
                         logger.debug("Executing workflow execution with id: {}", execution.workflowId());
                         execution.execute(
                                 finished -> {
                                     logger.debug("Workflow {} finished with status {}, removing it from repository",
                                                  execution.workflowId(),
                                                  finished.state().workflowStatus());
                                     liveWorkflows.leave(execution);
                                 }
                         ).whenComplete((ignored, failure) -> {
                             if (failure != null) {
                                 logger.error("Workflow {} stopped with an unhandled execution failure",
                                              execution.workflowId(), failure);
                             }
                         });
                     } catch (Throwable t) {
                         throw new RuntimeException("Error during workflow execution", t);
                     }
                 });
    }
}
