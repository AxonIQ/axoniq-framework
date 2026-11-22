/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

/**
 * Thrown on the workflow thread of a {@link SimpleWorkflowExecution} that the engine stopped, from every wait of its
 * body, so the body unwinds and its driver exits.
 * <p>
 * The engine stops an execution when the workflow hibernates, when its segment is released and on shutdown. Another
 * execution of the workflow may take over, so the stopped one must not act anymore: it runs no further task, and every
 * wait throws this signal at once. A body that swallowed it and waited again in a loop would spin on its thread, keep a
 * thread of the workflow executor busy and keep the stopped execution in memory. That is why it is an {@link Error},
 * in the spirit of {@code ThreadDeath}, and no {@link Exception}: a body that handles failed steps, or that catches
 * {@code Exception} or {@code RuntimeException} around a wait, still unwinds. A body that catches {@code Throwable}, as
 * Kotlin's {@code runCatching} does, must rethrow it.
 * <p>
 * It is internal to the workflow engine and is no failure: the driver that receives it ends without a terminal event,
 * fails neither a step nor the workflow, and the workflow continues from its events in the execution that takes over.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
final class WorkflowExecutionStoppedError extends Error {

    /**
     * Creates the signal for the given stopped workflow.
     *
     * @param workflowId the identifier of the workflow whose execution was stopped
     */
    WorkflowExecutionStoppedError(String workflowId) {
        super("Workflow execution " + workflowId + " was stopped", null, false, false);
    }
}
