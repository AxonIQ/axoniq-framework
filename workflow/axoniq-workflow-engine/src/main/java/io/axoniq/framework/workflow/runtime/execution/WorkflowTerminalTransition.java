/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

/**
 * Coordinates the local teardown and durable event publication of a workflow terminal transition.
 * <p>
 * The transition owns execution-mechanics cleanup. Callers provide only the workflow-level terminal-event action,
 * preventing primitives from directly manipulating the running-step registry or control-task queue.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
@FunctionalInterface
interface WorkflowTerminalTransition {

    /**
     * Performs a complete workflow terminal transition.
     *
     * @param terminalEventPublication action that publishes and applies the workflow terminal event
     */
    void transition(Runnable terminalEventPublication);
}
