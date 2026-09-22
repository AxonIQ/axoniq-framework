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
