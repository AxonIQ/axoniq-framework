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
package io.axoniq.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

import java.util.Set;

/**
 * Identifies workflow instances that have started and have not yet reached a terminal state.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
public interface RunningWorkflows {

    /**
     * Returns the current set of running workflow identifiers.
     *
     * @return immutable set of workflow identifiers
     */
    Set<String> workflowIds();

    /**
     * Checks whether the given workflow identifier is currently running.
     *
     * @param workflowId workflow identifier to inspect
     * @return {@code true} if the workflow is running
     */
    boolean contains(String workflowId);
}
