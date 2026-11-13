/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

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
