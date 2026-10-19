/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.api.execution.context;

import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Creates a context for workflow execution.
 *
 * @param <T> type of the context.
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@FunctionalInterface
public interface WorkflowContextFactory<T extends WorkflowContext> {

    /**
     * Creates a new workflow context.
     *
     * @param initialPayload        initial payload of the workflow.
     * @param workflowId            id of the workflow.
     * @param processingContext     processing context.
     * @param workflowConfiguration workflow configuration.
     * @return workflow context.
     */
    T createContext(
            Map<String, @Nullable Object> initialPayload,
            String workflowId,
            ProcessingContext processingContext,
            WorkflowConfiguration<?> workflowConfiguration
    );
}
