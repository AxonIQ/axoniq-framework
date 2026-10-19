/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.execution.WorkflowContextAdoptingExecutionFactory;
import org.axonframework.common.annotation.Internal;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Default {@link WorkflowConfiguration} implementation used by both the {@link DeclarativeWorkflowBuilder} and the
 * {@link AutoDetectingWorkflowBuilder}.
 * <p>
 * Filters out {@link WorkflowStatusChangeListener status change listeners} that are
 * {@link CompositeWorkflowStatusChangeListener#isEmpty() empty} during construction, so callers do not need to
 * duplicate that logic. The {@link WorkflowExecutionFactory} is created lazily from the {@link #workflowContextType()}
 * to preserve the original deferred-construction behavior of
 * {@link WorkflowContextAdoptingExecutionFactory WorkflowContextAdoptingExecutionFactory}.
 *
 * @param <C> the type of {@link WorkflowContext} used by the workflow
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
record SimpleWorkflowConfiguration<C extends WorkflowContext>(
        Class<C> workflowContextType,
        String workflowName,
        String workflowVersion,
        WorkflowDefinition<C> workflowDefinition,
        WorkflowContextFactory<C> workflowContextFactory,
        WorkflowIdProvider workflowIdProvider,
        EventNameCustomizer eventNameCustomizer,
        Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners,
        RecoverableWorkflowExceptionPolicy recoverableExceptionPolicy
) implements WorkflowConfiguration<C> {

    SimpleWorkflowConfiguration {
        if (workflowVersion == null || workflowVersion.isBlank()) {
            workflowVersion = Version.DEFAULT_VERSION;
        }
        var filtered = new HashMap<WorkflowStatus, WorkflowStatusChangeListener>();
        workflowStatusChangeListeners.forEach((status, listener) -> {
            if (listener instanceof CompositeWorkflowStatusChangeListener composite) {
                if (!composite.isEmpty()) {
                    filtered.put(status, listener);
                }
            } else {
                filtered.put(status, listener);
            }
        });
        workflowStatusChangeListeners = Collections.unmodifiableMap(filtered);
    }

    @Override
    public Class<C> getWorkflowContextType() {
        return workflowContextType;
    }

    @Override
    public WorkflowExecutionFactory workflowExecutionFactory() {
        return new WorkflowContextAdoptingExecutionFactory<>(workflowContextType);
    }
}
