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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.DSLAdoptingExecutionFactory;
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
 * {@link DSLAdoptingExecutionFactory DSLAdoptingExecutionFactory}.
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
        Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners
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
        return new DSLAdoptingExecutionFactory<>(workflowContextType);
    }
}
