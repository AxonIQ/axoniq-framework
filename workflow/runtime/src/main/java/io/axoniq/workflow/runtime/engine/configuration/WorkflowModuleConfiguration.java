/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.MessageWorkflowIdProvider;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encapsulates workflow module configuration.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowModuleConfiguration {

    private final String workflowName;
    protected final Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> workflowStatusListeners;
    protected EventNameCustomizer eventNameCustomizer;
    protected WorkflowIdProvider workflowIdProvider;

    @Internal
    WorkflowModuleConfiguration(
            @Nonnull String workflowName,
            @Nullable Configuration configuration
    ) {
        this.workflowName = Objects.requireNonNull(workflowName, "Workflow name must be provided");
        if (configuration != null) {
            this.eventNameCustomizer = configuration.getComponent(EventNameCustomizer.class,
                                                                  DefaultEventNameCustomizer.Builder::eventName);
            this.workflowIdProvider = configuration.getComponent(WorkflowIdProvider.class,
                                                                 MessageWorkflowIdProvider::new);
        } else {
            this.eventNameCustomizer = DefaultEventNameCustomizer.Builder.eventName();
            this.workflowIdProvider = new MessageWorkflowIdProvider();
        }
        this.workflowStatusListeners = new ConcurrentHashMap<>();
        Arrays.stream(WorkflowStatus.values()).forEach(workflowStatus -> {
            this.workflowStatusListeners.put(workflowStatus, new CompositeWorkflowStatusChangeListener(workflowStatus));
        });
    }

    @Internal
    WorkflowModuleConfiguration(@Nonnull WorkflowModuleConfiguration base) {
        Objects.requireNonNull(base, "Base configuration must not be null");
        this.workflowName = base.workflowName;
        this.eventNameCustomizer = base.eventNameCustomizer;
        this.workflowIdProvider = base.workflowIdProvider;
        this.workflowStatusListeners = base.workflowStatusListeners;
    }

    /**
     * Sets event name customizer.
     *
     * @param eventNameCustomizer event name customizer to set.
     * @return module configuration instance.
     */
    public WorkflowModuleConfiguration eventNameCustomizer(@Nonnull EventNameCustomizer eventNameCustomizer) {
        Objects.requireNonNull(eventNameCustomizer, "Event name customizer must not be null.");
        this.eventNameCustomizer = eventNameCustomizer;
        return this;
    }

    /**
     * Sets workflow id provider.
     *
     * @param workflowIdProvider workflow id provider to set.
     * @return module configuration instance.
     */
    public WorkflowModuleConfiguration workflowIdProvider(@Nonnull WorkflowIdProvider workflowIdProvider) {
        Objects.requireNonNull(workflowIdProvider, "Workflow id provider must not be null.");
        this.workflowIdProvider = workflowIdProvider;
        return this;
    }

    /**
     * Registers a workflow status change listener for given status.
     *
     * @param workflowStatus               workflow status to register for.
     * @param workflowStatusChangeListener workflow status change listener to register.
     * @return module configuration instance.
     */
    public WorkflowModuleConfiguration registerWorkflowStatusChangeListener(@Nonnull WorkflowStatus workflowStatus,
                                                                            @Nonnull WorkflowStatusChangeListener workflowStatusChangeListener) {
        Objects.requireNonNull(workflowStatus, "Workflow status must not be null");
        Objects.requireNonNull(workflowStatusChangeListener, "Workflow status change listener must not be null");
        this.workflowStatusListeners.get(workflowStatus).addListener(workflowStatusChangeListener);
        return this;
    }

    /**
     * Un-Registers a workflow status change listener for given status.
     *
     * @param workflowStatus               workflow status to register for.
     * @param workflowStatusChangeListener workflow status change listener to register.
     * @return module configuration instance.
     */
    public WorkflowModuleConfiguration unregisterWorkflowStatusChangeListener(@Nonnull WorkflowStatus workflowStatus,
                                                                              @Nonnull WorkflowStatusChangeListener workflowStatusChangeListener) {
        Objects.requireNonNull(workflowStatus, "Workflow status must not be null");
        Objects.requireNonNull(workflowStatusChangeListener, "Workflow status change listener must not be null");
        this.workflowStatusListeners.get(workflowStatus).removeListener(workflowStatusChangeListener);
        return this;
    }
}
