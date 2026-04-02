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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.history.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.engine.history.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.MessageWorkflowIdProvider;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
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
public class WorkflowCustomization {

    private final String workflowName;
    protected final Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> workflowStatusListeners;
    protected EventNameCustomizer eventNameCustomizer;
    protected WorkflowIdProvider workflowIdProvider;
    protected WorkflowEngine workflowEngine;
    protected WorkflowExecutionRepository workflowExecutionRepository;
    protected MutableWorkflowHistoryRepository workflowHistoryRepository;
    protected WorkflowHistoryProjector workflowHistoryProjector;

    /**
     * Create default module configuration.
     *
     * @param workflowName  name of the workflow.
     * @param configuration configuration to use.
     * @return workflow module configuration.
     */
    public static WorkflowCustomization defaultConfiguration(@Nonnull String workflowName,
                                                             @Nullable Configuration configuration) {
        return new WorkflowCustomization(workflowName, configuration);
    }


    /**
     * Constructs new workflow customization.
     *
     * @param workflowName  name of the workflow.
     * @param configuration configuration to use.
     */
    @Internal
    WorkflowCustomization(
            @Nonnull String workflowName,
            @Nullable Configuration configuration
    ) {
        this.workflowName = Objects.requireNonNull(workflowName, "Workflow name must be provided");
        if (configuration != null) {
            this.eventNameCustomizer = configuration.getComponent(EventNameCustomizer.class,
                                                                  DefaultEventNameCustomizer.Builder::defaults);
            this.workflowIdProvider = configuration.getComponent(WorkflowIdProvider.class,
                                                                 MessageWorkflowIdProvider::new);
            this.workflowEngine = configuration.getComponent(WorkflowEngine.class);
            this.workflowExecutionRepository = configuration.getComponent(WorkflowExecutionRepository.class);
            this.workflowHistoryRepository = configuration.getComponent(MutableWorkflowHistoryRepository.class);
            this.workflowHistoryProjector = configuration.getComponent(WorkflowHistoryProjector.class);
        } else {
            this.eventNameCustomizer = DefaultEventNameCustomizer.Builder.defaults();
            this.workflowIdProvider = new MessageWorkflowIdProvider();
        }
        this.workflowStatusListeners = new ConcurrentHashMap<>();
        Arrays.stream(WorkflowStatus.values()).forEach(workflowStatus -> {
            this.workflowStatusListeners.put(workflowStatus, new CompositeWorkflowStatusChangeListener(workflowStatus));
        });
    }

    /**
     * Copy constructor.
     *
     * @param base base to copy.
     */
    @Internal
    WorkflowCustomization(@Nonnull WorkflowCustomization base) {
        Objects.requireNonNull(base, "Base configuration must not be null");
        this.workflowName = base.workflowName;
        this.eventNameCustomizer = base.eventNameCustomizer;
        this.workflowIdProvider = base.workflowIdProvider;
        this.workflowStatusListeners = base.workflowStatusListeners;
        this.workflowEngine = base.workflowEngine;
        this.workflowExecutionRepository = base.workflowExecutionRepository;
        this.workflowHistoryRepository = base.workflowHistoryRepository;
        this.workflowHistoryProjector = base.workflowHistoryProjector;
    }

    /**
     * Sets workflow engine.
     *
     * @param workflowEngine workflow engine to set.
     * @return module configuration instance.
     */
    public WorkflowCustomization workflowEngine(@Nonnull WorkflowEngine workflowEngine) {
        Objects.requireNonNull(workflowEngine, "Workflow engine must not be null.");
        this.workflowEngine = workflowEngine;
        return this;
    }

    /**
     * Sets workflow execution repository.
     *
     * @param workflowExecutionRepository workflow execution repository to set.
     * @return module configuration instance.
     */
    public WorkflowCustomization workflowExecutionRepository(
            @Nonnull WorkflowExecutionRepository workflowExecutionRepository) {
        Objects.requireNonNull(workflowExecutionRepository, "Workflow execution repository must not be null.");
        this.workflowExecutionRepository = workflowExecutionRepository;
        return this;
    }

    /**
     * Sets workflow history repository.
     *
     * @param workflowHistoryRepository workflow history repository to set.
     * @return module configuration instance.
     */
    public WorkflowCustomization workflowHistoryRepository(
            @Nonnull MutableWorkflowHistoryRepository workflowHistoryRepository) {
        Objects.requireNonNull(workflowHistoryRepository, "Workflow history repository must not be null.");
        this.workflowHistoryRepository = workflowHistoryRepository;
        return this;
    }

    /**
     * Sets workflow history projector.
     *
     * @param workflowHistoryProjector workflow history projector to set.
     * @return module configuration instance.
     */
    public WorkflowCustomization workflowHistoryProjector(@Nonnull WorkflowHistoryProjector workflowHistoryProjector) {
        Objects.requireNonNull(workflowHistoryProjector, "Workflow history projector must not be null.");
        this.workflowHistoryProjector = workflowHistoryProjector;
        return this;
    }

    /**
     * Sets event name customizer.
     *
     * @param eventNameCustomizer event name customizer to set.
     * @return module configuration instance.
     */
    public WorkflowCustomization eventNameCustomizer(@Nonnull EventNameCustomizer eventNameCustomizer) {
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
    public WorkflowCustomization workflowIdProvider(@Nonnull WorkflowIdProvider workflowIdProvider) {
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
    public WorkflowCustomization registerWorkflowStatusChangeListener(@Nonnull WorkflowStatus workflowStatus,
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
    public WorkflowCustomization unregisterWorkflowStatusChangeListener(@Nonnull WorkflowStatus workflowStatus,
                                                                        @Nonnull WorkflowStatusChangeListener workflowStatusChangeListener) {
        Objects.requireNonNull(workflowStatus, "Workflow status must not be null");
        Objects.requireNonNull(workflowStatusChangeListener, "Workflow status change listener must not be null");
        this.workflowStatusListeners.get(workflowStatus).removeListener(workflowStatusChangeListener);
        return this;
    }
}
