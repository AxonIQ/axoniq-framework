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

import io.axoniq.framework.workflow.dsl.api.EventNameCustomizer;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.execution.MessageWorkflowIdProvider;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Encapsulates workflow module configuration.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class WorkflowCustomization {

    private final String workflowName;
    private final Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> workflowStatusListeners;
    protected EventNameCustomizer eventNameCustomizer;
    protected WorkflowIdProvider workflowIdProvider;
    private RecoverableWorkflowExceptionPolicy recoverableExceptionPolicy;
    private String workflowVersion = Version.DEFAULT_VERSION;

    /**
     * Constructs new workflow customization.
     *
     * @param workflowName  name of the workflow.
     * @param configuration configuration to use.
     */
    @Internal
    WorkflowCustomization(
            String workflowName,
            @Nullable Configuration configuration
    ) {
        this.workflowName = Objects.requireNonNull(workflowName, "Workflow name must be provided");
        if (configuration != null) {
            this.eventNameCustomizer = configuration.getComponent(EventNameCustomizer.class,
                                                                  DefaultEventNameCustomizer.Builder::defaults);
            this.workflowIdProvider = configuration.getComponent(WorkflowIdProvider.class,
                                                                 MessageWorkflowIdProvider::new);
            this.recoverableExceptionPolicy = configuration.getComponent(
                    RecoverableWorkflowExceptionPolicy.class, () -> RecoverableWorkflowExceptionPolicy.DEFAULT
            );
        } else {
            this.eventNameCustomizer = DefaultEventNameCustomizer.Builder.defaults();
            this.workflowIdProvider = new MessageWorkflowIdProvider();
            this.recoverableExceptionPolicy = RecoverableWorkflowExceptionPolicy.DEFAULT;
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
    WorkflowCustomization(WorkflowCustomization base) {
        Objects.requireNonNull(base, "Base configuration must not be null");
        this.workflowName = base.workflowName;
        this.eventNameCustomizer = base.eventNameCustomizer;
        this.workflowIdProvider = base.workflowIdProvider;
        this.recoverableExceptionPolicy = base.recoverableExceptionPolicy;
        this.workflowVersion = base.workflowVersion;
        this.workflowStatusListeners = base.workflowStatusListeners;
    }

    /**
     * Create default module configuration.
     *
     * @param workflowName  name of the workflow.
     * @param configuration configuration to use.
     * @return workflow module configuration.
     */
    public static WorkflowCustomization defaultConfiguration(String workflowName,
                                                             @Nullable Configuration configuration) {
        return new WorkflowCustomization(workflowName, configuration);
    }

    /**
     * Sets the workflow definition version (semver string, e.g. {@code "0.0.2"}). Optional — defaults to
     * {@link Version#DEFAULT_VERSION} ({@code "0.0.1"}).
     *
     * @param workflowVersion the version to use for this workflow definition.
     * @return module configuration instance.
     */
    public WorkflowCustomization workflowVersion(String workflowVersion) {
        Objects.requireNonNull(workflowVersion, "Workflow version must not be null.");
        Version.validate(workflowVersion);
        this.workflowVersion = workflowVersion;
        return this;
    }

    /**
     * @return the configured workflow definition version.
     */
    public String workflowVersion() {
        return this.workflowVersion;
    }

    /**
     * Sets event name customizer.
     *
     * @param eventNameCustomizer event name customizer to set.
     * @return module configuration instance.
     */
    public WorkflowCustomization eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
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
    public WorkflowCustomization workflowIdProvider(WorkflowIdProvider workflowIdProvider) {
        Objects.requireNonNull(workflowIdProvider, "Workflow id provider must not be null.");
        this.workflowIdProvider = workflowIdProvider;
        return this;
    }

    /**
     * Sets the policy that decides whether an exception escaping the workflow body pauses the workflow or fails it.
     * Defaults to the {@link RecoverableWorkflowExceptionPolicy} component, or
     * {@link RecoverableWorkflowExceptionPolicy#DEFAULT} when none is registered.
     *
     * @param recoverableExceptionPolicy the policy to use for this workflow
     * @return module configuration instance
     */
    public WorkflowCustomization recoverableExceptionPolicy(
            RecoverableWorkflowExceptionPolicy recoverableExceptionPolicy
    ) {
        Objects.requireNonNull(recoverableExceptionPolicy, "Recoverable exception policy must not be null.");
        this.recoverableExceptionPolicy = recoverableExceptionPolicy;
        return this;
    }

    /**
     * Returns the policy that decides whether an exception escaping the workflow body pauses the workflow or fails it.
     *
     * @return the configured recoverable exception policy
     */
    public RecoverableWorkflowExceptionPolicy recoverableExceptionPolicy() {
        return this.recoverableExceptionPolicy;
    }

    /**
     * Registers a workflow status change listener for given status.
     *
     * @param workflowStatus               workflow status to register for.
     * @param workflowStatusChangeListener workflow status change listener to register.
     * @return module configuration instance.
     */
    public WorkflowCustomization registerWorkflowStatusChangeListener(
            WorkflowStatus workflowStatus,
            WorkflowStatusChangeListener workflowStatusChangeListener
    ) {
        Objects.requireNonNull(workflowStatus, "Workflow status must not be null");
        Objects.requireNonNull(workflowStatusChangeListener, "Workflow status change listener must not be null");
        this.workflowStatusListeners.get(workflowStatus).addListener(workflowStatusChangeListener);
        return this;
    }

    /**
     * Un-Registers a workflow status change listener for the given status.
     *
     * @param workflowStatus               workflow status to register for.
     * @param workflowStatusChangeListener workflow status change listener to register.
     * @return module configuration instance.
     */
    public WorkflowCustomization unregisterWorkflowStatusChangeListener(
            WorkflowStatus workflowStatus,
            WorkflowStatusChangeListener workflowStatusChangeListener
    ) {
        Objects.requireNonNull(workflowStatus, "Workflow status must not be null");
        Objects.requireNonNull(workflowStatusChangeListener, "Workflow status change listener must not be null");
        this.workflowStatusListeners.get(workflowStatus).removeListener(workflowStatusChangeListener);
        return this;
    }

    /**
     * Returns the workflow status change listeners as a map typed by the {@link WorkflowStatusChangeListener}
     * interface.
     *
     * @return an unmodifiable view of the status change listeners
     */
    Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
        var filtered = new HashMap<WorkflowStatus, WorkflowStatusChangeListener>();
        workflowStatusListeners.forEach((status, composite) -> {
            if (!composite.isEmpty()) {
                filtered.put(status, composite);
            }
        });
        return Collections.unmodifiableMap(filtered);
    }
}
