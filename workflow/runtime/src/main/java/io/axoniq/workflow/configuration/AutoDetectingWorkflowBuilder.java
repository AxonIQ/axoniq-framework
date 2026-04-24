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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.DSLAdoptingExecutionFactory;
import io.axoniq.workflow.runtime.util.WorkflowReflectionUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.jspecify.annotations.NonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.axoniq.workflow.configuration.AutoDetectionUtils.*;

/**
 * Auto-detected component builder for a workflow configuration list.
 * <p>One builder operates on one class and can detect multiple workflow methods.</p>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
class AutoDetectingWorkflowBuilder<C extends WorkflowContext>
        implements WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<C> {

    /**
     * Creates a new auto-detected component builder.
     *
     * @param workflowContextType           workflow context type.
     * @param workflowContextFactoryBuilder workflow context factory builder.
     * @param parent                        parent workflow module.
     * @param instanceBuilder               instance builder to detect workflows on.
     */
    public AutoDetectingWorkflowBuilder(
            @Nonnull Class<C> workflowContextType,
            @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder,
            @Nonnull SimpleWorkflowModule<C> parent,
            @Nonnull ComponentBuilder<Object> instanceBuilder
    ) {
        Objects.requireNonNull(workflowContextType, "Workflow context type must not be null.");
        Objects.requireNonNull(workflowContextFactoryBuilder, "Workflow context factory builder must not be null.");
        Objects.requireNonNull(instanceBuilder, "Instance builder must not be null.");

        ComponentBuilder<List<SimpleWorkflowModule.ConditionedWorkflowConfiguration<C>>> workflowConfigurationBuilder = configuration -> {
            var instance = instanceBuilder.build(configuration);
            var type = instance.getClass();
            return AutoDetectionUtils
                    .workflowMethods(type, workflowContextType)
                    .map(t -> {

                        var attributes = t.attributes();
                        var method = t.method();

                        AutoDetectionUtils.validateAttributes(attributes, type, method);

                        var workflowName = workflowName(type, attributes, method);
                        var namespaceCustomizer = namespace(type, attributes);
                        var eventConditionBuilder = eventConditionComponentBuilder(attributes);
                        var workflowIdProviderComponentBuilder = workflowIdProviderComponentBuilder(attributes);
                        var statusChangeListeners = statusChangeListeners(instance,
                                                                          workflowContextType,
                                                                          workflowName);

                        return new SimpleWorkflowModule.ConditionedWorkflowConfiguration<>(
                                eventConditionBuilder.build(configuration),
                                new WorkflowConfiguration<C>() {
                                    @Override
                                    public Class<C> getWorkflowContextType() {
                                        return workflowContextType;
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowDefinition<C> workflowDefinition() {
                                        return workflowContext -> {
                                            Class<?>[] parameterTypes = method.getParameterTypes();
                                            Object[] args = new Object[parameterTypes.length];
                                            for (int i = 0; i < parameterTypes.length; i++) {
                                                Class<?> paramType = parameterTypes[i];
                                                if (paramType.isInstance(workflowContext)) {
                                                    args[i] = workflowContext;
                                                } else if (paramType.isInstance(instance)) {
                                                    args[i] = instance;
                                                } else {
                                                    Object wrap = AutoDetectionUtils.wrapIfPossible(paramType,
                                                                                                    workflowContext);
                                                    if (wrap != null) {
                                                        args[i] = wrap;
                                                    }
                                                }
                                            }
                                            WorkflowReflectionUtils.invoke(instance,
                                                                           method,
                                                                           args);
                                        };
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowContextFactory<C> workflowContextFactory() {
                                        return workflowContextFactoryBuilder.build(configuration);
                                    }

                                    @Override
                                    public @NonNull WorkflowExecutionFactory workflowExecutionFactory() {
                                        return new DSLAdoptingExecutionFactory<>(workflowContextType);
                                    }

                                    @Nonnull
                                    @Override
                                    public String workflowName() {
                                        return workflowName;
                                    }

                                    @Nonnull
                                    @Override
                                    public EventNameCustomizer eventNameCustomizer() {
                                        return namespaceCustomizer;
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowIdProvider workflowIdProvider() {
                                        return workflowIdProviderComponentBuilder.build(configuration);
                                    }

                                    @Nonnull
                                    @Override
                                    public Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
                                        var result = new HashMap<WorkflowStatus, WorkflowStatusChangeListener>();
                                        statusChangeListeners.forEach((k, v) -> {
                                            if (!v.isEmpty()) {
                                                result.put(k, v);
                                            }
                                        });
                                        return Collections.unmodifiableMap(result);
                                    }
                                }
                        );
                    })
                    .toList();
        };

        parent.workflowConfigurationBuilder(workflowConfigurationBuilder);
    }
}
