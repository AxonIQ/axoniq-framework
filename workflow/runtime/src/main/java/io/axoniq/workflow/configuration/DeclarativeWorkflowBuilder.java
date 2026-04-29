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

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
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
import jakarta.annotation.Nonnull;
import org.axonframework.common.Assert;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.jspecify.annotations.NonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Declarative component builder for a single workflow configuration.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
class DeclarativeWorkflowBuilder<C extends WorkflowContext> implements
        WorkflowModule.WorkflowDefinitionPhase.NamingPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.OnPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> {

    private final Class<C> workflowContextType;
    private final ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder;
    private final ComponentBuilder<WorkflowDefinition<C>> definitionBuilder;
    private final SimpleWorkflowModule<C> parent;
    private ComponentBuilder<EventCondition> startConditionBuilder;
    private String workflowName;

    /**
     * Constructs declarative component builder.
     *
     * @param workflowContextType           workflow context type.
     * @param workflowContextFactoryBuilder workflow context factory builder.
     * @param parent                        parent module.
     * @param componentBuilder              component builder.
     */
    @Internal
    public DeclarativeWorkflowBuilder(
            @Nonnull Class<C> workflowContextType,
            @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder,
            @Nonnull SimpleWorkflowModule<C> parent,
            @Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder
    ) {
        this.parent = parent;
        this.definitionBuilder = Objects.requireNonNull(componentBuilder,
                                                        "Workflow definition builder must not be null");
        this.workflowContextType = workflowContextType;
        this.workflowContextFactoryBuilder = workflowContextFactoryBuilder;
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase.OnPhase<C> workflowName(@Nonnull String workflowName) {
        this.workflowName = Assert.nonEmpty(workflowName, "Workflow name must not be null or blank");
        return this;
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> on(
            @Nonnull ComponentBuilder<EventCondition> startCondition) {
        this.startConditionBuilder = Objects.requireNonNull(startCondition, "Start condition must not be null");
        return this;
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<C> customized(
            @Nonnull BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
    ) {
        Objects.requireNonNull(instanceCustomization, "Customizations must not be null");
        ComponentBuilder<List<SimpleWorkflowModule.ConditionedWorkflowConfiguration<C>>> workflowConfigurationBuilder = c -> List.of(
                new ComponentBuilder<SimpleWorkflowModule.ConditionedWorkflowConfiguration<?>>() {

                    @Override
                    @Nonnull
                    public SimpleWorkflowModule.ConditionedWorkflowConfiguration<C> build(
                            @Nonnull Configuration configuration) {
                        var workflowModuleConfiguration = instanceCustomization.apply(
                                configuration,
                                WorkflowCustomization.defaultConfiguration(workflowName, configuration)
                        );

                        return new SimpleWorkflowModule.ConditionedWorkflowConfiguration<>(
                                startConditionBuilder.build(configuration),
                                new WorkflowConfiguration<>() {

                                    @Nonnull
                                    @Override
                                    public String workflowName() {
                                        return workflowName;
                                    }

                                    @Override
                                    public Class<C> getWorkflowContextType() {
                                        return workflowContextType;
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowDefinition<C> workflowDefinition() {
                                        return definitionBuilder.build(configuration);
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
                                    public WorkflowIdProvider workflowIdProvider() {
                                        return workflowModuleConfiguration.workflowIdProvider;
                                    }

                                    @Nonnull
                                    @Override
                                    public EventNameCustomizer eventNameCustomizer() {
                                        return workflowModuleConfiguration.eventNameCustomizer;
                                    }

                                    @Nonnull
                                    @Override
                                    public Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
                                        var result = new HashMap<WorkflowStatus, WorkflowStatusChangeListener>();
                                        workflowModuleConfiguration.workflowStatusListeners.forEach((k, v) -> {
                                            if (!v.isEmpty()) {
                                                result.put(k, v);
                                            }
                                        });
                                        return Collections.unmodifiableMap(result);
                                    }
                                }
                        );
                    }
                }.build(c));
        parent.workflowConfigurationBuilder(workflowConfigurationBuilder);
        return parent;
    }
}
