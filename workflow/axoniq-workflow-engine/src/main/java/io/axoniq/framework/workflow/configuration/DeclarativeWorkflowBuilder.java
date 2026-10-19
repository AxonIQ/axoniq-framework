/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import org.axonframework.common.Assert;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Builder for a single declarative workflow configuration.
 * <p>
 * Implements the {@link WorkflowModule.WorkflowDefinitionPhase.NamingPhase NamingPhase},
 * {@link WorkflowModule.WorkflowDefinitionPhase.OnPhase OnPhase}, and
 * {@link WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase WorkflowCustomizationPhase} of the
 * {@link WorkflowModule} building process. Once the
 * {@link WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase#customized(BiFunction) customized} step
 * completes, this builder constructs a {@link SimpleWorkflowModule.ConditionedWorkflowConfiguration} and registers it
 * on the parent {@link SimpleWorkflowModule}.
 *
 * @param <C> the type of {@link WorkflowContext} used by the workflow being built
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
class DeclarativeWorkflowBuilder<C extends WorkflowContext> implements
        WorkflowModule.WorkflowDefinitionPhase.NamingPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.OnPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> {

    private final Class<C> workflowContextType;
    private final ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder;
    private final SimpleWorkflowModule<C> parent;
    private final ComponentBuilder<WorkflowDefinition<C>> definitionBuilder;

    private String workflowName;
    private ComponentBuilder<EventCondition> startConditionBuilder;

    /**
     * Constructs a {@code DeclarativeWorkflowBuilder} for the given {@code parent} module.
     *
     * @param workflowContextType           the {@link WorkflowContext} type of the workflow being built
     * @param workflowContextFactoryBuilder a {@link ComponentBuilder} resolving the {@link WorkflowContextFactory}
     * @param parent                        the parent {@link SimpleWorkflowModule} to register the result on
     * @param definitionBuilder             a {@link ComponentBuilder} constructing the {@link WorkflowDefinition}
     */
    DeclarativeWorkflowBuilder(
            Class<C> workflowContextType,
            ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactoryBuilder,
            SimpleWorkflowModule<C> parent,
            ComponentBuilder<WorkflowDefinition<C>> definitionBuilder
    ) {
        this.workflowContextType = Objects.requireNonNull(
                workflowContextType, "The workflow context type must not be null."
        );
        this.workflowContextFactoryBuilder = Objects.requireNonNull(
                workflowContextFactoryBuilder, "The workflow context factory builder must not be null."
        );
        this.parent = Objects.requireNonNull(parent, "The parent workflow module must not be null.");
        this.definitionBuilder = Objects.requireNonNull(
                definitionBuilder, "The workflow definition builder must not be null."
        );
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase.OnPhase<C> workflowName(String workflowName) {
        this.workflowName = Assert.nonEmpty(workflowName, "Workflow name must not be null or blank");
        return this;
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> on(
            ComponentBuilder<EventCondition> startCondition
    ) {
        this.startConditionBuilder = Objects.requireNonNull(startCondition, "Start condition must not be null");
        return this;
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<C> customized(
            BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
    ) {
        Objects.requireNonNull(instanceCustomization, "Customizations must not be null");
        parent.workflowConfigurationBuilder(config -> workflowConfigurations(config, instanceCustomization));
        return parent;
    }

    private List<SimpleWorkflowModule.ConditionedWorkflowConfiguration<C>> workflowConfigurations(
            Configuration config,
            BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> workflowCustomization
    ) {
        var workflowModuleConfiguration = workflowCustomization.apply(
                config,
                WorkflowCustomization.defaultConfiguration(workflowName, config)
        );
        return List.of(new SimpleWorkflowModule.ConditionedWorkflowConfiguration<>(
                startConditionBuilder.build(config),
                new SimpleWorkflowConfiguration<>(
                        workflowContextType,
                        workflowName,
                        workflowModuleConfiguration.workflowVersion(),
                        definitionBuilder.build(config),
                        workflowContextFactoryBuilder.build(config),
                        workflowModuleConfiguration.workflowIdProvider,
                        workflowModuleConfiguration.eventNameCustomizer,
                        workflowModuleConfiguration.workflowStatusChangeListeners(),
                        workflowModuleConfiguration.recoverableExceptionPolicy()
                )
        ));
    }
}
