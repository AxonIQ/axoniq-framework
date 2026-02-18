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

import io.axoniq.workflow.runtime.api.AssociationProvider;
import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Workflow module used to create multiple {@link WorkflowConfiguration} (one per workflow definition) defined for the
 * given {@link WorkflowContext}. As a result the module will register its configuration in the
 * {@link WorkflowDefinitionRegistry}, used by the {@link io.axoniq.workflow.runtime.engine.impl.WorkflowEngine}.
 *
 * @param <C> type of workflow context.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
class SimpleWorkflowModule<C extends WorkflowContext> extends BaseModule<SimpleWorkflowModule<C>>
        implements WorkflowModule<C>,
        WorkflowModule.LanguagePhase.WorkflowContextFactoryPhase<C>,
        WorkflowModule.LanguagePhase.WorkflowStateFactoryPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.OnPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.DeclarativeDefinitionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.AssociationPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> {

    private final Class<C> workflowContextType;
    private final List<WorkflowConfigurationBuilder<?>> workflowConfigurations = new ArrayList<>();
    private ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory;
    private ComponentBuilder<WorkflowStateFactory> workflowStateFactory;
    private ComponentBuilder<WorkflowDefinition<C>> currentWorkflowDefinition;
    private ComponentBuilder<AssociationProvider> currentWorkflowAssociationProvider;
    private ComponentBuilder<EventCondition> currentStartCondition;
    private String currentWorkflowName;


    /**
     * Constructs new workflow module.
     *
     * @param workflowContextType workflow context class.
     */
    @Internal
    SimpleWorkflowModule(@Nonnull Class<C> workflowContextType) {
        this(workflowContextType.getName(), workflowContextType);
    }

    /**
     * Constructs new workflow module with given name.
     *
     * @param name                name of the workflow module.
     * @param workflowContextType workflow context class.
     */
    @Internal
    SimpleWorkflowModule(@NotNull String name, @Nonnull Class<C> workflowContextType) {
        super(name);
        this.workflowContextType = workflowContextType;
    }

    @Override
    public Class<C> getContextType() {
        return this.workflowContextType;
    }


    @Override
    public Configuration build(@NotNull Configuration parent, @NotNull LifecycleRegistry lifecycleRegistry) {
        Configuration configuration = super.build(parent, lifecycleRegistry);
        registerWorkflowDefinitions(configuration);
        return configuration;
    }

    private void registerWorkflowDefinitions(@Nonnull Configuration configuration) {
        WorkflowDefinitionRegistry<?> registry = configuration.getComponent(WorkflowDefinitionRegistry.class);
        workflowConfigurations
                .forEach(b -> registry.register(
                        b.buildStartCondition(configuration),
                        b.buildWorkflowConfiguration(configuration))
                );
    }

    @Override
    public LanguagePhase.WorkflowStateFactoryPhase<C> workflowContextFactory(
            @NotNull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory) {
        this.workflowContextFactory = Objects.requireNonNull(workflowContextFactory,
                                                             "Workflow context factory must no be null");
        return this;
    }

    @Override
    public WorkflowDefinitionPhase<C> workflowStateFactory(
            @NotNull ComponentBuilder<WorkflowStateFactory> workflowStateFactory) {
        this.workflowStateFactory = Objects.requireNonNull(workflowStateFactory,
                                                           "Workflow state factory must no be null");
        return this;
    }

    @Override
    public WorkflowModule<C> definitions(@NotNull Consumer<DetectionPhase<C>> definitions) {
        definitions.accept(this);
        return this;
    }

    @Override
    public OnPhase<C> declarative(@NotNull String name) {
        this.currentWorkflowName = Objects.requireNonNull(name, "Workflow name must not be null");
        return this;
    }

    @Override
    public DeclarativeDefinitionPhase<C> on(@NotNull ComponentBuilder<EventCondition> startCondition) {
        this.currentStartCondition = Objects.requireNonNull(startCondition, "Start condition must not be null");
        return this;
    }

    @Override
    public AssociationPhase<C> workflowDefinition(
            @NotNull ComponentBuilder<WorkflowDefinition<C>> workflowDefinition) {
        this.currentWorkflowDefinition = Objects.requireNonNull(workflowDefinition,
                                                                "Workflow definition must not be null");
        return this;
    }

    @Override
    public WorkflowCustomizationPhase<C> workflowIdProvider(
            @NotNull ComponentBuilder<AssociationProvider> workflowAssociationProvider) {
        this.currentWorkflowAssociationProvider = Objects.requireNonNull(workflowAssociationProvider,
                                                                         "Association provider must not be null");
        return this;
    }

    @Override
    public DetectionPhase<C> customized(
            @Nonnull BiFunction<Configuration, WorkflowModuleConfiguration, WorkflowModuleConfiguration> instanceCustomization
    ) {
        Objects.requireNonNull(instanceCustomization, "Customizations must not be null");
        this.workflowConfigurations.add(
                new WorkflowConfigurationBuilder<>(
                        this.currentWorkflowName,
                        // DSL
                        this.workflowContextFactory,
                        this.workflowStateFactory,
                        // Workflow
                        this.currentStartCondition,
                        this.currentWorkflowDefinition,
                        this.currentWorkflowAssociationProvider,
                        instanceCustomization
                )
        );

        this.currentWorkflowName = null;
        this.currentStartCondition = null;
        this.currentWorkflowDefinition = null;
        this.currentWorkflowAssociationProvider = null;

        return this;
    }

    private record WorkflowConfigurationBuilder<C extends WorkflowContext>(
            String workflowName,
            // DSL level
            ComponentBuilder<WorkflowContextFactory<C>> contextFactoryBuilder,
            ComponentBuilder<WorkflowStateFactory> stateFactoryBuilder,
            // workflow level
            ComponentBuilder<EventCondition> startConditionBuilder,
            ComponentBuilder<WorkflowDefinition<C>> definitionBuilder,
            ComponentBuilder<AssociationProvider> associationProviderBuilder,
            BiFunction<Configuration, WorkflowModuleConfiguration, WorkflowModuleConfiguration> instanceCustomization) {

        @Nonnull
        public EventCondition buildStartCondition(@Nonnull Configuration configuration) {
            return startConditionBuilder.build(configuration);
        }

        @Nonnull
        public WorkflowConfiguration<C> buildWorkflowConfiguration(@Nonnull Configuration configuration) {
            var workflowModuleConfiguration = instanceCustomization.apply(configuration,
                                                                          defaultConfiguration(workflowName,
                                                                                               configuration));
            return new WorkflowConfiguration<>() {
                @NotNull
                @Override
                public WorkflowDefinition<C> workflowDefinition() {
                    return definitionBuilder.build(configuration);
                }

                @NotNull
                @Override
                public AssociationProvider associationProvider() {
                    return associationProviderBuilder.build(configuration);
                }

                @NotNull
                @Override
                public WorkflowContextFactory<C> workflowContextFactory() {
                    return contextFactoryBuilder.build(configuration);
                }

                @NotNull
                @Override
                public WorkflowStateFactory workflowStateFactory() {
                    return stateFactoryBuilder.build(configuration);
                }

                @NotNull
                @Override
                public EventNameCustomizer eventNameCustomizer() {
                    return workflowModuleConfiguration.eventNameCustomizer;
                }
            };
        }
    }

    /**
     * Create default module configuration.
     *
     * @param workflowName  name of the workflow.
     * @param configuration configuration to use.
     * @return workflow module configuration.
     */
    private static WorkflowModuleConfiguration defaultConfiguration(@Nonnull String workflowName,
                                                                    @Nullable Configuration configuration) {
        return new WorkflowModuleConfiguration(workflowName, configuration);
    }
}
