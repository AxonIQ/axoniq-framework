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

import io.axoniq.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.COMPONENT_WORKFLOW_ENGINE;
import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.COMPONENT_WORKFLOW_HISTORY_PROJECTOR;

/**
 * Workflow module used to create multiple {@link WorkflowConfiguration} (one per workflow definition) defined for the
 * given {@link WorkflowContext}. As a result, the module will register its configuration in the
 * {@link WorkflowConfigurationRegistry}, used by the {@link io.axoniq.workflow.runtime.execution.WorkflowEngine}.
 *
 * @param <C> type of workflow context.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
class SimpleWorkflowModule<C extends WorkflowContext>
        extends BaseModule<SimpleWorkflowModule<C>>
        implements WorkflowModule<C>,
        WorkflowModule.ConfigurationPhase.WorkflowConfigurationRegistryPhase<C>,
        WorkflowModule.ConfigurationPhase.WorkflowExecutionRepositoryPhase<C>,
        WorkflowModule.ConfigurationPhase.HistoryPhase<C>,
        WorkflowModule.LanguagePhase.WorkflowContextFactoryPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<C> {

    private final String name;
    private final Class<C> workflowContextType;
    private final boolean defaultConfiguration;
    private ComponentBuilder<WorkflowConfigurationRegistry<?>> workflowConfigurationRegistryBuilder;
    private ComponentBuilder<WorkflowExecutionRepository> workflowExecutionRepositoryBuilder;
    private boolean useHistory = true;
    private ComponentBuilder<WorkflowHistoryProjector> workflowHistoryProjectorBuilder;
    private ComponentBuilder<List<ConditionedWorkflowConfiguration<C>>> workflowConfigurationBuilder;
    private ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory;


    record ConditionedWorkflowConfiguration<C extends WorkflowContext>(
            EventCondition eventCondition,
            WorkflowConfiguration<C> workflowConfiguration
    ) {

    }

    /**
     * Constructs a new workflow module with a given name.
     *
     * @param name name of the workflow module
     * @param workflowContextType the type of {@link WorkflowContext} of the workflow module being constructred
    */
    @Internal
    SimpleWorkflowModule(@Nonnull String name, @Nonnull Class<C> workflowContextType) {
        this(name, workflowContextType, false);
    }

    /**
     * Constructs a new workflow module with a given name.
     *
     * @param name                 name of the workflow module.
     * @param workflowContextType  workflow context class.
     * @param defaultConfiguration flag if the module should use the default configuration.
     */
    @Internal
    SimpleWorkflowModule(@Nonnull String name, @Nonnull Class<C> workflowContextType, boolean defaultConfiguration) {
        super(name);
        this.name = name;
        this.workflowContextType = Objects.requireNonNull(workflowContextType,
                                                          "Workflow context type must not be null");
        this.defaultConfiguration = defaultConfiguration;
    }

    @Override
    @Nonnull
    public Configuration build(
            @Nonnull Configuration parent,
            @Nonnull LifecycleRegistry lifecycleRegistry
    ) {
        componentRegistry(cr -> {
            if (!defaultConfiguration) {
                if (workflowConfigurationRegistryBuilder != null) {
                    cr.registerComponent(
                            WorkflowConfigurationRegistry.class,
                            (ComponentBuilder) workflowConfigurationRegistryBuilder
                    );
                }
                if (workflowExecutionRepositoryBuilder != null) {
                    cr.registerComponent(
                            WorkflowExecutionRepository.class,
                            workflowExecutionRepositoryBuilder
                    );
                }

                cr.registerComponent(
                        WorkflowEngine.class,
                        COMPONENT_WORKFLOW_ENGINE,
                        cfg -> new WorkflowEngine(
                                cfg.getComponent(WorkflowConfigurationRegistry.class),
                                cfg.getComponent(WorkflowExecutionRepository.class)
                        )
                );

                if (workflowHistoryProjectorBuilder != null) {
                    cr.registerComponent(
                            WorkflowHistoryProjector.class,
                            COMPONENT_WORKFLOW_HISTORY_PROJECTOR,
                            workflowHistoryProjectorBuilder
                    );
                }

                cr.registerEnhancer(new WorkflowEventProcessingRegistrationEnhancer(
                        name,
                        COMPONENT_WORKFLOW_ENGINE,
                        useHistory
                                ? COMPONENT_WORKFLOW_HISTORY_PROJECTOR
                                : null,
                        useHistory
                ));
            }
        });
        Configuration configuration = super.build(parent, lifecycleRegistry);
        registerWorkflowDefinitions(configuration);
        return configuration;
    }

    protected void registerWorkflowDefinitions(@Nonnull Configuration configuration) {
        WorkflowConfigurationRegistry<?> registry = configuration.getComponent(WorkflowConfigurationRegistry.class);
        var built = workflowConfigurationBuilder.build(configuration);
        built.forEach(workflowConfig -> {
                          registry.register(
                                  workflowConfig.eventCondition(),
                                  workflowConfig.workflowConfiguration()
                          );
                      }

        );
    }

    @Override
    public ConfigurationPhase.WorkflowExecutionRepositoryPhase<C> workflowConfigurationRegistry(
            @Nonnull ComponentBuilder<WorkflowConfigurationRegistry<?>> workflowConfigurationRegistry) {
        this.workflowConfigurationRegistryBuilder = Objects.requireNonNull(workflowConfigurationRegistry,
                                                                           "Workflow configuration registry must not be null");
        return this;
    }

    @Override
    public ConfigurationPhase.HistoryPhase<C> workflowExecutionRepository(
            @Nonnull ComponentBuilder<WorkflowExecutionRepository> workflowExecutionRepository) {
        this.workflowExecutionRepositoryBuilder = Objects.requireNonNull(workflowExecutionRepository,
                                                                         "Workflow execution repository must not be null");
        return this;
    }

    @Override
    public LanguagePhase.WorkflowContextFactoryPhase<C> withHistory(
            @Nonnull ComponentBuilder<WorkflowHistoryProjector> workflowHistoryProjector) {
        this.workflowHistoryProjectorBuilder = Objects.requireNonNull(workflowHistoryProjector,
                                                                      "Workflow history projector must not be null");
        this.useHistory = true;
        return this;
    }

    @Override
    public LanguagePhase.WorkflowContextFactoryPhase<C> withoutHistory() {
        this.useHistory = false;
        return this;
    }

    @Override
    public Class<C> getWorkflowContextType() {
        return this.workflowContextType;
    }


    @Override
    public WorkflowDefinitionPhase<C> workflowContextFactory(
            @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory) {
        this.workflowContextFactory = Objects.requireNonNull(workflowContextFactory,
                                                             "Workflow context factory must no be null");
        return this;
    }

    @Override
    public WorkflowModule<C> definition(@Nonnull Function<DetectionPhase<C>, FinalizedPhase<C>> definition) {
        definition.apply(this);
        return this;
    }

    @Override
    public NamingPhase<C> declarative(@Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder) {
        return new DeclarativeWorkflowBuilder<>(this.workflowContextType,
                                                this.workflowContextFactory,
                                                this,
                                                componentBuilder);
    }

    @Override
    public FinalizedPhase<C> autodetected(@Nonnull ComponentBuilder<Object> componentBuilder) {
        return new AutoDetectingWorkflowBuilder<>(this.workflowContextType,
                                                  this.workflowContextFactory,
                                                  this,
                                                  componentBuilder);
    }

    /**
     * Sets the workflow configuration builder for this module.
     * <p>Called by the {@link AutoDetectingWorkflowBuilder} and {@link DeclarativeWorkflowBuilder}</p>
     *
     * @param workflowConfigurationBuilder workflow configuration builder.
     */
    @Internal
    public void workflowConfigurationBuilder(
            @Nonnull ComponentBuilder<List<ConditionedWorkflowConfiguration<C>>> workflowConfigurationBuilder) {
        this.workflowConfigurationBuilder = Objects.requireNonNull(workflowConfigurationBuilder,
                                                                   "Workflow configuration builder must not be null");
    }
}
