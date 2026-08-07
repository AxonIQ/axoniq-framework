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
import io.axoniq.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.execution.WorkflowStore;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
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
    /**
     * Workflow-definition builders appended during configuration; concatenated at build time.
     */
    private final List<ComponentBuilder<List<ConditionedWorkflowConfiguration<C>>>> workflowConfigurationBuilders = new ArrayList<>();

    @Nullable
    private ComponentBuilder<WorkflowConfigurationRegistry<?>> workflowConfigurationRegistryBuilder;
    @Nullable
    private ComponentBuilder<WorkflowExecutionRepository> workflowExecutionRepositoryBuilder;
    private boolean useHistory = true;
    @Nullable
    private ComponentBuilder<WorkflowHistoryProjector> workflowHistoryProjectorBuilder;
    @Nullable
    private ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory;

    /**
     * Constructs a new workflow module with a given name.
     *
     * @param name                name of the workflow module
     * @param workflowContextType the type of {@link WorkflowContext} of the workflow module being constructed
     */
    @Internal
    SimpleWorkflowModule(String name, Class<C> workflowContextType) {
        this(name, workflowContextType, false);
    }

    /**
     * Constructs a new workflow module with a given name.
     *
     * @param name                 name of the workflow module
     * @param workflowContextType  workflow context class
     * @param defaultConfiguration flag if the module should use the default configuration
     */
    @Internal
    SimpleWorkflowModule(String name, Class<C> workflowContextType, boolean defaultConfiguration) {
        super(name);
        this.name = name;
        this.workflowContextType = Objects.requireNonNull(
                workflowContextType, "Workflow context type must not be null"
        );
        this.defaultConfiguration = defaultConfiguration;
    }

    @Override
    public Configuration build(Configuration parent, LifecycleRegistry lifecycleRegistry) {
        if (!defaultConfiguration) {
            registerGivenComponents();
        }
        Configuration configuration = super.build(parent, lifecycleRegistry);
        registerWorkflowDefinitions(configuration);
        return configuration;
    }

    /**
     * Invoked whenever the {@link #SimpleWorkflowModule(String, Class, boolean)} constructed has {@code false} for the
     * {@code defaultConfiguration} parameter. When it's set to {@code false}, user may have invoked any of the manual
     * configuration steps to override their desired settings.
     */
    private void registerGivenComponents() {
        componentRegistry(cr -> {
            if (workflowConfigurationRegistryBuilder != null) {
                //noinspection unchecked,rawtypes
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
            cr.registerIfNotPresent(WorkflowCancellationService.class,
                                    cfg -> new WorkflowCancellationService());

            cr.registerComponent(
                    WorkflowEngine.class,
                    COMPONENT_WORKFLOW_ENGINE,
                    cfg -> new WorkflowEngine(
                            cfg.getComponent(WorkflowConfigurationRegistry.class),
                            cfg.getComponent(WorkflowExecutionRepository.class),
                            cfg.getComponent(WorkflowCancellationService.class),
                            cfg.getComponent(WorkflowStore.class),
                            cfg.getComponent(UnitOfWorkFactory.class)
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
                    useHistory ? COMPONENT_WORKFLOW_HISTORY_PROJECTOR : null,
                    useHistory
            ));
        });
    }

    protected void registerWorkflowDefinitions(Configuration configuration) {
        WorkflowConfigurationRegistry<?> registry = configuration.getComponent(WorkflowConfigurationRegistry.class);
        List<ConditionedWorkflowConfiguration<C>> workflowConfigs = workflowConfigurationBuilders
                .stream()
                .flatMap(b -> b.build(configuration).stream())
                .toList();
        workflowConfigs.forEach(workflowConfig -> registry.register(
                workflowConfig.eventCondition(),
                workflowConfig.workflowConfiguration()
        ));
    }

    @Override
    public ConfigurationPhase.WorkflowExecutionRepositoryPhase<C> workflowConfigurationRegistry(
            ComponentBuilder<WorkflowConfigurationRegistry<?>> workflowConfigurationRegistry
    ) {
        this.workflowConfigurationRegistryBuilder = Objects.requireNonNull(
                workflowConfigurationRegistry, "Workflow configuration registry must not be null"
        );
        return this;
    }

    @Override
    public ConfigurationPhase.HistoryPhase<C> workflowExecutionRepository(
            ComponentBuilder<WorkflowExecutionRepository> workflowExecutionRepository
    ) {
        this.workflowExecutionRepositoryBuilder = Objects.requireNonNull(
                workflowExecutionRepository, "Workflow execution repository must not be null"
        );
        return this;
    }

    @Override
    public LanguagePhase.WorkflowContextFactoryPhase<C> withHistory(
            ComponentBuilder<WorkflowHistoryProjector> workflowHistoryProjector
    ) {
        this.workflowHistoryProjectorBuilder = Objects.requireNonNull(
                workflowHistoryProjector, "Workflow history projector must not be null"
        );
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
            ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory
    ) {
        this.workflowContextFactory = Objects.requireNonNull(
                workflowContextFactory, "Workflow context factory must no be null"
        );
        return this;
    }

    @Override
    public WorkflowModule<C> definition(Function<DetectionPhase<C>, FinalizedPhase<C>> definition) {
        definition.apply(this);
        return this;
    }

    @Override
    public NamingPhase<C> declarative(ComponentBuilder<WorkflowDefinition<C>> componentBuilder) {
        return new DeclarativeWorkflowBuilder<>(
                this.workflowContextType, this.workflowContextFactory, this, componentBuilder
        );
    }

    @Override
    public FinalizedPhase<C> autodetected(ComponentBuilder<Object> componentBuilder) {
        return new AutoDetectingWorkflowBuilder<>(
                this.workflowContextType, this.workflowContextFactory, this, componentBuilder
        );
    }

    /**
     * Sets the workflow configuration builder for this module.
     * <p>Called by the {@link AutoDetectingWorkflowBuilder} and {@link DeclarativeWorkflowBuilder}</p>
     *
     * @param workflowConfigurationBuilder workflow configuration builder.
     */
    @Internal
    public void workflowConfigurationBuilder(
            ComponentBuilder<List<ConditionedWorkflowConfiguration<C>>> workflowConfigurationBuilder
    ) {
        Objects.requireNonNull(workflowConfigurationBuilder, "Workflow configuration builder must not be null");
        // Append rather than replace: a single module can host multiple workflow definitions (e.g. several
        // @Workflow beans of the same context type, including multiple version variants of one workflow).
        this.workflowConfigurationBuilders.add(workflowConfigurationBuilder);
    }

    record ConditionedWorkflowConfiguration<C extends WorkflowContext>(
            EventCondition eventCondition,
            WorkflowConfiguration<C> workflowConfiguration
    ) {

    }
}
