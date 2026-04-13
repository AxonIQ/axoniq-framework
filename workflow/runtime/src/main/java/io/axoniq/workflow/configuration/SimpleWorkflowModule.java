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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.history.inmemory.WorkflowHistoryProjector;
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
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.util.WorkflowReflectionUtils;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.jspecify.annotations.NonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.axoniq.workflow.configuration.AutoDetectionUtils.*;
import static io.axoniq.workflow.configuration.WorkflowConfigurerDefaults.COMPONENT_WORKFLOW_ENGINE;
import static io.axoniq.workflow.configuration.WorkflowConfigurerDefaults.COMPONENT_WORKFLOW_HISTORY_PROJECTOR;

/**
 * Workflow module used to create multiple {@link WorkflowConfiguration} (one per workflow definition) defined for the
 * given {@link WorkflowContext}. As a result the module will register its configuration in the
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
     * @param name name of the workflow module.
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
     * @param defaultConfiguration flag if the module should use default configuration.
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
        return new DeclarativeComponentBuilder(this, componentBuilder);
    }

    @Override
    public FinalizedPhase<C> autodetected(@Nonnull ComponentBuilder<Object> componentBuilder) {
        return new AutoDetectingBuilder(this, componentBuilder);
    }

    /**
     * Declarative component builder for workflow configuration.
     */
    @Internal
    class DeclarativeComponentBuilder implements
            WorkflowModule.WorkflowDefinitionPhase.NamingPhase<C>,
            WorkflowModule.WorkflowDefinitionPhase.OnPhase<C>,
            WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> {

        private final ComponentBuilder<WorkflowDefinition<C>> definitionBuilder;
        private final SimpleWorkflowModule<C> parent;
        private ComponentBuilder<EventCondition> startConditionBuilder;
        private String workflowName;

        public DeclarativeComponentBuilder(
                @Nonnull SimpleWorkflowModule<C> parent,
                @Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder
        ) {
            this.parent = parent;
            this.definitionBuilder = Objects.requireNonNull(componentBuilder,
                                                            "Workflow definition builder must not be null");
        }

        @Override
        public WorkflowModule.WorkflowDefinitionPhase.OnPhase<C> workflowName(@Nonnull String workflowName) {
            this.workflowName = Objects.requireNonNull(workflowName, "Workflow name must not be null");
            return this;
        }

        @Override
        public WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> on(
                @Nonnull ComponentBuilder<EventCondition> startCondition) {
            this.startConditionBuilder = Objects.requireNonNull(startCondition, "Start condition must not be null");
            return this;
        }

        @Override
        public FinalizedPhase<C> customized(
                @Nonnull BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
        ) {
            Objects.requireNonNull(instanceCustomization, "Customizations must not be null");
            workflowConfigurationBuilder = c -> List.of(
                    new ComponentBuilder<ConditionedWorkflowConfiguration<?>>() {

                        @Override
                        @Nonnull
                        public ConditionedWorkflowConfiguration<C> build(@Nonnull Configuration configuration) {
                            var workflowModuleConfiguration = instanceCustomization.apply(
                                    configuration,
                                    WorkflowCustomization.defaultConfiguration(workflowName, configuration)
                            );

                            return new ConditionedWorkflowConfiguration<>(
                                    startConditionBuilder.build(configuration),
                                    new WorkflowConfiguration<>() {

                                        @Nonnull
                                        @Override
                                        public String workflowName() {
                                            return workflowName;
                                        }

                                        @Override
                                        public Class<C> getWorkflowContextType() {
                                            return parent.workflowContextType;
                                        }

                                        @Nonnull
                                        @Override
                                        public WorkflowDefinition<C> workflowDefinition() {
                                            return definitionBuilder.build(configuration);
                                        }

                                        @Nonnull
                                        @Override
                                        public WorkflowContextFactory<C> workflowContextFactory() {
                                            return workflowContextFactory.build(configuration);
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
            return parent;
        }
    }

    /**
     * Auto-detected component builder for a workflow configuration list.
     * <p>One builder operates on one class and can detect multiple workflow methods.</p>
     */
    class AutoDetectingBuilder implements FinalizedPhase<C> {

        public AutoDetectingBuilder(
                @Nonnull SimpleWorkflowModule<C> parent,
                @Nonnull ComponentBuilder<Object> instanceBuilder
        ) {
            Objects.requireNonNull(instanceBuilder, "Instance builder must not be null.");

            SimpleWorkflowModule.this.workflowConfigurationBuilder = configuration -> {
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

                            return new ConditionedWorkflowConfiguration<>(
                                    eventConditionBuilder.build(configuration),
                                    new WorkflowConfiguration<C>() {
                                        @Override
                                        public Class<C> getWorkflowContextType() {
                                            return parent.workflowContextType;
                                        }

                                        @Nonnull
                                        @Override
                                        public WorkflowDefinition<C> workflowDefinition() {
                                            return workflowContext -> {
                                                WorkflowReflectionUtils.invoke(instance,
                                                                               method,
                                                                               workflowContext);
                                            };
                                        }

                                        @Nonnull
                                        @Override
                                        public WorkflowContextFactory<C> workflowContextFactory() {
                                            return workflowContextFactory.build(configuration);
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
        }
    }
}
