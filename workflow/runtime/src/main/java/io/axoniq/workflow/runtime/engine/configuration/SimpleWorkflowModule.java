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

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.execution.DSLAdoptingExecutionFactory;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecutionRepository;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.history.MutableWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.engine.history.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.engine.util.WorkflowReflectionUtils;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.engine.configuration.AutoDetectionUtils.*;

/**
 * Workflow module used to create multiple {@link WorkflowConfiguration} (one per workflow definition) defined for the
 * given {@link WorkflowContext}. As a result, the module will register its configuration in the
 * {@link WorkflowConfigurationRegistry}, used by the {@link io.axoniq.workflow.runtime.engine.impl.WorkflowEngine}.
 *
 * @param <C> type of workflow context.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
class SimpleWorkflowModule<C extends WorkflowContext> extends BaseModule<SimpleWorkflowModule<C>>
        implements WorkflowModule<C>,
        WorkflowModule.LanguagePhase.WorkflowContextFactoryPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<C> {

    private final Class<C> workflowContextType;
    private final ComponentBuilder<WorkflowExecutionFactory> workflowExecutionFactory;
    private ComponentBuilder<ConditionedWorkflowConfiguration<C>> workflowConfiguration;
    private ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory;


    record ConditionedWorkflowConfiguration<C extends WorkflowContext>(
            EventCondition eventCondition,
            WorkflowConfiguration<C> workflowConfiguration
    ) {

    }

    /**
     * Constructs a new workflow module.
     *
     * @param workflowContextType workflow context class.
     */
    @Internal
    SimpleWorkflowModule(@Nonnull Class<C> workflowContextType) {
        this(workflowContextType.getName(), workflowContextType);
    }

    /**
     * Constructs a new workflow module with a given name.
     *
     * @param name                name of the workflow module.
     * @param workflowContextType workflow context class.
     */
    @Internal
    SimpleWorkflowModule(@Nonnull String name, @Nonnull Class<C> workflowContextType) {
        super(name);
        this.workflowContextType = workflowContextType;
        this.workflowExecutionFactory = c -> new DSLAdoptingExecutionFactory<>(workflowContextType);
    }

    @Override
    public Class<C> getContextType() {
        return this.workflowContextType;
    }


    @Override
    @Nonnull
    public Configuration build(@Nonnull Configuration parent, @Nonnull LifecycleRegistry lifecycleRegistry) {
        Configuration configuration = super.build(parent, lifecycleRegistry);

        lifecycleRegistry.onStart(WorkflowConfigurerDefaults.WORKFLOW_DEFAULTS_ENHANCER_ORDER + 1,
                                  () -> registerWorkflowDefinitions(configuration));
        return configuration;
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase<C> workflowContextFactory(
            @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory) {
        this.workflowContextFactory = Objects.requireNonNull(workflowContextFactory,
                                                             "Workflow context factory must no be null");
        return this;
    }

    @Override
    public WorkflowModule<C> definition(@Nonnull Consumer<DetectionPhase<C>> definition) {
        definition.accept(this);
        return this;
    }

    @Override
    public WorkflowModule.WorkflowDefinitionPhase.NamingPhase<C> declarative(
            @Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder) {
        return new DeclarativeComponentBuilder().declarative(componentBuilder);
    }

    protected void registerWorkflowDefinitions(@Nonnull Configuration configuration) {
        final WorkflowConfigurationRegistry<?> registry = configuration.getComponent(WorkflowConfigurationRegistry.class);
        if (workflowConfiguration != null) {
            var x = workflowConfiguration.build(configuration);
            registry.register(x.eventCondition(), x.workflowConfiguration());
        }
    }

    @Internal
    class DeclarativeComponentBuilder implements
            WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<C>,
            WorkflowModule.WorkflowDefinitionPhase.NamingPhase<C>,
            WorkflowModule.WorkflowDefinitionPhase.OnPhase<C>,
            WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> {

        private ComponentBuilder<WorkflowDefinition<C>> currentWorkflowDefinition;
        private ComponentBuilder<EventCondition> currentStartCondition;
        private String currentWorkflowName;

        @Override
        public WorkflowModule.WorkflowDefinitionPhase.NamingPhase<C> declarative(
                @Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder) {
            this.currentWorkflowDefinition = Objects.requireNonNull(componentBuilder,
                                                                    "Workflow definition builder must not be null");
            return this;
        }

        @Override
        public WorkflowModule.WorkflowDefinitionPhase.OnPhase<C> workflowName(@Nonnull String workflowName) {
            this.currentWorkflowName = Objects.requireNonNull(workflowName, "Workflow name must not be null");
            return this;
        }

        @Override
        public WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> on(
                @Nonnull ComponentBuilder<EventCondition> startCondition) {
            this.currentStartCondition = Objects.requireNonNull(startCondition, "Start condition must not be null");
            return this;
        }

        @Override
        public WorkflowModule<C> customized(
                @Nonnull BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
        ) {
            Objects.requireNonNull(instanceCustomization, "Customizations must not be null");
            var workflowName = this.currentWorkflowName;
            var startConditionBuilder = this.currentStartCondition;
            var definitionBuilder = this.currentWorkflowDefinition;
            workflowConfiguration =
                    c -> {
                        var workflowModuleConfiguration = instanceCustomization.apply(c,
                                                                                      WorkflowCustomization.defaultConfiguration(
                                                                                              workflowName,
                                                                                              c));

                        return new ConditionedWorkflowConfiguration<>(
                                startConditionBuilder.build(c),
                                new WorkflowConfiguration<>() {

                                    @Nonnull
                                    @Override
                                    public String workflowName() {
                                        return workflowName;
                                    }

                                    @Override
                                    public WorkflowDefinition<C> workflowDefinition() {
                                        return definitionBuilder.build(c);
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowContextFactory<C> workflowContextFactory() {
                                        return workflowContextFactory.build(c);
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowExecutionFactory workflowExecutionFactory() {
                                        return workflowExecutionFactory.build(c);
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

                                    @Nonnull
                                    @Override
                                    public WorkflowEngine workflowEngine() {
                                        return workflowModuleConfiguration.workflowEngine;
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowExecutionRepository workflowExecutionRepository() {
                                        return workflowModuleConfiguration.workflowExecutionRepository;
                                    }

                                    @Nonnull
                                    @Override
                                    public MutableWorkflowHistoryRepository workflowHistoryRepository() {
                                        return workflowModuleConfiguration.workflowHistoryRepository;
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowHistoryProjector workflowHistoryProjector() {
                                        return workflowModuleConfiguration.workflowHistoryProjector;
                                    }
                                }
                        );
                    };
            this.currentWorkflowName = null;
            this.currentStartCondition = null;
            this.currentWorkflowDefinition = null;
            return SimpleWorkflowModule.this;
        }
    }

    /**
     * Registers a workflow detected via reflection.
     *
     * @param workflowName                       workflow name.
     * @param eventConditionBuilder              start condition builder.
     * @param instanceBuilder                    builder for the instance containing the workflow method.
     * @param method                             workflow method.
     * @param namespaceCustomizer                event name customizer.
     * @param workflowIdProviderComponentBuilder id provider builder.
     * @param statusChangeListeners              status change listeners.
     */
    void registerDetectedWorkflow(
            @Nonnull String workflowName,
            @Nonnull ComponentBuilder<EventCondition> eventConditionBuilder,
            @Nonnull ComponentBuilder<Object> instanceBuilder,
            @Nonnull java.lang.reflect.Method method,
            @Nonnull EventNameCustomizer namespaceCustomizer,
            @Nonnull ComponentBuilder<WorkflowIdProvider> workflowIdProviderComponentBuilder,
            @Nonnull Map<WorkflowStatus, CompositeWorkflowStatusChangeListener> statusChangeListeners
    ) {
        this.workflowConfiguration = config -> {
            var instance = instanceBuilder.build(config);
            var workflowModuleConfiguration = WorkflowCustomization.defaultConfiguration(workflowName, config);
            workflowModuleConfiguration.eventNameCustomizer(namespaceCustomizer);
            workflowModuleConfiguration.workflowIdProvider(workflowIdProviderComponentBuilder.build(config));
            statusChangeListeners.forEach((k, v) -> workflowModuleConfiguration.registerWorkflowStatusChangeListener(k,
                                                                                                                     v));

            return new ConditionedWorkflowConfiguration<>(
                    eventConditionBuilder.build(config),
                    new WorkflowConfiguration<C>() {

                        @Override
                        public WorkflowDefinition<C> workflowDefinition() {
                            return workflowContext -> WorkflowReflectionUtils.invoke(instance,
                                                                                     method,
                                                                                     (Object) workflowContext);
                        }

                        @Nonnull
                        @Override
                        public WorkflowContextFactory<C> workflowContextFactory() {
                            return workflowContextFactory.build(config);
                        }

                        @Nonnull
                        @Override
                        public WorkflowExecutionFactory workflowExecutionFactory() {
                            return workflowExecutionFactory.build(config);
                        }

                        @Nonnull
                        @Override
                        public String workflowName() {
                            return workflowName;
                        }

                        @Nonnull
                        @Override
                        public EventNameCustomizer eventNameCustomizer() {
                            return workflowModuleConfiguration.eventNameCustomizer;
                        }

                        @Nonnull
                        @Override
                        public WorkflowIdProvider workflowIdProvider() {
                            return workflowModuleConfiguration.workflowIdProvider;
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

                        @Nonnull
                        @Override
                        public WorkflowEngine workflowEngine() {
                            return workflowModuleConfiguration.workflowEngine;
                        }

                        @Nonnull
                        @Override
                        public WorkflowExecutionRepository workflowExecutionRepository() {
                            return workflowModuleConfiguration.workflowExecutionRepository;
                        }

                        @Nonnull
                        @Override
                        public MutableWorkflowHistoryRepository workflowHistoryRepository() {
                            return workflowModuleConfiguration.workflowHistoryRepository;
                        }

                        @Nonnull
                        @Override
                        public WorkflowHistoryProjector workflowHistoryProjector() {
                            return workflowModuleConfiguration.workflowHistoryProjector;
                        }
                    }
            );
        };
    }
}
