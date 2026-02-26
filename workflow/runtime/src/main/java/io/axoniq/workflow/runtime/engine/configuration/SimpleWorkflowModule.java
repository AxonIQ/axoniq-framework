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

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.annotation.Workflow;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

/**
 * Workflow module used to create multiple {@link WorkflowConfiguration} (one per workflow definition) defined for the
 * given {@link WorkflowContext}. As a result the module will register its configuration in the
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
        WorkflowModule.LanguagePhase.WorkflowStateFactoryPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<C> {

    private final Class<C> workflowContextType;
    private final List<ComponentBuilder<List<ConditionedWorkflowConfiguration<C>>>> workflowConfigurations;
    private ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory;
    private ComponentBuilder<WorkflowStateFactory> workflowStateFactory;


    record ConditionedWorkflowConfiguration<C extends WorkflowContext>(
            EventCondition eventCondition,
            WorkflowConfiguration<C> workflowConfiguration
    ) {

    }

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
        this.workflowConfigurations = new ArrayList<>();
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
    public WorkflowModule<C> definitions(@NotNull UnaryOperator<DetectionPhase<C>> definitions) {
        definitions.apply(this);
        return this;
    }

    @Override
    public NamingPhase<C> declarative(@Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder) {
        return new DeclarativeComponentBuilder().declarative(componentBuilder);
    }

    @Override
    public DetectionPhase<C> autodetected(@Nonnull ComponentBuilder<Object> componentBuilder,
                                          @Nonnull Class<C> workflowContextType) {
        AutoDetectingBuilder autodetection = new AutoDetectingBuilder(componentBuilder);
        this.workflowConfigurations.add(autodetection);
        return autodetection;
    }


    protected void registerWorkflowDefinitions(@Nonnull Configuration configuration) {
        WorkflowConfigurationRegistry<?> registry = configuration.getComponent(WorkflowConfigurationRegistry.class);
        if (workflowConfigurations.isEmpty()) {
            // sanity
            throw new IllegalStateException(
                    "A module must define at least one workflow configuration, but none were registered.");
        }
        workflowConfigurations.forEach(b -> b.build(configuration)
                                             .forEach(x -> registry.register(x.eventCondition(),
                                                                             x.workflowConfiguration()))

        );
    }

    /**
     * Declarative component builder for workflow configuration.
     */
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
        public DetectionPhase<C> autodetected(@Nonnull ComponentBuilder<Object> componentBuilder,
                                              @Nonnull Class<C> workflowContextType) {
            return SimpleWorkflowModule.this.autodetected(componentBuilder, workflowContextType);
        }

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
        public WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<C> customized(
                @Nonnull BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
        ) {
            Objects.requireNonNull(instanceCustomization, "Customizations must not be null");
            var workflowName = this.currentWorkflowName;
            var startConditionBuilder = this.currentStartCondition;
            var definitionBuilder = this.currentWorkflowDefinition;
            workflowConfigurations.add(
                    c -> List.of(
                            new ComponentBuilder<ConditionedWorkflowConfiguration<?>>() {

                                @Override
                                public ConditionedWorkflowConfiguration<C> build(@Nonnull Configuration configuration) {
                                    var workflowModuleConfiguration = instanceCustomization.apply(configuration,
                                                                                                  WorkflowCustomization.defaultConfiguration(
                                                                                                          workflowName,
                                                                                                          configuration));

                                    return new ConditionedWorkflowConfiguration<>(
                                            startConditionBuilder.build(configuration),
                                            new WorkflowConfiguration<>() {

                                                @Nonnull
                                                @Override
                                                public String workflowName() {
                                                    return workflowName;
                                                }

                                                @NotNull
                                                @Override
                                                public WorkflowDefinition<C> workflowDefinition() {
                                                    return definitionBuilder.build(configuration);
                                                }

                                                @NotNull
                                                @Override
                                                public WorkflowContextFactory<C> workflowContextFactory() {
                                                    return workflowContextFactory.build(configuration);
                                                }

                                                @NotNull
                                                @Override
                                                public WorkflowStateFactory workflowStateFactory() {
                                                    return workflowStateFactory.build(configuration);
                                                }

                                                @NotNull
                                                @Override
                                                public WorkflowIdProvider workflowIdProvider() {
                                                    return workflowModuleConfiguration.workflowIdProvider;
                                                }

                                                @NotNull
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
                            }.build(c))
            );
            this.currentWorkflowName = null;
            this.currentStartCondition = null;
            this.currentWorkflowDefinition = null;
            return this;
        }
    }

    /**
     * Auto-detected component builder for workflow configuration list.
     * <p>One builder operates on one class and can detect multiple workflow methods.</p>
     */
    class AutoDetectingBuilder implements DetectionPhase<C>,
            ComponentBuilder<List<ConditionedWorkflowConfiguration<C>>> {

        private final ComponentBuilder<Object> instanceBuilder;

        public AutoDetectingBuilder(@Nonnull ComponentBuilder<Object> instanceBuilder) {
            this.instanceBuilder = Objects.requireNonNull(instanceBuilder, "Instance builder must not be null.");
        }

        @Override
        public List<ConditionedWorkflowConfiguration<C>> build(@Nonnull Configuration config) {
            var instance = instanceBuilder.build(config);
            var type = instance.getClass();
            var methodCandidates = ((Collection<Method>) ReflectionUtils.methodsOf(type));
            return methodCandidates
                    .stream()
                    .filter(AutoDetectionUtils.firstParameterOfType(workflowContextType)) // FIXME using parameter resolver
                    .map(AutoDetectionUtils.extractAnnotatedMethods(Workflow.class))
                    .filter(Objects::nonNull)
                    .map(t -> {

                        var attributes = t.attributes();
                        var method = t.method();

                        AutoDetectionUtils.validateAttributes(attributes, type, method);

                        var workflowName = AutoDetectionUtils.workflowName(type, attributes, method);
                        var namespaceCustomizer = AutoDetectionUtils.namespace(type, attributes);
                        var eventConditionBuilder = AutoDetectionUtils.eventConditionComponentBuilder(attributes);
                        var workflowIdProviderComponentBuilder = AutoDetectionUtils.workflowIdProviderComponentBuilder(
                                attributes);

                        return new ConditionedWorkflowConfiguration<>(
                                eventConditionBuilder.build(config),
                                new WorkflowConfiguration<C>() {
                                    @Nonnull
                                    @Override
                                    public WorkflowDefinition<C> workflowDefinition() {
                                        return workflowContext -> {
                                            try {
                                                method.invoke(instance, workflowContext);
                                            } catch (InvocationTargetException | IllegalAccessException e) {
                                                throw new RuntimeException(e.getCause());
                                            }
                                        };
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowContextFactory<C> workflowContextFactory() {
                                        return workflowContextFactory.build(config);
                                    }

                                    @Nonnull
                                    @Override
                                    public WorkflowStateFactory workflowStateFactory() {
                                        return workflowStateFactory.build(config);
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
                                        return workflowIdProviderComponentBuilder.build(config);
                                    }
                                }
                        );
                    })
                    .toList();
        }

        @Override
        public NamingPhase<C> declarative(@Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder) {
            return SimpleWorkflowModule.this.declarative(componentBuilder);
        }

        @Override
        public DetectionPhase<C> autodetected(@Nonnull ComponentBuilder<Object> componentBuilder,
                                              @Nonnull Class<C> workflowContextType) {
            return SimpleWorkflowModule.this.autodetected(componentBuilder, workflowContextType);
        }
    }
}
