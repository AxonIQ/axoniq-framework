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

import io.axoniq.workflow.runtime.api.*;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class SimpleWorkflowModule<C extends WorkflowContext> extends BaseModule<SimpleWorkflowModule<C>>
  implements WorkflowModule<C>,
  WorkflowModule.LanguagePhase.WorkflowContextFactoryPhase<C>,
  WorkflowModule.LanguagePhase.WorkflowStateFactoryPhase<C>,
  WorkflowModule.WorkflowDefinitionPhase<C>,
  WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<C>,
  WorkflowModule.WorkflowDefinitionPhase.OnPhase<C>,
  WorkflowModule.WorkflowDefinitionPhase.NamingPhase<C>,
  WorkflowModule.WorkflowDefinitionPhase.WorkflowDefinitionPhaseForWorkflow<C>,
  WorkflowModule.WorkflowDefinitionPhase.AssociationPhase<C>,
  WorkflowModule.WorkflowDefinitionPhase.WorkflowCustomizationPhase<C> {

  private final Class<C> workflowContextType;
  private ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory;
  private ComponentBuilder<WorkflowStateFactory> workflowStateFactory;

  private ComponentBuilder<WorkflowDefinition<C>> currentWorkflowDefinition;
  private ComponentBuilder<AssociationProvider> currentWorkflowAssociationProvider;
  private ComponentBuilder<EventNameCustomizerProvider> currentEventNameCustomizerProvider;
  private String currentWorkflowName;
  private ComponentBuilder<EventCondition> currentStartCondition;

  private final List<WorkflowConfigurationBuilder<?>> workflowConfigurations = new ArrayList<>();

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
  public LanguagePhase.WorkflowStateFactoryPhase<C> workflowContextFactory(@NotNull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory) {
    this.workflowContextFactory = Objects.requireNonNull(workflowContextFactory);
    return this;
  }

  @Override
  public WorkflowDefinitionPhase<C> workflowStateFactory(@NotNull ComponentBuilder<WorkflowStateFactory> workflowStateFactory) {
    this.workflowStateFactory = Objects.requireNonNull(workflowStateFactory);
    return this;
  }

  @Override
  public WorkflowModule<C> definitions(@NotNull Consumer<DefinitionPhase<C>> definitions) {
    definitions.accept(this);
    return this;
  }

  @Override
  public OnPhase<C> declarative(@NotNull String name) {
    this.currentWorkflowName = Objects.requireNonNull(name);
    return this;
  }

  @Override
  public NamingPhase<C> on(@NotNull ComponentBuilder<EventCondition> startCondition) {
    this.currentStartCondition = Objects.requireNonNull(startCondition);
    return this;
  }

  @Override
  public WorkflowDefinitionPhaseForWorkflow<C> workflowDefinition(@NotNull ComponentBuilder<WorkflowDefinition<C>> workflowDefinition) {
    this.currentWorkflowDefinition = Objects.requireNonNull(workflowDefinition);
    return this;
  }

  @Override
  public AssociationPhase<C> eventNameCustomizer(@NotNull ComponentBuilder<EventNameCustomizerProvider> eventNameCustomizerProvider) {
    this.currentEventNameCustomizerProvider = Objects.requireNonNull(eventNameCustomizerProvider);
    return this;
  }

  @Override
  public WorkflowCustomizationPhase<C> workflowIdProvider(@NotNull ComponentBuilder<AssociationProvider> workflowAssociationProvider) {
    this.currentWorkflowAssociationProvider = Objects.requireNonNull(workflowAssociationProvider);
    return this;
  }

  @Override
  public DefinitionPhase<C> notCustomized() {
    this.workflowConfigurations.add(new WorkflowConfigurationBuilder<C>(
      this.currentWorkflowName,
      this.currentStartCondition,
      this.currentWorkflowDefinition,
      this.currentWorkflowAssociationProvider,
      this.currentEventNameCustomizerProvider,
      this.workflowContextFactory,
      this.workflowStateFactory
    ));

    this.currentWorkflowName = null;
    this.currentStartCondition = null;
    this.currentWorkflowDefinition = null;
    this.currentWorkflowAssociationProvider = null;
    this.currentEventNameCustomizerProvider = null;

    return this;
  }

  private static class WorkflowConfigurationBuilder<C extends WorkflowContext> {
    private final String workflowName;
    private final ComponentBuilder<EventCondition> startConditionBuilder;
    private final ComponentBuilder<WorkflowDefinition<C>> definitionBuilder;
    private final ComponentBuilder<AssociationProvider> associationProviderBuilder;
    private final ComponentBuilder<EventNameCustomizerProvider> eventNameCustomizerProviderBuilder;
    private final ComponentBuilder<WorkflowContextFactory<C>> contextFactoryBuilder;
    private final ComponentBuilder<WorkflowStateFactory> stateFactoryBuilder;

    public WorkflowConfigurationBuilder(String workflowName,
                                        ComponentBuilder<EventCondition> startConditionBuilder,
                                        ComponentBuilder<WorkflowDefinition<C>> definitionBuilder,
                                        ComponentBuilder<AssociationProvider> associationProviderBuilder,
                                        ComponentBuilder<EventNameCustomizerProvider> eventNameCustomizerProviderBuilder,
                                        ComponentBuilder<WorkflowContextFactory<C>> contextFactoryBuilder,
                                        ComponentBuilder<WorkflowStateFactory> stateFactoryBuilder) {
      this.workflowName = workflowName;
      this.startConditionBuilder = startConditionBuilder;
      this.definitionBuilder = definitionBuilder;
      this.associationProviderBuilder = associationProviderBuilder;
      this.eventNameCustomizerProviderBuilder = eventNameCustomizerProviderBuilder;
      this.contextFactoryBuilder = contextFactoryBuilder;
      this.stateFactoryBuilder = stateFactoryBuilder;
    }

    @Nonnull
    public EventCondition buildStartCondition(@Nonnull Configuration configuration) {
      return startConditionBuilder.build(configuration);
    }

    @Nonnull
    public WorkflowConfiguration<C> buildWorkflowConfiguration(@Nonnull Configuration configuration) {
      return new WorkflowConfiguration<C>() {
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
          return eventNameCustomizerProviderBuilder.build(configuration).get();
        }
      };
    }
  }
}
