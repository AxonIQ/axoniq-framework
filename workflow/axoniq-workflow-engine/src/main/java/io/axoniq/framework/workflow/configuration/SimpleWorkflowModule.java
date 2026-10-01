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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.api.manager.WorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.EventHandlingComponentHandlingAny;
import io.axoniq.framework.workflow.runtime.execution.InMemoryWorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.SimpleWorkflowManager;
import io.axoniq.framework.workflow.runtime.execution.WorkflowCancellationService;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.framework.workflow.runtime.execution.WorkflowEngineCheckpointingSupport;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.execution.WorkflowStore;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.BaseModule;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SequenceOverridingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Workflow module used to create multiple {@link WorkflowConfiguration} (one per workflow definition) defined for the
 * given {@link WorkflowContext}. As a result, the module will register its configuration in the
 * {@link WorkflowConfigurationRegistry}, used by the
 * {@link io.axoniq.framework.workflow.runtime.execution.WorkflowEngine}.
 *
 * @param <C> type of workflow context
 * @author Simon Zambrovski
 * @author Steven van Beelen
 * @since 5.4.0
 */
@Internal
class SimpleWorkflowModule<C extends WorkflowContext>
        extends BaseModule<SimpleWorkflowModule<C>>
        implements WorkflowModule<C>,
        WorkflowModule.WorkflowConfigurationRegistryPhase<C>,
        WorkflowModule.WorkflowEngineEventProcessorPhase<C>,
        WorkflowModule.HistoryPhase<C>,
        WorkflowModule.WorkflowContextFactoryPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<C>,
        WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<C> {

    /**
     * Phase in which the engine's executions are dropped on shutdown: a workflow is a message handler, extensively
     * wrapped, so it is dropped where message handlers are.
     * <p>
     * Shutdown handlers run from the highest phase down, so this runs after the event processor has stopped at
     * {@link Phase#INBOUND_EVENT_CONNECTORS} and its drain has stored the token. Sharing the processor's own phase
     * would make the two race: clearing the repository first leaves the drain with nothing to hold the token back, and
     * it stores a position whose wakes were never applied.
     */
    private static final int POST_PROCESSOR_SHUTDOWN_PHASE = Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS;
    private static final int PRE_PROCESSOR_START_PHASE = Phase.INBOUND_EVENT_CONNECTORS - 10;

    private final Class<C> contextType;
    private final List<ComponentBuilder<List<ConditionedWorkflowConfiguration<C>>>> configBuilders = new ArrayList<>();

    private Function<PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration> engineConfigCustomizer =
            psepConfig -> psepConfig;
    private ComponentBuilder<WorkflowConfigurationRegistry<?>> configurationRegistry =
            config -> new SimpleWorkflowConfigurationRegistry();
    private boolean useHistory = true;
    private ComponentBuilder<MutableWorkflowHistoryRepository> historyRepository =
            config -> new InMemoryWorkflowHistoryRepository();
    private Function<PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration> historyConfigCustomizer =
            psepConfig -> psepConfig;
    @Nullable
    private ComponentBuilder<WorkflowContextFactory<C>> contextFactory;

    /**
     * Constructs a new workflow module with a given name.
     *
     * @param name                name of the workflow module
     * @param workflowContextType the type of {@link WorkflowContext} of the workflow module being constructed
     */
    @Internal
    SimpleWorkflowModule(String name, Class<C> workflowContextType) {
        super(name);
        this.contextType = requireNonNull(workflowContextType, "Workflow context type must not be null");
    }

    @Override
    public WorkflowConfigurationRegistryPhase<C> processorConfiguration(
            Function<PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration> processorConfiguration
    ) {
        this.engineConfigCustomizer =
                requireNonNull(processorConfiguration, "Processor configuration function must not be null.");
        return this;
    }

    @Override
    public HistoryPhase<C> configurationRegistry(
            ComponentBuilder<WorkflowConfigurationRegistry<?>> configurationRegistry
    ) {
        this.configurationRegistry =
                requireNonNull(configurationRegistry, "Workflow configuration registry must not be null.");
        return this;
    }

    @Override
    public WorkflowContextFactoryPhase<C> withHistory(
            ComponentBuilder<MutableWorkflowHistoryRepository> historyRepository
    ) {
        this.historyRepository = requireNonNull(historyRepository, "Workflow history repository must not be null");
        this.useHistory = true;
        return this;
    }

    @Override
    public WorkflowContextFactoryPhase<C> withoutHistory() {
        this.useHistory = false;
        return this;
    }

    @Override
    public HistoryPhase<C> historyProcessorConfiguration(
            Function<PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration> historyProcessorConfiguration
    ) {
        this.historyConfigCustomizer = requireNonNull(
                historyProcessorConfiguration, "History processor configuration function must not be null"
        );
        return this;
    }

    @Override
    public WorkflowDefinitionPhase<C> contextFactory(ComponentBuilder<WorkflowContextFactory<C>> contextFactory) {
        this.contextFactory = requireNonNull(contextFactory, "Workflow context factory must no be null");
        return this;
    }

    @Override
    public NamingPhase<C> declarative(ComponentBuilder<WorkflowDefinition<C>> componentBuilder) {
        return new DeclarativeWorkflowBuilder<>(this.contextType, this.contextFactory, this, componentBuilder);
    }

    @Override
    public FinalizedPhase<C> autodetected(ComponentBuilder<Object> componentBuilder) {
        return new AutoDetectingWorkflowBuilder<>(this.contextType, this.contextFactory, this, componentBuilder);
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
        requireNonNull(workflowConfigurationBuilder, "Workflow configuration builder must not be null");
        // Append rather than replace: a single module can host multiple workflow definitions (e.g. several
        // @Workflow beans of the same context type, including multiple version variants of one workflow).
        this.configBuilders.add(workflowConfigurationBuilder);
    }

    @Override
    public WorkflowModule<C> definition(Function<DetectionPhase<C>, FinalizedPhase<C>> definition) {
        definition.apply(this);
        return this;
    }

    @Override
    public Class<C> contextType() {
        return this.contextType;
    }

    @Override
    public Configuration build(Configuration parent, LifecycleRegistry lifecycleRegistry) {
        registerComponents();
        Configuration configuration = super.build(parent, lifecycleRegistry);
        registerWorkflowDefinitions(configuration);
        return configuration;
    }

    private void registerComponents() {
        componentRegistry(cr -> {
            cr.registerComponent(workflowConfigurationRegistry());
            cr.registerComponent(workflowExecutionRepository());
            cr.registerComponent(workflowCancellationService());
            cr.registerIfNotPresent(
                    WorkflowEngineCheckpointingSupport.class,
                    c -> new WorkflowEngineCheckpointingSupport(c.getComponent(WorkflowEngine.class, engineName()))
            );
            cr.registerComponent(workflowEngine());
            cr.registerComponent(workflowHistoryRepository());
            cr.registerComponent(workflowManager());
            cr.registerComponent(workflowSegmentChangeListener());
            cr.registerModule(workflowEngineEventProcessor());
            if (useHistory) {
                cr.registerComponent(historyProjector());
                cr.registerModule(historyEventProcessor());
            }
        });
    }

    private ComponentDefinition<WorkflowExecutionRepository> workflowExecutionRepository() {
        return ComponentDefinition.ofTypeAndName(WorkflowExecutionRepository.class, executionRepositoryName())
                                  .withBuilder(config -> new InMemoryWorkflowExecutionRepository());
    }

    private String executionRepositoryName() {
        return "WorkflowExecutionRepository[" + name + "]";
    }

    private ComponentDefinition<WorkflowConfigurationRegistry> workflowConfigurationRegistry() {
        return ComponentDefinition.ofTypeAndName(WorkflowConfigurationRegistry.class, configurationRegistryName())
                                  .withBuilder(configurationRegistry);
    }

    private String configurationRegistryName() {
        return "WorkflowConfigurationRegistry[" + name + "]";
    }

    private ComponentDefinition<WorkflowEngine> workflowEngine() {
        return ComponentDefinition.ofTypeAndName(WorkflowEngine.class, engineName())
                                  .withBuilder(c -> new WorkflowEngine(
                                          c.getComponent(
                                                  WorkflowConfigurationRegistry.class, configurationRegistryName()
                                          ),
                                          c.getComponent(WorkflowExecutionRepository.class, executionRepositoryName()),
                                          c.getComponent(WorkflowCancellationService.class, cancellationServiceName()),
                                          c.getComponent(WorkflowStore.class),
                                          c.getComponent(UnitOfWorkFactory.class)
                                  ))
                                  .onStart(Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS, (c, engine) -> {
                                      engine.setCheckpointingSupport(
                                              c.getComponent(WorkflowEngineCheckpointingSupport.class)
                                      );
                                  })
                                  .onShutdown(POST_PROCESSOR_SHUTDOWN_PHASE, WorkflowEngine::shutdown);
    }

    private String engineName() {
        return "WorkflowEngine[" + name + "]";
    }

    private ComponentDefinition<MutableWorkflowHistoryRepository> workflowHistoryRepository() {
        return ComponentDefinition.ofTypeAndName(MutableWorkflowHistoryRepository.class, historyRepositoryName())
                                  .withBuilder(historyRepository);
    }

    private String historyRepositoryName() {
        return "MutableWorkflowHistoryRepository[" + name + "]";
    }

    private ComponentDefinition<WorkflowManager> workflowManager() {
        return ComponentDefinition.ofTypeAndName(WorkflowManager.class, managerName())
                                  .withBuilder(c -> new SimpleWorkflowManager(
                                          c.getComponent(MutableWorkflowHistoryRepository.class, historyRepositoryName()),
                                          c.getComponent(WorkflowExecutionRepository.class, executionRepositoryName()),
                                          c.getComponent(WorkflowCancellationService.class, cancellationServiceName()),
                                          c.getComponent(WorkflowStore.class),
                                          c.getComponent(UnitOfWorkFactory.class),
                                          c.getComponent(
                                                  ExecutorService.class,
                                                  WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR
                                          )
                                  ));
    }

    private String managerName() {
        return "WorkflowManager[" + name + "]";
    }

    private ComponentDefinition<WorkflowSegmentChangeListener> workflowSegmentChangeListener() {
        return ComponentDefinition.ofTypeAndName(WorkflowSegmentChangeListener.class, segmentChangeListenerName())
                                  .withBuilder(c -> new WorkflowSegmentChangeListener(
                                          name,
                                          c.getComponent(UnitOfWorkFactory.class),
                                          () -> c.getComponent(WorkflowEngine.class, engineName())
                                  ));
    }

    private String segmentChangeListenerName() {
        return "WorkflowSegmentChangeListener[" + name + "]";
    }

    private ComponentDefinition<WorkflowCancellationService> workflowCancellationService() {
        return ComponentDefinition.ofTypeAndName(WorkflowCancellationService.class, cancellationServiceName())
                                  .withBuilder(c -> new WorkflowCancellationService());
    }

    private String cancellationServiceName() {
        return "WorkflowCancellationService[" + name + "]";
    }

    private PooledStreamingEventProcessorModule workflowEngineEventProcessor() {
        String engineName = engineName();
        return EventProcessorModule.pooledStreaming(name)
                                   .eventHandlingComponents(ehc -> ehc.declarative(
                                           engineName + "EventHandlingComponent",
                                           this::engineHandlingComponent
                                   ))
                                   .customized(this::workflowEngineProcessorConfig)
                                   .componentRegistry(componentRegistry -> componentRegistry.registerComponent(
                                           engineStartHandlerComponent(name, engineName)
                                   ))
                                   .build();
    }

    private EventHandlingComponent engineHandlingComponent(Configuration config) {
        WorkflowEngine workflowEngine = config.getComponent(WorkflowEngine.class, engineName());
        return new SequenceOverridingEventHandlingComponent(
                workflowEngine.segmentedRouting(),
                new EventHandlingComponentHandlingAny(
                        workflowEngine,
                        config.getComponent(WorkflowEngineCheckpointingSupport.class)
                )
        );
    }

    private PooledStreamingEventProcessorConfiguration workflowEngineProcessorConfig(
            Configuration config,
            PooledStreamingEventProcessorConfiguration psepConfig
    ) {
        return engineConfigCustomizer.apply(defaultProcessorConfig(config, psepConfig))
                                     // Start at the head of the stream. A workflow reacts to events published after
                                     // it was deployed. The pooled streaming default (first token) would start a
                                     // workflow for every historical start event, duplicating work still owned by
                                     // the process it replaces.
                                     .initialToken(source -> source.latestToken(null))
                                     .eventCriteria(SimpleWorkflowModule::eventCriteria)
                                     .addSegmentChangeListener(config.getComponent(
                                             WorkflowSegmentChangeListener.class, segmentChangeListenerName()
                                     ));
    }

    private ComponentDefinition<Object> engineStartHandlerComponent(String moduleName, String engineName) {
        return ComponentDefinition.ofTypeAndName(Object.class, moduleName + "EngineStartHook")
                                  .withInstance(new Object())
                                  .onStart(
                                          PRE_PROCESSOR_START_PHASE,
                                          (c, ignored) -> {
                                              requireEventStore(moduleName, c);
                                              WorkflowEngine engine = c.getComponent(WorkflowEngine.class, engineName);
                                              return c.getComponent(StreamableEventSource.class)
                                                      .latestToken(null)
                                                      .thenCompose(engine::start);
                                          }
                                  );
    }

    private void requireEventStore(String moduleName, Configuration config) {
        if (!config.hasComponent(EventStore.class)) {
            throw new AxonConfigurationException(
                    "The workflow engine of module " + moduleName + " requires an EventStore. A "
                            + "WorkflowConfigurer configures one through the EventSourcingConfigurer. The engine "
                            + "appends every workflow event under an AppendCondition, which only an event-store "
                            + "transaction carries."
            );
        }
    }

    private ComponentDefinition<WorkflowHistoryProjector> historyProjector() {
        return ComponentDefinition.ofTypeAndName(WorkflowHistoryProjector.class, historyProjectorName())
                                  .withBuilder(c -> new WorkflowHistoryProjector(c.getComponent(
                                          MutableWorkflowHistoryRepository.class, historyRepositoryName()
                                  )));
    }

    private PooledStreamingEventProcessorModule historyEventProcessor() {
        String historyProcessorName = historyProjectorName();
        return EventProcessorModule.pooledStreaming(historyProcessorName)
                                   .eventHandlingComponents(ehc -> ehc.declarative(
                                           historyProcessorName + "EventHandlingComponent",
                                           this::historyHandlingComponent
                                   ))
                                   .customized(this::historyProcessorConfig)
                                   .build();
    }

    private EventHandlingComponent historyHandlingComponent(Configuration config) {
        return new SequenceOverridingEventHandlingComponent(
                config.getComponent(WorkflowEngine.class, engineName())
                      .segmentedRouting(),
                new EventHandlingComponentHandlingAny(
                        config.getComponent(WorkflowHistoryProjector.class, historyProjectorName())
                )
        );
    }

    private String historyProjectorName() {
        return "WorkflowHistoryProjector[" + name + "]";
    }

    private PooledStreamingEventProcessorConfiguration historyProcessorConfig(
            Configuration config,
            PooledStreamingEventProcessorConfiguration psepConfig
    ) {
        return historyConfigCustomizer.apply(defaultProcessorConfig(config, psepConfig))
                                      .eventCriteria(SimpleWorkflowModule::eventCriteria);
    }

    /**
     * Mirrors {@code PooledStreamingEventProcessorsConfigurer}'s own default customization: fills in the ambient,
     * unnamed {@link StreamableEventSource} and {@link TokenStore} the application registered.
     * <p>
     * Runs before the caller's own {@code processorConfiguration}/{@code historyProcessorConfiguration} customization,
     * so a caller can still override either.
     */
    private static PooledStreamingEventProcessorConfiguration defaultProcessorConfig(
            Configuration config,
            PooledStreamingEventProcessorConfiguration psepConfig
    ) {
        config.getOptionalComponent(StreamableEventSource.class).ifPresent(psepConfig::eventSource);
        config.getOptionalComponent(TokenStore.class).ifPresent(psepConfig::tokenStore);
        return psepConfig;
    }

    private static EventCriteria eventCriteria(Set<QualifiedName> set) {
        return set.isEmpty() ? EventCriteria.havingAnyTag() : EventCriteria.havingAnyTag().andBeingOneOfTypes(set);
    }

    protected void registerWorkflowDefinitions(Configuration config) {
        WorkflowConfigurationRegistry<?> registry =
                config.getComponent(WorkflowConfigurationRegistry.class, configurationRegistryName());
        List<ConditionedWorkflowConfiguration<C>> workflowConfigs =
                configBuilders.stream()
                              .flatMap(b -> b.build(config).stream())
                              .toList();
        workflowConfigs.forEach(workflowConfig -> registry.register(
                workflowConfig.eventCondition(),
                workflowConfig.workflowConfiguration()
        ));
    }

    record ConditionedWorkflowConfiguration<C extends WorkflowContext>(
            EventCondition eventCondition,
            WorkflowConfiguration<C> workflowConfiguration
    ) {

    }
}
