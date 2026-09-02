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
import io.axoniq.workflow.runtime.execution.EventHandlingComponentHandlingAny;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowEngineCheckpointingSupport;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer.CompletePhase;
import org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer.RequiredComponentPhase;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SequenceOverridingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Enhancer for registration of the workflow engine event processing.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
@Internal
public class WorkflowEventProcessingRegistrationEnhancer implements ConfigurationEnhancer {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowEventProcessingRegistrationEnhancer.class);

    private static final int PRE_PROCESSOR_START_PHASE = Phase.INBOUND_EVENT_CONNECTORS - 10;

    /**
     * Name of the top-level workflow module.
     */
    public static final String DEFAULT_MODULE_NAME = "Workflow";

    private final String moduleName;
    @Nullable
    private final String engineComponentName;
    @Nullable
    private final String projectorComponentName;
    private final boolean registerHistoryProjector;
    @Nullable
    private final Integer initialSegmentCount;

    /**
     * Creates a workflow event processing registration enhancer leaving the segment count to the event processing
     * configuration, responsible for registering the workflow engine and history projector components to the event
     * processing module.
     *
     * @param moduleName               name of the event processing module
     * @param engineComponentName      name of the workflow engine component
     * @param projectorComponentName   name of the workflow history projector component
     * @param registerHistoryProjector flag indicating whether to register the history projector component
     */
    public WorkflowEventProcessingRegistrationEnhancer(
            String moduleName,
            @Nullable String engineComponentName,
            @Nullable String projectorComponentName,
            boolean registerHistoryProjector
    ) {
        this(moduleName, engineComponentName, projectorComponentName, registerHistoryProjector, null);
    }

    /**
     * Creates a workflow event processing registration enhancer, responsible for registering the workflow engine and
     * history projector components to the event processing module.
     *
     * @param moduleName               name of the event processing module
     * @param engineComponentName      name of the workflow engine component
     * @param projectorComponentName   name of the workflow history projector component
     * @param registerHistoryProjector flag indicating whether to register the history projector component
     * @param initialSegmentCount      number of segments to initialize the event processor with, or {@code null} to
     *                                 leave it to the event processing configuration. Workflow instances are
     *                                 partitioned over segments by workflow id
     */
    public WorkflowEventProcessingRegistrationEnhancer(
            String moduleName,
            @Nullable String engineComponentName,
            @Nullable String projectorComponentName,
            boolean registerHistoryProjector,
            @Nullable Integer initialSegmentCount
    ) {
        this.moduleName = moduleName;
        this.engineComponentName = engineComponentName;
        this.projectorComponentName = projectorComponentName;
        this.registerHistoryProjector = registerHistoryProjector;
        this.initialSegmentCount = initialSegmentCount;
    }

    /**
     * Order for this enhancer.
     * <p>
     * Enhancer math: we have to run AFTER the event souring part is set up and let some space for others to register.
     * </p>
     */
    public static final int WORKFLOW_EVENTING_ENHANCER_ORDER = EventSourcingConfigurationDefaults.ENHANCER_ORDER + 20;

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        componentRegistry.registerModule(
                EventProcessorModule
                        .pooledStreaming(moduleName)
                        .eventHandlingComponents(eventHandlingComponents())
                        .customized(processorCustomization())
                        .componentRegistry(this::registerWorkflowEngineStartHandler)
                        .build()
        );
    }

    /**
     * Configures the workflow event processor: it streams any event, partitioned over segments, with workflow instances
     * distributed over segments by their workflow id.
     * <p>
     * Everything set here is either the wiring the engine needs or the segment count. The segment change listener moves
     * instances with their segment, and the event source, token store and unit of work factory are resolved from the
     * components of this application. Every other processor setting is left to the standard event processing
     * configuration.
     * <p>
     * The criteria narrow to the registered start events, and admit any event when the engine reports none. That empty
     * case is the reason they are set at all: a processor narrowed to an empty set of types matches no event, while an
     * engine that reports no start events still has to receive the events its running instances wait for.
     * <p>
     * The processor claims its segments in the unnamed {@link TokenStore} component of the application, as a durable
     * store makes those claims visible across nodes, the precondition for multi-node sharding. Without one, an
     * {@link InMemoryTokenStore} is used and claims stay process-local (single-node operation).
     * <p>
     * Note that a batch size above 1 is not supported yet, see
     * <a href="https://github.com/AxonIQ/AxonFramework/issues/4323">AxonFramework#4323</a>.
     */
    private BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
            PooledStreamingEventProcessorConfiguration> processorCustomization() {
        return (cfg, processorConfiguration) -> withSegmentCount(processorConfiguration)
                .eventCriteria(set -> set.isEmpty()
                        ? EventCriteria.havingAnyTag()
                        : EventCriteria.havingAnyTag().andBeingOneOfTypes(set))
                .eventSource(cfg.getComponent(StreamableEventSource.class))
                .tokenStore(cfg.getOptionalComponent(TokenStore.class).orElseGet(() -> {
                    logger.warn("No unnamed TokenStore component is configured, so the workflow event processor of "
                                        + "module {} falls back to an in-memory token store. Segment claims are then "
                                        + "process-local: multi-node sharding and failover require a durable "
                                        + "TokenStore registered as an unnamed component.", moduleName);
                    return new InMemoryTokenStore();
                }))
                .unitOfWorkFactory(cfg.getComponent(UnitOfWorkFactory.class))
                .addSegmentChangeListener(segmentChangeListener(cfg));
    }

    /**
     * Applies the configured segment count, and leaves the incoming configuration untouched when none was configured.
     * <p>
     * Only an explicitly configured count is applied. Without one the processor keeps the count it already carries,
     * which is the default of the event processing configuration or whatever a customization of this application set,
     * so this module does not hold a competing default of its own.
     */
    private PooledStreamingEventProcessorConfiguration withSegmentCount(
            PooledStreamingEventProcessorConfiguration processorConfiguration
    ) {
        return initialSegmentCount == null
                ? processorConfiguration
                : processorConfiguration.initialSegmentCount(initialSegmentCount);
    }

    private SegmentChangeListener segmentChangeListener(Configuration cfg) {
        return new WorkflowSegmentChangeListener(moduleName,
                                                 cfg.getComponent(UnitOfWorkFactory.class),
                                                 () -> workflowEngine(cfg));
    }

    private WorkflowEngine workflowEngine(Configuration cfg) {
        return engineComponentName != null
                ? cfg.getComponent(WorkflowEngine.class, engineComponentName)
                : cfg.getComponent(WorkflowEngine.class);
    }

    private Function<RequiredComponentPhase, CompletePhase> eventHandlingComponents() {
        return req -> {
            var engineRegistration = req
                    .declarative(engineComponentName != null
                                         ? engineComponentName + "ExecutionEventing"
                                         : DEFAULT_MODULE_NAME + "ExecutionEventing",
                                 cfg -> {
                                     // The workflow routing overrides the component's default sequencing so engine
                                     // events and unique start candidates reach the owning segment and all other
                                     // business events are broadcast to every segment.
                                     var workflowEngine = workflowEngine(cfg);
                                     EventHandlingComponentHandlingAny component;
                                     if (engineComponentName != null) {
                                         component = new EventHandlingComponentHandlingAny(
                                                 workflowEngine,
                                                 cfg.getComponent(WorkflowEngineCheckpointingSupport.class)
                                         );
                                     } else {
                                         component = new EventHandlingComponentHandlingAny(workflowEngine);
                                     }
                                     return new SequenceOverridingEventHandlingComponent(
                                             workflowEngine.segmentedRouting(),
                                             component
                                     );
                                 }
                    );
            if (registerHistoryProjector) {
                engineRegistration = engineRegistration.
                        declarative(
                                projectorComponentName != null
                                        ? projectorComponentName + "Eventing"
                                        : DEFAULT_MODULE_NAME + "HistoryEventing",
                                cfg -> {
                                    // The projector shares the engine's routing policy so both components yield
                                    // the same sequence identifier per event and no extra segment deliveries occur.
                                    var projector = projectorComponentName != null
                                            ? cfg.getComponent(WorkflowHistoryProjector.class, projectorComponentName)
                                            : cfg.getComponent(WorkflowHistoryProjector.class);
                                    // FIXME: eventually history projector doesn't need to be replayed.
                                    // configure this separately InMemoryHistoryRepo = InMemoryTokeStore and replay
                                    return new SequenceOverridingEventHandlingComponent(
                                            workflowEngine(cfg).segmentedRouting(),
                                            new EventHandlingComponentHandlingAny(projector)
                                    );
                                }
                        );
            }
            return engineRegistration;
        };
    }

    /**
     * Registers the start handler of a {@link WorkflowEngine}.
     * <p>
     * Uses the given {@code registry} to register a {@link ComponentDefinition} with an
     * {@link ComponentDefinition#onStart(int, BiConsumer)} as the real start handler. Ideally, the
     * {@link org.axonframework.common.configuration.LifecycleRegistry} would be used instead here. However, the handler
     * is to be registered with what's exposed by the {@link EventProcessorModule}, which does not expose the
     * aforementioned {@code LifecycleRegistry}. Hence, a {@code ComponentDefinition} spoof is used instead, leading to
     * the same behavior in the end: the {@code WorkflowEngine} is initialized.
     */
    private void registerWorkflowEngineStartHandler(ComponentRegistry registry) {
        registry.registerComponent(
                ComponentDefinition.ofTypeAndName(Object.class, moduleName + "EngineStartHook")
                                   .withInstance(new Object())
                                   .onStart(
                                           PRE_PROCESSOR_START_PHASE,
                                           (c, ignored) -> (CompletableFuture<Void>) workflowEngineStartHandler(c)
                                   )
        );
    }

    private CompletableFuture<Void> workflowEngineStartHandler(Configuration config) {
        requireEventStore(config);
        WorkflowEngine workflowEngine = workflowEngine(config);
        return config.getComponent(StreamableEventSource.class).latestToken(null).thenCompose(workflowEngine::start);
    }

    void requireEventStore(Configuration config) {
        EventStore eventStore = config.getOptionalComponent(EventStore.class).orElse(null);
        if (eventStore == null) {
            throw new AxonConfigurationException(
                    "The workflow engine of module " + moduleName + " requires an EventStore. A WorkflowConfigurer "
                            + "configures one through the EventSourcingConfigurer. The engine appends every workflow "
                            + "event under an AppendCondition, which only an event-store transaction carries."
            );
        }
    }

    @Override
    public int order() {
        return WORKFLOW_EVENTING_ENHANCER_ORDER;
    }

    /**
     * Returns the name under which a
     * {@link org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule}
     * publishes the {@link TokenStore} its processor ended up with, so the start handler reads back the very store the
     * processor claims its segments in.
     * <p>
     * This names an output of the processor configuration, not an input. An application supplies its token store as an
     * unnamed {@link TokenStore} component, which {@link #processorCustomization()} resolves; registering one under
     * this name instead collides with the component the module already publishes.
     *
     * @param moduleName module name
     * @return name of the token store component
     */
    private static String tokenStoreName(String moduleName) {
        return "TokenStore[" + moduleName + "]";
    }
}
