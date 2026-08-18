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
import io.axoniq.workflow.runtime.execution.WorkflowEngineReplaySupport;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer.CompletePhase;
import org.axonframework.messaging.eventhandling.configuration.EventHandlingComponentsConfigurer.RequiredComponentPhase;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SegmentChangeListener;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.SequenceOverridingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.completedFuture;

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

    /**
     * Default number of segments the workflow event processor is initialized with.
     * <p>
     * Multiple segments are safe since workflow instances are partitioned over segments by workflow id:
     * engine events and unique spawn candidates are sequenced to the owning segment, and correlated business events
     * are sequenced by {@code SequencingPolicy#BROADCAST}, delivering them to every segment. Four balances
     * instance-level parallelism against the per-segment broadcast delivery cost; override via
     * {@link #WorkflowEventProcessingRegistrationEnhancer(String, String, String, boolean, int)} or the
     * {@code axoniq.workflow.initial-segment-count} Spring property.
     */
    public static final int DEFAULT_INITIAL_SEGMENT_COUNT = 4;

    private final String moduleName;
    @Nullable
    private final String engineComponentName;
    @Nullable
    private final String projectorComponentName;
    private final boolean registerHistoryProjector;
    private final int initialSegmentCount;

    /**
     * Creates a workflow event processing registration enhancer with the
     * {@link #DEFAULT_INITIAL_SEGMENT_COUNT default segment count}, responsible for registering the workflow engine
     * and history projector components to the event processing module.
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
        this(moduleName, engineComponentName, projectorComponentName, registerHistoryProjector,
             DEFAULT_INITIAL_SEGMENT_COUNT);
    }

    /**
     * Creates a workflow event processing registration enhancer, responsible for registering the workflow engine and
     * history projector components to the event processing module.
     *
     * @param moduleName               name of the event processing module
     * @param engineComponentName      name of the workflow engine component
     * @param projectorComponentName   name of the workflow history projector component
     * @param registerHistoryProjector flag indicating whether to register the history projector component
     * @param initialSegmentCount      number of segments to initialize the event processor with; workflow instances
     *                                 are partitioned over segments by workflow id
     */
    public WorkflowEventProcessingRegistrationEnhancer(
            String moduleName,
            @Nullable String engineComponentName,
            @Nullable String projectorComponentName,
            boolean registerHistoryProjector,
            int initialSegmentCount
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
     * Configures the workflow event processor: it streams any event, partitioned over
     * {@link #initialSegmentCount} segments, with workflow instances distributed over segments by their workflow id.
     * <p>
     * The processor uses the {@link TokenStore} registered as a component when present — a durable store makes
     * segment claims visible across nodes, the precondition for multi-node sharding. Without one, an
     * {@link InMemoryTokenStore} is used and claims stay process-local (single-node operation).
     */
    private BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
            PooledStreamingEventProcessorConfiguration> processorCustomization() {
        return (cfg, processorConfiguration) -> processorConfiguration
                .eventCriteria(set -> set.isEmpty()
                        ? EventCriteria.havingAnyTag()
                        : EventCriteria.havingAnyTag().andBeingOneOfTypes(set))
                .eventSource(cfg.getComponent(StreamableEventSource.class))
                .tokenStore(cfg.getOptionalComponent(TokenStore.class).orElseGet(() -> {
                    logger.warn("No TokenStore component configured for the workflow event processor — falling "
                                        + "back to an in-memory token store. Segment claims are process-local: "
                                        + "multi-node sharding and failover require a durable TokenStore.");
                    return new InMemoryTokenStore();
                }))
                .unitOfWorkFactory(cfg.getComponent(UnitOfWorkFactory.class))
                .initialSegmentCount(initialSegmentCount)
                // FIXME -> should be configurable? currently only 1 is supported / working blocked by https://github.com/AxonIQ/AxonFramework/issues/4323
                .batchSize(1)
                .addSegmentChangeListener(segmentChangeListener(cfg));
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
                                     // events and unique spawn candidates reach the owning segment and all other
                                     // business events are broadcast to every segment.
                                     var workflowEngine = workflowEngine(cfg);
                                     var component = engineComponentName != null
                                             ? new EventHandlingComponentHandlingAny(
                                                     workflowEngine,
                                                     cfg.getComponent(WorkflowEngineReplaySupport.class),
                                                     cfg.getComponent(WorkflowEngineCheckpointingSupport.class))
                                             : new EventHandlingComponentHandlingAny(workflowEngine);
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
     * Registers the start handler of a {@link WorkflowEngine}, ensuring a stream is correctly initialized and a replay
     * is triggered when applicable.
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
                ComponentDefinition.ofTypeAndName(Object.class, moduleName + "ReplayResetHook")
                                   .withInstance(new Object())
                                   .onStart(
                                           PRE_PROCESSOR_START_PHASE,
                                           (c, ignored) -> (CompletableFuture<Void>) workflowEngineStartHandler(c)
                                   )
        );
    }

    private CompletableFuture<Void> workflowEngineStartHandler(Configuration config) {
        TokenStore tokenStore = config.getComponent(TokenStore.class, tokenStoreName(moduleName));
        StreamableEventSource eventSource = config.getComponent(StreamableEventSource.class);
        WorkflowEngineReplaySupport replaySupport = config.getComponent(WorkflowEngineReplaySupport.class);
        WorkflowEngine workflowEngine = workflowEngine(config);
        return ensureSegmentsInitialized(tokenStore, eventSource).thenCompose(
                processorToken -> eventSource.latestToken(null).thenCompose(latestToken -> initializeWorkflowEngine(
                        workflowEngine,
                        replaySupport,
                        processorToken,
                        latestToken
                ))
        );
    }

    CompletableFuture<TrackingToken> ensureSegmentsInitialized(
            TokenStore tokenStore,
            StreamableEventSource eventSource
    ) {
        return tokenStore.fetchSegments(moduleName, null)
                         .thenCompose(segments -> segments.isEmpty()
                                 ? initializeSegments(tokenStore, eventSource)
                                 : adoptExistingSegments(tokenStore, segments));
    }

    /**
     * Reads the replay position of segments that already exist, and reports the count this node ends up running when it
     * is not the count this node asked for.
     * <p>
     * The configured segment count only takes effect on a virgin token store: the layout is created once, by whichever
     * node starts first, and every later node adopts it. That is the right behaviour - re-partitioning a live processor
     * from a starting node would move instances out from under their owners - but silence about it is not. Without this
     * the count a node runs and the count it was configured with diverge invisibly, so a half-finished rolling deploy,
     * a value edited after the first start and a typo all look exactly like a working cluster.
     */
    private CompletableFuture<TrackingToken> adoptExistingSegments(TokenStore tokenStore, List<Segment> segments) {
        if (segments.size() != initialSegmentCount) {
            logger.warn("Processor {} is configured for {} segment(s) but its token store already holds {}; running "
                                + "with {}. The configured count only applies to an empty token store - the layout is"
                                + " created once by the node that starts first and adopted by every node after it. To"
                                + " change it, stop every node and delete the processor's tokens.",
                        moduleName, initialSegmentCount, segments.size(), segments.size());
        }
        return earliestSegmentToken(tokenStore, segments);
    }

    /**
     * Initializes the segments, tolerating a replica that got there first: reading the segments and initializing them
     * is not one atomic step, so replicas starting at the same time all see an empty store and all try to initialize.
     * The losers read back the segments the winner created instead of failing to start.
     */
    private CompletableFuture<TrackingToken> initializeSegments(TokenStore tokenStore,
                                                                StreamableEventSource eventSource) {
        return eventSource
                .firstToken(null)
                .thenCompose(firstToken -> tokenStore
                        .initializeTokenSegments(moduleName, initialSegmentCount, firstToken, null)
                        .thenApply(ignored -> firstToken)
                        .exceptionallyCompose(ex -> tokenStore
                                .fetchSegments(moduleName, null)
                                .thenCompose(segments -> segments.isEmpty()
                                        ? CompletableFuture.<TrackingToken>failedFuture(ex)
                                        : earliestSegmentToken(tokenStore, segments))));
    }

    private CompletableFuture<TrackingToken> earliestSegmentToken(TokenStore tokenStore,
                                                                  List<Segment> segments) {
        return SegmentTokenScan.earliestSegmentToken(tokenStore, moduleName, segments);
    }

    CompletableFuture<Void> initializeWorkflowEngine(
            WorkflowEngine workflowEngine,
            WorkflowEngineReplaySupport replaySupport,
            @Nullable TrackingToken processorToken,
            @Nullable TrackingToken latestToken
    ) {
        replaySupport.setInitialEngineTokens(processorToken, latestToken);
        return workflowEngine.start(processorToken, requiresReplay(processorToken, latestToken));
    }

    boolean requiresReplay(@Nullable TrackingToken resetToken, @Nullable TrackingToken latestToken) {
        if (resetToken == null || latestToken == null) {
            return false;
        } else {
            return !resetToken.samePositionAs(latestToken);
        }
    }

    @Override
    public int order() {
        return WORKFLOW_EVENTING_ENHANCER_ORDER;
    }

    /**
     * Returns the name of the token store component for the given module used for workflow event processing.
     *
     * @param moduleName module name
     * @return name of the token store component
     */
    public static String tokenStoreName(String moduleName) {
        return "TokenStore[" + moduleName + "]";
    }
}
