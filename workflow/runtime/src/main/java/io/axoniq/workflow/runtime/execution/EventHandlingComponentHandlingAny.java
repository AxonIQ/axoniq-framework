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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.framework.messaging.eventstreaming.checkpoint.CheckpointTrigger;
import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.HierarchicalSequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequentialPerAggregatePolicy;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * Event handling component handling any event.
 * <p>
 * This component always participates in checkpoint coordination, including when it wraps a plain {@link EventHandler}.
 * A plain handler acknowledges requested checkpoint tokens immediately. This keeps every handler in the workflow
 * processor checkpoint-aware, preserving the engine's deferred checkpointing behavior.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class EventHandlingComponentHandlingAny implements EventHandlingComponent, Checkpointing {

    private static final Logger logger = LoggerFactory.getLogger(EventHandlingComponentHandlingAny.class);

    private static final boolean ANY_EVENT = true;

    /**
     * Processor customization streaming any event into a single segment. Kept for behavioral compatibility; see
     * {@link #anyEventInSegments(int)} for the multi-segment variant.
     */
    public static final BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
            PooledStreamingEventProcessorConfiguration> ANY_EVENT_IN_ONE_SEGMENT = anyEventInSegments(1);

    /**
     * Creates a processor customization streaming any event, partitioned over the given number of segments. Workflow
     * instances are distributed over segments by their workflow id
     * <p>
     * The processor uses the {@link TokenStore} registered as a component when present — a durable store makes
     * segment claims visible across nodes, the precondition for multi-node sharding. Without one, an
     * {@link InMemoryTokenStore} is used and claims stay process-local (single-node operation).
     *
     * @param initialSegmentCount number of segments to use when initializing the processor's tracking tokens.
     * @return processor customization.
     */
    public static BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
            PooledStreamingEventProcessorConfiguration> anyEventInSegments(int initialSegmentCount) {
        return (c, pcepc) ->
                pcepc.eventCriteria(
                             set -> {
                                 if (set.isEmpty()) {
                                     return EventCriteria.havingAnyTag();
                                 } else {
                                     return EventCriteria.havingAnyTag().andBeingOneOfTypes(set);
                                 }
                             }
                     )
                     .eventSource(c.getComponent(StreamableEventSource.class))
                     .tokenStore(c.getOptionalComponent(TokenStore.class).orElseGet(() -> {
                         logger.warn("No TokenStore component configured for the workflow event processor — falling "
                                             + "back to an in-memory token store. Segment claims are process-local: "
                                             + "multi-node sharding and failover require a durable TokenStore.");
                         return new InMemoryTokenStore();
                     }))
                     .unitOfWorkFactory(c.getComponent(UnitOfWorkFactory.class))
                     .initialSegmentCount(initialSegmentCount)
                     .batchSize(1); // FIXME -> should be configurable? currently only 1 is supported / working blocked by https://github.com/AxonIQ/AxonFramework/issues/4323
    }

    private final EventHandler eventHandler;
    private final SequencingPolicy<EventMessage> sequencingPolicy;
    @Nullable
    private final Checkpointing checkpointingHandler;
    @Nullable
    private final ReplayStatusChangedHandler replayStatusChangedHandler;

    /**
     * Constructs the component for a generic event handler.
     *
     * @param eventHandler event handler to wrap
     */
    public EventHandlingComponentHandlingAny(EventHandler eventHandler) {
        this.eventHandler = requireNonNull(eventHandler, "Event handler must not be null");
        this.sequencingPolicy = new HierarchicalSequencingPolicy<>(
                SequentialPerAggregatePolicy.INSTANCE,
                SequentialPolicy.INSTANCE
        );
        this.checkpointingHandler = null;
        this.replayStatusChangedHandler = null;
    }

    /**
     * Constructs the component for a workflow engine.
     *
     * @param workflowEngine             workflow engine to deliver events to
     * @param replayStatusChangedHandler handler to notify when replay status changes
     * @param checkpointingHandler       handler to notify when checkpointing is required
     */
    public EventHandlingComponentHandlingAny(@NonNull WorkflowEngine workflowEngine,
                                             @NonNull ReplayStatusChangedHandler replayStatusChangedHandler,
                                             @NonNull Checkpointing checkpointingHandler) {
        this.eventHandler = requireNonNull(workflowEngine, "Workflow engine handler must not be null");
        this.sequencingPolicy = new HierarchicalSequencingPolicy<>(
                SequentialPerAggregatePolicy.INSTANCE,
                SequentialPolicy.INSTANCE
        );
        this.checkpointingHandler = requireNonNull(checkpointingHandler, "Checkpointing handler must not be null");
        this.replayStatusChangedHandler =
                requireNonNull(replayStatusChangedHandler, "Replay status changed handler must not be null");
    }

    @Override
    public MessageStream.Empty<Message> handle(@NonNull EventMessage event, @NonNull ProcessingContext context) {
        logger.debug("Handling event {}", event);
        return eventHandler.handle(event, context);
    }

    @NonNull
    @Override
    public Set<QualifiedName> supportedEvents() {
        return Set.of();
    }

    @Override
    public boolean supports(@NonNull QualifiedName eventName) {
        return ANY_EVENT;
    }

    @NonNull
    @Override
    public Object sequenceIdentifierFor(@NonNull EventMessage event,
                                        @NonNull ProcessingContext context) {
        return sequencingPolicy.sequenceIdentifierFor(event, context);
    }

    @Override
    public MessageStream.Empty<Message> handle(@NonNull ReplayStatusChanged statusChange,
                                               @NonNull ProcessingContext context) {
        return replayStatusChangedHandler != null
                ? replayStatusChangedHandler.handle(statusChange, context)
                : MessageStream.empty();
    }

    @NonNull
    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(@NonNull Segment segment,
                                                                 @NonNull TrackingToken requested) {
        return checkpointingHandler != null
                ? checkpointingHandler.onCheckpointAdvanced(segment, requested)
                : CompletableFuture.completedFuture(requested);
    }

    @Override
    public void onSegmentClaimed(@NonNull Segment segment,
                                 @Nullable TrackingToken from,
                                 @NonNull CheckpointTrigger trigger) {
        if (checkpointingHandler != null) {
            checkpointingHandler.onSegmentClaimed(segment, from, trigger);
        }
    }

    @NonNull
    @Override
    public CompletableFuture<TrackingToken> onSegmentReleased(@NonNull Segment segment,
                                                              @NonNull TrackingToken requested) {
        if (checkpointingHandler != null) {
            return checkpointingHandler.onSegmentReleased(segment, requested);
        }
        // Plain handlers add no checkpoint work but must acknowledge to preserve deferred checkpoint coordination.
        return CompletableFuture.completedFuture(requested);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("eventHandler", eventHandler.getClass());
        if (checkpointingHandler != null) {
            descriptor.describeProperty("checkpointingHandler", checkpointingHandler.getClass());
        }
        if (replayStatusChangedHandler != null) {
            descriptor.describeProperty("replayStatusChangedHandler", replayStatusChangedHandler.getClass());
        }
    }
}
