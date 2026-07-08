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

import io.axoniq.framework.messaging.eventstreaming.checkpoint.Checkpointing;
import jakarta.annotation.Nonnull;
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
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Event handling component handling all events.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class AllEventEventHandlingComponent implements EventHandlingComponent, Checkpointing {

    private static final Logger logger = LoggerFactory.getLogger(AllEventEventHandlingComponent.class);
    public static final BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
            PooledStreamingEventProcessorConfiguration> ANY_EVENT_IN_ONE_SEGMENT = (c, pcepc) ->
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
                 .tokenStore(new InMemoryTokenStore())
                 .unitOfWorkFactory(c.getComponent(UnitOfWorkFactory.class))
                 .initialSegmentCount(1) // FIXME #190 (https://github.com/AxonIQ/extension-workflow/issues/190) -> should be configurable?
                 .batchSize(1); // FIXME -> should be configurable? currently only 1 is supported / working blocked by https://github.com/AxonIQ/AxonFramework/issues/4323
    private final SequencingPolicy<EventMessage> sequencingPolicy;
    private final EventHandler eventHandler;
    @Nullable
    private final Checkpointing checkpointingHandler;
    @Nullable
    private final ReplayStatusChangedHandler replayStatusChangedHandler;

    /**
     * Constructs the component.
     *
     * @param eventHandler event handler to wrap.
     */
    public AllEventEventHandlingComponent(EventHandler eventHandler) {
        this.eventHandler = Objects.requireNonNull(eventHandler, "Event handler must not be null");
        this.sequencingPolicy = new HierarchicalSequencingPolicy<>(
                SequentialPerAggregatePolicy.INSTANCE,
                SequentialPolicy.INSTANCE
        );
        if (eventHandler instanceof Checkpointing checkpointing) {
            checkpointingHandler = checkpointing;
        } else {
            checkpointingHandler = null;
        }
        if (eventHandler instanceof ReplayStatusChangedHandler) {
            replayStatusChangedHandler = (ReplayStatusChangedHandler) eventHandler;
        } else {
            replayStatusChangedHandler = null;
        }
    }


    @Override
    public MessageStream.Empty<Message> handle(EventMessage event, ProcessingContext context) {
        logger.debug("Handling event {}", event);
        return eventHandler.handle(event, context);
    }

    @Override
    public Set<QualifiedName> supportedEvents() {
        return Set.of();
    }

    @Override
    public boolean supports(QualifiedName eventName) {
        return true;
    }


    @Override
    public Object sequenceIdentifierFor(EventMessage event,
                                        ProcessingContext context) {
        return sequencingPolicy.sequenceIdentifierFor(event, context);
    }

    @Override
    public MessageStream.Empty<Message> handle(ReplayStatusChanged statusChange,
                                               ProcessingContext context) {
        if (replayStatusChangedHandler != null) { // just forward
            return replayStatusChangedHandler.handle(statusChange, context);
        }
        return MessageStream.empty();
    }

    @Override
    public CompletableFuture<TrackingToken> onCheckpointAdvanced(@Nonnull Segment segment,
                                                                 @Nonnull TrackingToken requested) {
        if (checkpointingHandler != null) {
            return checkpointingHandler.onCheckpointAdvanced(segment, requested);
        }
        return CompletableFuture.completedFuture(requested);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("event-handler", eventHandler.getClass());
        if (checkpointingHandler != null) {
            descriptor.describeProperty("checkpointing-handler", checkpointingHandler.getClass());
        }
        if (replayStatusChangedHandler != null) {
            descriptor.describeProperty("replay-status-changed-handler", replayStatusChangedHandler.getClass());
        }
    }
}
