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
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChangedHandler;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken.createReplayToken;

/**
 * Event handling component handling all events.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class AllEventEventHandlingComponent implements EventHandlingComponent {

    private static final Logger logger = LoggerFactory.getLogger(AllEventEventHandlingComponent.class);
    @SuppressWarnings("NullableProblems") public static BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
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
                 .tokenStore(c.getComponent(TokenStore.class))
                 .unitOfWorkFactory(c.getComponent(UnitOfWorkFactory.class))
                 .initialSegmentCount(1) // FIXME -> should be configurable?
                 .batchSize(1) // FIXME -> should be configurable? currently only 1 is supported / working
                 .initialToken(s -> c.getComponent(StreamableEventSource.class)
                                     .latestToken(null)
                                     .thenCompose(latestToken -> {
                                         if (latestToken.position().isPresent()
                                                 && latestToken.position().getAsLong() > 0) {
                                             return CompletableFuture.completedFuture(
                                                     createReplayToken(
                                                             latestToken,
                                                             // TODO change after MVP
                                                             // for the MVP we do a full replay
                                                             new GlobalSequenceTrackingToken(0)
                                                     )
                                             );
                                         } else {
                                             // FIXME -> check how to handle replay if there are no events in the store
                                             return CompletableFuture.completedFuture(
                                                     createReplayToken(
                                                             new GlobalSequenceTrackingToken(1)
                                                     )
                                             );
                                         }
                                     })
                 );
    private final SequencingPolicy<EventMessage> sequencingPolicy;
    private final EventHandler eventHandler;
    private final ReplayStatusChangedHandler replayStatusChangedHandler;

    /**
     * Constructs the component.
     *
     * @param eventHandler event handler to wrap.
     */
    public AllEventEventHandlingComponent(@Nonnull EventHandler eventHandler) {
        this.eventHandler = Objects.requireNonNull(eventHandler, "Event handler must not be null");
        this.sequencingPolicy = new HierarchicalSequencingPolicy<>(
                SequentialPerAggregatePolicy.INSTANCE,
                SequentialPolicy.INSTANCE
        );
        if (eventHandler instanceof ReplayStatusChangedHandler) {
            replayStatusChangedHandler = (ReplayStatusChangedHandler) eventHandler;
        } else {
            replayStatusChangedHandler = null;
        }
    }

    @Nonnull
    @Override
    public MessageStream.Empty<Message> handle(@Nonnull EventMessage event, @Nonnull ProcessingContext context) {
        logger.debug("Handling event {}", event);
        return eventHandler.handle(event, context);
    }

    @Override
    @Nonnull
    public Set<QualifiedName> supportedEvents() {
        return Set.of();
    }

    @Override
    public boolean supports(@Nonnull QualifiedName eventName) {
        return true;
    }

    @Nonnull
    @Override
    public Object sequenceIdentifierFor(@Nonnull EventMessage event,
                                        @Nonnull ProcessingContext context) {
        return sequencingPolicy.sequenceIdentifierFor(event, context);
    }

    @Override
    @Nonnull
    public MessageStream.Empty<Message> handle(@Nonnull ReplayStatusChanged statusChange,
                                               @Nonnull ProcessingContext context) {
        if (replayStatusChangedHandler != null) { // just forward
            return replayStatusChangedHandler.handle(statusChange, context);
        }
        return MessageStream.empty();
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("event-handler", eventHandler.getClass());
        if (replayStatusChangedHandler != null) {
            descriptor.describeProperty("replay-status-changed-handler", replayStatusChangedHandler.getClass());
        }
    }
}
