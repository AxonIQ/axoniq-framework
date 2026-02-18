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

import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.sequencing.HierarchicalSequencingPolicy;
import org.axonframework.messaging.eventhandling.sequencing.SequencingPolicy;
import org.axonframework.messaging.eventhandling.sequencing.SequentialPerAggregatePolicy;
import org.axonframework.messaging.eventhandling.sequencing.SequentialPolicy;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Event handling component handling all events.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class AllEventEventHandlingComponent implements EventHandlingComponent {

    private static final Logger logger = LoggerFactory.getLogger(AllEventEventHandlingComponent.class);
    public static BiFunction<Configuration, PooledStreamingEventProcessorConfiguration,
            PooledStreamingEventProcessorConfiguration> ANY_EVENT_IN_ONE_SEGMENT = (c, pcepc) ->
            pcepc.eventCriteria(
                    set -> {
                        if (set.isEmpty()) {
                            return EventCriteria.havingAnyTag();
                        } else {
                            return EventCriteria.havingAnyTag().andBeingOneOfTypes(set);
                        }
                    }
            ).initialSegmentCount(1);
    private final SequencingPolicy sequencingPolicy;
    private final EventHandler eventHandler;

    public AllEventEventHandlingComponent(@Nonnull EventHandler eventHandler) {
        this.eventHandler = Objects.requireNonNull(eventHandler, "Event handler must not be null");
        this.sequencingPolicy = new HierarchicalSequencingPolicy(
                SequentialPerAggregatePolicy.instance(),
                SequentialPolicy.INSTANCE
        );
    }

    @NotNull
    @Override
    public MessageStream.Empty<Message> handle(@NotNull EventMessage event, @NotNull ProcessingContext context) {
        logger.debug("Handling event {}", event);
        return eventHandler.handle(event, context);
    }

    @Override
    public Set<QualifiedName> supportedEvents() {
        return Set.of();
    }

    @Override
    public boolean supports(@NotNull QualifiedName eventName) {
        return true;
    }

    @NotNull
    @Override
    public Object sequenceIdentifierFor(@NotNull EventMessage event, @NotNull ProcessingContext context) {
        return sequencingPolicy.getSequenceIdentifierFor(event, context);
    }

    @Override
    public void describeTo(@NotNull ComponentDescriptor descriptor) {
        descriptor.describeProperty("event-handler", eventHandler.getClass());
    }
}
