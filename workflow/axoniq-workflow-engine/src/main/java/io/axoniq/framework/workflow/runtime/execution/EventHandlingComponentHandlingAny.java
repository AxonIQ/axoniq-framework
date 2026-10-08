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

package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.sequencing.HierarchicalSequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.sequencing.SequentialPerAggregatePolicy;
import org.axonframework.messaging.core.sequencing.SequentialPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventHandler;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Event handling component handling any event.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class EventHandlingComponentHandlingAny implements EventHandlingComponent {

    private static final Logger logger = LoggerFactory.getLogger(EventHandlingComponentHandlingAny.class);

    private static final boolean ANY_EVENT = true;

    private final EventHandler eventHandler;
    private final SequencingPolicy<EventMessage> sequencingPolicy;

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
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event, ProcessingContext context) {
        logger.trace("Handling event {}", event);
        return eventHandler.handle(event, context);
    }

    @Override
    public Set<QualifiedName> supportedEvents() {
        return Set.of();
    }

    @Override
    public boolean supports(QualifiedName eventName) {
        return ANY_EVENT;
    }

    @Override
    public Object sequenceIdentifierFor(EventMessage event,
                                        ProcessingContext context) {
        return sequencingPolicy.sequenceIdentifierFor(event, context);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("eventHandler", eventHandler.getClass());
    }
}
