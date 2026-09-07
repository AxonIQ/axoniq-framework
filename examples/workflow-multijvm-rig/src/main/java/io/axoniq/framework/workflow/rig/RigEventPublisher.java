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
package io.axoniq.framework.workflow.rig;

import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.stereotype.Component;

/**
 * Appends one rig event to the shared event store.
 * <p>
 * Used both by the observation endpoint, which injects events from outside any handler, and by
 * {@link ParentChildWorkflow}, whose parent step spawns its child by publishing the child's start event.
 */
@Component
public class RigEventPublisher {

    private final EventSink eventSink;
    private final MessageTypeResolver messageTypeResolver;
    private final EventConverter eventConverter;

    RigEventPublisher(EventSink eventSink, MessageTypeResolver messageTypeResolver, EventConverter eventConverter) {
        this.eventSink = eventSink;
        this.messageTypeResolver = messageTypeResolver;
        this.eventConverter = eventConverter;
    }

    /**
     * Publishes the payload in a unit of work of its own.
     * <p>
     * Deliberately not joined to the caller's processing context: a spawn that rolled back with the step that made it
     * would hide the very hand-off {@link ParentChildWorkflow} exists to exercise, and the event would then be
     * committed twice on the retry.
     *
     * @param payload the event payload to append.
     */
    public void publish(Object payload) {
        var message = new GenericEventMessage(messageTypeResolver.resolveOrThrow(payload), payload)
                .withConverter(eventConverter);
        eventSink.publish(null, message).join();
    }
}
