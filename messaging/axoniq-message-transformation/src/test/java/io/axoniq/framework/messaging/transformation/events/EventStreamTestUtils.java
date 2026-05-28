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

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared helpers for event-transformation tests.
 */
abstract class EventStreamTestUtils {

    private EventStreamTestUtils() {
    }

    /**
     * Consume every {@link EventMessage} from {@code stream} and return them as a
     * {@link List}. After this call the stream is exhausted.
     */
    static List<EventMessage> collectMessages(MessageStream<? extends EventMessage> stream) {
        List<EventMessage> collected = new ArrayList<>();
        stream.<Void>reduce(null, (acc, entry) -> {
            collected.add(entry.message());
            return null;
        }).join();
        return collected;
    }

    /**
     * Constructs a {@link GenericEventMessage} with the given {@link MessageType} and payload.
     */
    static EventMessage eventOf(MessageType type, Object payload) {
        return new GenericEventMessage(type, payload);
    }
}
