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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
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

    /**
     * A {@link MessageConverter} stand-in for tests whose payloads already match the
     * transformer's declared input type. The chain's fast path returns the payload directly
     * for already-typed inputs, so the converter is never invoked. Any actual call here
     * fails the test loudly with an {@link AssertionError}, surfacing accidental reliance
     * on conversion in tests that should not need it.
     */
    static MessageConverter neverInvokedConverter() {
        return new NeverInvokedMessageConverter();
    }

    private static final class NeverInvokedMessageConverter implements MessageConverter {
        @Override
        public <M extends Message, T> @Nullable T convertPayload(M message, @NonNull Type targetType) {
            throw new AssertionError(
                    "MessageConverter.convertPayload was unexpectedly invoked in a test; "
                            + "ensure payload type matches the transformer's declared input type.");
        }

        @Override
        public <M extends Message> M convertMessage(M message, @NonNull Type targetType) {
            throw new AssertionError("MessageConverter.convertMessage was unexpectedly invoked in a test.");
        }

        @Override
        public <T> T convert(@Nullable Object input, @NonNull Type targetType) {
            throw new AssertionError("MessageConverter.convert was unexpectedly invoked in a test.");
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("kind", "neverInvokedConverter");
        }
    }
}
