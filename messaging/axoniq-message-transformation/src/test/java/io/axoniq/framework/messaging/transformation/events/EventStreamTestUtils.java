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
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Shared helpers for event-transformation tests.
 */
final class EventStreamTestUtils {

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

    /**
     * Records each {@code convertPayload(...)} invocation and returns
     * {@code converterFunction.apply(message)}; the test supplies the conversion behaviour.
     * Use this when the test's subject-under-test IS the chain's slow-path conversion call
     * (stored payload class differs from the transformer's declared input type).
     */
    static <T> RecordingMessageConverter<T> recordingConverter(Function<Message, T> converterFunction) {
        return new RecordingMessageConverter<>(converterFunction);
    }

    /**
     * Recording {@link MessageConverter} returning {@code converterFunction.apply(message)} from
     * {@code convertPayload(...)}. Captures the most recent {@code (message, targetType)} pair so
     * tests can assert the framework supplied the declared input {@link Type}. Other converter
     * methods fail loudly: they should not be needed on the chain's payload-conversion path.
     */
    static final class RecordingMessageConverter<T> implements MessageConverter {

        private final Function<Message, T> converterFunction;
        private final AtomicInteger invocationCount = new AtomicInteger();
        private final AtomicReference<@Nullable Type> lastRequestedType = new AtomicReference<>();
        private final AtomicReference<@Nullable Message> lastRequestedMessage = new AtomicReference<>();

        private RecordingMessageConverter(Function<Message, T> converterFunction) {
            this.converterFunction = converterFunction;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <M extends Message, U> @Nullable U convertPayload(M message, @NonNull Type targetType) {
            invocationCount.incrementAndGet();
            lastRequestedType.set(targetType);
            lastRequestedMessage.set(message);
            return (U) converterFunction.apply(message);
        }

        @Override
        public <M extends Message> M convertMessage(M message, @NonNull Type targetType) {
            throw new AssertionError("MessageConverter.convertMessage was unexpectedly invoked in a test.");
        }

        @Override
        public <U> U convert(@Nullable Object input, @NonNull Type targetType) {
            throw new AssertionError("MessageConverter.convert was unexpectedly invoked in a test.");
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("kind", "recordingConverter");
            descriptor.describeProperty("invocationCount", invocationCount.get());
        }

        int invocationCount() {
            return invocationCount.get();
        }

        @Nullable
        Type lastRequestedType() {
            return lastRequestedType.get();
        }

        @Nullable
        Message lastRequestedMessage() {
            return lastRequestedMessage.get();
        }
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

    /**
     * A {@link MessageTypeResolver} stand-in for tests that do not care about the chain's
     * output identity check. It always returns {@link Optional#empty()}, which the chain
     * treats as "skip the check". Useful for any test whose subject-under-test is not the
     * identity check itself.
     */
    static MessageTypeResolver alwaysEmptyMessageTypeResolver() {
        return alwaysEmpty -> Optional.empty();
    }

    /**
     * A {@link MessageTypeResolver} stand-in for tests where the chain's identity check must
     * NOT be reached (e.g. non-matching pass-through tests). Any invocation fails the test
     * with an {@link AssertionError}.
     */
    static MessageTypeResolver neverInvokedMessageTypeResolver() {
        return cls -> {
            throw new AssertionError(
                    "MessageTypeResolver.resolve was unexpectedly invoked in a test; "
                            + "no transformer should have matched this event.");
        };
    }

    /**
     * A {@link MessageTypeResolver} stand-in that resolves the given {@code resolvedClass}
     * to {@code resolvedType} and returns {@link Optional#empty()} for every other class.
     * Useful for the output-identity-check tests where one specific output class must
     * resolve to a known type while the rest of the world is treated as untyped.
     */
    static MessageTypeResolver resolverFor(Class<?> resolvedClass, MessageType resolvedType) {
        return cls -> cls == resolvedClass ? Optional.of(resolvedType) : Optional.empty();
    }
}
