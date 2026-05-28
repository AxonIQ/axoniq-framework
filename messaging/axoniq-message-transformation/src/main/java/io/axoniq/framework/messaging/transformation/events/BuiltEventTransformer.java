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

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * The {@link EventTransformer} implementation produced by the {@link EventTransformation}
 * factory. Carries the matching metadata ({@link #matcher}, {@link #toType},
 * {@link #inputType}) that the {@link EventTransformerChain} reads at routing time,
 * alongside the user-supplied payload mapper.
 * <p>
 * Standalone invocation of {@link #transform(EventMessage, ProcessingContext)} works when
 * the input event's payload's runtime class is assignable to the raw class of
 * {@link #inputType} (the implementation uses {@code Message.payloadAs(Type, null)} with
 * no converter, plus a same-class fast-path). Chain-driven invocation goes through a
 * {@code MessageConverter} so any registered conversion applies regardless of input type
 * shape.
 * <p>
 * Internal because the {@code EventTransformer} interface is the public lambda-friendly
 * SPI, and consumers should only ever get instances through the factory.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
@Internal
final class BuiltEventTransformer implements EventTransformer {

    private final FromMatcher matcher;
    private final MessageType toType;
    private final Type inputType;
    private final BiFunction<@Nullable Object, @Nullable ProcessingContext, @Nullable Object> mapper;

    /**
     * Constructs a built transformer.
     *
     * @param matcher   the {@code from}-side matcher
     * @param toType    the {@code to} identity applied to the output message
     * @param inputType the {@link Type} the payload is converted to before invoking the mapper
     * @param mapper    the user-supplied payload mapping function
     */
    BuiltEventTransformer(FromMatcher matcher,
                          MessageType toType,
                          Type inputType,
                          BiFunction<?, @Nullable ProcessingContext, ?> mapper) {
        this.matcher = requireNonNull(matcher, "matcher");
        this.toType = requireNonNull(toType, "toType");
        this.inputType = requireNonNull(inputType, "inputType");
        this.mapper = widenMapper(requireNonNull(mapper, "mapper"));
    }

    /**
     * Widens the user-supplied {@code BiFunction<T, ?, U>} to the field type. Localizes the
     * unchecked cast: the {@code T} and {@code U} type parameters are erased at runtime,
     * and the chain feeds the mapper a value already converted to {@link #inputType}, so
     * the cast is safe.
     */
    @SuppressWarnings("unchecked")
    private static BiFunction<@Nullable Object, @Nullable ProcessingContext, @Nullable Object> widenMapper(
            BiFunction<?, @Nullable ProcessingContext, ?> mapper) {
        return (BiFunction<@Nullable Object, @Nullable ProcessingContext, @Nullable Object>) mapper;
    }

    @Override
    public MessageStream<? extends EventMessage> transform(EventMessage message,
                                                           @Nullable ProcessingContext context) {
        if (!matcher.matches(message.type())) {
            return MessageStream.just(message);
        }
        @Nullable Object typedPayload = extractTypedPayload(message);
        @Nullable Object mappedPayload = mapper.apply(typedPayload, context);
        EventMessage output = new GenericEventMessage(
                message.identifier(),
                toType,
                mappedPayload,
                message.metadata(),
                message.timestamp()
        );
        return MessageStream.just(output);
    }

    /**
     * Same-class fast path so the standalone (no-converter) invocation works symmetrically
     * for both the {@code Class<T>} and {@code TypeReference<T>} overloads: if the payload
     * is already an instance of {@link #inputType}'s raw class, hand it through without
     * converter involvement. Falls back to {@code payloadAs(Type, null)} otherwise -- which
     * itself raises a {@code ConversionException} when no converter is available.
     */
    private @Nullable Object extractTypedPayload(EventMessage message) {
        @Nullable Object payload = message.payload();
        @Nullable Class<?> rawInputType = rawClassOf(inputType);
        if (rawInputType != null && rawInputType.isInstance(payload)) {
            return payload;
        }
        return message.payloadAs(inputType, null);
    }

    private static @Nullable Class<?> rawClassOf(Type type) {
        return switch (type) {
            case Class<?> cls -> cls;
            case ParameterizedType pt -> pt.getRawType() instanceof Class<?> raw ? raw : null;
            default -> null;
        };
    }

    /**
     * The {@code from}-side matcher driving routing. Used by {@link EventTransformerChain}
     * via pattern matching on {@link FromMatcher}'s permitted subtypes: a
     * {@link FromMatcher.Concrete} is bucketed into the
     * {@link org.axonframework.messaging.core.QualifiedName}-keyed index, a
     * {@link FromMatcher.PredicateBased} goes onto the predicate scan list.
     *
     * @return the {@code from}-side matcher
     */
    FromMatcher matcher() {
        return matcher;
    }

    /**
     * The declared {@code to} identity applied to outputs.
     *
     * @return the {@code to} identity
     */
    MessageType toType() {
        return toType;
    }

    /**
     * The {@link Type} the payload is converted to before invoking the mapper.
     * {@code MessageConverter.convertPayload(message, Type)} accepts this directly when a
     * converter is available; standalone invocation falls back to a same-class fast path
     * plus {@code Message.payloadAs(Type, null)}.
     *
     * @return the input {@link Type}
     */
    Type inputType() {
        return inputType;
    }

    @Override
    public String toString() {
        return "BuiltEventTransformer{from=" + matcher
                + ", to=" + toType
                + ", inputType=" + inputType.getTypeName()
                + '}';
    }
}
