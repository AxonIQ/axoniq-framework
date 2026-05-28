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
 * factory.
 * <p>
 * In production this runs inside an {@link EventTransformerChain}, which deserializes the
 * stored payload to the declared input type via {@code MessageConverter} before invoking
 * the user's mapper. For unit tests that invoke {@code transform(...)} directly without a
 * chain, supply an event whose payload is already an instance of the declared input type
 * (e.g. a {@code JsonNode} when the transformer was registered with {@code JsonNode.class}).
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

    /** Erases the mapper's generic input / output types; safe because the framework only
     *  invokes it with values already converted to {@link #inputType}. */
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
        return MessageStream.just(applyTo(message, context));
    }

    /**
     * Applies this transformer to {@code message} assuming the {@link #matcher} has already
     * matched. The chain calls this directly to skip a redundant match check.
     *
     * @param message the matched input
     * @param context the active processing context, or {@code null}
     * @return the transformed output
     */
    EventMessage applyTo(EventMessage message, @Nullable ProcessingContext context) {
        var typedPayload = extractTypedPayload(message);
        var mappedPayload = mapper.apply(typedPayload, context);
        return new GenericEventMessage(
                message.identifier(),
                toType,
                mappedPayload,
                message.metadata(),
                message.timestamp()
        );
    }

    /** Returns the payload typed as {@link #inputType}: handed through directly if it is
     *  already an instance of the input type, otherwise asks {@code payloadAs(...)} to
     *  convert (which requires a converter on the message, or fails). */
    private @Nullable Object extractTypedPayload(EventMessage message) {
        var payload = message.payload();
        var rawInputType = rawClassOf(inputType);
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
     * The {@code from}-side matcher this transformer was built with.
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
     * The input payload type declared at registration. The chain passes this to its
     * {@code MessageConverter} before invoking the mapper.
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
