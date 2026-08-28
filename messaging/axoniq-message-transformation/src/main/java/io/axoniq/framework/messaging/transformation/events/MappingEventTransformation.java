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

import io.axoniq.framework.messaging.transformation.ChainConfigurationException;
import io.axoniq.framework.messaging.transformation.FromMatcher;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * A 1:1 payload-mapping {@link EventTransformation}, carrying the matching and mapping behavior for a single
 * registered transformation.
 *
 * @param <T> the input payload type declared at registration
 * @param <U> the output payload type the mapper produces
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class MappingEventTransformation<T, U> implements EventTransformation {

    private final FromMatcher matcher;
    private final MessageType toType;
    private final DeclaredInputType<T> inputType;
    private final BiFunction<T, @Nullable ProcessingContext, U> mapper;

    /**
     * Constructs a 1:1 payload-mapping transformation.
     *
     * @param matcher   the {@code from}-side matcher
     * @param toType    the {@code to} identity applied to the output message
     * @param inputType the declared input type, preserving any generic parameters
     * @param mapper    the user-supplied payload mapping function
     */
    MappingEventTransformation(FromMatcher matcher,
                               MessageType toType,
                               TypeReference<T> inputType,
                               BiFunction<T, @Nullable ProcessingContext, U> mapper) {
        this.matcher = requireNonNull(matcher, "matcher may not be null");
        this.toType = requireNonNull(toType, "toType may not be null");
        this.inputType = DeclaredInputType.of(inputType);
        this.mapper = requireNonNull(mapper, "mapper may not be null");
    }

    /**
     * Transforms the matched message, returning the result as a single-element {@link MessageStream}.
     *
     * @param message the matched input message
     * @param context the per-message {@link TransformationContext}
     * @return a single-element stream carrying the transformed output message
     * @throws ChainConfigurationException if the mapper's output resolves to a {@link MessageType} other than the
     *                                     declared {@code to}
     */
    @Override
    public MessageStream<EventMessage> transform(EventMessage message, TransformationContext context) {
        requireNonNull(context, "context may not be null");
        T typedPayload = inputType.resolvePayload(message, context);
        U mappedPayload = mapper.apply(typedPayload, context.processingContext());
        verifyOutputIdentity(mappedPayload, message, context);
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
     * Verifies the mapper's output resolves to the declared {@link #toType}, skipping when the resolver returns
     * {@link Optional#empty()}.
     *
     * @throws ChainConfigurationException if the resolved identity differs from {@link #toType}
     */
    private void verifyOutputIdentity(U mappedPayload, EventMessage inputMessage, TransformationContext context) {
        Optional<MessageType> resolved = context.messageTypeResolver().resolve(mappedPayload.getClass());
        if (resolved.isEmpty() || resolved.get().equals(toType)) {
            return;
        }
        throw new ChainConfigurationException("""
                Mapper output identity does not match the declared 'to' type. \
                Failing transformation: from=%s, declared to=%s. \
                Input event: %s. \
                Mapper output class=%s resolved to %s. \
                Either align the mapper's output class with the declared 'to', \
                or change the declared 'to' to match.""".formatted(
                matcher,
                toType,
                EventDescriptions.describe(inputMessage, context.entryContext()),
                mappedPayload.getClass().getName(),
                resolved.get()));
    }

    @Override
    public FromMatcher matcher() {
        return matcher;
    }

    /**
     * The declared {@code to} identity applied to this transformation's output.
     *
     * @return the declared {@code to} {@link MessageType}
     */
    MessageType toType() {
        return toType;
    }

    @Override
    public String toString() {
        return "MappingEventTransformation{from=" + matcher
                + ", to=" + toType
                + ", inputType=" + inputType.type().getTypeName()
                + '}';
    }
}
