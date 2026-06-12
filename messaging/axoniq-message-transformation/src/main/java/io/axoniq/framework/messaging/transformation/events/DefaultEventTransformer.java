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
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * The {@link EventTransformer} implementation, carrying the matching and payload-mapping behavior for a single
 * registered transformation.
 *
 * @param <T> the input payload type declared at registration
 * @param <U> the output payload type the mapper produces
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class DefaultEventTransformer<T, U> implements EventTransformer {

    private final FromMatcher matcher;
    private final MessageType toType;
    private final Type inputType;
    private final Class<T> rawInputClass;
    private final BiFunction<T, @Nullable ProcessingContext, U> mapper;
    private final boolean skipIdentityCheck;

    /**
     * Constructs a 1:1 payload-mapping transformer with the output-identity check active.
     *
     * @param matcher       the {@code from}-side matcher
     * @param toType        the {@code to} identity applied to the output message
     * @param inputType     the declared input {@link Type}, preserving any generic parameters
     * @param rawInputClass the raw {@link Class} of the input type
     * @param mapper        the user-supplied payload mapping function
     */
    DefaultEventTransformer(FromMatcher matcher,
                          MessageType toType,
                          Type inputType,
                          Class<T> rawInputClass,
                          BiFunction<T, @Nullable ProcessingContext, U> mapper) {
        this(matcher, toType, inputType, rawInputClass, mapper, false);
    }

    /**
     * Full constructor exposing the {@code skipIdentityCheck} flag.
     *
     * @param matcher           the {@code from}-side matcher
     * @param toType            the {@code to} identity applied to the output message
     * @param inputType         the declared input {@link Type}
     * @param rawInputClass     the raw {@link Class} of the input type
     * @param mapper            the user-supplied payload mapping function
     * @param skipIdentityCheck {@code true} to skip the output-identity check, for transformers whose output
     *                          identity is owned by the framework rather than the mapper
     */
    DefaultEventTransformer(FromMatcher matcher,
                          MessageType toType,
                          Type inputType,
                          Class<T> rawInputClass,
                          BiFunction<T, @Nullable ProcessingContext, U> mapper,
                          boolean skipIdentityCheck) {
        this.matcher = requireNonNull(matcher, "matcher");
        this.toType = requireNonNull(toType, "toType");
        this.inputType = requireNonNull(inputType, "inputType");
        this.rawInputClass = requireNonNull(rawInputClass, "rawInputClass");
        this.mapper = requireNonNull(mapper, "mapper");
        this.skipIdentityCheck = skipIdentityCheck;
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
        requireNonNull(context, "context");
        T typedPayload = extractTypedPayload(message, context);
        U mappedPayload = mapper.apply(typedPayload, context.processingContext());
        if (!skipIdentityCheck) {
            verifyOutputIdentity(mappedPayload, message, context);
        }
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

    /**
     * Returns the payload typed as {@link #rawInputClass}, converting via the {@link MessageConverter} when the
     * payload is not already an instance of it.
     *
     * @throws IllegalStateException if the converter resolves the stored payload to {@code null}
     */
    private T extractTypedPayload(EventMessage message, TransformationContext context) {
        Object payload = message.payload();
        if (rawInputClass.isInstance(payload)) {
            return rawInputClass.cast(payload);
        }
        T converted = context.converter().convertPayload(message, inputType);
        if (converted == null) {
            throw new IllegalStateException("""
                    MessageConverter resolved the stored payload to null for declared input type %s. \
                    Input event: %s. \
                    The stored payload is missing or malformed.""".formatted(
                    inputType.getTypeName(),
                    EventDescriptions.describe(message, context.entryContext())));
        }
        return converted;
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
     * The declared {@code to} identity applied to this transformer's output.
     *
     * @return the declared {@code to} {@link MessageType}
     */
    MessageType toType() {
        return toType;
    }

    @Override
    public String toString() {
        return "DefaultEventTransformer{from=" + matcher
                + ", to=" + toType
                + ", inputType=" + inputType.getTypeName()
                + '}';
    }
}
