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
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * The {@link EventTransformer} implementation produced by the {@link EventTransformation}
 * factory. Internal because the {@code EventTransformer} interface is the public
 * lambda-friendly SPI, and consumers should only ever get instances through the factory.
 *
 * @param <T> the input payload type declared at registration
 * @param <U> the output payload type the mapper produces
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
final class BuiltEventTransformer<T, U> implements EventTransformer {

    private final FromMatcher matcher;
    private final MessageType toType;
    private final Type inputType;
    private final Class<T> rawInputClass;
    private final BiFunction<T, @Nullable ProcessingContext, U> mapper;

    /**
     * Constructs a built transformer.
     *
     * @param matcher       the {@code from}-side matcher
     * @param toType        the {@code to} identity applied to the output message
     * @param inputType     the declared input {@link Type}, preserved with any generic
     *                      parameters for the framework's {@code MessageConverter}
     * @param rawInputClass the raw {@link Class} of the input type, used for the same-class
     *                      fast path (avoiding a no-op converter call when the payload
     *                      already satisfies the input type)
     * @param mapper        the user-supplied payload mapping function
     */
    BuiltEventTransformer(FromMatcher matcher,
                          MessageType toType,
                          Type inputType,
                          Class<T> rawInputClass,
                          BiFunction<T, @Nullable ProcessingContext, U> mapper) {
        this.matcher = requireNonNull(matcher, "matcher");
        this.toType = requireNonNull(toType, "toType");
        this.inputType = requireNonNull(inputType, "inputType");
        this.rawInputClass = requireNonNull(rawInputClass, "rawInputClass");
        this.mapper = requireNonNull(mapper, "mapper");
    }

    /**
     * Direct invocation is unsupported. Factory-built transformers must be registered with an
     * {@link EventTransformerChain} which supplies the framework's {@link MessageConverter}
     * and threads the {@link ProcessingContext}. Tests exercise the same chain path that
     * production uses, ensuring there is no test-only behavior divergence.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public MessageStream<? extends EventMessage> transform(EventMessage message,
                                                           @Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Factory-built EventTransformer instances must be invoked through "
                        + "EventTransformerChain.transform(stream, context, converter); "
                        + "direct invocation is not supported.");
    }

    /**
     * Applies this transformer to {@code message} assuming the {@link #matcher} has already
     * matched. The chain calls this directly to skip a redundant match check; the framework
     * supplies the {@link MessageConverter} for input type / payload type mismatches and
     * threads the active {@link ProcessingContext} to the user's mapper per FR-009.
     *
     * @param message   the matched input
     * @param context   the active processing context, or {@code null} on the tracking
     *                  processor read path
     * @param converter the framework's payload converter
     * @return the transformed output
     */
    EventMessage applyTo(EventMessage message,
                         @Nullable ProcessingContext context,
                         MessageConverter converter) {
        requireNonNull(converter, "converter");
        T typedPayload = extractTypedPayload(message, converter);
        U mappedPayload = mapper.apply(typedPayload, context);
        return new GenericEventMessage(
                message.identifier(),
                toType,
                mappedPayload,
                message.metadata(),
                message.timestamp()
        );
    }

    /**
     * Returns the payload typed as {@link #rawInputClass}. Fast path: when the payload is
     * already an instance of the raw input class, hand it through via the checked
     * {@code Class.cast}. Otherwise {@code payloadAs(inputType, converter)} delegates
     * deserialization to the framework's converter, which preserves the generic parameters
     * carried by {@link #inputType} (relevant for {@code TypeReference<T>} registrations).
     */
    private T extractTypedPayload(EventMessage message, MessageConverter converter) {
        Object payload = message.payload();
        if (rawInputClass.isInstance(payload)) {
            return rawInputClass.cast(payload);
        }
        return message.payloadAs(inputType, converter);
    }

    /**
     * The {@code from}-side matcher this transformer was built with.
     *
     * @return the {@code from}-side matcher
     */
    FromMatcher matcher() {
        return matcher;
    }

    @Override
    public String toString() {
        return "BuiltEventTransformer{from=" + matcher
                + ", to=" + toType
                + ", inputType=" + inputType.getTypeName()
                + '}';
    }
}
