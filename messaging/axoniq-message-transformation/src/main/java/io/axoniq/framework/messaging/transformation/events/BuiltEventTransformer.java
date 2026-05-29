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
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Optional;
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
    private final boolean skipIdentityCheck;

    /**
     * Constructs a 1:1 payload-mapping transformer with the output-identity check active. The
     * check fires after every mapper invocation and verifies the mapper's output resolves to
     * the declared {@code to}.
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
        this(matcher, toType, inputType, rawInputClass, mapper, false);
    }

    /**
     Full constructor exposing the skipIdentityCheck flag.
     <p>
     Set the flag to true only when the framework, not the mapper, owns the output
     identity. Needed for a rename which keeps the input
     payload unchanged. The default check would resolve that payload's class to the source
     identity and reject the newly declared to as a mismatch, even though the rename is
     intentional.
     *
     * @param matcher           the {@code from}-side matcher
     * @param toType            the {@code to} identity applied to the output message
     * @param inputType         the declared input {@link Type}
     * @param rawInputClass     the raw {@link Class} of the input type
     * @param mapper            the user-supplied payload mapping function
     * @param skipIdentityCheck {@code true} only when the framework owns the output identity
     *                          (currently: the rename factory); {@code false} for every
     *                          payload-mapping transformer
     */
    BuiltEventTransformer(FromMatcher matcher,
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
     * Direct invocation is unsupported. Factory-built transformers must be registered with an
     * {@link EventTransformerChain} which supplies the framework's {@link MessageConverter}
     * and {@link MessageTypeResolver}, and threads the {@link ProcessingContext}. Tests
     * exercise the same chain path that production uses, ensuring there is no test-only
     * behavior divergence.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public MessageStream<EventMessage> transform(EventMessage message,
                                                           @Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Factory-built EventTransformer instances must be invoked through "
                        + "EventTransformerChain.transform(stream, context, converter, messageTypeResolver); "
                        + "direct invocation is not supported.");
    }

    /**
     * Framework-internal counterpart to the {@link EventTransformer#transform public SPI},
     * called by {@link EventTransformerChain} once it has matched this transformer against
     * the input. Extracts the typed payload via the runtime's {@link MessageConverter},
     * invokes the user's mapper, verifies the mapper's output identity against the declared
     * {@link #toType} unless {@code skipIdentityCheck} is set, and returns the result
     * wrapped with the declared {@link MessageType} and the input's envelope preserved.
     *
     * @param message the matched input message
     * @param runtime per-event framework bundle (entry context, processing context,
     *                converter, resolver)
     * @return the transformed output message
     * @throws ChainConfigurationException if the resolver resolves the mapper's output to a
     *                                     {@link MessageType} other than the declared
     *                                     {@link #toType}
     */
    EventMessage applyTo(EventMessage message, ChainRuntime runtime) {
        requireNonNull(runtime, "runtime");
        T typedPayload = extractTypedPayload(message, runtime);
        U mappedPayload = mapper.apply(typedPayload, runtime.processingContext());
        if (!skipIdentityCheck) {
            verifyOutputIdentity(mappedPayload, message, runtime);
        }
        return new GenericEventMessage(
                message.identifier(),
                toType,
                mappedPayload,
                message.metadata(),
                message.timestamp()
        );
    }

    /**
     * Verifies the mapper's output identity against the declared {@link #toType}. Skips
     * silently when the resolver returns {@link Optional#empty()}. This is the only possible
     * behavior for untyped representations whose runtime class carries no identity
     * annotation.
     * <p>
     * The thrown exception identifies the failing transformation (matcher + declared
     * {@code to}), the input event ({@link EventMessage#type()} + identifier + stream
     * position from the runtime when available), and the mismatching output (class +
     * resolved {@link MessageType}).
     */
    private void verifyOutputIdentity(U mappedPayload, EventMessage inputMessage, ChainRuntime runtime) {
        Optional<MessageType> resolved = runtime.messageTypeResolver().resolve(mappedPayload.getClass());
        if (resolved.isEmpty() || resolved.get().equals(toType)) {
            return;
        }
        throw new ChainConfigurationException(
                "Mapper output identity does not match the declared 'to' type. "
                        + "Failing transformation: from=" + matcher + ", declared to=" + toType + ". "
                        + "Input event: " + describeEvent(inputMessage, runtime.entryContext()) + ". "
                        + "Mapper output class=" + mappedPayload.getClass().getName()
                        + " resolved to " + resolved.get() + ". "
                        + "Either align the mapper's output class with the declared 'to', "
                        + "or change the declared 'to' to match.");
    }

    /**
     * Returns the payload typed as {@link #rawInputClass}. Fast path: when the payload is
     * already an instance of the raw input class, hand it through via the checked
     * {@code Class.cast}. Otherwise, the framework's
     * {@link MessageConverter#convertPayload(org.axonframework.messaging.core.Message, Type)}
     * is invoked with the full message and the declared {@link #inputType}; the converter
     * preserves the generic parameters carried by {@code inputType} (relevant for
     * {@code TypeReference<T>} registrations).
     * <p>
     * A converter that resolves to {@code null} for the declared input type indicates a
     * malformed stored payload; we surface that immediately as an
     * {@link IllegalStateException} so the diagnostic lands at the chain layer with the
     * input event's identity rather than as a downstream {@code NullPointerException}.
     */
    private T extractTypedPayload(EventMessage message, ChainRuntime runtime) {
        Object payload = message.payload();
        if (rawInputClass.isInstance(payload)) {
            return rawInputClass.cast(payload);
        }
        T converted = runtime.converter().convertPayload(message, inputType);
        if (converted == null) {
            throw new IllegalStateException(
                    "MessageConverter resolved the stored payload to null for declared input type "
                            + inputType.getTypeName() + ". "
                            + "Input event: " + describeEvent(message, runtime.entryContext())
                            + ". The stored payload is missing or malformed.");
        }
        return converted;
    }

    /**
     * Renders the input event for inclusion in diagnostic exception messages: type, identifier,
     * and stream position (when the storage engine populated a {@link TrackingToken} on the
     * entry's context). Position is omitted on entity-load / DCB-source paths where the
     * engine does not attach a token.
     */
    static String describeEvent(EventMessage message, Context entryContext) {
        StringBuilder description = new StringBuilder()
                .append("type=").append(message.type())
                .append(", identifier=").append(message.identifier());
        TrackingToken.fromContext(entryContext)
                     .ifPresent(token -> description.append(", position=").append(token));
        return description.toString();
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
