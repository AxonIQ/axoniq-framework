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
     * Full constructor allowing the output-identity check to be bypassed. Used by the rename
     * factory: a pure rename's output identity is set by the framework, not derived from the
     * mapper's return class, so the check would always reject an annotated source POJO even
     * though the rename is correct by construction.
     *
     * @param matcher           the {@code from}-side matcher
     * @param toType            the {@code to} identity applied to the output message
     * @param inputType         the declared input {@link Type}
     * @param rawInputClass     the raw {@link Class} of the input type
     * @param mapper            the user-supplied payload mapping function
     * @param skipIdentityCheck {@code true} only for transformers where the framework owns
     *                          the output identity (currently: pure renames); {@code false}
     *                          for all payload-mapping transformers
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
     * behaviour divergence.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public MessageStream<? extends EventMessage> transform(EventMessage message,
                                                           @Nullable ProcessingContext context) {
        throw new UnsupportedOperationException(
                "Factory-built EventTransformer instances must be invoked through "
                        + "EventTransformerChain.transform(stream, context, converter, messageTypeResolver); "
                        + "direct invocation is not supported.");
    }

    /**
     * Applies this transformer to {@code message} assuming the {@link #matcher} has already
     * matched. The chain calls this directly to skip a redundant match check; the framework
     * supplies the {@link MessageConverter} for input type / payload type mismatches, threads
     * the active {@link ProcessingContext} to the user's mapper, and uses the supplied
     * {@link MessageTypeResolver} to verify the mapper's output identity against the declared
     * {@link #toType} (unless this transformer was constructed with
     * {@code skipIdentityCheck = true}). The {@code entryContext} is the per-event
     * {@link Context} that the storage engine attached to the stream entry; the chain reads
     * the {@link TrackingToken} from it to enrich diagnostic exceptions with stream position.
     *
     * @param message              the matched input
     * @param entryContext         per-entry context carrying the {@link TrackingToken}; used
     *                             only for error diagnostics
     * @param context              the active processing context, or {@code null} on the
     *                             tracking processor read path
     * @param converter            the framework's payload converter
     * @param messageTypeResolver  the resolver used to verify the mapper's output identity
     *                             against the declared {@link #toType}; skipped when the
     *                             resolver returns {@link Optional#empty()} (typical for
     *                             untyped representations such as {@code JsonNode} or
     *                             {@code Map})
     * @return the transformed output
     * @throws ChainConfigurationException if the resolver resolves the mapper's output to a
     *                                     {@link MessageType} other than the declared
     *                                     {@link #toType}
     */
    EventMessage applyTo(EventMessage message,
                         Context entryContext,
                         @Nullable ProcessingContext context,
                         MessageConverter converter,
                         MessageTypeResolver messageTypeResolver) {
        requireNonNull(entryContext, "entryContext");
        requireNonNull(converter, "converter");
        requireNonNull(messageTypeResolver, "messageTypeResolver");
        T typedPayload = extractTypedPayload(message, entryContext, converter);
        U mappedPayload = mapper.apply(typedPayload, context);
        if (!skipIdentityCheck) {
            verifyOutputIdentity(mappedPayload, message, entryContext, messageTypeResolver);
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
     * silently when the resolver returns {@link Optional#empty()} -- the only feasible
     * behaviour for untyped representations whose runtime class carries no identity
     * annotation.
     * <p>
     * The thrown exception identifies the failing transformation (matcher + declared
     * {@code to}), the input event ({@link EventMessage#type()} + identifier + stream
     * position read from {@code entryContext} when available), and the mismatching
     * output (class + resolved {@link MessageType}).
     */
    private void verifyOutputIdentity(U mappedPayload,
                                      EventMessage inputMessage,
                                      Context entryContext,
                                      MessageTypeResolver messageTypeResolver) {
        Optional<MessageType> resolved = messageTypeResolver.resolve(mappedPayload.getClass());
        if (resolved.isEmpty() || resolved.get().equals(toType)) {
            return;
        }
        throw new ChainConfigurationException(
                "Mapper output identity does not match the declared 'to' type. "
                        + "Failing transformation: from=" + matcher + ", declared to=" + toType + ". "
                        + "Input event: " + describeEvent(inputMessage, entryContext) + ". "
                        + "Mapper output class=" + mappedPayload.getClass().getName()
                        + " resolved to " + resolved.get() + ". "
                        + "Either align the mapper's output class with the declared 'to', "
                        + "or change the declared 'to' to match.");
    }

    /**
     * Returns the payload typed as {@link #rawInputClass}. Fast path: when the payload is
     * already an instance of the raw input class, hand it through via the checked
     * {@code Class.cast}. Otherwise the framework's
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
    private T extractTypedPayload(EventMessage message,
                                  Context entryContext,
                                  MessageConverter converter) {
        Object payload = message.payload();
        if (rawInputClass.isInstance(payload)) {
            return rawInputClass.cast(payload);
        }
        T converted = converter.convertPayload(message, inputType);
        if (converted == null) {
            throw new IllegalStateException(
                    "MessageConverter resolved the stored payload to null for declared input type "
                            + inputType.getTypeName() + ". "
                            + "Input event: " + describeEvent(message, entryContext)
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
