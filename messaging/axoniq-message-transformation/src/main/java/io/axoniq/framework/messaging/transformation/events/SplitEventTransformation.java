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

import io.axoniq.framework.messaging.transformation.FromMatcher;
import io.axoniq.framework.messaging.transformation.TransformationContext;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * A 1:N split {@link EventTransformation}: it replaces an event matched by exact identity {@code source} with its
 * declared outputs, each pairing a produced {@link MessageType} with the mapper deriving its payload. The stored
 * payload is converted to the declared input type once, then every output mapper derives its payload from that
 * converted input. Outputs are emitted in declaration order, all at the input's stream position.
 * <p>
 * Each output is emitted under the {@link MessageType} it was declared with. No output-identity check applies: an
 * output payload may be a stored-shape type whose own resolved identity differs from the declared one, so a split
 * can emit an older-version payload that then re-enters the chain to be lifted the rest of the way.
 *
 * @param <T> the input payload type declared at registration
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
final class SplitEventTransformation<T> implements EventTransformation {

    private final MessageType source;
    private final FromMatcher matcher;
    private final DeclaredInputType<T> inputType;
    private final List<Output<T>> outputs;
    private final Set<QualifiedName> declaredToTypes;

    /**
     * One event a split produces, pairing the produced identity with the mapper deriving its payload from the
     * converted input payload.
     *
     * @param <T>    the input payload type shared by all of a split's outputs
     * @param type   the produced event's identity
     * @param mapper maps the input payload and processing context to the produced event's payload
     */
    record Output<T>(MessageType type, BiFunction<T, @Nullable ProcessingContext, ?> mapper) {

    }

    /**
     * Constructs a 1:N split transformation.
     *
     * @param source    the {@code from} identity matched by exact equality
     * @param inputType the declared input type, preserving any generic parameters
     * @param outputs   the declared outputs, emitted in this order for every matched event
     */
    SplitEventTransformation(MessageType source, TypeReference<T> inputType, List<Output<T>> outputs) {
        this.source = requireNonNull(source, "source may not be null");
        this.matcher = new FromMatcher.Exact(source);
        this.inputType = DeclaredInputType.of(inputType);
        this.outputs = List.copyOf(requireNonNull(outputs, "outputs may not be null"));
        this.declaredToTypes = this.outputs.stream()
                                           .map(output -> output.type().qualifiedName())
                                           .collect(toUnmodifiableSet());
    }

    /**
     * Transforms the matched message into the declared outputs, in declaration order.
     *
     * @param message the matched input message
     * @param context the per-message {@link TransformationContext}
     * @return a stream of the produced events
     */
    @Override
    public MessageStream<EventMessage> transform(EventMessage message, TransformationContext context) {
        requireNonNull(context, "context may not be null");
        T typedPayload = inputType.resolvePayload(message, context);
        List<EventMessage> produced = new ArrayList<>(outputs.size());
        for (Output<T> output : outputs) {
            Object payload = output.mapper().apply(typedPayload, context.processingContext());
            requireNonNull(payload, "A split output mapper may not return null. A split emits every declared output.");
            produced.add(new GenericEventMessage(
                    message.identifier(),
                    output.type(),
                    payload,
                    message.metadata(),
                    message.timestamp()
            ));
        }
        return MessageStream.fromIterable(produced);
    }

    @Override
    public FromMatcher matcher() {
        return matcher;
    }

    /**
     * The type names of the events this split produces, derived from the declared outputs and used to widen a
     * type-filtering read back to the {@code source}.
     *
     * @return the produced {@code to} type names
     */
    Set<QualifiedName> declaredToTypes() {
        return declaredToTypes;
    }

    @Override
    public String toString() {
        return "SplitEventTransformation{from=" + source
                + ", to=" + declaredToTypes
                + ", inputType=" + inputType.type().getTypeName()
                + '}';
    }
}
