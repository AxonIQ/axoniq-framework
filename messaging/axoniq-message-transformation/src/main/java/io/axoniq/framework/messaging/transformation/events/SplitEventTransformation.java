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

/**
 * A 1:N split {@link EventTransformation}: it replaces an event matched by exact identity {@code source} with the
 * events its mapper produces, all delivered at the input's stream position. A split declares no single {@code to}
 * identity, so no output-identity check applies.
 *
 * @param <T> the input payload type declared at registration
 * @author Laura Devriendt
 * @since 5.2.1
 */
@Internal
final class SplitEventTransformation<T> implements EventTransformation {

    private final MessageType source;
    private final FromMatcher matcher;
    private final Set<QualifiedName> declaredToTypes;
    private final DeclaredInputType<T> inputType;
    private final BiFunction<T, @Nullable ProcessingContext, List<TransformedEvent>> mapper;

    /**
     * Constructs a 1:N split transformation.
     *
     * @param source          the {@code from} identity matched by exact equality
     * @param declaredToTypes the type names of the events this split produces, widening a type-filtering read
     * @param inputType       the declared input type, preserving any generic parameters
     * @param mapper          the user-supplied mapper producing the events in read-stream order
     */
    SplitEventTransformation(MessageType source,
                             Set<QualifiedName> declaredToTypes,
                             TypeReference<T> inputType,
                             BiFunction<T, @Nullable ProcessingContext, List<TransformedEvent>> mapper) {
        this.source = requireNonNull(source, "source may not be null");
        this.matcher = new FromMatcher.Exact(source);
        this.declaredToTypes = Set.copyOf(requireNonNull(declaredToTypes, "declaredToTypes may not be null"));
        this.inputType = DeclaredInputType.of(inputType);
        this.mapper = requireNonNull(mapper, "mapper may not be null");
    }

    /**
     * Transforms the matched message into the events its mapper produces, in order.
     *
     * @param message the matched input message
     * @param context the per-message {@link TransformationContext}
     * @return a stream of the produced events, empty when the mapper produces none
     */
    @Override
    public MessageStream<EventMessage> transform(EventMessage message, TransformationContext context) {
        requireNonNull(context, "context may not be null");
        T typedPayload = inputType.resolvePayload(message, context);
        List<TransformedEvent> producedEvents = mapper.apply(typedPayload, context.processingContext());
        requireNonNull(producedEvents, "A split mapper may not return null; return an empty list to drop the event.");
        List<EventMessage> outputs = new ArrayList<>(producedEvents.size());
        for (TransformedEvent produced : producedEvents) {
            requireNonNull(produced, "A split mapper may not return a null event.");
            outputs.add(new GenericEventMessage(
                    message.identifier(),
                    produced.type(),
                    produced.payload(),
                    message.metadata(),
                    message.timestamp()
            ));
        }
        return MessageStream.fromIterable(outputs);
    }

    @Override
    public FromMatcher matcher() {
        return matcher;
    }

    /**
     * The declared type names of the events this split produces, used to widen a type-filtering read.
     *
     * @return the declared {@code to} type names
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
