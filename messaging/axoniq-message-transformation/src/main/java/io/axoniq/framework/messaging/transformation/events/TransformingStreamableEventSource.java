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
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * A {@link StreamableEventSource} decorator applying an {@link EventTransformerChain} to every event read from the
 * delegate, so that event processors reading from a source other than the event store see transformed events as
 * well.
 * <p>
 * This is the streaming counterpart of the read path of {@link TransformingEventStore}: events are transformed as they
 * are read, and the criteria of a streaming condition are widened so that a type-filtering read also fetches the
 * source types the chain transforms into a queried type. Transformed events keep the context of the event they came
 * from, including its tracking token, so that a processor's progress is unaffected.
 * <p>
 * Connectors that read events from outside the application, such as the Kafka connector, wrap their sources with this
 * decorator when an {@link EventTransformerChain} is configured:
 * <pre>{@code
 * StreamableEventSource transformed =
 *         new TransformingStreamableEventSource(kafkaSource, chain, messageConverter, messageTypeResolver);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class TransformingStreamableEventSource implements StreamableEventSource, DescribableComponent {

    private final StreamableEventSource delegate;
    private final EventTransformerChain chain;
    private final MessageConverter converter;
    private final MessageTypeResolver messageTypeResolver;

    /**
     * Constructs the decorator.
     *
     * @param delegate            the source to read events from
     * @param chain               the {@link EventTransformerChain} to apply
     * @param converter           the {@link MessageConverter} used to convert payloads to each transformation's input
     *                            type
     * @param messageTypeResolver the {@link MessageTypeResolver} used to verify each mapper's output identity
     */
    public TransformingStreamableEventSource(StreamableEventSource delegate,
                                             EventTransformerChain chain,
                                             MessageConverter converter,
                                             MessageTypeResolver messageTypeResolver) {
        this.delegate = requireNonNull(delegate, "delegate may not be null");
        this.chain = requireNonNull(chain, "chain may not be null");
        this.converter = requireNonNull(converter, "converter may not be null");
        this.messageTypeResolver = requireNonNull(messageTypeResolver, "messageTypeResolver may not be null");
    }

    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition, @Nullable ProcessingContext context) {
        EventCriteria original = condition.criteria();
        EventCriteria widened = chain.widen(original);
        StreamingCondition widenedCondition = widened == original ? condition : condition.withCriteria(widened);
        return chain.transform(delegate.open(widenedCondition, context), context, converter, messageTypeResolver);
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
        return delegate.firstToken(context);
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
        return delegate.latestToken(context);
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
        return delegate.tokenAt(at, context);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", delegate);
        descriptor.describeProperty("chain", chain);
        descriptor.describeProperty("converter", converter);
        descriptor.describeProperty("messageTypeResolver", messageTypeResolver);
    }
}
