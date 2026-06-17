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

import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * {@link EventStore} decorator that applies an {@link EventTransformerChain} to every
 * read path (entity loads, DCB reads, tracking-processor reads, ...). Installed automatically
 * by {@code EventTransformationConfigurationEnhancer}; not constructed by users.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
@Internal
public final class TransformingEventStore implements EventStore {

    /**
     * Decoration order: outer (later) than {@link InterceptingEventStore#DECORATION_ORDER}, with
     * headroom for other decorators to slot in between.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 1000;

    private final EventStore delegate;
    private final EventTransformerChain chain;
    private final MessageConverter converter;
    private final MessageTypeResolver messageTypeResolver;

    /**
     * Constructs the decorator.
     *
     * @param delegate            the inner {@link EventStore} to wrap
     * @param chain               the {@link EventTransformerChain} to apply
     * @param converter           the {@link MessageConverter} used to convert payloads to each transformation's input type
     * @param messageTypeResolver the {@link MessageTypeResolver} used to verify each mapper's output identity
     */
    public TransformingEventStore(EventStore delegate,
                                   EventTransformerChain chain,
                                   MessageConverter converter,
                                   MessageTypeResolver messageTypeResolver) {
        this.delegate = requireNonNull(delegate, "delegate may not be null");
        this.chain = requireNonNull(chain, "chain may not be null");
        this.converter = requireNonNull(converter, "converter may not be null");
        this.messageTypeResolver = requireNonNull(messageTypeResolver, "messageTypeResolver may not be null");
    }

    @Override
    public EventStoreTransaction transaction(ProcessingContext processingContext) {
        // No per-context caching: this decorator only transforms the read path (source(...)).
        // Append callbacks register on the delegate transaction, which the delegate caches if
        // needed, so the thin wrapper carries no state worth sharing across calls. Returning a
        // fresh wrapper also avoids ConcurrentHashMap recursive-update detection that a nested
        // computeResourceIfAbsent could trigger against a per-context caching delegate.
        return new TransformingEventStoreTransaction(
                delegate.transaction(processingContext), chain, processingContext,
                converter, messageTypeResolver);
    }

    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition,
                                            @Nullable ProcessingContext context) {
        return chain.transform(delegate.open(condition, context), context, converter, messageTypeResolver);
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context,
                                            List<? extends EventMessage> events) {
        return delegate.publish(context, events);
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
    public Registration subscribe(
            BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventsBatchConsumer) {
        // Intentionally not transformed: this is the EventBus live dispatch to subscribed listeners,
        // not a read of stored events. Transformation applies only to the read paths (open / source).
        return delegate.subscribe(eventsBatchConsumer);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", delegate);
        descriptor.describeProperty("chain", chain);
        descriptor.describeProperty("converter", converter);
        descriptor.describeProperty("messageTypeResolver", messageTypeResolver);
    }
}
