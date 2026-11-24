/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots;

import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.EventStoreTransaction;
import org.axonframework.messaging.core.MessageStream;
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
 * An {@link EventStore} decorator applying a {@link SnapshotTransformerChain} to snapshot-based sourcing.
 * <p>
 * When an entity is sourced with a snapshot, the snapshot is also looked up at every identity the chain can lift into
 * the entity's current type, including an aggregate-based snapshot when the chain declares one. A snapshot found at
 * one of those identities is lifted into the current type before it reaches the entity. A snapshot that cannot be
 * lifted is passed on as found, which the entity rejects, sourcing itself in full instead.
 * <p>
 * Only sourcing is affected; appending, streaming and subscribing pass through untouched. This decorator is
 * registered by the snapshot transformation configuration enhancer when a {@link SnapshotTransformerChain} is
 * configured.
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
@Internal
public final class SnapshotTransformingEventStore implements EventStore {

    /**
     * The order in which this decorator is applied to the {@link EventStore}.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 1100;

    private final EventStore delegate;
    private final SnapshotTransformerChain chain;
    private final Converter converter;

    /**
     * Constructs a new instance.
     *
     * @param delegate  the event store to decorate, cannot be {@code null}
     * @param chain     the chain of snapshot transformations to apply, cannot be {@code null}
     * @param converter the converter reading snapshots stored in the application's current format, cannot be
     *                  {@code null}
     * @throws NullPointerException if any argument is {@code null}
     */
    public SnapshotTransformingEventStore(EventStore delegate, SnapshotTransformerChain chain, Converter converter) {
        this.delegate = requireNonNull(delegate, "delegate may not be null");
        this.chain = requireNonNull(chain, "chain may not be null");
        this.converter = requireNonNull(converter, "converter may not be null");
    }

    @Override
    public EventStoreTransaction transaction(ProcessingContext processingContext) {
        return new SnapshotTransformingEventStoreTransaction(
                delegate.transaction(processingContext), chain, converter, processingContext
        );
    }

    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition, @Nullable ProcessingContext context) {
        return delegate.open(condition, context);
    }

    @Override
    public CompletableFuture<Void> publish(@Nullable ProcessingContext context, List<? extends EventMessage> events) {
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
        return delegate.subscribe(eventsBatchConsumer);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("chain", chain);
        descriptor.describeProperty("converter", converter);
    }
}
