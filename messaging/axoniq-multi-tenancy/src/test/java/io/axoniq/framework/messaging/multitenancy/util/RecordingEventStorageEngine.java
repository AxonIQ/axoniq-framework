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

package io.axoniq.framework.messaging.multitenancy.util;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.SourcingStrategy;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link EventStorageEngine} test double that resolves no snapshots of its own, standing in for an engine such as
 * {@code AxonServerEventStorageEngine}.
 * <p>
 * It is not a {@link SnapshotStore}, so it has to be decorated with a snapshot store to serve a
 * {@link SourcingStrategy.Snapshot} strategy. It records how often it was written to and sourced from, so tests can
 * assert which tenant's engine a routing engine delegated to. Read operations are otherwise inert.
 * <p>
 * Use {@link RecordingSnapshotResolvingEventStorageEngine} for an engine that resolves snapshots within its own
 * {@link #source(SourcingCondition, ProcessingContext) source} call.
 */
public class RecordingEventStorageEngine implements EventStorageEngine {

    private int appendCount;
    private int sourceCount;
    private boolean sourcedWithSnapshotStrategy;

    /**
     * @return how often {@link #appendEvents(AppendCondition, ProcessingContext, List)} was called
     */
    public int appendCount() {
        return appendCount;
    }

    /**
     * @return how often {@link #source(SourcingCondition, ProcessingContext)} was called
     */
    public int sourceCount() {
        return sourceCount;
    }

    /**
     * Tells whether this engine was sourced with the {@link SourcingStrategy.Snapshot} strategy, meaning the strategy
     * was not consumed before reaching it.
     *
     * @return {@code true} when a snapshot sourcing strategy reached this engine
     */
    public boolean sourcedWithSnapshotStrategy() {
        return sourcedWithSnapshotStrategy;
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                @Nullable ProcessingContext context,
                                                                List<TaggedEventMessage<?>> events) {
        appendCount++;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition, @Nullable ProcessingContext context) {
        sourceCount++;
        if (condition.strategy() instanceof SourcingStrategy.Snapshot) {
            sourcedWithSnapshotStrategy = true;
        }
        return MessageStream.empty().cast();
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        return MessageStream.empty().cast();
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        // no-op
    }
}
