/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SourcingCondition;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.messaging.core.DelegatingMessageStream;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamingCondition;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The event store as one configuration sees it: the shared store, which stops notifying that configuration once it
 * stopped, the way a connection to a durable event store ends with its process. The in-memory store itself keeps
 * notifying every stream it ever opened, so a later append would call into the stopped event processor and fail.
 *
 * @author Stefan Dragisic
 */
final class DisconnectableStorageEngine implements EventStorageEngine {

    private final EventStorageEngine delegate;
    private final AtomicBoolean connected = new AtomicBoolean(true);

    DisconnectableStorageEngine(EventStorageEngine delegate) {
        this.delegate = delegate;
    }

    void disconnect() {
        connected.set(false);
    }

    @Override
    public CompletableFuture<AppendTransaction<?>> appendEvents(AppendCondition condition,
                                                                ProcessingContext processingContext,
                                                                List<TaggedEventMessage<?>> events) {
        return delegate.appendEvents(condition, processingContext, events);
    }

    @Override
    public MessageStream<EventMessage> source(SourcingCondition condition, ProcessingContext processingContext) {
        return delegate.source(condition, processingContext);
    }

    @Override
    public MessageStream<EventMessage> stream(StreamingCondition condition) {
        return new DelegatingMessageStream<EventMessage, EventMessage>(delegate.stream(condition)) {
            @Override
            public Optional<Entry<EventMessage>> next() {
                return delegate().next();
            }

            @Override
            public Optional<Entry<EventMessage>> peek() {
                return delegate().peek();
            }

            @Override
            public void setCallback(Runnable callback) {
                super.setCallback(() -> {
                    if (connected.get()) {
                        callback.run();
                    }
                });
            }
        };
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken() {
        return delegate.firstToken();
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken() {
        return delegate.latestToken();
    }

    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at) {
        return delegate.tokenAt(at);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
    }
}
