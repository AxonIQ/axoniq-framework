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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The event store as one configuration sees it: the shared store, which stops notifying that configuration once it
 * stopped, the way a connection to a durable event store ends with its process. The in-memory store itself keeps
 * notifying every stream it ever opened, so a later append would call into the stopped event processor and fail.
 * <p>
 * The streams of a running configuration can also be paused, so the configuration sees no event appended after the
 * pause until they are resumed, the way a slow node lags behind the event store.
 *
 * @author Stefan Dragisic
 */
final class DisconnectableStorageEngine implements EventStorageEngine {

    private final EventStorageEngine delegate;
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final Set<Runnable> callbacks = ConcurrentHashMap.newKeySet();

    DisconnectableStorageEngine(EventStorageEngine delegate) {
        this.delegate = delegate;
    }

    void disconnect() {
        connected.set(false);
    }

    /**
     * Stops delivering events on every stream of this configuration, until {@link #resumeStreams()}.
     */
    void pauseStreams() {
        paused.set(true);
    }

    /**
     * Delivers the events on every stream of this configuration again, starting with those appended while paused.
     */
    void resumeStreams() {
        paused.set(false);
        callbacks.forEach(Runnable::run);
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
                return paused.get() ? Optional.empty() : delegate().next();
            }

            @Override
            public Optional<Entry<EventMessage>> peek() {
                return paused.get() ? Optional.empty() : delegate().peek();
            }

            @Override
            public boolean hasNextAvailable() {
                return !paused.get() && delegate().hasNextAvailable();
            }

            @Override
            public void setCallback(Runnable callback) {
                Runnable connectedCallback = () -> {
                    if (connected.get() && !paused.get()) {
                        callback.run();
                    }
                };
                callbacks.add(connectedCallback);
                super.setCallback(connectedCallback);
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
