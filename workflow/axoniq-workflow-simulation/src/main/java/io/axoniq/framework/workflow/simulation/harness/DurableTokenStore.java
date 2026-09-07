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
package io.axoniq.framework.workflow.simulation.harness;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable processor {@link TokenStore} for the simulator — an in-memory store that, unlike a per-process token store,
 * <strong>survives a simulated crash</strong> (it is reused across {@link EngineInstance} restarts).
 * <p>
 * The engine's recovery anchor is the processor token: on start the workflow event-processing enhancer reads the
 * earliest stored segment token and replays from it. Keeping this store across an {@code EngineInstance} restart is
 * what lets the recovered engine reset to the correct point and rebuild state, so this class plays the role a durable
 * token table plays in production.
 * <p>
 * On top of the plain delegate it offers the crash-modelling controls the harness needs: {@link #freeze()} drops token
 * writes (a dying process cannot advance its checkpoint past in-flight work) and {@link #forceToken(TrackingToken)}
 * re-pins the crash-time token regardless of the freeze state.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public final class DurableTokenStore implements TokenStore {

    private final InMemoryTokenStore delegate = new InMemoryTokenStore();
    private final Set<String> processorNames = ConcurrentHashMap.newKeySet();

    private volatile boolean frozen;

    @Override
    public CompletableFuture<List<Segment>> initializeTokenSegments(String processorName,
                                                                    int segmentCount,
                                                                    @Nullable TrackingToken initialToken,
                                                                    @Nullable ProcessingContext context) {
        processorNames.add(processorName);
        return delegate.initializeTokenSegments(processorName, segmentCount, initialToken, context);
    }

    @Override
    public CompletableFuture<Void> storeToken(@Nullable TrackingToken token,
                                              String processorName,
                                              int segment,
                                              @Nullable ProcessingContext context) {
        if (frozen) {
            return CompletableFuture.completedFuture(null);
        }
        return delegate.storeToken(token, processorName, segment, context);
    }

    @Override
    public CompletableFuture<TrackingToken> fetchToken(String processorName,
                                                       int segment,
                                                       @Nullable ProcessingContext context) {
        return delegate.fetchToken(processorName, segment, context);
    }

    @Override
    public CompletableFuture<Void> releaseClaim(String processorName,
                                                int segment,
                                                @Nullable ProcessingContext context) {
        return delegate.releaseClaim(processorName, segment, context);
    }

    @Override
    public CompletableFuture<Void> initializeSegment(@Nullable TrackingToken token,
                                                     String processorName,
                                                     Segment segment,
                                                     @Nullable ProcessingContext context) {
        processorNames.add(processorName);
        return delegate.initializeSegment(token, processorName, segment, context);
    }

    @Override
    public CompletableFuture<Void> deleteToken(String processorName,
                                               int segment,
                                               @Nullable ProcessingContext context) {
        return delegate.deleteToken(processorName, segment, context);
    }

    @Override
    public CompletableFuture<Segment> fetchSegment(String processorName,
                                                   int segmentId,
                                                   @Nullable ProcessingContext context) {
        return delegate.fetchSegment(processorName, segmentId, context);
    }

    @Override
    public CompletableFuture<List<Segment>> fetchSegments(String processorName,
                                                          @Nullable ProcessingContext context) {
        return delegate.fetchSegments(processorName, context);
    }

    @Override
    public CompletableFuture<List<Segment>> fetchAvailableSegments(String processorName,
                                                                   @Nullable ProcessingContext context) {
        return delegate.fetchAvailableSegments(processorName, context);
    }

    @Override
    public CompletableFuture<String> retrieveStorageIdentifier(@Nullable ProcessingContext context) {
        return delegate.retrieveStorageIdentifier(context);
    }

    /**
     * Returns the durable recovery anchor: the lower bound of every stored segment token, or {@code null} when nothing
     * has been stored yet. This is the token a recovered engine resets to.
     *
     * @return the lower bound of the stored segment tokens, or {@code null}.
     */
    @Nullable
    public TrackingToken currentToken() {
        TrackingToken lowest = null;
        for (String processorName : processorNames) {
            for (Segment segment : delegate.fetchSegments(processorName, null).join()) {
                lowest = lowerBound(lowest,
                                    delegate.fetchToken(processorName, segment.getSegmentId(), null).join());
            }
        }
        return lowest;
    }

    /**
     * Forces {@code token} onto every known segment regardless of the freeze state (used by the harness to re-pin the
     * crash-time recovery anchor).
     *
     * @param token the token to pin.
     */
    public void forceToken(TrackingToken token) {
        for (String processorName : processorNames) {
            for (Segment segment : delegate.fetchSegments(processorName, null).join()) {
                // storeToken requires a live ProcessingContext; re-initializing the segment is the context-free way
                // to overwrite a segment's token.
                delegate.deleteToken(processorName, segment.getSegmentId(), null).join();
                delegate.initializeSegment(token, processorName, segment, null).join();
            }
        }
    }

    /**
     * Freezes the store so {@link #storeToken} is ignored — models a crash where the dying process cannot advance its
     * checkpoint past its in-flight work.
     */
    public void freeze() {
        frozen = true;
    }

    /**
     * Unfreezes the store so the recovered engine can persist tokens again.
     */
    public void unfreeze() {
        frozen = false;
    }

    @Nullable
    private static TrackingToken lowerBound(@Nullable TrackingToken left, @Nullable TrackingToken right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return left.lowerBound(right);
    }
}
