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
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.EventTrackerStatus;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.TrackingTokenSource;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A {@link StreamingEventProcessor} for tests that records {@link #start()} and {@link #shutdown()} calls and tracks
 * its running state. Only the lifecycle methods relevant to restart behaviour are implemented. The segment and reset
 * operations are unsupported.
 */
public class RecordingStreamingEventProcessor implements StreamingEventProcessor {

    private final String name;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger startCount = new AtomicInteger();
    private final AtomicInteger shutdownCount = new AtomicInteger();

    /**
     * Constructs a processor with the given {@code name} and initial running state.
     *
     * @param name    the processor name
     * @param running whether the processor is running initially
     */
    public RecordingStreamingEventProcessor(String name, boolean running) {
        this.name = name;
        this.running.set(running);
    }

    /**
     * Returns the number of times {@link #start()} was invoked.
     *
     * @return the start invocation count
     */
    public int startCount() {
        return startCount.get();
    }

    /**
     * Returns the number of times {@link #shutdown()} was invoked.
     *
     * @return the shutdown invocation count
     */
    public int shutdownCount() {
        return shutdownCount.get();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CompletableFuture<Void> start() {
        running.set(true);
        startCount.incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        running.set(false);
        shutdownCount.incrementAndGet();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("name", name);
    }

    @Override
    public String getTokenStoreIdentifier() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> releaseSegment(int segmentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> releaseSegment(int segmentId, long releaseDuration, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> splitSegment(int segmentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> mergeSegment(int segmentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> resetTokens() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> CompletableFuture<Void> resetTokens(@Nullable R resetContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> resetTokens(
            Function<TrackingTokenSource, CompletableFuture<TrackingToken>> initialTrackingTokenSupplier
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> CompletableFuture<Void> resetTokens(
            Function<TrackingTokenSource, CompletableFuture<TrackingToken>> initialTrackingTokenSupplier,
            @Nullable R resetContext
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> CompletableFuture<Void> resetTokens(TrackingToken startPosition, @Nullable R resetContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxCapacity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, EventTrackerStatus> processingStatus() {
        throw new UnsupportedOperationException();
    }
}
