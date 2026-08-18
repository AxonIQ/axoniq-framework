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
package io.axoniq.workflow.configuration;

import jakarta.annotation.Nullable;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToClaimTokenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Reads the earliest position over a processor's segment tokens, the input to the workflow engine's startup replay
 * decision.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 0.3.0
 */
final class SegmentTokenScan {

    private static final Logger logger = LoggerFactory.getLogger(SegmentTokenScan.class);

    private SegmentTokenScan() {
    }

    /**
     * Returns the earliest position over the segment tokens this node can read. With multiple segments the replay
     * decision and the engine's replay tracking must consider the segment that is furthest behind, otherwise catch-up
     * work of lagging segments would be treated as already-live processing.
     * <p>
     * A segment owned by another node is skipped: it is already being processed there and is not part of this node's
     * replay decision. If no segment can be read the result is {@code null}, and the node starts without a replay;
     * its executions are restored per segment by the segment change listener once it actually claims one.
     * <p>
     * Segments are read one at a time and the claim the read takes is released before the next one is read, so a
     * starting node never holds a claim on one segment while reading another. Holding them makes two nodes starting
     * at the same time block each other.
     *
     * @param tokenStore    token store holding the processor's segment tokens
     * @param processorName name of the processor whose segments are read
     * @param segments      the processor's segments
     * @return the earliest readable position, or {@code null} when no segment can be read or a token is unknown
     */
    static CompletableFuture<TrackingToken> earliestSegmentToken(TokenStore tokenStore,
                                                                 String processorName,
                                                                 List<Segment> segments) {
        var readable = new ArrayList<TrackingToken>();
        CompletableFuture<Void> scan = completedFuture(null);
        for (var segment : segments) {
            var segmentId = segment.getSegmentId();
            scan = scan.thenCompose(ignored -> fetchTokenAndReleaseClaim(tokenStore, processorName, segmentId)
                    .thenAccept(readable::add)
                    .exceptionally(ex -> skipSegmentOwnedByAnotherNode(ex, processorName, segmentId)));
        }
        return scan.thenApply(ignored -> earliest(readable));
    }

    /**
     * Swallows the failure of a segment owned by another node, and rethrows anything else.
     */
    @Nullable
    private static Void skipSegmentOwnedByAnotherNode(Throwable ex, String processorName, int segmentId) {
        var cause = ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
        if (!(cause instanceof UnableToClaimTokenException)) {
            throw cause instanceof RuntimeException runtime ? runtime : new CompletionException(cause);
        }
        logger.info("Segment {} of processor {} is owned by another node; leaving it out of the replay decision.",
                    segmentId, processorName);
        return null;
    }

    /**
     * Returns the lowest of the given tokens, or {@code null} when there is nothing to fold or a token is unknown.
     */
    @Nullable
    private static TrackingToken earliest(List<TrackingToken> tokens) {
        if (tokens.isEmpty() || tokens.contains(null)) {
            return null;
        }
        var earliest = tokens.get(0);
        for (var token : tokens) {
            earliest = earliest.lowerBound(token);
        }
        return earliest;
    }

    private static CompletableFuture<TrackingToken> fetchTokenAndReleaseClaim(TokenStore tokenStore,
                                                                              String processorName,
                                                                              int segmentId) {
        return tokenStore.fetchToken(processorName, segmentId, null)
                         .thenCompose(token -> tokenStore.releaseClaim(processorName, segmentId, null)
                                                         .thenApply(ignored -> token));
    }
}
