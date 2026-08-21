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

import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToClaimTokenException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link SegmentTokenScan} feeds the startup replay decision: the earliest readable segment position, with segments
 * owned by other nodes left out and each read's claim released before the next segment is read.
 */
class SegmentTokenScanTest {

    private static final String PROCESSOR = "Workflow";
    private static final List<Segment> FOUR_SEGMENTS = IntStream.range(0, 4)
                                                                .mapToObj(id -> new Segment(id, 3))
                                                                .toList();

    private final TokenStore tokenStore = mock(TokenStore.class);

    @Test
    void returnsTheEarliestPositionAcrossAllReadableSegments() {
        stubToken(0, token(10));
        stubToken(1, token(5));
        stubToken(2, token(42));
        stubToken(3, token(9));

        assertThat(scan()).isEqualTo(token(5));
    }

    @Test
    void returnsNullWhenAnySegmentPositionIsUnknown() {
        stubToken(0, token(10));
        stubToken(1, null);
        stubToken(2, token(42));
        stubToken(3, token(9));

        assertThat(scan())
                .as("an unknown segment position means the replay lower bound is unknown, so no replay is decided")
                .isNull();
    }

    @Test
    void skipsSegmentsOwnedByAnotherNode() {
        stubToken(0, token(10));
        stubUnclaimable(1);
        stubToken(2, token(7));
        stubToken(3, token(9));

        assertThat(scan())
                .as("a segment processed on another node is not part of this node's replay decision")
                .isEqualTo(token(7));
    }

    @Test
    void returnsNullWhenEverySegmentIsOwnedByAnotherNode() {
        stubUnclaimable(0);
        stubUnclaimable(1);
        stubUnclaimable(2);
        stubUnclaimable(3);

        assertThat(scan())
                .as("a node owning nothing starts without a replay; claims restore its segments later")
                .isNull();
    }

    @Test
    void rethrowsFailuresThatAreNotOwnershipClaims() {
        stubToken(0, token(10));
        when(tokenStore.fetchToken(eq(PROCESSOR), eq(1), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("store broken")));
        stubToken(2, token(7));
        stubToken(3, token(9));

        assertThatThrownBy(this::scan)
                .as("only ownership conflicts are expected at startup; anything else must surface")
                .isInstanceOf(CompletionException.class)
                .hasRootCauseMessage("store broken");
    }

    @Test
    void releasesEachReadsClaimBeforeReadingTheNextSegment() {
        stubToken(0, token(10));
        stubToken(1, token(5));
        stubToken(2, token(42));
        stubToken(3, token(9));

        scan();

        // Holding a claim while reading the next segment makes two starting nodes block each other.
        var order = inOrder(tokenStore);
        order.verify(tokenStore).fetchToken(eq(PROCESSOR), eq(0), any());
        order.verify(tokenStore).releaseClaim(eq(PROCESSOR), eq(0), any());
        order.verify(tokenStore).fetchToken(eq(PROCESSOR), eq(1), any());
        order.verify(tokenStore).releaseClaim(eq(PROCESSOR), eq(1), any());
        order.verify(tokenStore).fetchToken(eq(PROCESSOR), eq(2), any());
        order.verify(tokenStore).releaseClaim(eq(PROCESSOR), eq(2), any());
        order.verify(tokenStore).fetchToken(eq(PROCESSOR), eq(3), any());
        order.verify(tokenStore).releaseClaim(eq(PROCESSOR), eq(3), any());
    }

    private TrackingToken scan() {
        return SegmentTokenScan.earliestSegmentToken(tokenStore, PROCESSOR, FOUR_SEGMENTS).join();
    }

    private void stubToken(int segmentId, TrackingToken token) {
        when(tokenStore.fetchToken(eq(PROCESSOR), eq(segmentId), any()))
                .thenReturn(CompletableFuture.completedFuture(token));
        when(tokenStore.releaseClaim(eq(PROCESSOR), eq(segmentId), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void stubUnclaimable(int segmentId) {
        when(tokenStore.fetchToken(eq(PROCESSOR), eq(segmentId), any()))
                .thenReturn(CompletableFuture.failedFuture(
                        new UnableToClaimTokenException("owned by another node")));
    }

    private static TrackingToken token(long position) {
        return new GlobalSequenceTrackingToken(position);
    }
}
