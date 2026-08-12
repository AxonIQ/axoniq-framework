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

import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.execution.WorkflowEngineReplaySupport;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.UnableToClaimTokenException;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

/**
 * Test for processor-token based replay initialization.
 */
class WorkflowEventProcessingRegistrationEnhancerTest {

    @Test
    void earlierProcessorTokenStartsCheckpointCatchUpAfterRehydration() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var workflowEngine = mock(WorkflowEngine.class);
        var replaySupport = mock(WorkflowEngineReplaySupport.class);
        var processorToken = token(18);
        var latestToken = token(192);
        when(workflowEngine.start(processorToken, true)).thenReturn(CompletableFuture.completedFuture(null));

        enhancer.initializeWorkflowEngine(
                workflowEngine,
                replaySupport,
                processorToken,
                latestToken
        ).join();

        var inOrder = inOrder(replaySupport, workflowEngine);
        inOrder.verify(replaySupport).setInitialEngineTokens(processorToken, latestToken);
        inOrder.verify(workflowEngine).start(processorToken, true);
    }

    @Test
    void matchingProcessorAndLatestTokenSwitchesToLiveMode() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var workflowEngine = mock(WorkflowEngine.class);
        var replaySupport = mock(WorkflowEngineReplaySupport.class);
        var token = token(192);
        when(workflowEngine.start(token, false)).thenReturn(CompletableFuture.completedFuture(null));

        enhancer.initializeWorkflowEngine(
                workflowEngine,
                replaySupport,
                token,
                token
        ).join();

        verify(replaySupport).setInitialEngineTokens(token, token);
        verify(workflowEngine).start(token, false);
    }

    @Test
    void replayIsNotRequiredWhenEitherTokenIsMissing() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        assertThat(enhancer.requiresReplay(null, token(1))).isFalse();
        assertThat(enhancer.requiresReplay(token(1), null)).isFalse();
    }

    // -------------------------------------------------------------------------------------------------------------
    // Startup against a shared, durable token store. A second node must be able to start while a peer holds claims,
    // and simultaneously starting replicas must not knock each other over.
    // -------------------------------------------------------------------------------------------------------------

    @Test
    void segmentsOwnedByAnotherNodeAreSkippedWhenFoldingTheEarliestToken() {
        var tokenStore = tokenStore(segments(3));
        readable(tokenStore, 0, token(10));
        ownedByAnotherNode(tokenStore, 1);
        readable(tokenStore, 2, token(30));

        var processorToken = enhancer().ensureSegmentsInitialized(tokenStore, eventSource(token(0))).join();

        assertThat(processorToken).as("the earliest token over the segments this node could read")
                                  .isEqualTo(token(10));
        verify(tokenStore).releaseClaim("Workflow", 0, null);
        verify(tokenStore).releaseClaim("Workflow", 2, null);
        verify(tokenStore, never()).releaseClaim("Workflow", 1, null);
    }

    @Test
    void startupCompletesWhenEverySegmentIsOwnedByAnotherNode() {
        var tokenStore = tokenStore(segments(3));
        ownedByAnotherNode(tokenStore, 0);
        ownedByAnotherNode(tokenStore, 1);
        ownedByAnotherNode(tokenStore, 2);

        var processorToken = enhancer().ensureSegmentsInitialized(tokenStore, eventSource(token(0))).join();

        assertThat(processorToken).as("a node that owns no segment has nothing to replay").isNull();
    }

    @Test
    void eachSegmentClaimIsReleasedBeforeTheNextSegmentIsRead() {
        var tokenStore = tokenStore(segments(3));
        var calls = new ArrayList<String>();
        readable(tokenStore, 0, token(10));
        readable(tokenStore, 1, token(20));
        readable(tokenStore, 2, token(30));
        for (var segmentId = 0; segmentId < 3; segmentId++) {
            var id = segmentId;
            doAnswer(invocation -> {
                calls.add("fetch" + id);
                return CompletableFuture.completedFuture(token(10L * (id + 1)));
            }).when(tokenStore).fetchToken("Workflow", id, null);
            doAnswer(invocation -> {
                calls.add("release" + id);
                return CompletableFuture.completedFuture(null);
            }).when(tokenStore).releaseClaim("Workflow", id, null);
        }

        enhancer().ensureSegmentsInitialized(tokenStore, eventSource(token(0))).join();

        assertThat(calls).as("a starting node must not hold a claim on one segment while reading another")
                         .containsExactly("fetch0", "release0", "fetch1", "release1", "fetch2", "release2");
    }

    @Test
    void aFailureOtherThanAForeignClaimStillFailsStartup() {
        var tokenStore = tokenStore(segments(2));
        readable(tokenStore, 0, token(10));
        when(tokenStore.fetchToken("Workflow", 1, null))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("connection lost")));

        assertThatThrownBy(() -> enhancer().ensureSegmentsInitialized(tokenStore, eventSource(token(0))).join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void losingTheInitializationRaceReadsBackTheSegmentsTheWinnerCreated() {
        var tokenStore = mock(TokenStore.class);
        // First read sees an empty store, the read after the failed initialization sees the winner's segments.
        when(tokenStore.fetchSegments("Workflow", null))
                .thenReturn(CompletableFuture.completedFuture(List.of()))
                .thenReturn(CompletableFuture.completedFuture(segments(2)));
        when(tokenStore.initializeTokenSegments(eq("Workflow"), anyInt(), any(), isNull()))
                .thenReturn(CompletableFuture.failedFuture(
                        new UnableToClaimTokenException("Could not initialize segments.")));
        readable(tokenStore, 0, token(5));
        readable(tokenStore, 1, token(9));

        var processorToken = enhancer().ensureSegmentsInitialized(tokenStore, eventSource(token(0))).join();

        assertThat(processorToken).isEqualTo(token(5));
    }

    @Test
    void anInitializationFailureIsPropagatedWhenNoSegmentsAppear() {
        var tokenStore = mock(TokenStore.class);
        when(tokenStore.fetchSegments("Workflow", null)).thenReturn(CompletableFuture.completedFuture(List.of()));
        when(tokenStore.initializeTokenSegments(eq("Workflow"), anyInt(), any(), isNull()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("token table missing")));

        assertThatThrownBy(() -> enhancer().ensureSegmentsInitialized(tokenStore, eventSource(token(0))).join())
                .isInstanceOf(CompletionException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    private static WorkflowEventProcessingRegistrationEnhancer enhancer() {
        return new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
    }

    private static List<Segment> segments(int count) {
        return Segment.splitBalanced(Segment.ROOT_SEGMENT, count - 1);
    }

    private static TokenStore tokenStore(List<Segment> segments) {
        var tokenStore = mock(TokenStore.class);
        when(tokenStore.fetchSegments("Workflow", null)).thenReturn(CompletableFuture.completedFuture(segments));
        return tokenStore;
    }

    private static void readable(TokenStore tokenStore, int segmentId, TrackingToken token) {
        when(tokenStore.fetchToken("Workflow", segmentId, null))
                .thenReturn(CompletableFuture.completedFuture(token));
        when(tokenStore.releaseClaim("Workflow", segmentId, null))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private static void ownedByAnotherNode(TokenStore tokenStore, int segmentId) {
        when(tokenStore.fetchToken("Workflow", segmentId, null))
                .thenReturn(CompletableFuture.failedFuture(new UnableToClaimTokenException(
                        "Unable to claim token 'Workflow[Segment[" + segmentId + "]]'. It is owned by 'peer'")));
    }

    private static StreamableEventSource eventSource(TrackingToken firstToken) {
        var eventSource = mock(StreamableEventSource.class);
        when(eventSource.firstToken(isNull())).thenReturn(CompletableFuture.completedFuture(firstToken));
        return eventSource;
    }

    private static TrackingToken token(long globalIndex) {
        return new GlobalSequenceTrackingToken(globalIndex);
    }
}
