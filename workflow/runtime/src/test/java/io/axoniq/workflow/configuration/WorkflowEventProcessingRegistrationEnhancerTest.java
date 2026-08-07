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
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
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
        inOrder.verify(replaySupport).initializeReplayTracking(processorToken, latestToken);
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

        verify(replaySupport).initializeReplayTracking(token, token);
        verify(workflowEngine).start(token, false);
    }

    @Test
    void replayIsNotRequiredWhenEitherTokenIsMissing() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        assertThat(enhancer.requiresReplay(null, token(1))).isFalse();
        assertThat(enhancer.requiresReplay(token(1), null)).isFalse();
    }

    private static TrackingToken token(long globalIndex) {
        return new GlobalSequenceTrackingToken(globalIndex);
    }
}
