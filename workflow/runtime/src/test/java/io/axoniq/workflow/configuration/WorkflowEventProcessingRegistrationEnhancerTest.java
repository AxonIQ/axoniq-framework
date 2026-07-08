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
import io.axoniq.workflow.runtime.execution.SafePointStore;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GapAwareTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test for reset / resume logic based on provided token.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class WorkflowEventProcessingRegistrationEnhancerTest {

    @Test
    void storedSafepointTokenResetsProcessorFromStoredToken() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var eventSource = mock(StreamableEventSource.class);
        var processor = mock(StreamingEventProcessor.class);
        var workflowEngine = mock(WorkflowEngine.class);
        var safePointStore = mock(SafePointStore.class);
        var configuration = mock(Configuration.class);
        var unitOfWorkFactory = unitOfWorkFactory();
        TrackingToken storedToken = token(18);

        when(safePointStore.fetchSafePointToken()).thenReturn(CompletableFuture.completedFuture(storedToken));
        when(processor.resetTokens(any(TrackingToken.class))).thenReturn(CompletableFuture.completedFuture(null));

        enhancer.resetOrSwitchToLiveMode(configuration,
                                         eventSource,
                                         processor,
                                         workflowEngine,
                                         safePointStore,
                                         unitOfWorkFactory,
                                         token(192)).join();

        var tokenCaptor = org.mockito.ArgumentCaptor.forClass(TrackingToken.class);
        verify(processor).resetTokens(tokenCaptor.capture());
        assertSameToken(tokenCaptor.getValue(), storedToken);
        verify(workflowEngine).initializeSafePoint(storedToken);
        verify(workflowEngine).rehydrateRunningWorkflows(any(ProcessingContext.class), any(ProcessingContext.class));
        verify(workflowEngine).startRehydratedExecutions();
        verifyNoInteractions(eventSource);
        verify(workflowEngine, never()).switchToLiveMode();
    }

    @Test
    void missingStoredTokenFallsBackToFirstEventSourceToken() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var eventSource = mock(StreamableEventSource.class);
        var processor = mock(StreamingEventProcessor.class);
        var workflowEngine = mock(WorkflowEngine.class);
        var safePointStore = mock(SafePointStore.class);
        var configuration = mock(Configuration.class);
        var unitOfWorkFactory = unitOfWorkFactory();
        TrackingToken firstToken = token(5);

        when(safePointStore.fetchSafePointToken()).thenReturn(CompletableFuture.completedFuture(null));
        when(eventSource.firstToken(null)).thenReturn(CompletableFuture.completedFuture(firstToken));
        when(processor.resetTokens(any(TrackingToken.class))).thenReturn(CompletableFuture.completedFuture(null));

        enhancer.resetOrSwitchToLiveMode(configuration,
                                         eventSource,
                                         processor,
                                         workflowEngine,
                                         safePointStore,
                                         unitOfWorkFactory,
                                         token(192)).join();

        var tokenCaptor = org.mockito.ArgumentCaptor.forClass(TrackingToken.class);
        verify(processor).resetTokens(tokenCaptor.capture());
        assertSameToken(tokenCaptor.getValue(), firstToken);
        verify(workflowEngine).initializeSafePoint(firstToken);
        verify(workflowEngine).rehydrateRunningWorkflows(any(ProcessingContext.class), any(ProcessingContext.class));
        verify(workflowEngine).startRehydratedExecutions();
        verify(workflowEngine, never()).switchToLiveMode();
    }

    @Test
    void latestTrackingTokenSeedsEngineAndSwitchesToLiveModeWhenReplayIsNotRequired() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var eventSource = mock(StreamableEventSource.class);
        var processor = mock(StreamingEventProcessor.class);
        var workflowEngine = mock(WorkflowEngine.class);
        var safePointStore = mock(SafePointStore.class);
        var configuration = mock(Configuration.class);
        var unitOfWorkFactory = unitOfWorkFactory();
        TrackingToken latestToken = token(192);

        when(safePointStore.fetchSafePointToken()).thenReturn(CompletableFuture.completedFuture(latestToken));

        enhancer.resetOrSwitchToLiveMode(configuration,
                                         eventSource,
                                         processor,
                                         workflowEngine,
                                         safePointStore,
                                         unitOfWorkFactory,
                                         latestToken)
                .join();

        verify(workflowEngine).initializeSafePoint(latestToken);
        verify(workflowEngine).rehydrateRunningWorkflows(any(ProcessingContext.class), any(ProcessingContext.class));
        verify(workflowEngine).startRehydratedExecutions();
        verify(workflowEngine).switchToLiveMode();
        verify(processor, never()).resetTokens(any(TrackingToken.class));
        verifyNoInteractions(eventSource);
    }

    private static UnitOfWorkFactory unitOfWorkFactory() {
        var unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        var unitOfWork = mock(UnitOfWork.class);
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.resources()).thenReturn(java.util.Map.of());
        when(unitOfWorkFactory.create(anyString())).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any())).thenAnswer(invocation ->
                invocation.<java.util.function.Function<ProcessingContext, CompletableFuture<Void>>>getArgument(0)
                          .apply(processingContext)
        );
        return unitOfWorkFactory;
    }

    private static TrackingToken token(long globalIndex) {
        return new GlobalSequenceTrackingToken(globalIndex);
    }

    private static void assertSameToken(TrackingToken actual, TrackingToken expected) {
        assertThat(actual.lowerBound(expected)).isEqualTo(expected);
        assertThat(expected.lowerBound(actual)).isEqualTo(actual);
    }
}
