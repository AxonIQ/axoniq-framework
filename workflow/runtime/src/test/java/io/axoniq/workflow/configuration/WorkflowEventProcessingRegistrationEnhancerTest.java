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
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.Test;

import java.util.Map;
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
        var configuration = mock(Configuration.class);
        var unitOfWorkFactory = unitOfWorkFactory();
        var processor = mock(StreamingEventProcessor.class);
        var processorToken = token(18);
        var latestToken = token(192);

        enhancer.initializeWorkflowEngine(
                configuration,
                workflowEngine,
                unitOfWorkFactory,
                processor,
                processorToken,
                latestToken
        ).join();

        var inOrder = inOrder(workflowEngine);
        inOrder.verify(workflowEngine).initializeCheckpointing(processorToken, latestToken);
        inOrder.verify(workflowEngine).rehydrateRunningWorkflows(any(ProcessingContext.class), any(ProcessingContext.class));
        inOrder.verify(workflowEngine).startCheckpointCatchUp();
        verify(workflowEngine, never()).switchToLiveMode();
        verifyNoInteractions(processor);
    }

    @Test
    void matchingProcessorAndLatestTokenSwitchesToLiveMode() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);
        var workflowEngine = mock(WorkflowEngine.class);
        var configuration = mock(Configuration.class);
        var unitOfWorkFactory = unitOfWorkFactory();
        var processor = mock(StreamingEventProcessor.class);
        var token = token(192);

        enhancer.initializeWorkflowEngine(
                configuration,
                workflowEngine,
                unitOfWorkFactory,
                processor,
                token,
                token
        ).join();

        verify(workflowEngine).initializeCheckpointing(token, token);
        verify(workflowEngine).rehydrateRunningWorkflows(any(ProcessingContext.class), any(ProcessingContext.class));
        verify(workflowEngine).switchToLiveMode();
        verify(workflowEngine, never()).startCheckpointCatchUp();
        verifyNoInteractions(processor);
    }

    @Test
    void replayIsNotRequiredWhenEitherTokenIsMissing() {
        var enhancer = new WorkflowEventProcessingRegistrationEnhancer("Workflow", null, null, true);

        assertThat(enhancer.requiresReplay(null, token(1))).isFalse();
        assertThat(enhancer.requiresReplay(token(1), null)).isFalse();
    }

    private static UnitOfWorkFactory unitOfWorkFactory() {
        var unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        var unitOfWork = mock(UnitOfWork.class);
        var processingContext = mock(ProcessingContext.class);
        when(processingContext.resources()).thenReturn(Map.of());
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
}
