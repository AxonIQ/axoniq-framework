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
package io.axoniq.workflow.runtime.execution;

import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The segment change listener is how workflow executions follow their segment: a claim restores the segment's
 * instances inside a fresh unit of work, a release drops them again.
 */
class WorkflowSegmentChangeListenerTest {

    private static final Segment SEGMENT = new Segment(1, 3);

    private final WorkflowEngine workflowEngine = mock(WorkflowEngine.class);
    private final WorkflowSegmentChangeListener listener = new WorkflowSegmentChangeListener(
            "Workflow",
            new SimpleUnitOfWorkFactory(new StubApplicationContext()),
            () -> workflowEngine
    );

    @Test
    void claimRestoresTheSegmentWithSeparateSourcingAndExecutionContexts() {
        var from = new GlobalSequenceTrackingToken(7);
        when(workflowEngine.restoreWorkflowsFor(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        listener.onSegmentClaimed(SEGMENT, from).join();

        var sourcing = ArgumentCaptor.forClass(ProcessingContext.class);
        var execution = ArgumentCaptor.forClass(ProcessingContext.class);
        verify(workflowEngine).restoreWorkflowsFor(eq(SEGMENT), eq(from), sourcing.capture(), execution.capture());
        assertThat(execution.getValue())
                .as("""
                    The execution context parents restored workflow bodies that outlive the claim callback, so it \
                    must not be the short-lived sourcing context the claim loads durable state in.""")
                .isNotSameAs(sourcing.getValue());
    }

    @Test
    void releaseDropsTheSegmentsInstances() {
        listener.onSegmentReleased(SEGMENT).join();

        verify(workflowEngine).releaseWorkflowsFor(SEGMENT);
    }

    private record StubApplicationContext() implements ApplicationContext {

        @Override
        public <C> C component(Class<C> type, String name) {
            throw new IllegalArgumentException("No component of type " + type);
        }
    }
}
