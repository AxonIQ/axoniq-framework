/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.execution.WorkflowEngine;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * The segment change listener is how workflow executions follow their segment: a claim restores the segment's instances
 * inside a fresh unit of work, a release drops them again.
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
