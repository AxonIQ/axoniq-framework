/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.StepCancellationException;
import io.axoniq.framework.workflow.dsl.api.WorkflowCancelledException;
import io.axoniq.framework.workflow.dsl.api.WorkflowFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Proves that only cancellation exceptions trigger a step cancelled event, while {@link InterruptedException} (thread
 * interrupt / shutdown) does not.
 * <p>
 * This mirrors the {@code .exceptionally()} handler pattern used in {@link WaitForDelegate}.
 *
 * @author Stefan Dragisic
 */
class WaitForDelegateCancelTest {

    private WorkflowExecution workflowExecution;

    @BeforeEach
    void setUp() {
        workflowExecution = mock(WorkflowExecution.class);
    }

    @Test
    void stepCancellationExceptionTriggersStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowExecution.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new StepCancellationException("cancelled"));

        verify(workflowExecution).appendTask(any());
    }

    @Test
    void workflowCancelledExceptionTriggersStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowExecution.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new WorkflowCancelledException("cancelled"));

        verify(workflowExecution).appendTask(any());
    }

    @Test
    void workflowFailedExceptionTriggersStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowExecution.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new WorkflowFailedException("failed"));

        verify(workflowExecution).appendTask(any());
    }

    @Test
    void interruptedExceptionDoesNotTriggerStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowExecution.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new InterruptedException("shutdown"));

        verify(workflowExecution, never()).appendTask(any());
    }
}
