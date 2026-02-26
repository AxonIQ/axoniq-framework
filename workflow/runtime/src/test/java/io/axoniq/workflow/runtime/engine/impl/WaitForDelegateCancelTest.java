/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.StepCancellationException;
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Proves that only cancellation exceptions trigger a step cancelled event,
 * while {@link InterruptedException} (thread interrupt / shutdown) does not.
 * <p>
 * This mirrors the {@code .exceptionally()} handler pattern used in {@link WaitForDelegate}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class WaitForDelegateCancelTest {

    private WorkflowState workflowState;

    @BeforeEach
    void setUp() {
        workflowState = mock(WorkflowState.class);
    }

    @Test
    void stepCancellationExceptionTriggersStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowState.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new StepCancellationException("cancelled"));

        verify(workflowState).appendTask(any());
    }

    @Test
    void workflowCancelledExceptionTriggersStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowState.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new WorkflowCancelledException("cancelled"));

        verify(workflowState).appendTask(any());
    }

    @Test
    void workflowFailedExceptionTriggersStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowState.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new WorkflowFailedException("failed"));

        verify(workflowState).appendTask(any());
    }

    @Test
    void interruptedExceptionDoesNotTriggerStepCancelledEvent() {
        var root = new CompletableFuture<Void>();
        root.exceptionally(e -> {
            if (AbstractStepExecutor.isCancellation(e)) {
                workflowState.appendTask(any());
            }
            return null;
        });

        root.completeExceptionally(new InterruptedException("shutdown"));

        verify(workflowState, never()).appendTask(any());
    }
}
