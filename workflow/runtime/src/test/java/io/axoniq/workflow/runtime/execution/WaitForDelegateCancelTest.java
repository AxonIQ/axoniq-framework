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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Proves that only cancellation exceptions trigger a step cancelled event,
 * while {@link InterruptedException} (thread interrupt / shutdown) does not.
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
