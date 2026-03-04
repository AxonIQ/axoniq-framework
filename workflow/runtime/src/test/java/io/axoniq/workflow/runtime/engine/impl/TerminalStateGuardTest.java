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

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@code sendStepEvent()} returns a failed future (not a silently completed one)
 * when the workflow or step is already in a terminal state.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class TerminalStateGuardTest {

    private WorkflowContext workflowContext;
    private WorkflowState workflowState;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private UnitOfWorkFactory unitOfWorkFactory;
    private Executor executor;
    private EventNameCustomizer eventNameCustomizer;

    /**
     * Concrete subclass to expose the protected methods for testing.
     */
    private static class TestableStepExecutor extends AbstractStepExecutor {

        TestableStepExecutor(WorkflowContext workflowContext, WorkflowState workflowState,
                             EventNameCustomizer parentEventNameCustomizer, Clock clock,
                             UnitOfWorkFactory unitOfWorkFactory, EventSink eventSink, Executor executor) {
            super(workflowContext, workflowState, parentEventNameCustomizer, clock, unitOfWorkFactory, eventSink,
                  executor);
        }

        CompletableFuture<Void> testCompleted(String stepName, Map<String, Object> payload,
                                              EventNameCustomizer eventNameCustomizer) {
            return completed(stepName, payload, eventNameCustomizer);
        }

        CompletableFuture<Void> testFailed(String stepName, Throwable ex,
                                           EventNameCustomizer eventNameCustomizer) {
            return failed(stepName, ex, eventNameCustomizer);
        }

        CompletableFuture<Void> testStarted(String stepName, Map<String, Object> payload,
                                            EventNameCustomizer eventNameCustomizer) {
            return started(stepName, payload, eventNameCustomizer);
        }
    }

    private TestableStepExecutor stepExecutor;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowState = mock(WorkflowState.class);
        eventSink = mock(EventSink.class);
        processingContext = mock(ProcessingContext.class);
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        executor = Runnable::run;

        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(any(String.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any(Function.class)))
                .thenAnswer(invocation -> {
                    Function<ProcessingContext, CompletableFuture<?>> action = invocation.getArgument(0);
                    return action.apply(processingContext);
                });

        when(workflowContext.processingContext()).thenReturn(processingContext);
        when(workflowContext.workflowId()).thenReturn("wf-1");
        when(workflowContext.workflowPayload()).thenReturn(Map.of());
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = new DefaultEventNameCustomizer();

        stepExecutor = new TestableStepExecutor(
                workflowContext, workflowState, eventNameCustomizer,
                Clock.systemUTC(), unitOfWorkFactory, eventSink, executor
        );
    }

    @Test
    void completedReturnsFailedFutureWhenWorkflowIsTerminal() {
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);

        var future = stepExecutor.testCompleted("step-1", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void failedReturnsFailedFutureWhenWorkflowIsTerminal() {
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.FAILED);

        var future = stepExecutor.testFailed("step-1", new RuntimeException("boom"), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void startedReturnsFailedFutureWhenWorkflowIsTerminal() {
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.CANCELLED);

        var future = stepExecutor.testStarted("step-1", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void completedReturnsFailedFutureWhenStepIsTerminal() {
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(workflowState.containsStep("step-1")).thenReturn(true);
        when(workflowState.getStep("step-1")).thenReturn(
                WorkflowStep.completed("step-1", Map.of(), Instant.now(), processingContext));

        var future = stepExecutor.testCompleted("step-1", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("terminal state");
    }

    @Test
    void failedFutureContainsStepNameInMessage() {
        when(workflowState.containsStep("my-step")).thenReturn(true);
        when(workflowState.getStep("my-step")).thenReturn(
                WorkflowStep.failed("my-step", new RuntimeException("err"), Instant.now(), processingContext));

        var future = stepExecutor.testCompleted("my-step", Map.of(), eventNameCustomizer);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("my-step");
    }
}
