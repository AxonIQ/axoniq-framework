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
import io.axoniq.workflow.runtime.api.TerminatePrimitive.TerminateCommand;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.execution.SimpleWorkflowState;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class TerminateDelegateFailTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private UnitOfWorkFactory unitOfWorkFactory;
    private Executor executor;
    private EventNameCustomizer eventNameCustomizer;
    private TerminateDelegate delegate;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
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

        when(workflowExecution.state()).thenReturn(new SimpleWorkflowState(workflowContext, Map.of()));
        when(workflowContext.processingContext()).thenReturn(processingContext);
        when(workflowContext.workflowId()).thenReturn("wf-1");
        when(workflowContext.workflowPayload()).thenReturn(Map.of());
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = new DefaultEventNameCustomizer();

        delegate = new TerminateDelegate(
                workflowContext,
                workflowExecution,
                eventSink,
                "test-workflow",
                unitOfWorkFactory,
                executor
        );
    }

    @Test
    void terminateFailCancelsAllRunningSteps() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.fail(new RuntimeException("boom"), eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(workflowExecution).cancelAllRunningSteps(isA(WorkflowFailedException.class));
    }

    @Test
    void terminateFailWithNullCausePassesWorkflowFailedExceptionToSteps() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.fail(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(workflowExecution).cancelAllRunningSteps(isA(WorkflowFailedException.class));
    }

    @Test
    void terminateFailPublishesFailedWorkflowEvent() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.fail(new RuntimeException("boom"), eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(eventSink).publish(eq(processingContext), any(EventMessage.class));
    }

    @Test
    void terminateFailThrowsWorkflowFailedExceptionWithCause() {
        var cause = new RuntimeException("boom");

        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.fail(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasCause(cause);
    }

    @Test
    void terminateFailWithNullCauseThrowsWorkflowFailedExceptionWithMessage() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.fail(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasRootCauseMessage("Workflow failed");
    }

    @Test
    void terminateFailExecutesStepsInOrder() {
        var cause = new RuntimeException("boom");
        var order = inOrder(workflowExecution, eventSink);

        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.fail(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        order.verify(workflowExecution).cancelAllRunningSteps(any());
        order.verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void terminateFailInvokesFailedStatusChangeListener() throws InterruptedException {
        var listener = mock(WorkflowStatusChangeListener.class);
        SimpleWorkflowState state = new SimpleWorkflowState(workflowContext, Map.of(WorkflowStatus.FAILED, listener));
        when(workflowExecution.state()).thenReturn(state);
        doAnswer(invocation -> {
            state.setStatus(WorkflowStatus.FAILED, null);
            return null;
        }).when(workflowExecution).awaitStateChange(any(Predicate.class));

        assertThatThrownBy(() -> delegate.terminate(
                new TerminateCommand(true, new RuntimeException("boom"), eventNameCustomizer, "test-workflow", null)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(listener).onWorkflowStatus(eq(WorkflowStatus.FAILED), eq(workflowContext));
    }
}
