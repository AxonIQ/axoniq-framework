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
import io.axoniq.workflow.runtime.api.StepCancellationException;
import io.axoniq.workflow.runtime.api.TerminatePrimitive.TerminateCommand;
import io.axoniq.workflow.runtime.api.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class TerminateDelegateCancelTest {

    private WorkflowContext workflowContext;
    private WorkflowState workflowState;
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
        when(workflowContext.getWorkflowId()).thenReturn("wf-1");
        when(workflowContext.getPayload()).thenReturn(Map.of());
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = new DefaultEventNameCustomizer();

        delegate = new TerminateDelegate(
                workflowContext,
                workflowState,
                eventSink,
                "test-workflow",
                unitOfWorkFactory,
                executor
        );
    }

    @Test
    void terminateCancelCancelsAllRunningSteps() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(workflowState).cancelAllRunningSteps(isA(WorkflowCancelledException.class));
    }

    @Test
    void terminateCancelWithCausePassesWorkflowCancelledExceptionToSteps() {
        var cause = new RuntimeException("user requested cancellation");

        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(workflowState).cancelAllRunningSteps(isA(WorkflowCancelledException.class));
    }

    @Test
    void terminateCancelPublishesCancelledWorkflowEvent() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(eventSink).publish(eq(processingContext), any(EventMessage.class));
    }

    @Test
    void terminateCancelAppliesStateChange() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(workflowState).applyStateChange(any(EventMessage.class), eq(processingContext));
    }

    @Test
    void terminateCancelWithNullCauseThrowsWorkflowCancelledExceptionWithMessage() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class)
                .hasMessage("Workflow cancelled");
    }

    @Test
    void terminateCancelWithCauseThrowsWorkflowCancelledExceptionWithCause() {
        var cause = new RuntimeException("user requested cancellation");

        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class)
                .hasCause(cause);
    }

    @Test
    void terminateCancelExecutesStepsInOrder() {
        var order = inOrder(workflowState, eventSink);

        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        order.verify(workflowState).cancelAllRunningSteps(any());
        order.verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
        order.verify(workflowState).applyStateChange(any(EventMessage.class), eq(processingContext));
    }

    @SuppressWarnings("unchecked")
    @Test
    void terminateCancelInvokesCancelledStatusChangeListener() {
        var listener = mock(WorkflowStatusChangeListener.class);
        var configuration = mock(WorkflowConfiguration.class);
        when(configuration.workflowStatusChangeListeners()).thenReturn(Map.of(WorkflowStatus.CANCELLED, listener));

        assertThatThrownBy(() -> delegate.terminate(
                new TerminateCommand(false, null, eventNameCustomizer, "test-workflow", configuration, null)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(listener).onWorkflowStatus(eq(WorkflowStatus.CANCELLED), eq(workflowContext));
    }

    @Test
    void terminateCancelledStepCancelsSpecificStep() {
        delegate.terminate(TerminateCommand.cancelledStep("step-a", null, eventNameCustomizer));
        verify(workflowState).cancelRunningStep(eq("step-a"), isA(StepCancellationException.class));
    }
}
