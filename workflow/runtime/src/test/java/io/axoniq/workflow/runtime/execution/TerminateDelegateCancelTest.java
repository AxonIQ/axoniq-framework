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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.TerminatePrimitive.TerminateCommand;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class TerminateDelegateCancelTest {

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
        when(workflowExecution.state()).thenReturn(new EventSourcedWorkflowState(Map.of(), workflowContext, Map.of()));
        when(workflowContext.processingContext()).thenReturn(processingContext);
        when(workflowExecution.workflowName()).thenReturn("test-workflow");
        when(workflowContext.workflowId()).thenReturn("wf-1");
        when(workflowContext.workflowPayload()).thenReturn(Map.of());
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = defaults();

        delegate = new TerminateDelegate(
                workflowContext,
                workflowExecution,
                unitOfWorkFactory,
                eventSink,
                executor
        );
    }

    @Test
    void terminateCancelCancelsAllRunningSteps() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(workflowExecution).cancelAllRunningSteps(isA(WorkflowCancelledException.class));
    }

    @Test
    void terminateCancelWithCausePassesWorkflowCancelledExceptionToSteps() {
        var cause = new RuntimeException("user requested cancellation");

        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(workflowExecution).cancelAllRunningSteps(isA(WorkflowCancelledException.class));
    }

    @Test
    void terminateCancelPublishesCancelledWorkflowEvent() {
        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(eventSink).publish(eq(processingContext), any(EventMessage.class));
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
        var order = inOrder(workflowExecution, eventSink);

        assertThatThrownBy(() -> delegate.terminate(TerminateCommand.cancel(eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        order.verify(workflowExecution).cancelAllRunningSteps(any());
        order.verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void terminateCancelInvokesCancelledStatusChangeListener() throws InterruptedException {
        var listener = mock(WorkflowStatusChangeListener.class);
        EventSourcedWorkflowState state = new EventSourcedWorkflowState(Map.of(), workflowContext, Map.of(WorkflowStatus.CANCELLED, listener));
        when(workflowExecution.state()).thenReturn(state);

        assertThatThrownBy(() -> delegate.terminate(
                new TerminateCommand(false, null, eventNameCustomizer, "test-workflow", null)))
                .isInstanceOf(WorkflowCancelledException.class);

        state.evolve(EventMessageUtils.cancelledWorkflow(workflowContext, "test-workflow", null, eventNameCustomizer), processingContext);

        verify(listener).onWorkflowStatus(eq(WorkflowStatus.CANCELLED), eq(workflowContext));
    }

    @Test
    void terminateCancelledStepCancelsSpecificStep() {
        delegate.terminate(TerminateCommand.cancelledStep("step-a", null, eventNameCustomizer));
        verify(workflowExecution).cancelRunningStep(eq("step-a"), isA(StepCancellationException.class));
    }
}
