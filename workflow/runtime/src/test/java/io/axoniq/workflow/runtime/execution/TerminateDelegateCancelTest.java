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

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.TerminatePrimitive.CancelStep;
import io.axoniq.workflow.runtime.api.execution.context.TerminatePrimitive.CancelWorkflow;
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
    private RunningSteps runningSteps;
    private Runnable terminalTeardown;
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
        runningSteps = new RunningSteps();
        terminalTeardown = mock(Runnable.class);

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
                runningSteps,
                new WorkflowStepProgress(),
                terminalTeardown,
                unitOfWorkFactory,
                eventSink,
                executor,
                defaults()
        );
    }

    @Test
    void terminateCancelInterruptsRunningStepsWithoutPerStepEvent() {
        assertThatThrownBy(() -> delegate.cancelWorkflow(new CancelWorkflow(null, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowCancelledException.class);

        // Whole-workflow cancel interrupts running steps + discards the queue; no per-step cancellation is published.
        verify(terminalTeardown).run();
    }

    @Test
    void terminateCancelWithCauseInterruptsRunningSteps() {
        var cause = new RuntimeException("user requested cancellation");

        assertThatThrownBy(() -> delegate.cancelWorkflow(new CancelWorkflow(cause, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(terminalTeardown).run();
    }

    @Test
    void terminateCancelPublishesCancelledWorkflowEvent() {
        assertThatThrownBy(() -> delegate.cancelWorkflow(new CancelWorkflow(null, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(eventSink).publish(eq(processingContext), any(EventMessage.class));
    }

    @Test
    void terminateCancelWithNullCauseThrowsWorkflowCancelledExceptionWithMessage() {
        assertThatThrownBy(() -> delegate.cancelWorkflow(new CancelWorkflow(null, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowCancelledException.class)
                .hasMessage("Workflow cancelled");
    }

    @Test
    void terminateCancelWithCauseThrowsWorkflowCancelledExceptionWithCause() {
        var cause = new RuntimeException("user requested cancellation");

        assertThatThrownBy(() -> delegate.cancelWorkflow(new CancelWorkflow(cause, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowCancelledException.class)
                .hasCause(cause);
    }

    @Test
    void terminateCancelExecutesStepsInOrder() {
        var order = inOrder(terminalTeardown, eventSink);

        assertThatThrownBy(() -> delegate.cancelWorkflow(new CancelWorkflow(null, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowCancelledException.class);

        order.verify(terminalTeardown).run();
        order.verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void terminateCancelInvokesCancelledStatusChangeListener() throws InterruptedException {
        var listener = mock(WorkflowStatusChangeListener.class);
        EventSourcedWorkflowState state = new EventSourcedWorkflowState(Map.of(), workflowContext, Map.of(WorkflowStatus.CANCELLED, listener));
        when(workflowExecution.state()).thenReturn(state);

        assertThatThrownBy(() -> delegate.cancelWorkflow(
                new CancelWorkflow(null, eventNameCustomizer, "test-workflow")))
                .isInstanceOf(WorkflowCancelledException.class);

        state.evolve(EventMessageUtils.cancelledWorkflow(workflowContext, "test-workflow", null, eventNameCustomizer), processingContext);

        verify(listener).onWorkflowStatus(eq(WorkflowStatus.CANCELLED), eq(workflowContext));
    }

    @Test
    void terminateCancelledStepCompletesFutureAndAwaitsTerminalWithoutDirectPublish() throws InterruptedException {
        // Single-step cancel does NOT author <step>:CANCELLED itself and never touches the event sink: it completes the
        // step's registered future exceptionally, then awaits the durable terminal record that the owning executor's
        // completion handler publishes through its guarded path.
        var state = mock(io.axoniq.workflow.runtime.api.execution.state.WorkflowState.class);
        var step = new io.axoniq.workflow.runtime.api.execution.state.WorkflowStep(
                "step-a", io.axoniq.workflow.runtime.api.execution.status.StepStatus.STARTED,
                null, null, java.time.Instant.now(), null);
        when(state.containsStep("step-a")).thenReturn(true);
        when(state.getStep("step-a")).thenReturn(step);
        when(workflowExecution.state()).thenReturn(state);
        var runningFuture = new CompletableFuture<Void>();
        runningSteps.register("step-a", runningFuture);

        boolean result = delegate.cancelStep(new CancelStep("step-a", null, eventNameCustomizer));

        org.assertj.core.api.Assertions.assertThat(result).isTrue();
        org.assertj.core.api.Assertions.assertThat(runningFuture).isCompletedExceptionally();
        verify(workflowExecution).awaitStateChange(any());
        verify(eventSink, never()).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @Test
    void terminateCancelledStepReturnsFalseWhenNoRunningFuture() {
        // Non-terminal step but nothing running to complete: no cancellation is driven and nothing is published.
        var state = mock(io.axoniq.workflow.runtime.api.execution.state.WorkflowState.class);
        var step = new io.axoniq.workflow.runtime.api.execution.state.WorkflowStep(
                "step-a", io.axoniq.workflow.runtime.api.execution.status.StepStatus.STARTED,
                null, null, java.time.Instant.now(), null);
        when(state.containsStep("step-a")).thenReturn(true);
        when(state.getStep("step-a")).thenReturn(step);
        when(workflowExecution.state()).thenReturn(state);
        boolean result = delegate.cancelStep(new CancelStep("step-a", null, eventNameCustomizer));

        org.assertj.core.api.Assertions.assertThat(result).isFalse();
        verify(eventSink, never()).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @Test
    void terminateCancelledStepReturnsFalseWhenStepAlreadyTerminal() {
        var state = mock(io.axoniq.workflow.runtime.api.execution.state.WorkflowState.class);
        var step = new io.axoniq.workflow.runtime.api.execution.state.WorkflowStep(
                "step-a", io.axoniq.workflow.runtime.api.execution.status.StepStatus.COMPLETED,
                null, null, java.time.Instant.now(), null);
        when(state.containsStep("step-a")).thenReturn(true);
        when(state.getStep("step-a")).thenReturn(step);
        when(workflowExecution.state()).thenReturn(state);

        boolean result = delegate.cancelStep(new CancelStep("step-a", null, eventNameCustomizer));

        org.assertj.core.api.Assertions.assertThat(result).isFalse();
        verify(eventSink, never()).publish(any(ProcessingContext.class), any(EventMessage.class));
    }
}
