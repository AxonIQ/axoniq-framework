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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowCancelledException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
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

import static io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands.cancelStep;
import static io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands.cancelWorkflow;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;

/**
 * @author Stefan Dragisic
 */
class WorkflowLifecycleControlDelegateCancelTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private UnitOfWorkFactory unitOfWorkFactory;
    private Executor executor;
    private EventNameCustomizer eventNameCustomizer;
    private RunningSteps runningSteps;
    private WorkflowTerminalTransition terminalTransition;
    private WorkflowLifecycleControlDelegate delegate;

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
        terminalTransition = mock(WorkflowTerminalTransition.class);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(terminalTransition).transition(any(Runnable.class));

        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(any(String.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any(Function.class)))
                .thenAnswer(invocation -> {
                    Function<ProcessingContext, CompletableFuture<?>> action = invocation.getArgument(0);
                    return action.apply(processingContext);
                });
        when(workflowExecution.state()).thenReturn(workflowState(Map.of()));
        when(workflowContext.processingContext()).thenReturn(processingContext);
        when(workflowExecution.workflowName()).thenReturn("test-workflow");
        when(workflowContext.workflowId()).thenReturn("wf-1");
        when(workflowContext.workflowPayload()).thenReturn(Map.of());
        when(workflowExecution.appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = defaults();

        delegate = new WorkflowLifecycleControlDelegate(
                workflowContext,
                workflowExecution,
                runningSteps,
                new ReachedSteps(),
                terminalTransition
        );
    }

    @Test
    void cancelWorkflowInterruptsRunningStepsWithoutPerStepEvent() {
        assertThatThrownBy(() -> delegate.cancelWorkflow(cancelWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        // Whole-workflow cancel interrupts running steps + discards the queue; no per-step cancellation is published.
        verify(terminalTransition).transition(any(Runnable.class));
    }

    @Test
    void cancelWorkflowWithCauseInterruptsRunningSteps() {
        var cause = new RuntimeException("user requested cancellation");

        assertThatThrownBy(() -> delegate.cancelWorkflow(cancelWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(terminalTransition).transition(any(Runnable.class));
    }

    @Test
    void cancelWorkflowPublishesCancelledWorkflowEvent() {
        assertThatThrownBy(() -> delegate.cancelWorkflow(cancelWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        verify(workflowExecution).appendWorkflowEvent(any(EventMessage.class), eq(processingContext));
    }

    @Test
    void cancelWorkflowWithNullCauseThrowsWorkflowCancelledExceptionWithMessage() {
        assertThatThrownBy(() -> delegate.cancelWorkflow(cancelWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class)
                .hasMessage("Workflow cancelled");
    }

    @Test
    void cancelWorkflowWithCauseThrowsWorkflowCancelledExceptionWithCause() {
        var cause = new RuntimeException("user requested cancellation");

        assertThatThrownBy(() -> delegate.cancelWorkflow(cancelWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class)
                .hasCause(cause);
    }

    @Test
    void cancelWorkflowExecutesStepsInOrder() {
        var order = inOrder(terminalTransition, workflowExecution);

        assertThatThrownBy(() -> delegate.cancelWorkflow(cancelWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        order.verify(terminalTransition).transition(any(Runnable.class));
        order.verify(workflowExecution).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void cancelWorkflowInvokesCancelledStatusChangeListener() throws InterruptedException {
        var listener = mock(WorkflowStatusChangeListener.class);
        EventSourcedWorkflowState state = workflowState(Map.of(WorkflowStatus.CANCELLED, listener));
        when(workflowExecution.state()).thenReturn(state);

        assertThatThrownBy(() -> delegate.cancelWorkflow(
                cancelWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowCancelledException.class);

        state.evolve(EventMessageUtils.cancelledWorkflow(workflowContext,
                                                         "test-workflow",
                                                         null,
                                                         state.workflowDefinitionId(),
                                                         eventNameCustomizer), processingContext);

        verify(listener).onWorkflowStatus(eq(WorkflowStatus.CANCELLED), eq(workflowContext));
    }

    @Test
    void cancelStepCompletesFutureAndAwaitsTerminalWithoutDirectPublish() throws InterruptedException {
        // Single-step cancel does NOT author <step>:CANCELLED itself and never touches the event sink: it completes the
        // step's registered future exceptionally, then awaits the durable terminal record that the owning executor's
        // completion handler publishes through its guarded path.
        var state = mock(WorkflowState.class);
        var step = new WorkflowStep(
                "step-a", StepStatus.STARTED,
                null, null, java.time.Instant.now(), null);
        when(state.containsStep("step-a")).thenReturn(true);
        when(state.getStep("step-a")).thenReturn(step);
        when(workflowExecution.state()).thenReturn(state);
        var runningFuture = new CompletableFuture<Void>();
        runningSteps.register("step-a", runningFuture);

        boolean result = delegate.cancelStep(cancelStep("step-a", null, eventNameCustomizer));

        assertThat(result).isTrue();
        assertThat(runningFuture).isCompletedExceptionally();
        verify(workflowExecution).awaitStateChange(any());
        verify(eventSink, never()).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @Test
    void cancelStepReturnsFalseWhenNoRunningFuture() {
        // Non-terminal step but nothing running to complete: no cancellation is driven and nothing is published.
        var state = mock(WorkflowState.class);
        var step = new WorkflowStep(
                "step-a", StepStatus.STARTED,
                null, null, java.time.Instant.now(), null);
        when(state.containsStep("step-a")).thenReturn(true);
        when(state.getStep("step-a")).thenReturn(step);
        when(workflowExecution.state()).thenReturn(state);
        boolean result = delegate.cancelStep(cancelStep("step-a", null, eventNameCustomizer));

        assertThat(result).isFalse();
        verify(eventSink, never()).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @Test
    void cancelStepReturnsFalseWhenStepAlreadyTerminal() {
        var state = mock(WorkflowState.class);
        var step = new WorkflowStep(
                "step-a", StepStatus.COMPLETED,
                null, null, java.time.Instant.now(), null);
        when(state.containsStep("step-a")).thenReturn(true);
        when(state.getStep("step-a")).thenReturn(step);
        when(workflowExecution.state()).thenReturn(state);

        boolean result = delegate.cancelStep(cancelStep("step-a", null, eventNameCustomizer));

        assertThat(result).isFalse();
        verify(eventSink, never()).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    private EventSourcedWorkflowState workflowState(Map<WorkflowStatus, WorkflowStatusChangeListener> listeners) {
        return new EventSourcedWorkflowState("wf-1",
                                             Map.of(),
                                             new MessageType(new QualifiedName("test-workflow"), "0.0.1"),
                                             workflowContext,
                                             listeners);
    }
}
