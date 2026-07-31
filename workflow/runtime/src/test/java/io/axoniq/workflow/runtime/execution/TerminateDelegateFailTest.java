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
import io.axoniq.workflow.runtime.api.execution.context.TerminatePrimitive.FailWorkflow;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
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
        terminalTeardown = mock(Runnable.class);

        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(any(String.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(any(String.class), any(Function.class))).thenReturn(unitOfWork);
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
                new RunningSteps(),
                new WorkflowStepProgress(),
                terminalTeardown,
                unitOfWorkFactory,
                eventSink,
                executor,
                defaults()
        );
    }

    @Test
    void terminateFailInterruptsRunningStepsWithoutPerStepEvent() {
        assertThatThrownBy(() -> delegate.failWorkflow(new FailWorkflow(new RuntimeException("boom"), eventNameCustomizer, null)))
                .isInstanceOf(WorkflowFailedException.class);

        // Whole-workflow fail interrupts running steps + discards the queue; no per-step cancellation is published.
        verify(terminalTeardown).run();
    }

    @Test
    void terminateFailWithNullCauseInterruptsRunningSteps() {
        assertThatThrownBy(() -> delegate.failWorkflow(new FailWorkflow(null, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(terminalTeardown).run();
    }

    @Test
    void terminateFailPublishesFailedWorkflowEvent() {
        assertThatThrownBy(() -> delegate.failWorkflow(new FailWorkflow(new RuntimeException("boom"), eventNameCustomizer, null)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(eventSink).publish(eq(processingContext), any(EventMessage.class));
    }

    @Test
    void terminateFailThrowsWorkflowFailedExceptionWithCause() {
        var cause = new RuntimeException("boom");

        assertThatThrownBy(() -> delegate.failWorkflow(new FailWorkflow(cause, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasCause(cause);
    }

    @Test
    void terminateFailWithNullCauseThrowsWorkflowFailedExceptionWithMessage() {
        assertThatThrownBy(() -> delegate.failWorkflow(new FailWorkflow(null, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasRootCauseMessage("Workflow failed");
    }

    @Test
    void terminateFailExecutesStepsInOrder() {
        var cause = new RuntimeException("boom");
        var order = inOrder(terminalTeardown, eventSink);

        assertThatThrownBy(() -> delegate.failWorkflow(new FailWorkflow(cause, eventNameCustomizer, null)))
                .isInstanceOf(WorkflowFailedException.class);

        order.verify(terminalTeardown).run();
        order.verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void terminateFailInvokesFailedStatusChangeListener() throws InterruptedException {
        var listener = mock(WorkflowStatusChangeListener.class);
        EventSourcedWorkflowState state = new EventSourcedWorkflowState(Map.of(), workflowContext, Map.of(WorkflowStatus.FAILED, listener));
        when(workflowExecution.state()).thenReturn(state);

        var cause = new RuntimeException("boom");
        assertThatThrownBy(() -> delegate.failWorkflow(
                new FailWorkflow(cause, eventNameCustomizer, "test-workflow")))
                .isInstanceOf(WorkflowFailedException.class);

        state.evolve(EventMessageUtils.failedWorkflow(workflowContext, "test-workflow", cause, eventNameCustomizer), processingContext);

        verify(listener).onWorkflowStatus(eq(WorkflowStatus.FAILED), eq(workflowContext));
    }
}
