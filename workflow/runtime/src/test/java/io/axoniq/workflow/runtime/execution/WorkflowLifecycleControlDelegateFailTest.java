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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
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

import static io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands.failWorkflow;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;

/**
 * @author Stefan Dragisic
 */
class WorkflowLifecycleControlDelegateFailTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private UnitOfWorkFactory unitOfWorkFactory;
    private Executor executor;
    private EventNameCustomizer eventNameCustomizer;
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
        terminalTransition = mock(WorkflowTerminalTransition.class);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(terminalTransition).transition(any(Runnable.class));

        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(any(String.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(any(String.class), any(Function.class))).thenReturn(unitOfWork);
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
        when(eventSink.publish(any(ProcessingContext.class), any(EventMessage.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = defaults();

        delegate = new WorkflowLifecycleControlDelegate(
                workflowContext,
                workflowExecution,
                new RunningSteps(),
                new ReachedSteps(),
                terminalTransition,
                unitOfWorkFactory,
                eventSink,
                executor
        );
    }

    @Test
    void failWorkflowInterruptsRunningStepsWithoutPerStepEvent() {
        assertThatThrownBy(() -> delegate.failWorkflow(failWorkflow(new RuntimeException("boom"), eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        // Whole-workflow fail interrupts running steps + discards the queue; no per-step cancellation is published.
        verify(terminalTransition).transition(any(Runnable.class));
    }

    @Test
    void failWorkflowWithNullCauseInterruptsRunningSteps() {
        assertThatThrownBy(() -> delegate.failWorkflow(failWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(terminalTransition).transition(any(Runnable.class));
    }

    @Test
    void failWorkflowPublishesFailedWorkflowEvent() {
        assertThatThrownBy(() -> delegate.failWorkflow(failWorkflow(new RuntimeException("boom"), eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(eventSink).publish(eq(processingContext), any(EventMessage.class));
    }

    @Test
    void failWorkflowThrowsWorkflowFailedExceptionWithCause() {
        var cause = new RuntimeException("boom");

        assertThatThrownBy(() -> delegate.failWorkflow(failWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasCause(cause);
    }

    @Test
    void failWorkflowWithNullCauseThrowsWorkflowFailedExceptionWithMessage() {
        assertThatThrownBy(() -> delegate.failWorkflow(failWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasRootCauseMessage("Workflow failed");
    }

    @Test
    void failWorkflowExecutesStepsInOrder() {
        var cause = new RuntimeException("boom");
        var order = inOrder(terminalTransition, eventSink);

        assertThatThrownBy(() -> delegate.failWorkflow(failWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        order.verify(terminalTransition).transition(any(Runnable.class));
        order.verify(eventSink).publish(any(ProcessingContext.class), any(EventMessage.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void failWorkflowInvokesFailedStatusChangeListener() throws InterruptedException {
        var listener = mock(WorkflowStatusChangeListener.class);
        EventSourcedWorkflowState state = workflowState(Map.of(WorkflowStatus.FAILED, listener));
        when(workflowExecution.state()).thenReturn(state);

        var cause = new RuntimeException("boom");
        assertThatThrownBy(() -> delegate.failWorkflow(
                failWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        state.evolve(EventMessageUtils.failedWorkflow(workflowContext,
                                                      "test-workflow",
                                                      cause,
                                                      state.workflowDefinitionId(),
                                                      eventNameCustomizer), processingContext);

        verify(listener).onWorkflowStatus(eq(WorkflowStatus.FAILED), eq(workflowContext));
    }

    private EventSourcedWorkflowState workflowState(Map<WorkflowStatus, WorkflowStatusChangeListener> listeners) {
        return new EventSourcedWorkflowState("wf-1",
                                             Map.of(),
                                             new MessageType(new QualifiedName("test-workflow"), "0.0.1"),
                                             workflowContext,
                                             listeners);
    }
}
