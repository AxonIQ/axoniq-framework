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
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowFailedException;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands.failWorkflow;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.eq;

/**
 * Test class validating the {@link WorkflowLifecycleControlDelegate} failure flow.
 *
 * @author Stefan Dragisic
 */
class WorkflowLifecycleControlDelegateFailTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private ProcessingContext processingContext;
    private EventNameCustomizer eventNameCustomizer;
    private WorkflowTerminalTransition terminalTransition;

    private WorkflowLifecycleControlDelegate testSubject;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        processingContext = mock(ProcessingContext.class);
        UnitOfWorkFactory unitOfWorkFactory = mock(UnitOfWorkFactory.class);
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
        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.workflowName()).thenReturn("test-workflow");
        when(workflowContext.workflowId()).thenReturn("wf-1");
        when(workflowContext.workflowPayload()).thenReturn(Map.of());
        when(workflowExecution.appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        eventNameCustomizer = defaults();

        testSubject = new WorkflowLifecycleControlDelegate(
                workflowContext,
                workflowExecution,
                new RunningSteps(),
                new ReachedSteps(),
                terminalTransition
        );
    }

    @Test
    void failWorkflowInterruptsRunningStepsWithoutPerStepEvent() {
        assertThatThrownBy(
                () -> testSubject.failWorkflow(failWorkflow(new RuntimeException("boom"), eventNameCustomizer))
        ).isInstanceOf(WorkflowFailedException.class);

        // Whole-workflow fail interrupts running steps + discards the queue; no per-step cancellation is published.
        verify(terminalTransition).transition(any(Runnable.class));
    }

    @Test
    void failWorkflowWithNullCauseInterruptsRunningSteps() {
        assertThatThrownBy(
                () -> testSubject.failWorkflow(failWorkflow(null, eventNameCustomizer))
        ).isInstanceOf(WorkflowFailedException.class);

        verify(terminalTransition).transition(any(Runnable.class));
    }

    @Test
    void failWorkflowPublishesFailedWorkflowEvent() {
        assertThatThrownBy(() -> testSubject.failWorkflow(failWorkflow(new RuntimeException("boom"), eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        verify(workflowExecution).appendWorkflowEvent(any(EventMessage.class), eq(processingContext));
    }

    @Test
    void failWorkflowThrowsWorkflowFailedExceptionWithCause() {
        var cause = new RuntimeException("boom");

        assertThatThrownBy(() -> testSubject.failWorkflow(failWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasCause(cause);
    }

    @Test
    void failWorkflowWithNullCauseThrowsWorkflowFailedExceptionWithMessage() {
        assertThatThrownBy(() -> testSubject.failWorkflow(failWorkflow(null, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class)
                .hasRootCauseMessage("Workflow failed");
    }

    @Test
    void failWorkflowExecutesStepsInOrder() {
        var cause = new RuntimeException("boom");
        var order = inOrder(terminalTransition, workflowExecution);

        assertThatThrownBy(() -> testSubject.failWorkflow(failWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        order.verify(terminalTransition).transition(any(Runnable.class));
        order.verify(workflowExecution).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    @Test
    void failWorkflowLogsAndRethrowsUnwrappedPublicationFailure() {
        var publicationFailure = new IllegalStateException("publication failed");
        when(workflowExecution.appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class)))
                .thenReturn(CompletableFuture.failedFuture(publicationFailure));
        Logger logger = (Logger) LogManager.getLogger(WorkflowLifecycleControlDelegate.class);
        var appender = new ListAppender("WorkflowLifecycleControlDelegateFailTest");
        appender.start();
        logger.addAppender(appender);

        try {
            assertThatThrownBy(
                    () -> testSubject.failWorkflow(failWorkflow(new RuntimeException("boom"), eventNameCustomizer))
            ).isSameAs(publicationFailure);

            assertThat(appender.getEvents())
                    .anySatisfy(event -> {
                        assertThat(event.getMessage().getFormattedMessage())
                                .contains("Failed to publish FAILED terminal event for workflow 'wf-1'");
                        assertThat(event.getThrown().getMessage()).isEqualTo("publication failed");
                    });
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }
    }

    @Test
    void failWorkflowInvokesFailedStatusChangeListener() {
        var listener = mock(WorkflowStatusChangeListener.class);
        EventSourcedWorkflowState state = workflowState(Map.of(WorkflowStatus.FAILED, listener));
        when(workflowExecution.state()).thenReturn(state);

        RuntimeException cause = new RuntimeException("boom");
        assertThatThrownBy(() -> testSubject.failWorkflow(
                failWorkflow(cause, eventNameCustomizer)))
                .isInstanceOf(WorkflowFailedException.class);

        state.evolve(
                EventMessageUtils.failedWorkflow(
                        workflowContext, "test-workflow", cause, state.workflowDefinitionId(), eventNameCustomizer
                ),
                processingContext
        );

        verify(listener).onWorkflowStatus(
                eq(WorkflowStatus.FAILED), eq(workflowContext), any(EventMessage.class), eq(processingContext)
        );
    }

    private EventSourcedWorkflowState workflowState(Map<WorkflowStatus, WorkflowStatusChangeListener> listeners) {
        return new EventSourcedWorkflowState("wf-1",
                                             Map.of(),
                                             VersionedType.of(new QualifiedName("test-workflow"), "0.0.1"),
                                             workflowContext,
                                             listeners);
    }
}
