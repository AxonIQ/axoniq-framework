/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionFixture.CancellationCapableExecution;
import org.junit.jupiter.api.*;

import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies that the {@link WorkflowCancellationService} finds the workflows to cancel through the {@link LiveWorkflows}
 * of its module, and that a workflow cancellation the execution did not perform yet fails once the execution left
 * memory.
 *
 * @author Simon Zambrovski
 */
class WorkflowCancellationServiceTest {

    private static final String WORKFLOW_ID = "workflow-id";

    private final LiveWorkflows liveWorkflows = new LiveWorkflows(new InMemoryWorkflowExecutionRepository());
    private final WorkflowCancellationService service =
            new WorkflowCancellationService(liveWorkflows, () -> NoHibernation.INSTANCE);

    private static WorkflowExecution execution(String workflowId, WorkflowCancellation cancellation) {
        var execution = mock(CancellationCapableExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        when(execution.workflowCancellation()).thenReturn(cancellation);
        return execution;
    }

    /**
     * Brings an execution of the given workflow into memory, taking cancellation requests through the given
     * cancellation, as the engine does with an execution it starts.
     */
    private WorkflowExecution cancellable(String workflowId, WorkflowCancellation cancellation) {
        var execution = execution(workflowId, cancellation);
        liveWorkflows.enter(workflowId, false, () -> execution);
        liveWorkflows.markCancellable(execution);
        return execution;
    }

    private void assertNoRunningWorkflow(String workflowId) {
        assertThatThrownBy(() -> service.requestWorkflowCancellation(workflowId, null))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("No running workflow found with id '" + workflowId + "'");
        assertThatThrownBy(() -> service.requestStepCancellation(workflowId, "step", null))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("No running workflow found with id '" + workflowId + "'");
        assertThatThrownBy(() -> service.requestCancellationOfAllSteps(workflowId, null))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("No running workflow found with id '" + workflowId + "'");
    }

    @Nested
    class Requests {

        @Test
        void delegateToTheCancellationOfTheExecutionInMemory() {
            // given
            var cancellation = mock(WorkflowCancellation.class);
            cancellable(WORKFLOW_ID, cancellation);
            var stepCause = new IllegalStateException("step cancellation");
            var runningStepsCause = new IllegalStateException("running step cancellations");
            var workflowCause = new IllegalStateException("workflow cancellation");
            var stepCancellation = CompletableFuture.completedFuture(true);
            var runningStepCancellations = CompletableFuture.completedFuture(2);
            var workflowCancellation = CompletableFuture.<Void>completedFuture(null);
            when(cancellation.requestStepCancellation("step", stepCause)).thenReturn(stepCancellation);
            when(cancellation.requestCancellationOfAllSteps(runningStepsCause)).thenReturn(runningStepCancellations);
            when(cancellation.requestWorkflowCancellation(workflowCause)).thenReturn(workflowCancellation);

            // when / then
            assertThat(service.requestStepCancellation(WORKFLOW_ID, "step", stepCause)).isSameAs(stepCancellation);
            assertThat(service.requestCancellationOfAllSteps(WORKFLOW_ID, runningStepsCause))
                    .isSameAs(runningStepCancellations);
            assertThat(service.requestWorkflowCancellation(WORKFLOW_ID, workflowCause))
                    .isSameAs(workflowCancellation);
        }

        @Test
        void forAnUnknownWorkflowThrowNoSuchElementException() {
            assertNoRunningWorkflow(WORKFLOW_ID);
        }

        @Test
        void forAnExecutionInMemoryThatTakesNoRequestsYetThrowNoSuchElementException() {
            // given: an execution a claim restored, which takes requests only once the claim caught up
            var cancellation = mock(WorkflowCancellation.class);
            var execution = execution(WORKFLOW_ID, cancellation);
            liveWorkflows.enter(WORKFLOW_ID, false, () -> execution);

            // when / then
            assertNoRunningWorkflow(WORKFLOW_ID);
            verifyNoInteractions(cancellation);
        }

        @Test
        void forAWorkflowWhoseExecutionLeftMemoryThrowNoSuchElementException() {
            // given
            var execution = cancellable(WORKFLOW_ID, mock(WorkflowCancellation.class));

            // when
            liveWorkflows.leave(execution);

            // then
            assertNoRunningWorkflow(WORKFLOW_ID);
        }

        @Test
        void reachTheNextExecutionOfAWorkflowWhenTheExecutionThatLeftMemoryBeforeItLeavesAgain() {
            // given: an execution that left memory, and the next execution of its workflow
            var departed = cancellable(WORKFLOW_ID, mock(WorkflowCancellation.class));
            liveWorkflows.leave(departed);
            var next = mock(WorkflowCancellation.class);
            when(next.requestWorkflowCancellation(null)).thenReturn(CompletableFuture.completedFuture(null));
            cancellable(WORKFLOW_ID, next);

            // when: the cleanup of the departed execution runs once more
            liveWorkflows.leave(departed);

            // then
            assertThat(service.requestWorkflowCancellation(WORKFLOW_ID, null)).isCompleted();
            verify(next).requestWorkflowCancellation(null);
        }
    }

    @Nested
    class PendingWorkflowCancellation {

        @Test
        void isAbortedWhenTheExecutionLeavesMemory() {
            // given
            var cancellation = mock(WorkflowCancellation.Request.class);
            var execution = cancellable(WORKFLOW_ID, cancellation);

            // when
            liveWorkflows.leave(execution);

            // then
            verify(cancellation).abortPendingWorkflowCancellation(argThat(reason -> reason.getMessage().equals(
                    "Workflow execution completed before cancellation was performed"
            )));
        }

        @Test
        void isAbortedWhenTheEngineShutsDown() {
            // given
            var cancellation = mock(WorkflowCancellation.Request.class);
            cancellable(WORKFLOW_ID, cancellation);

            // when
            liveWorkflows.clear();

            // then
            verify(cancellation).abortPendingWorkflowCancellation(argThat(reason -> reason.getMessage().equals(
                    "Workflow engine shut down before cancellation was performed"
            )));
            assertNoRunningWorkflow(WORKFLOW_ID);
        }

        @Test
        void isAbortedWhenTheExecutionLeftMemoryWhileTheRequestReachedIt() {
            // given: a cancellation whose request becomes pending only after the execution left memory, which leaving
            // does without the lock of the workflow
            var cancellation = mock(WorkflowCancellation.Request.class);
            var execution = cancellable(WORKFLOW_ID, cancellation);
            var pending = new AtomicReference<CompletableFuture<Void>>();
            doAnswer(invocation -> {
                var request = pending.get();
                if (request != null) {
                    request.completeExceptionally(invocation.getArgument(0));
                }
                return null;
            }).when(cancellation).abortPendingWorkflowCancellation(any());
            when(cancellation.requestWorkflowCancellation(null)).thenAnswer(invocation -> {
                liveWorkflows.leave(execution);
                var request = new CompletableFuture<Void>();
                pending.set(request);
                return request;
            });

            // when
            var request = service.requestWorkflowCancellation(WORKFLOW_ID, null);

            // then: the request fails, instead of waiting for an execution that never performs it
            assertThat(request).isCompletedExceptionally();
            assertThatThrownBy(request::join).isInstanceOf(CancellationException.class)
                                             .hasMessage("Workflow execution completed before cancellation was "
                                                                 + "performed");
        }
    }
}
