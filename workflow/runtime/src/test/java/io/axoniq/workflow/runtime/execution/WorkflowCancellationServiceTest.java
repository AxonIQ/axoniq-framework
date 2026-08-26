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

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

/**
 * @author Simon Zambrovski
 * @since 0.3.0
 */
class WorkflowCancellationServiceTest {

    private static final String WORKFLOW_ID = "workflow-id";

    @Test
    void cancellationRequestsDelegateToRegisteredCoordinator() {
        var service = new WorkflowCancellationService();
        var cancellation = mock(WorkflowCancellation.class);
        var stepCause = new IllegalStateException("step cancellation");
        var runningStepsCause = new IllegalStateException("running step cancellations");
        var workflowCause = new IllegalStateException("workflow cancellation");
        var stepCancellation = CompletableFuture.completedFuture(true);
        var runningStepCancellations = CompletableFuture.completedFuture(2);
        var workflowCancellation = CompletableFuture.<Void>completedFuture(null);
        service.register(WORKFLOW_ID, cancellation);
        when(cancellation.requestStepCancellation("step", stepCause)).thenReturn(stepCancellation);
        when(cancellation.requestCancellationOfAllSteps(runningStepsCause)).thenReturn(runningStepCancellations);
        when(cancellation.requestWorkflowCancellation(workflowCause)).thenReturn(workflowCancellation);

        assertThat(service.requestStepCancellation(WORKFLOW_ID, "step", stepCause)).isSameAs(stepCancellation);
        assertThat(service.requestCancellationOfAllSteps(WORKFLOW_ID, runningStepsCause))
                .isSameAs(runningStepCancellations);
        assertThat(service.requestWorkflowCancellation(WORKFLOW_ID, workflowCause)).isSameAs(workflowCancellation);

        verify(cancellation).requestStepCancellation("step", stepCause);
        verify(cancellation).requestCancellationOfAllSteps(runningStepsCause);
        verify(cancellation).requestWorkflowCancellation(workflowCause);
    }

    @Test
    void unregisterAndClearRemoveCancellationCoordinators() {
        var service = new WorkflowCancellationService();
        var firstCancellation = mock(WorkflowCancellation.class);
        var secondCancellation = mock(WorkflowCancellation.class);
        service.register("first", firstCancellation);
        service.register("second", secondCancellation);

        service.unregister("first");

        assertNoRunningWorkflow(service, "first");
        service.requestWorkflowCancellation("second", null);
        verify(secondCancellation).requestWorkflowCancellation(null);

        service.clear();

        assertNoRunningWorkflow(service, "second");
    }

    @Test
    void unregisterAndClearAbortUnconsumedWorkflowCancellationRequests() {
        var service = new WorkflowCancellationService();
        var firstCancellation = mock(WorkflowCancellation.Request.class);
        var secondCancellation = mock(WorkflowCancellation.Request.class);
        service.register("first", firstCancellation);
        service.register("second", secondCancellation);

        service.unregister("first");
        service.clear();

        verify(firstCancellation).abortPendingWorkflowCancellation(any(CancellationException.class));
        verify(secondCancellation).abortPendingWorkflowCancellation(any(CancellationException.class));
    }

    @Test
    void cancellationRequestsForUnknownWorkflowThrowNoSuchElementException() {
        var service = new WorkflowCancellationService();

        assertNoRunningWorkflow(service, WORKFLOW_ID);
        assertThatThrownBy(() -> service.requestStepCancellation(WORKFLOW_ID, "step", null))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("No running workflow found with id 'workflow-id'");
        assertThatThrownBy(() -> service.requestCancellationOfAllSteps(WORKFLOW_ID, null))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("No running workflow found with id 'workflow-id'");
    }

    private static void assertNoRunningWorkflow(WorkflowCancellationService service, String workflowId) {
        assertThatThrownBy(() -> service.requestWorkflowCancellation(workflowId, null))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessage("No running workflow found with id '" + workflowId + "'");
    }
}
