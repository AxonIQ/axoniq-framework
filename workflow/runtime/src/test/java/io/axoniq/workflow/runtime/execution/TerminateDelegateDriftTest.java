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

import io.axoniq.workflow.runtime.api.execution.context.TerminatePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests for the replay-drift guard in {@link TerminateDelegate}. Drift detection must fire on
 * workflow-level fail/cancel AND per-step cancellation, since both publish events that would
 * corrupt an in-flight workflow if old code ran past this point.
 *
 * @author Stefan Dragisic
 */
class TerminateDelegateDriftTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private TerminateDelegate delegate;
    private final WorkflowStepProgress workflowStepProgress = new WorkflowStepProgress();

    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        EventSink eventSink = mock(EventSink.class);
        UnitOfWorkFactory unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        Executor executor = Runnable::run;

        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.workflowName()).thenReturn("TestWorkflow");
        when(workflowExecution.state()).thenReturn(state);

        delegate = new TerminateDelegate(
                workflowContext, workflowExecution, new RunningSteps(), workflowStepProgress, () -> { }, unitOfWorkFactory, eventSink, executor,
                DefaultEventNameCustomizer.Builder.defaults()
        );
    }

    @Test
    void terminate_throwsDrift_forWorkflowFail_whenOrphansAhead() {
        workflowStepProgress.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        var cmd = new TerminatePrimitive.FailWorkflow(
                new RuntimeException("oops"), DefaultEventNameCustomizer.Builder.defaults(), null);

        assertThatThrownBy(() -> delegate.failWorkflow(cmd))
                .isInstanceOf(WorkflowReplayDriftException.class)
                .satisfies(ex -> {
                    var drift = (WorkflowReplayDriftException) ex;
                    assertThat(drift.aboutToExecute()).isEqualTo("<terminate>");
                    assertThat(drift.orphans()).containsExactly("B");
                });
    }

    @Test
    void terminate_throwsDrift_forWorkflowCancel_whenOrphansAhead() {
        workflowStepProgress.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        var cmd = new TerminatePrimitive.CancelWorkflow(
                null, DefaultEventNameCustomizer.Builder.defaults(), null);

        assertThatThrownBy(() -> delegate.cancelWorkflow(cmd))
                .isInstanceOf(WorkflowReplayDriftException.class);
    }

    @Test
    void terminate_throwsDrift_forStepCancellation_whenOrphansAhead() {
        workflowStepProgress.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        var cmd = new TerminatePrimitive.CancelStep(
                "newCancelTarget", null, DefaultEventNameCustomizer.Builder.defaults());

        assertThatThrownBy(() -> delegate.cancelStep(cmd))
                .isInstanceOf(WorkflowReplayDriftException.class)
                .satisfies(ex -> {
                    var drift = (WorkflowReplayDriftException) ex;
                    assertThat(drift.aboutToExecute()).isEqualTo("newCancelTarget");
                    assertThat(drift.orphans()).containsExactly("B");
                });
    }

    @Test
    void terminate_doesNotThrow_whenAllStepsReferenced() {
        workflowStepProgress.record("A");
        workflowStepProgress.record("B");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        // Workflow-level fail does eventually publish — we only assert no drift was thrown by
        // catching any non-drift throwable as "passes" for our purposes.
        var cmd = new TerminatePrimitive.FailWorkflow(
                new RuntimeException("expected"), DefaultEventNameCustomizer.Builder.defaults(), null);

        assertThatCode(() -> {
            try {
                delegate.failWorkflow(cmd);
            } catch (WorkflowReplayDriftException e) {
                throw e;
            } catch (Throwable t) {
                // any non-drift exception means the guard passed and the publish path was reached
            }
        }).doesNotThrowAnyException();
    }

    private WorkflowStep terminalStep(String name) {
        return new WorkflowStep(name, StepStatus.COMPLETED, null, null, Instant.now(), null);
    }
}
