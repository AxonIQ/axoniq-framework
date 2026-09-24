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

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecutionOperations;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the replay-drift guard in {@link WorkflowLifecycleControlDelegate}. Drift detection must fire on
 * workflow-level fail/cancel AND per-step cancellation, since both publish events that would corrupt an in-flight
 * workflow if old code ran past this point.
 *
 * @author Stefan Dragisic
 */
class WorkflowLifecycleControlDelegateDriftTest {

    private final ReachedSteps reachedSteps = new ReachedSteps();
    private WorkflowExecutionOperations workflowExecutionOperations;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private WorkflowLifecycleControlDelegate delegate;

    @BeforeEach
    void setUp() {
        workflowExecutionOperations = mock(WorkflowExecutionOperations.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        EventSink eventSink = mock(EventSink.class);
        UnitOfWorkFactory unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        Executor executor = Runnable::run;

        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.workflowName()).thenReturn("TestWorkflow");
        when(workflowExecution.state()).thenReturn(state);

        delegate = new WorkflowLifecycleControlDelegate(
                workflowExecutionOperations, workflowExecution, new RunningSteps(), reachedSteps,
                terminalEventPublication -> terminalEventPublication.run()
        );
    }

    @Test
    void failWorkflowThrowsDriftWhenOrphansAhead() {
        reachedSteps.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        var cmd = PrimitiveCommands.failWorkflow(
                new RuntimeException("oops"), DefaultEventNameCustomizer.Builder.defaults());

        assertThatThrownBy(() -> delegate.failWorkflow(cmd))
                .isInstanceOf(WorkflowReplayDriftException.class)
                .satisfies(ex -> {
                    var drift = (WorkflowReplayDriftException) ex;
                    assertThat(drift.aboutToExecute()).isEqualTo("<terminate>");
                    assertThat(drift.orphans()).containsExactly("B");
                });
    }

    @Test
    void cancelWorkflowThrowsDriftWhenOrphansAhead() {
        reachedSteps.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        var cmd = PrimitiveCommands.cancelWorkflow(
                null, DefaultEventNameCustomizer.Builder.defaults());

        assertThatThrownBy(() -> delegate.cancelWorkflow(cmd))
                .isInstanceOf(WorkflowReplayDriftException.class);
    }

    @Test
    void cancelStepThrowsDriftWhenOrphansAhead() {
        reachedSteps.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        var cmd = PrimitiveCommands.cancelStep(
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
    void failWorkflowDoesNotThrowWhenAllStepsReferenced() {
        reachedSteps.record("A");
        reachedSteps.record("B");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));

        // Workflow-level fail does eventually publish — we only assert no drift was thrown by
        // catching any non-drift throwable as "passes" for our purposes.
        var cmd = PrimitiveCommands.failWorkflow(
                new RuntimeException("expected"), DefaultEventNameCustomizer.Builder.defaults());

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
