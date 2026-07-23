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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.api.management.WorkflowManager.CancellationReason;
import io.axoniq.workflow.runtime.api.management.WorkflowManager.WorkflowHandle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the fluent {@link io.axoniq.workflow.runtime.api.management.WorkflowManager} implementation
 * {@link DefaultWorkflowManager}: single-instance handles ({@code workflow(id)}) and point-in-time selections
 * ({@code workflows(pred)}), covering the present / unknown / already-terminal cases and the matched/affected/ids
 * aggregation.
 *
 * @author Stefan Dragisic
 * @since 0.3.0
 */
class DefaultWorkflowManagerTest {

    // --- workflow(id).cancel ---

    @Test
    void workflowCancel_onRunningInstance_enqueuesCancellation_returnsTrue() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = runningExecution("wf", "EU");
        when(repository.findById("wf")).thenReturn(Optional.of(execution));

        var manager = new DefaultWorkflowManager(repository);
        boolean cancelled = manager.workflow("wf").cancel(CancellationReason.of("stop"));

        assertThat(cancelled).isTrue();
        verify(execution).requestWorkflowCancellation(any());
    }

    @Test
    void workflowCancel_onUnknownId_returnsFalse() {
        var repository = mock(WorkflowExecutionRepository.class);
        when(repository.findById("no-such")).thenReturn(Optional.empty());

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("no-such").cancel(CancellationReason.none())).isFalse();
    }

    @Test
    void workflowCancel_onAlreadyTerminalInstance_returnsFalse_doesNotEnqueue() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        when(execution.state()).thenReturn(state);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.CANCELLED);
        when(repository.findById("wf-terminal")).thenReturn(Optional.of(execution));

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("wf-terminal").cancel(CancellationReason.of("too late"))).isFalse();
        verify(execution, never()).requestWorkflowCancellation(any());
    }

    // --- workflow(id).state() ---

    @Test
    void workflowState_onKnownId_returnsLiveState() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = runningExecution("wf", "EU");
        when(repository.findById("wf")).thenReturn(Optional.of(execution));

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("wf").state()).contains(execution.state());
        assertThat(manager.workflow("wf").id()).isEqualTo("wf");
    }

    @Test
    void workflowState_onUnknownId_returnsEmpty() {
        var repository = mock(WorkflowExecutionRepository.class);
        when(repository.findById("no-such")).thenReturn(Optional.empty());

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("no-such").state()).isEmpty();
    }

    // --- workflow(id).cancelStep ---

    @Test
    void workflowCancelStep_onRunningStep_returnsTrue() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = runningExecution("wf", "EU");
        when(execution.requestStepCancellation(eq("approve"), any())).thenReturn(true);
        when(repository.findById("wf")).thenReturn(Optional.of(execution));

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("wf").cancelStep("approve", CancellationReason.of("op"))).isTrue();
    }

    @Test
    void workflowCancelStep_onAlreadyTerminalStep_returnsFalse() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = runningExecution("wf", "EU");
        when(execution.requestStepCancellation(eq("prepared"), any())).thenReturn(false);
        when(repository.findById("wf")).thenReturn(Optional.of(execution));

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("wf").cancelStep("prepared", CancellationReason.of("too late"))).isFalse();
    }

    @Test
    void workflowCancelStep_onUnknownWorkflow_returnsFalse() {
        var repository = mock(WorkflowExecutionRepository.class);
        when(repository.findById("no-such")).thenReturn(Optional.empty());

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("no-such").cancelStep("approve", CancellationReason.none())).isFalse();
    }

    // --- workflow(id).cancelAllRunningSteps ---

    @Test
    void workflowCancelAllRunningSteps_returnsCount() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = runningExecution("wf", "EU");
        when(execution.requestAllRunningStepsCancellation(any())).thenReturn(2);
        when(repository.findById("wf")).thenReturn(Optional.of(execution));

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("wf").cancelAllRunningSteps(CancellationReason.of("all"))).isEqualTo(2);
    }

    @Test
    void workflowCancelAllRunningSteps_onUnknownWorkflow_returnsZero() {
        var repository = mock(WorkflowExecutionRepository.class);
        when(repository.findById("no-such")).thenReturn(Optional.empty());

        var manager = new DefaultWorkflowManager(repository);

        assertThat(manager.workflow("no-such").cancelAllRunningSteps(CancellationReason.none())).isZero();
    }

    // --- workflows(pred).cancel ---

    @Test
    void workflowsCancel_aggregatesMatchedAffectedAndIds() {
        var repository = mock(WorkflowExecutionRepository.class);
        var eu1 = runningExecution("eu-1", "EU");
        var eu2 = runningExecution("eu-2", "EU");
        var us = runningExecution("us-1", "US");
        when(repository.findAll()).thenReturn(new LinkedHashSet<>(List.of(eu1, eu2, us)));

        var manager = new DefaultWorkflowManager(repository);
        var result = manager.workflows(state -> "EU".equals(state.payload().get("region")))
                            .cancel(CancellationReason.of("cancel EU"));

        assertThat(result.matched()).isEqualTo(2);
        assertThat(result.affected()).isEqualTo(2);
        assertThat(result.workflowIds()).containsExactlyInAnyOrder("eu-1", "eu-2");
        verify(eu1).requestWorkflowCancellation(any());
        verify(eu2).requestWorkflowCancellation(any());
        verify(us, never()).requestWorkflowCancellation(any());
    }

    @Test
    void workflowsCancel_excludesTerminalInstancesFromSelection() {
        var repository = mock(WorkflowExecutionRepository.class);
        var running = runningExecution("running", "EU");
        var terminal = mock(WorkflowExecution.class);
        var terminalState = mock(WorkflowState.class);
        when(terminal.state()).thenReturn(terminalState);
        when(terminalState.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);
        when(repository.findAll()).thenReturn(new LinkedHashSet<>(List.of(running, terminal)));

        var manager = new DefaultWorkflowManager(repository);
        var result = manager.workflows(state -> true).cancel(CancellationReason.none());

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.affected()).isEqualTo(1);
        assertThat(result.workflowIds()).containsExactly("running");
        verify(terminal, never()).requestWorkflowCancellation(any());
    }

    @Test
    void workflowsCancel_toleratesInstanceTerminatingBetweenSelectionAndAction() {
        var repository = mock(WorkflowExecutionRepository.class);
        var execution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        when(execution.workflowId()).thenReturn("racing");
        when(execution.state()).thenReturn(state);
        // Non-terminal at selection, terminal by the time the cancel command re-checks.
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED, WorkflowStatus.CANCELLED);
        when(repository.findAll()).thenReturn(new LinkedHashSet<>(List.of(execution)));

        var manager = new DefaultWorkflowManager(repository);
        var result = manager.workflows(s -> true).cancel(CancellationReason.none());

        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.affected()).isZero();
        assertThat(result.workflowIds()).isEmpty();
        verify(execution, never()).requestWorkflowCancellation(any());
    }

    // --- iterating / streaming a selection ---

    @Test
    void workflowsSelection_isIterable_andActsPerHandle() {
        var repository = mock(WorkflowExecutionRepository.class);
        var eu1 = runningExecution("eu-1", "EU");
        var eu2 = runningExecution("eu-2", "EU");
        when(repository.findAll()).thenReturn(new LinkedHashSet<>(List.of(eu1, eu2)));

        var manager = new DefaultWorkflowManager(repository);
        var selection = manager.workflows(state -> "EU".equals(state.payload().get("region")));

        var seen = new ArrayList<String>();
        for (WorkflowHandle handle : selection) {
            seen.add(handle.id());
            assertThat(handle.state()).isPresent();
            handle.cancel(CancellationReason.of("per-handle"));
        }

        assertThat(seen).containsExactlyInAnyOrder("eu-1", "eu-2");
        verify(eu1).requestWorkflowCancellation(any());
        verify(eu2).requestWorkflowCancellation(any());
    }

    @Test
    void workflowsSelection_streamYieldsHandles() {
        var repository = mock(WorkflowExecutionRepository.class);
        var eu1 = runningExecution("eu-1", "EU");
        var eu2 = runningExecution("eu-2", "EU");
        when(repository.findAll()).thenReturn(new LinkedHashSet<>(List.of(eu1, eu2)));

        var manager = new DefaultWorkflowManager(repository);

        var ids = manager.workflows(state -> "EU".equals(state.payload().get("region")))
                         .stream()
                         .map(WorkflowHandle::id)
                         .toList();

        assertThat(ids).containsExactlyInAnyOrder("eu-1", "eu-2");
    }

    // --- workflows(pred).cancelStep across multiple matches ---

    @Test
    void workflowsCancelStep_cancelsNamedStepInEachMatch() {
        var repository = mock(WorkflowExecutionRepository.class);
        var eu1 = runningExecution("eu-1", "EU");
        var eu2 = runningExecution("eu-2", "EU");
        when(eu1.requestStepCancellation(eq("approve"), any())).thenReturn(true);
        when(eu2.requestStepCancellation(eq("approve"), any())).thenReturn(true);
        when(repository.findAll()).thenReturn(new LinkedHashSet<>(List.of(eu1, eu2)));

        var manager = new DefaultWorkflowManager(repository);
        var result = manager.workflows(state -> "EU".equals(state.payload().get("region")))
                            .cancelStep("approve", CancellationReason.of("cancel approve step"));

        assertThat(result.matched()).isEqualTo(2);
        assertThat(result.affected()).isEqualTo(2);
        assertThat(result.workflowIds()).containsExactlyInAnyOrder("eu-1", "eu-2");
        verify(eu1).requestStepCancellation(eq("approve"), any());
        verify(eu2).requestStepCancellation(eq("approve"), any());
    }

    private static WorkflowExecution runningExecution(String id, String region) {
        var execution = mock(WorkflowExecution.class);
        var state = mock(WorkflowState.class);
        when(execution.workflowId()).thenReturn(id);
        when(execution.state()).thenReturn(state);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(state.payload()).thenReturn(Map.of("region", region));
        return execution;
    }
}
