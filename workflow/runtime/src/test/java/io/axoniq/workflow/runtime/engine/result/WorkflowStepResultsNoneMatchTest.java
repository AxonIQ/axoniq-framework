/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.result;

import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.impl.NoneMatchCombinatorDelegate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowStepResults#noneMatch(WorkflowState, java.util.function.Predicate, WorkflowStepResult...)}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class WorkflowStepResultsNoneMatchTest {

    private WorkflowState workflowState;

    @BeforeEach
    void setUp() {
        workflowState = mock(WorkflowState.class);
        when(workflowState.firstCompletedAmong(any())).thenReturn(Optional.empty());
    }

    // --- All complete without match → success ---

    @Test
    void noneMatch_allCompleteWithoutMatch_isSuccess() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A");
        var r2 = WorkflowStepResults.completed("stepB", "ok-B");

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.isFailure()).isFalse();
        assertThat(result.isCanceled()).isFalse();
        assertThat(result.isTimeout()).isFalse();
    }

    @Test
    void noneMatch_allCompleteWithoutMatch_resultIsEmpty() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A");
        var r2 = WorkflowStepResults.completed("stepB", "ok-B");

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.<Object>result()).isEmpty();
        assertThat(result.error()).isEmpty();
    }

    // --- Short-circuit on first match ---

    @Test
    void noneMatch_shortCircuitsOnFirstMatch() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok");

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isFailure()).isTrue();
        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    void noneMatch_shortCircuit_delegatesToViolator() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok");

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isFailure()).isTrue();
        assertThat(result.error()).isPresent();
        assertThat(result.<Object>result()).isEmpty();
    }

    @Test
    void noneMatch_shortCircuit_cancelsRemainingWithDisqualifiedReason() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        var r3 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");
        when(r3.getStepName()).thenReturn("stepC");

        when(r1.isCompleted()).thenReturn(false);
        when(r1.isFailure()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isFailure()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(false);
        when(r3.isCompleted()).thenReturn(false);
        when(r3.isFailure()).thenReturn(false);

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2, r3);

        // Trigger resolution
        result.isSuccess();

        verify(r1).cancel("Disqualified by stepB");
        verify(r3).cancel("Disqualified by stepB");
        verify(r2, never()).cancel(anyString());
    }

    // --- isCompleted behavior ---

    @Test
    void noneMatch_isCompletedFalseWhileStillRunningAndNoMatch() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.isFailure()).thenReturn(false);

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isFalse();
    }

    @Test
    void noneMatch_isCompletedTrueOnShortCircuit() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.isFailure()).thenReturn(false);

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
    }

    @Test
    void noneMatch_isCompletedTrueWhenAllCompleteNoMatch() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isFailure()).thenReturn(false);

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
    }

    // --- cancel propagation ---

    @Test
    void noneMatch_cancelPropagatesToAll() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        result.cancel();

        verify(r1).cancel();
        verify(r2).cancel();
    }

    @Test
    void noneMatch_cancelWithReasonPropagatesToAll() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        result.cancel("workflow shutdown");

        verify(r1).cancel("workflow shutdown");
        verify(r2).cancel("workflow shutdown");
    }

    // --- Step name format ---

    @Test
    void noneMatch_stepNameFormat() {
        var r1 = WorkflowStepResults.completed("stepA", null);
        var r2 = WorkflowStepResults.completed("stepB", null);

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.getStepName()).isEqualTo("noneMatch(stepA, stepB)");
    }

    // --- Event ordering determines violator ---

    @Test
    void noneMatch_eventOrderDeterminesViolator() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(true);
        when(r1.isSuccess()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isFailure()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(false);

        // Event-sourced state says stepB failed first
        when(workflowState.firstCompletedAmong(Set.of("stepA", "stepB")))
                .thenReturn(Optional.of("stepB"));

        var result = new NoneMatchCombinatorDelegate(workflowState).noneMatch(WorkflowStepResult::isFailure, r1, r2);

        // Trigger resolution
        result.isSuccess();

        // stepB should be the violator despite being second in array order
        verify(r1).cancel("Disqualified by stepB");
        verify(r2, never()).cancel(anyString());
    }
}
