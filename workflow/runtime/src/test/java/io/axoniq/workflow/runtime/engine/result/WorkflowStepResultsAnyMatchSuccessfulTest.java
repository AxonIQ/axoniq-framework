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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowStepResults#anyMatch(WorkflowState, java.util.function.Predicate, WorkflowStepResult...)}
 * with the {@link WorkflowStepResult#isSuccess()} predicate (formerly {@code anySuccessful()}).
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class WorkflowStepResultsAnyMatchSuccessfulTest {

    private WorkflowState workflowState;

    @BeforeEach
    void setUp() {
        workflowState = mock(WorkflowState.class);
        when(workflowState.firstCompletedAmong(any())).thenReturn(Optional.empty());
    }

    // --- getStepName ---

    @Test
    void stepNameCombinesAllResultNames() {
        var r1 = WorkflowStepResults.completed("stepA", null);
        var r2 = WorkflowStepResults.completed("stepB", null);
        var r3 = WorkflowStepResults.completed("stepC", null);

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2, r3);

        assertThat(result.getStepName()).isEqualTo("anyMatch(stepA, stepB, stepC)");
    }

    // --- First success wins ---

    @Test
    void firstSuccessBecomesWinner() {
        var r1 = WorkflowStepResults.completed("stepA", "payload-A");
        var r2 = WorkflowStepResults.completed("stepB", "payload-B");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.<String>result()).contains("payload-A");
    }

    @Test
    void singleSuccessBecomesWinnerImmediately() {
        var r1 = WorkflowStepResults.completed("onlyStep", "only-value");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.<String>result()).contains("only-value");
        assertThat(result.getStepName()).isEqualTo("anyMatch(onlyStep)");
    }

    // --- Failures are ignored while steps are still running ---

    @Test
    void failuresIgnoredWhileStepsStillRunning() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        // stepA failed, stepB still running
        when(r1.isCompleted()).thenReturn(true);
        when(r1.isSuccess()).thenReturn(false);
        when(r1.isFailure()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.isSuccess()).thenReturn(false);

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.isCompleted()).isFalse();
    }

    @Test
    void successWinsOverPriorFailure() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        // stepA failed, stepB succeeded
        when(r1.isCompleted()).thenReturn(true);
        when(r1.isSuccess()).thenReturn(false);
        when(r1.isFailure()).thenReturn(true);

        when(r2.isCompleted()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(true);
        when(r2.await()).thenReturn(true);

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isSuccess()).isTrue();

        verify(r1).cancel("Superseded by stepB");
        verify(r2, never()).cancel(anyString());
    }

    // --- All failed → composite failure ---

    @Test
    void allFailedReturnsCompositeFailure() {
        var error1 = new RuntimeException("boom1");
        var error2 = new RuntimeException("boom2");
        var r1 = WorkflowStepResults.failed("stepA", error1);
        var r2 = WorkflowStepResults.failed("stepB", error2);

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isFailure()).isTrue();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isPresent();
    }

    // --- Losers cancelled with "Superseded by successful" reason ---

    @Test
    void losersCancelledWithSupersededBySuccessfulReason() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        var r3 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");
        when(r3.getStepName()).thenReturn("stepC");

        when(r1.isCompleted()).thenReturn(false);
        when(r1.isSuccess()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(true);
        when(r2.await()).thenReturn(true);
        when(r3.isCompleted()).thenReturn(false);
        when(r3.isSuccess()).thenReturn(false);

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2, r3);

        // Trigger winner resolution
        result.isSuccess();

        verify(r1).cancel("Superseded by stepB");
        verify(r3).cancel("Superseded by stepB");
        verify(r2, never()).cancel(anyString());
    }

    // --- cancel propagates to all results ---

    @Test
    void cancelCancelsAllResults() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        result.cancel();

        verify(r1).cancel();
        verify(r2).cancel();
    }

    @Test
    void cancelWithReasonCancelsAllResults() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        result.cancel("workflow shutdown");

        verify(r1).cancel("workflow shutdown");
        verify(r2).cancel("workflow shutdown");
    }

    // --- isCompleted is non-blocking ---

    @Test
    void isCompletedReturnsFalseWhenNoneSuccessfulAndNotAllCompleted() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.isCompleted()).thenReturn(false);
        when(r1.isSuccess()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.isSuccess()).thenReturn(false);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.isCompleted()).isFalse();
    }

    @Test
    void isCompletedReturnsTrueWhenOneSuccessful() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.isCompleted()).thenReturn(true);
        when(r1.isSuccess()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.isSuccess()).thenReturn(false);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.isCompleted()).isTrue();
    }

    @Test
    void isCompletedReturnsTrueWhenAllFailedEvenWithNoSuccess() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.isCompleted()).thenReturn(true);
        when(r1.isSuccess()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(false);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.isCompleted()).isTrue();
    }

    // --- awaitStateChange-based winner resolution ---

    @Test
    void resolveWinnerBlocksUntilResultSucceeds() throws InterruptedException {
        var r1 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        // First loop: isCompleted=false (guard filter), false (allMatch) → awaitStateChange
        // Second loop after wake-up: isCompleted=true → found
        when(r1.isCompleted()).thenReturn(false, false, true);
        when(r1.isSuccess()).thenReturn(false, true);
        when(r1.await()).thenReturn(true);

        doAnswer(invocation -> null).when(workflowState).awaitStateChange(any());

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess, r1);

        assertThat(result.isSuccess()).isTrue();
        verify(workflowState).awaitStateChange(any());
    }

    // --- Event-sourcing replay: winner determined by event order, not array order ---

    @Test
    void winnerIsDeterminedByEventOrderNotArrayOrder() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.isSuccess()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isSuccess()).thenReturn(true);

        // Event-sourced state says stepB succeeded first
        when(workflowState.firstCompletedAmong(Set.of("stepA", "stepB")))
                .thenReturn(Optional.of("stepB"));

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        // Trigger winner resolution
        result.isSuccess();

        // stepB should be the winner despite being second in array order
        verify(r1).cancel("Superseded by stepB");
        verify(r2, never()).cancel(anyString());
    }

    // --- Winner caching ---

    @Test
    void winnerIsCachedAcrossMultipleCalls() {
        var r1 = WorkflowStepResults.completed("stepA", "payload-A");
        var r2 = WorkflowStepResults.completed("stepB", "payload-B");

        var result = WorkflowStepResults.anyMatch(workflowState, WorkflowStepResult::isSuccess,r1, r2);

        assertThat(result.<String>result()).contains("payload-A");
        assertThat(result.<String>result()).contains("payload-A");
        assertThat(result.isSuccess()).isTrue();
    }
}
