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
import io.axoniq.workflow.runtime.engine.impl.AnyMatchCombinatorDelegate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowStepResults#anyMatch(WorkflowState, java.util.function.Predicate, WorkflowStepResult...)}
 * with non-standard predicates ({@code isFailure}, {@code isTimeout}, {@code isCanceled}).
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class WorkflowStepResultsAnyMatchTest {

    private WorkflowState workflowState;

    @BeforeEach
    void setUp() {
        workflowState = mock(WorkflowState.class);
        when(workflowState.firstCompletedAmong(any())).thenReturn(Optional.empty());
    }

    // --- anyMatch with isFailure predicate ---

    @Test
    void anyMatch_FAILED_firstToFailWins() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok");

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isFailure()).isTrue();
        assertThat(result.error()).isPresent();
    }

    @Test
    void anyMatch_FAILED_successIgnoredWhileRunning() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        // stepA succeeded, stepB still running
        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(false);
        when(r1.isSuccess()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.isFailure()).thenReturn(false);

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isFalse();
    }

    @Test
    void anyMatch_FAILED_allSucceedFallsBackToFirstCompleted() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A");
        var r2 = WorkflowStepResults.completed("stepB", "ok-B");

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        // Fallback: first completed wins, which is a success
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.<String>result()).contains("ok-A");
    }

    @Test
    void anyMatch_FAILED_losersCancelledWithSupersededReason() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(true);
        when(r1.isSuccess()).thenReturn(false);
        when(r1.await()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.isFailure()).thenReturn(false);

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isFailure, r1, r2);

        result.isFailure();

        verify(r2).cancel("Superseded by stepA");
        verify(r1, never()).cancel(anyString());
    }

    // --- anyMatch with isTimeout predicate ---

    @Test
    void anyMatch_TIMED_OUT_firstToTimeoutWins() {
        var r1 = WorkflowStepResults.timeout("stepA", Duration.ofSeconds(5));
        var r2 = WorkflowStepResults.completed("stepB", "ok");

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isTimeout, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isTimeout()).isTrue();
    }

    // --- anyMatch with isCanceled predicate ---

    @Test
    void anyMatch_CANCELLED_firstCancelledWins() {
        var r1 = WorkflowStepResults.cancelled("stepA");
        var r2 = WorkflowStepResults.completed("stepB", "ok");

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isCanceled, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.isCanceled()).isTrue();
    }

    // --- Step name format ---

    @Test
    void anyMatch_stepNameFormat() {
        var r1 = WorkflowStepResults.completed("stepA", null);
        var r2 = WorkflowStepResults.completed("stepB", null);

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isFailure, r1, r2);

        assertThat(result.getStepName()).isEqualTo("anyMatch(stepA, stepB)");
    }

    // --- Fallback does not cancel losers ---

    @Test
    void anyMatch_fallbackDoesNotCancelLosers() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        // Both completed but none failed → fallback path
        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(false);
        when(r1.isSuccess()).thenReturn(true);
        when(r1.await()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isFailure()).thenReturn(false);
        when(r2.isSuccess()).thenReturn(true);
        when(r2.await()).thenReturn(true);

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isFailure, r1, r2);

        // Trigger winner resolution
        result.isSuccess();

        // No cancellation in fallback — all already terminal
        verify(r1, never()).cancel(anyString());
        verify(r2, never()).cancel(anyString());
    }

    // --- Fallback uses event-sourced timestamp ordering ---

    @Test
    void anyMatch_fallbackUsesEventTimestampOrdering() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        // Both completed, none failed → fallback
        when(r1.isCompleted()).thenReturn(true);
        when(r1.isFailure()).thenReturn(false);
        when(r1.isSuccess()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.isFailure()).thenReturn(false);
        when(r2.isSuccess()).thenReturn(true);
        when(r2.await()).thenReturn(true);

        // Event-sourced ordering says stepB completed first
        when(workflowState.firstCompletedAmong(Set.of("stepA", "stepB")))
                .thenReturn(Optional.of("stepB"));

        var result = new AnyMatchCombinatorDelegate(workflowState).anyMatch(WorkflowStepResult::isFailure, r1, r2);

        // The fallback winner should be stepB (event-ordered)
        result.await();

        verify(r2).await();
    }
}
