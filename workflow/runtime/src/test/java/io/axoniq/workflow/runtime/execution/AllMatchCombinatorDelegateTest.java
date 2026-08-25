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
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AllMatchCombinatorDelegate#allMatch(java.util.function.Predicate, WorkflowStepResult...)}.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class AllMatchCombinatorDelegateTest {

    private WorkflowState workflowState;
    private WorkflowExecution workflowExecution;

    @BeforeEach
    void setUp() {
        workflowState = mock(WorkflowState.class);
        workflowExecution = mock(WorkflowExecution.class);
        when(workflowExecution.state()).thenReturn(workflowState);
        when(workflowState.firstCompletedAmong(any())).thenReturn(Optional.empty());
        when(workflowState.sortedCompletedAmong(any())).thenReturn(List.of());
    }

    // --- All complete and all match → success ---

    @Test
    void allMatchAllCompleteAndAllMatchSuccess() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.success()).isTrue();
        assertThat(result.failure()).isFalse();
        assertThat(result.canceled()).isFalse();
        assertThat(result.timeout()).isFalse();
    }

    @Test
    void allMatchAllCompleteAndAllMatchResultIsEmpty() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.<Object>result()).isEmpty();
        assertThat(result.error()).isEmpty();
    }

    // --- Short-circuit on first non-match ---

    @Test
    void allMatchShortCircuitsOnFirstNonMatch() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok", TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.failure()).isTrue();
        assertThat(result.success()).isFalse();
    }

    @Test
    void allMatchShortCircuitDelegatesToViolator() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok", TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.failure()).isTrue();
        assertThat(result.error()).isPresent();
        assertThat(result.<Object>result()).isEmpty();
    }

    // --- isCompleted behavior ---

    @Test
    void allMatchIsCompletedFalseWhileRunningAndAllMatchSoFar() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.success()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.success()).thenReturn(false);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isFalse();
    }

    @Test
    void allMatchIsCompletedTrueOnShortCircuit() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.success()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.success()).thenReturn(false);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
    }

    @Test
    void allMatchIsCompletedTrueWhenAllCompleteAllMatch() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.success()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.success()).thenReturn(true);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
    }

    // --- cancel propagation ---

    @Test
    void allMatchCancelPropagatesToAll() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        result.cancel();

        verify(r1).cancel();
        verify(r2).cancel();
    }

    @Test
    void allMatchCancelWithReasonPropagatesToAll() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        result.cancel("workflow shutdown");

        verify(r1).cancel("workflow shutdown");
        verify(r2).cancel("workflow shutdown");
    }

    // --- Step name format ---

    @Test
    void allMatchStepNameFormat() {
        var r1 = WorkflowStepResults.completed("stepA", null, TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", null, TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.getStepName()).isEqualTo("allMatch(stepA, stepB)");
    }

    // --- Event ordering determines violator ---

    @Test
    void allMatchEventOrderDeterminesViolator() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.success()).thenReturn(false);
        when(r1.failure()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.success()).thenReturn(false);
        when(r2.failure()).thenReturn(true);

        // Event-sourced state says stepB failed first
        when(workflowState.firstCompletedAmong(Set.of("stepA", "stepB")))
                .thenReturn(Optional.of("stepB"));

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        // Trigger resolution — violator should be stepB despite array order
        assertThat(result.failure()).isTrue();
    }

    // --- matched() / unmatched() ---

    @Test
    void allMatchMatchedReturnsSuccessfulSteps() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactlyInAnyOrder("stepA", "stepB");
        assertThat(result.unmatched()).isEmpty();
    }

    @Test
    void allMatchUnmatchedReturnsViolatorOnShortCircuit() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok", TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.unmatched()).extracting(WorkflowStepResult::getStepName)
                                      .containsExactly("stepA");
        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactly("stepB");
    }

    @Test
    void allMatchUnmatchedEmptyWhenAllMatch() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.unmatched()).isEmpty();
    }

    @Test
    void allMatchUnmatchedIncludesNonCompletedResults() {
        var r1 = WorkflowStepResults.failed("failingStep", new RuntimeException("boom"));
        var r2 = mock(WorkflowStepResult.class);
        when(r2.getStepName()).thenReturn("slowStep");
        when(r2.isCompleted()).thenReturn(false);
        when(r2.success()).thenReturn(false);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        // No step succeeded → matched is empty
        assertThat(result.matched()).isEmpty();
        // Both the violator and the still-running step are in unmatched
        assertThat(result.unmatched()).extracting(WorkflowStepResult::getStepName)
                                      .containsExactly("failingStep", "slowStep");
    }

    @Test
    void allMatchUnmatchedIncludesViolatorAndRunningStep() {
        var r1 = WorkflowStepResults.completed("successStep", "ok", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.failed("failingStep", new RuntimeException("boom"));
        var r3 = mock(WorkflowStepResult.class);
        when(r3.getStepName()).thenReturn("slowStep");
        when(r3.isCompleted()).thenReturn(false);
        when(r3.success()).thenReturn(false);

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success,
                                                                                r1,
                                                                                r2,
                                                                                r3);

        // successStep completed and matched success → matched
        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactly("successStep");
        // failingStep (violator) and slowStep (still running) → unmatched
        assertThat(result.unmatched()).extracting(WorkflowStepResult::getStepName)
                                      .containsExactly("failingStep", "slowStep");
    }

    @Test
    void allMatchCategoriesSortedByEventSourcedTimestamp() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        // Event-sourced order: stepB before stepA
        when(workflowState.sortedCompletedAmong(Set.of("stepA", "stepB")))
                .thenReturn(List.of("stepB", "stepA"));

        var result = new AllMatchCombinatorDelegate(workflowExecution).allMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactly("stepB", "stepA");
    }
}
