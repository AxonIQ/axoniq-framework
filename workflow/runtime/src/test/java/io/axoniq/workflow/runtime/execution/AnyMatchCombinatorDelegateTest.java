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
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link AnyMatchCombinatorDelegate#anyMatch(java.util.function.Predicate, WorkflowStepResult...)}.
 *
 * @author Stefan Dragisic
 */
class AnyMatchCombinatorDelegateTest {

    private WorkflowState workflowState;
    private WorkflowExecution workflowExecution;

    @BeforeEach
    void setUp() {
        workflowState = mock(WorkflowState.class);
        workflowExecution = mock(WorkflowExecution.class);
        when(workflowExecution.state()).thenReturn(workflowState);
    }

    private void givenTerminalStep(String name, Instant timestamp) {
        when(workflowState.getStep(name)).thenReturn(WorkflowStep.completed(name, null, timestamp, null));
    }

    // --- getStepName ---

    @Test
    void stepNameCombinesAllResultNames() {
        var r1 = WorkflowStepResults.completed("stepA", null, TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", null, TestEventConverter.INSTANCE);
        var r3 = WorkflowStepResults.completed("stepC", null, TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success,
                                                                                r1,
                                                                                r2,
                                                                                r3);

        assertThat(result.getStepName()).isEqualTo("anyMatch(stepA, stepB, stepC)");
    }

    // --- First success wins ---

    @Test
    void firstSuccessBecomesWinner() {
        var r1 = WorkflowStepResults.completed("stepA", "payload-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "payload-B", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.success()).isTrue();
        assertThat(result.<String>resultAs(String.class).get()).isEqualTo("payload-A");
    }

    @Test
    void singleSuccessBecomesWinnerImmediately() {
        var r1 = WorkflowStepResults.completed("onlyStep", "only-value", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.success()).isTrue();
        assertThat(result.<String>resultAs(String.class).get()).isEqualTo("only-value");
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
        when(r1.success()).thenReturn(false);
        when(r1.failure()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.success()).thenReturn(false);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

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
        when(r1.success()).thenReturn(false);
        when(r1.failure()).thenReturn(true);

        when(r2.isCompleted()).thenReturn(true);
        when(r2.success()).thenReturn(true);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.success()).isTrue();
    }

    // --- All failed → composite failure ---

    @Test
    void allFailedReturnsCompositeFailure() {
        var error1 = new RuntimeException("boom1");
        var error2 = new RuntimeException("boom2");
        var r1 = WorkflowStepResults.failed("stepA", error1);
        var r2 = WorkflowStepResults.failed("stepB", error2);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.failure()).isTrue();
        assertThat(result.success()).isFalse();
        assertThat(result.error()).isPresent();
    }

    // --- cancel propagates to all results ---

    @Test
    void cancelCancelsAllResults() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

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

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

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
        when(r1.success()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.success()).thenReturn(false);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isFalse();
    }

    @Test
    void isCompletedReturnsTrueWhenOneSuccessful() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.isCompleted()).thenReturn(true);
        when(r1.success()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.success()).thenReturn(false);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.isCompleted()).isTrue();
    }

    @Test
    void isCompletedReturnsTrueWhenAllFailedEvenWithNoSuccess() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.isCompleted()).thenReturn(true);
        when(r1.success()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.success()).thenReturn(false);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

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
        when(r1.success()).thenReturn(false, true);

        doAnswer(invocation -> null).when(workflowExecution).awaitStateChange(any());

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1);

        assertThat(result.success()).isTrue();
        verify(workflowExecution).awaitStateChange(any());
    }

    // --- Event-sourcing replay: winner determined by event order, not array order ---

    @Test
    void winnerIsDeterminedByEventOrderNotArrayOrder() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        when(r1.isCompleted()).thenReturn(true);
        when(r1.success()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.success()).thenReturn(true);

        // Event-sourced state says stepB succeeded first.
        givenTerminalStep("stepA", Instant.ofEpochMilli(2));
        givenTerminalStep("stepB", Instant.ofEpochMilli(1));

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        // stepB should be the winner despite being second in array order
        result.await();

        verify(r2).await();
    }

    // --- Winner caching ---

    @Test
    void winnerIsCachedAcrossMultipleCalls() {
        var r1 = WorkflowStepResults.completed("stepA", "payload-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "payload-B", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.resultAs(String.class).get()).isEqualTo("payload-A");
        assertThat(result.resultAs(String.class).get()).isEqualTo("payload-A");
        assertThat(result.success()).isTrue();
    }

    // --- matched() / unmatched() with success predicate ---

    @Test
    void anyMatch_matched_returnsWinners() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.failed("stepB", new RuntimeException("boom"));

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactly("stepA");
        assertThat(result.unmatched()).extracting(WorkflowStepResult::getStepName)
                                      .containsExactly("stepB");
    }

    @Test
    void anyMatch_unmatched_returnsLosers() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactlyInAnyOrder("stepA", "stepB");
        assertThat(result.unmatched()).isEmpty();
    }

    @Test
    void anyMatch_unmatched_includesNonCompletedResults() {
        var r1 = WorkflowStepResults.completed("fastStep", "ok", TestEventConverter.INSTANCE);
        var r2 = mock(WorkflowStepResult.class);
        when(r2.getStepName()).thenReturn("slowStep");
        when(r2.isCompleted()).thenReturn(false);
        when(r2.success()).thenReturn(false);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactly("fastStep");
        assertThat(result.unmatched()).extracting(WorkflowStepResult::getStepName)
                                      .containsExactly("slowStep");
    }

    @Test
    void anyMatch_matched_sortedByTimestamp() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        // Event-sourced order: stepB before stepA.
        givenTerminalStep("stepA", Instant.ofEpochMilli(2));
        givenTerminalStep("stepB", Instant.ofEpochMilli(1));

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::success, r1, r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactly("stepB", "stepA");
    }

    // --- anyMatch with failure predicate ---

    @Test
    void anyMatch_FAILED_firstToFailWins() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::failure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.failure()).isTrue();
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
        when(r1.failure()).thenReturn(false);
        when(r1.success()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(false);
        when(r2.failure()).thenReturn(false);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::failure, r1, r2);

        assertThat(result.isCompleted()).isFalse();
    }

    @Test
    void anyMatch_FAILED_allSucceedFallsBackToFirstCompleted() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::failure, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        // Fallback: first completed wins, which is a success
        assertThat(result.success()).isTrue();
        assertThat(result.resultAs(String.class).get()).isEqualTo("ok-A");
    }

    // --- anyMatch with timeout predicate ---

    @Test
    void anyMatch_TIMED_OUT_firstToTimeoutWins() {
        var r1 = WorkflowStepResults.timeout("stepA", Duration.ofSeconds(5));
        var r2 = WorkflowStepResults.completed("stepB", "ok", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::timeout, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.timeout()).isTrue();
    }

    // --- anyMatch with canceled predicate ---

    @Test
    void anyMatch_canceled_firstcanceledWins() {
        var r1 = WorkflowStepResults.canceled("stepA");
        var r2 = WorkflowStepResults.completed("stepB", "ok", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::canceled, r1, r2);

        assertThat(result.isCompleted()).isTrue();
        assertThat(result.canceled()).isTrue();
    }

    // --- Step name format with non-success predicate ---

    @Test
    void anyMatch_stepNameFormat() {
        var r1 = WorkflowStepResults.completed("stepA", null, TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", null, TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::failure, r1, r2);

        assertThat(result.getStepName()).isEqualTo("anyMatch(stepA, stepB)");
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
        when(r1.failure()).thenReturn(false);
        when(r1.success()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.failure()).thenReturn(false);
        when(r2.success()).thenReturn(true);

        // Event-sourced ordering says stepB completed first.
        givenTerminalStep("stepA", Instant.ofEpochMilli(2));
        givenTerminalStep("stepB", Instant.ofEpochMilli(1));

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::failure, r1, r2);

        // The fallback winner should be stepB (event-ordered)
        result.await();

        verify(r2).await();
    }

    // --- matched() / unmatched() with failure predicate ---

    @Test
    void anyMatch_matched_withFailurePredicate() {
        var r1 = WorkflowStepResults.failed("stepA", new RuntimeException("boom"));
        var r2 = WorkflowStepResults.completed("stepB", "ok", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::failure, r1, r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactly("stepA");
    }
}
