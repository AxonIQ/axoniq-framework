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
 * Tests for {@link AnyMatchCombinatorDelegate#anyMatch(java.util.function.Predicate, WorkflowStepResult...)} with the
 * {@link WorkflowStepResult#isCompleted()} predicate (terminal semantics, formerly {@code race()}).
 *
 * @author Stefan Dragisic
 */
class AnyMatchCombinatorDelegateCompletesTest {

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

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted,
                                                                              r1,
                                                                              r2,
                                                                              r3);

        assertThat(race.getStepName()).isEqualTo("anyMatch(stepA, stepB, stepC)");
    }

    // --- First completed result becomes the winner ---

    @Test
    void firstCompletedResultBecomesWinner() {
        var r1 = WorkflowStepResults.completed("stepA", "payload-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "payload-B", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        assertThat(race.isCompleted()).isTrue();
        assertThat(race.success()).isTrue();
        assertThat(race.<String>resultAs(String.class).get()).isEqualTo("payload-A");
    }

    @Test
    void singleResultBecomesWinnerImmediately() {
        var r1 = WorkflowStepResults.completed("onlyStep", "only-value", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1);

        assertThat(race.isCompleted()).isTrue();
        assertThat(race.success()).isTrue();
        assertThat(race.<String>resultAs(String.class).get()).isEqualTo("only-value");
        assertThat(race.getStepName()).isEqualTo("anyMatch(onlyStep)");
    }

    // --- Delegates state queries to winner ---

    @Test
    void delegatesSuccessToWinner() {
        var r1 = WorkflowStepResults.completed("winner", "value", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("loser", "other", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        assertThat(race.success()).isTrue();
        assertThat(race.failure()).isFalse();
        assertThat(race.canceled()).isFalse();
        assertThat(race.timeout()).isFalse();
        assertThat(race.resultAs(String.class).get()).isEqualTo("value");
        assertThat(race.error()).isEmpty();
    }

    @Test
    void delegatesFailureToWinner() {
        var failed = WorkflowStepResults.failed("failStep", new RuntimeException("boom"));
        var success = WorkflowStepResults.completed("successStep", "value", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted,
                                                                              failed,
                                                                              success);

        assertThat(race.success()).isFalse();
        assertThat(race.failure()).isTrue();
        assertThat(race.error()).isPresent();
    }

    @Test
    void delegatesCancelledToWinner() {
        var cancelled = WorkflowStepResults.canceled("cancelStep");
        var success = WorkflowStepResults.completed("successStep", "value", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted,
                                                                              cancelled,
                                                                              success);

        assertThat(race.success()).isFalse();
        assertThat(race.canceled()).isTrue();
    }

    @Test
    void delegatesTimeoutToWinner() {
        var timedOut = WorkflowStepResults.timeout("timeoutStep", Duration.ofSeconds(5));
        var success = WorkflowStepResults.completed("successStep", "value", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted,
                                                                              timedOut,
                                                                              success);

        assertThat(race.success()).isFalse();
        assertThat(race.timeout()).isTrue();
    }

    // --- await delegates to winner ---

    @Test
    void awaitDelegatesToWinner() {
        var r1 = WorkflowStepResults.completed("step", "value", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1);

        assertThat(race.isCompleted()).isTrue();
    }

    // --- cancel propagates to all results ---

    @Test
    void cancelCancelsAllResults() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        race.cancel();

        verify(r1).cancel();
        verify(r2).cancel();
    }

    @Test
    void cancelWithReasonCancelsAllResults() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        race.cancel("workflow shutdown");

        verify(r1).cancel("workflow shutdown");
        verify(r2).cancel("workflow shutdown");
    }

    // --- isCompleted is non-blocking ---

    @Test
    void isCompletedReturnsFalseWhenNoneCompleted() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.isCompleted()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(false);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        assertThat(race.isCompleted()).isFalse();
    }

    @Test
    void isCompletedReturnsTrueWhenOneCompleted() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);
        when(r1.isCompleted()).thenReturn(false);
        when(r2.isCompleted()).thenReturn(true);
        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        assertThat(race.isCompleted()).isTrue();
    }

    // --- awaitStateChange-based winner resolution ---

    @Test
    void resolveWinnerBlocksUntilResultCompletes() throws InterruptedException {
        var r1 = mock(WorkflowStepResult.class);
        when(r1.getStepName()).thenReturn("stepA");
        // findFirstMatching calls isCompleted() twice (guard + predicate) in the first loop,
        // then allMatch calls it once more. After awaitStateChange, the next calls return true.
        when(r1.isCompleted()).thenReturn(false, false, false, true);
        when(r1.success()).thenReturn(true);

        doAnswer(invocation -> null).when(workflowExecution).awaitStateChange(any());

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1);

        assertThat(race.success()).isTrue();
        verify(workflowExecution).awaitStateChange(any());
    }

    // --- Event-sourcing replay: winner determined by event order, not array order ---

    @Test
    void winnerIsDeterminedByEventOrderNotArrayOrder() {
        var r1 = mock(WorkflowStepResult.class);
        var r2 = mock(WorkflowStepResult.class);

        when(r1.getStepName()).thenReturn("stepA");
        when(r2.getStepName()).thenReturn("stepB");

        // Both completed (race condition: B completed first)
        when(r1.isCompleted()).thenReturn(true);
        when(r2.isCompleted()).thenReturn(true);
        when(r2.success()).thenReturn(true);

        // Event-sourced state says stepB completed first.
        givenTerminalStep("stepA", Instant.ofEpochMilli(2));
        givenTerminalStep("stepB", Instant.ofEpochMilli(1));

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        // stepB should be the winner despite being second in array order
        race.await();

        verify(r2).await();
    }

    @Test
    void winnerIsCachedAcrossMultipleCalls() {
        var r1 = WorkflowStepResults.completed("stepA", "payload-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "payload-B", TestEventConverter.INSTANCE);

        var race = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted, r1, r2);

        // Call multiple times — same winner each time
        assertThat(race.resultAs(String.class).get()).isEqualTo("payload-A");
        assertThat(race.resultAs(String.class).get()).isEqualTo("payload-A");
        assertThat(race.success()).isTrue();
    }

    // --- matched() / unmatched() ---

    @Test
    void anyMatch_terminal_allCompletedGoToMatched() {
        var r1 = WorkflowStepResults.completed("stepA", "ok-A", TestEventConverter.INSTANCE);
        var r2 = WorkflowStepResults.completed("stepB", "ok-B", TestEventConverter.INSTANCE);

        var result = new AnyMatchCombinatorDelegate(workflowExecution).anyMatch(WorkflowStepResult::isCompleted,
                                                                                r1,
                                                                                r2);

        assertThat(result.matched()).extracting(WorkflowStepResult::getStepName)
                                    .containsExactlyInAnyOrder("stepA", "stepB");
        assertThat(result.unmatched()).isEmpty();
    }
}
