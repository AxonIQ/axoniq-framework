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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.simulation.scenarios.BackoffCancelScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the <strong>cancellation-during-retry-backoff gap</strong> (candidate finding, documented in
 * {@code formal/POC-TLA-DST.adoc}): while a retrying step waits out its backoff, the only registered running future is
 * {@code RetryableExecuteDelegate.scheduleRetryAttempt}'s backoff-launch future, which has no cancellation-to-publish
 * wiring (its {@code .exceptionally} is chained on the upstream {@code runAsync} stage, so a
 * {@code completeExceptionally} on the registered future runs nothing) and whose scheduled launch task is never
 * unscheduled. Both cancellation surfaces are therefore broken in the backoff window, and this test asserts the gap
 * <em>as it exists today</em> (expected-gap style, like {@code F13DuplicateCancelRecordTest}): it passes while the gap
 * is present and flips when the engine gains proper backoff-window cancellation (at which point the doomed retry
 * attempt must no longer run and the assertions here invert).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class BackoffCancelSwallowedTest {

    /**
     * In-body {@code ctx.cancel(...)} while a step is in its backoff window: the cancel is held hostage by the retry
     * machinery — no CANCELLED lands until the backoff elapses, the "cancelled" step's retry attempt still runs its
     * side effect and completes, and only then does the workflow record CANCELLED.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void inBodyCancelDuringBackoff_isHeldHostage_andTheDoomedRetryStillRuns() {
        BackoffCancelScenario.InBodyOutcome outcome = BackoffCancelScenario.runInBodyCancel(0L, "A");

        assertThat(outcome.cancelledDuringBackoff())
                .as("the ctx.cancel must NOT take effect while the flaky step waits out its backoff "
                            + "(the documented gap: cancelAllRunningSteps blocks on a step nothing drives terminal)")
                .isFalse();
        assertThat(outcome.effectsBeforeAdvance())
                .as("before the backoff fires, only the first (failed) attempt has run")
                .isEqualTo(1);
        assertThat(outcome.effectsAfterAdvance())
                .as("the cancelled workflow's retry attempt still RAN its side effect after the cancel was requested")
                .isEqualTo(2);
        assertThat(outcome.flakyTerminalStatus())
                .as("the 'cancelled' step completes normally instead of recording CANCELLED")
                .isEqualTo(StepStatus.COMPLETED);
        assertThat(outcome.workflowCancelled())
                .as("the workflow does finally record CANCELLED once the step self-terminates")
                .isTrue();
    }

    /**
     * External {@code cancelRunningStep(...)} of a step in its backoff window: silently swallowed — no CANCELLED
     * record, the step stays durably RETRYING, and the retry runs to COMPLETED as if never cancelled (contrast with
     * {@link ExternalStepCancellationTest}, where the same surface works for a parked wait step).
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void externalCancelDuringBackoff_isSilentlySwallowed_andTheRetryRunsAnyway() {
        BackoffCancelScenario.ExternalOutcome outcome = BackoffCancelScenario.runExternalCancel(0L, "B");

        assertThat(outcome.stepCancelRecorded())
                .as("no CANCELLED record is ever committed for the externally cancelled retrying step")
                .isFalse();
        assertThat(outcome.effectsBeforeAdvance())
                .as("before the backoff fires, only the first (failed) attempt has run")
                .isEqualTo(1);
        assertThat(outcome.effectsAfterAdvance())
                .as("the externally cancelled step's retry attempt still RAN its side effect")
                .isEqualTo(2);
        assertThat(outcome.flakyTerminalStatus())
                .as("the externally cancelled step completes normally as if no cancel was requested")
                .isEqualTo(StepStatus.COMPLETED);
        assertThat(outcome.workflowCompleted())
                .as("the workflow reaches its normal terminal COMPLETED")
                .isTrue();
    }
}
