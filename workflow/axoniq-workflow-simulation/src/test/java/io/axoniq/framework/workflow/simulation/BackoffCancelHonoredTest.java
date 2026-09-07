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
 * Pins finding F-23 as fixed: cancelling a step parked in its retry backoff window is honored on both surfaces. The
 * backoff phase is registered as a parked step, so an in-body {@code ctx.cancel(...)} records the workflow CANCELLED at
 * once and an external {@code cancelRunningStep(...)} records the step CANCELLED; in both cases the scheduled retry
 * attempt is dropped and never runs its side effect.
 * <p>
 * One documented gap remains and is asserted here: a workflow that goes terminal while a step is retrying leaves that
 * step without a terminal step record (the terminal transition drops the queued step CANCELLED task), so the flaky step
 * stays durably RETRYING.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class BackoffCancelHonoredTest {

    /**
     * In-body {@code ctx.cancel(...)} while a step is in its backoff window: CANCELLED lands at once, the retry attempt
     * never runs, and the retrying step keeps no terminal record of its own.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void inBodyCancelDuringBackoff_cancelsAtOnce_andTheRetryNeverRuns() {
        BackoffCancelScenario.InBodyOutcome outcome = BackoffCancelScenario.runInBodyCancel(0L, "A");
        assertThat(outcome.cancelledDuringBackoff())
                .as("ctx.cancel takes effect while the flaky step waits out its backoff")
                .isTrue();
        assertThat(outcome.effectsBeforeAdvance())
                .as("before the backoff fires, only the first (failed) attempt has run")
                .isEqualTo(1);
        assertThat(outcome.effectsAfterAdvance())
                .as("the cancelled retry attempt never runs its side effect")
                .isEqualTo(1);
        assertThat(outcome.flakyTerminalStatus())
                .as("documented gap: the terminal workflow leaves its retrying step without a terminal step record")
                .isNull();
        assertThat(outcome.workflowCancelled())
                .as("the workflow records CANCELLED")
                .isTrue();
    }

    /**
     * External {@code cancelRunningStep(...)} of a step in its backoff window: the step records CANCELLED, the retry is
     * unscheduled, and the workflow completes normally (the body never awaits the cancelled step).
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void externalCancelDuringBackoff_recordsCancelled_andTheRetryNeverRuns() {
        BackoffCancelScenario.ExternalOutcome outcome = BackoffCancelScenario.runExternalCancel(0L, "B");
        assertThat(outcome.stepCancelRecorded())
                .as("a CANCELLED record is committed for the externally cancelled retrying step")
                .isTrue();
        assertThat(outcome.effectsBeforeAdvance())
                .as("before the backoff fires, only the first (failed) attempt has run")
                .isEqualTo(1);
        assertThat(outcome.effectsAfterAdvance())
                .as("the cancelled retry attempt never runs its side effect")
                .isEqualTo(1);
        assertThat(outcome.flakyTerminalStatus())
                .as("the externally cancelled step's terminal record is CANCELLED")
                .isEqualTo(StepStatus.CANCELLED);
        assertThat(outcome.workflowCompleted())
                .as("the workflow reaches its normal terminal COMPLETED")
                .isTrue();
    }
}
