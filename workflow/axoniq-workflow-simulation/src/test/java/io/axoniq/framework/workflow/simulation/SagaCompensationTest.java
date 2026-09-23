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
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.simulation.scenarios.SagaCompensationScenario;
import io.axoniq.framework.workflow.simulation.workflow.SagaOrderWorkflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase-1 production-realism pins: the {@link SagaOrderWorkflow} compensation saga driven through the windows a real
 * production deployment meets (see {@link SagaCompensationScenario} for each run's mechanism). The healthy paths
 * assert the engine honours the saga contract; the crash-mid-compensation pair contrasts the fragile (no-retry) and
 * recommended (retrying) compensation authoring under the SAME crash window. An uncaught step failure or step timeout
 * that escapes the catch block ends the saga FAILED with exactly one terminal record; the duplicate-FAILED run pins
 * the F-13-class FAIL-path duplicate terminal record.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class SagaCompensationTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void happyPath_completesWithoutCompensation() {
        var outcome = SagaCompensationScenario.happyPath(0L, "H1");

        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.reserveCount()).as("reserveStock effect ran exactly once").isEqualTo(1);
        assertThat(outcome.chargeCount()).as("chargePayment effect ran exactly once").isEqualTo(1);
        assertThat(outcome.notifyCount()).as("notifyCustomer effect ran exactly once").isEqualTo(1);
        assertThat(outcome.releaseCount()).as("no compensation on the happy path").isEqualTo(0);
        assertThat(outcome.refundCount()).as("no compensation on the happy path").isEqualTo(0);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void chargeDeclined_runsFailureCompensationThenFails() {
        var outcome = SagaCompensationScenario.chargeDeclined(0L, "D1");

        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.FAILED);
        // The charge action ran once per attempt: 1 + CHARGE_MAX_RETRIES.
        assertThat(outcome.chargeCount())
                .as("charge attempts = maxRetries + 1")
                .isEqualTo(SagaOrderWorkflow.CHARGE_MAX_RETRIES + 1);
        assertThat(outcome.releaseCount()).as("failure branch releases the stock").isEqualTo(1);
        assertThat(outcome.refundCount()).as("failure branch never charged, so never refunds").isEqualTo(0);
        assertThat(outcome.notifyCount()).as("forward path never resumed after the failure").isEqualTo(0);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void fulfillmentTimeout_runsFullCompensationThenCancels() {
        var outcome = SagaCompensationScenario.fulfillmentTimeout(0L, "T1");

        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.CANCELLED);
        assertThat(outcome.chargeCount()).as("charge succeeded before the timeout").isEqualTo(1);
        assertThat(outcome.releaseCount()).as("timeout branch releases the stock").isEqualTo(1);
        assertThat(outcome.refundCount()).as("timeout branch refunds the charge").isEqualTo(1);
        assertThat(outcome.notifyCount()).as("forward path never resumed after the timeout").isEqualTo(0);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void crashMidCompensation_noRetry_failsHalfCompensatedWithOneTerminalRecord() {
        var outcome = SagaCompensationScenario.crashMidCompensation(0L, "C1", false);

        // The engine's at-most-once resolution turned the crash-interrupted releaseStock into a FAILED
        // (StepIndeterminateException) without re-running its action — correct in isolation.
        assertThat(outcome.releaseEffectCount())
                .as("the interrupted compensation attempt is NOT re-run (at-most-once holds)")
                .isEqualTo(1);
        assertThat(outcome.releaseTerminalStatus())
                .as("the interrupted attempt resolves through the regular error flow to FAILED")
                .isEqualTo(StepStatus.FAILED);

        // The StepFailedException escapes the catch block uncaught, so the saga ends FAILED instead of staying
        // non-terminal. The refund and the intended ctx.cancel() are never reached.
        assertThat(outcome.terminalStatus())
                .as("an uncaught step failure after a crash ends the saga FAILED")
                .isEqualTo(WorkflowStatus.FAILED);
        assertThat(outcome.refundEffectCount()).as("the refund compensation was never reached").isEqualTo(0);
        assertThat(outcome.cancelledRecords()).as("the intended ctx.cancel() was never reached").isEqualTo(0);
        // Nothing after the terminal: a further restart neither re-drives the instance nor publishes again.
        assertThat(outcome.workflowTerminalRecords()).as("exactly one workflow terminal record").isEqualTo(1);
        assertThat(outcome.liveAtEnd()).as("the failed saga is not re-driven after a restart").isFalse();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void crashMidCompensation_withRetryPolicy_recoversAndCancelsCleanly() {
        var outcome = SagaCompensationScenario.crashMidCompensation(0L, "C2", true);

        // The recommended authoring under the SAME crash window: the interrupted attempt resolves to RETRYING + a
        // fresh attempt (per-attempt at-most-once: the action legitimately runs again), compensation completes, and
        // the saga terminates CANCELLED as designed.
        assertThat(outcome.terminalStatus())
                .as("retrying compensation completes and the saga cancels cleanly")
                .isEqualTo(WorkflowStatus.CANCELLED);
        assertThat(outcome.releaseTerminalStatus()).isEqualTo(StepStatus.COMPLETED);
        assertThat(outcome.releaseEffectCount())
                .as("the crashed attempt + the fresh retry attempt")
                .isEqualTo(2);
        assertThat(outcome.refundEffectCount()).as("the refund compensation completed").isEqualTo(1);
        assertThat(outcome.cancelledRecords()).isEqualTo(1);
        assertThat(outcome.workflowTerminalRecords()).isEqualTo(1);
        assertThat(outcome.liveAtEnd()).isFalse();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void confirmationVsTimeout_eventFirst_isConsistent() {
        var outcome = SagaCompensationScenario.confirmationVsTimeoutRace(0L, "R1", true);

        assertRaceConsistency(outcome);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void confirmationVsTimeout_timeoutFirst_isConsistent() {
        var outcome = SagaCompensationScenario.confirmationVsTimeoutRace(0L, "R2", false);

        assertRaceConsistency(outcome);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void failPath_recordsOneWorkflowTerminalRecordAcrossReDrive_F13ClassClosed() {
        var outcome = SagaCompensationScenario.duplicateFailedTerminalRecord(0L, "F1");

        // A crash/replay alone appends nothing after terminal — exactly one <workflow>:FAILED.
        assertThat(outcome.failedRecordsBeforeReDrive())
                .as("after the fail + a crash/replay alone, exactly one <workflow>:FAILED")
                .isEqualTo(1);

        // F-13-class closed: every workflow-terminal publish is a conditioned append. The re-drive (the F-3
        // start-event redelivery) is fenced at its ORIGIN-anchored first append, so no second <workflow>:FAILED lands.
        assertThat(outcome.failedRecordsAfterReDrive())
                .as("F-13-class closed: the re-driven fail path's second <workflow>:FAILED is rejected by the "
                            + "append condition")
                .isEqualTo(1);

        // The fenced re-spawn never gets past its first append, so the compensation side effect stays at one run.
        assertThat(outcome.releaseEffectCount())
                .as("the re-spawn is fenced at its first append, so the compensation effect runs once")
                .isEqualTo(1);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void doomedCompensation_actionRunsButResultDiscardedAsTimedOut_sagaFails() {
        var outcome = SagaCompensationScenario.doomedCompensationAfterClockJump(0L, "DT1");

        // EXPECTED GAP (doomed attempt): ExecuteDelegate computes the per-attempt deadline at entry but dispatches the
        // action BEFORE checking it (ExecuteDelegate.java:154-181); on the past-deadline path the action's result
        // future has NO completion handler. The side effect runs...
        assertThat(outcome.releaseEffectCount())
                .as("the doomed attempt's action RUNS even though its deadline had already elapsed at entry")
                .isEqualTo(1);
        // ...but is never recorded COMPLETED — the step records TIMED_OUT and the executed result is discarded.
        assertThat(outcome.releaseCompletedRecords())
                .as("the executed action's result is discarded — no COMPLETED record")
                .isEqualTo(0);
        assertThat(outcome.releaseTerminalStatus()).isEqualTo(StepStatus.TIMED_OUT);

        // The StepTimedOutException escapes the saga's catch block uncaught, so the saga ends FAILED instead of
        // staying non-terminal. A further restart neither re-drives the instance nor publishes again.
        assertThat(outcome.workflowTerminalStatus())
                .as("an uncaught step timeout ends the saga FAILED")
                .isEqualTo(WorkflowStatus.FAILED);
        assertThat(outcome.workflowTerminalRecords()).as("exactly one workflow terminal record").isEqualTo(1);
        assertThat(outcome.liveAtEnd()).as("the failed saga is not re-driven after a restart").isFalse();
        assertThat(outcome.releaseEffectCount()).as("the doomed action does not run again").isEqualTo(1);
    }

    private static void assertRaceConsistency(SagaCompensationScenario.RaceOutcome outcome) {
        assertThat(outcome.waitTerminalRecords())
                .as("exactly one terminal record for the racing wait step (no COMPLETED+TIMED_OUT double-terminal)")
                .isEqualTo(1);
        assertThat(outcome.workflowTerminalRecords())
                .as("exactly one workflow terminal record after the race")
                .isEqualTo(1);

        if (outcome.waitTerminalStatus() == StepStatus.COMPLETED) {
            assertThat(outcome.workflowTerminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(outcome.notifyCount()).as("confirmation won: forward path finished").isEqualTo(1);
            assertThat(outcome.releaseCount()).as("confirmation won: no compensation").isEqualTo(0);
            assertThat(outcome.refundCount()).isEqualTo(0);
        } else {
            assertThat(outcome.waitTerminalStatus()).isEqualTo(StepStatus.TIMED_OUT);
            assertThat(outcome.workflowTerminalStatus()).isEqualTo(WorkflowStatus.CANCELLED);
            assertThat(outcome.notifyCount()).as("timeout won: forward path never resumed").isEqualTo(0);
            assertThat(outcome.releaseCount()).as("timeout won: full compensation").isEqualTo(1);
            assertThat(outcome.refundCount()).isEqualTo(1);
        }
    }
}
