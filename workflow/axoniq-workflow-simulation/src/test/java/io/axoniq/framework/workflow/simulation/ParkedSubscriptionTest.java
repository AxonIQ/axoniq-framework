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

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.simulation.scenarios.ParkedSubscriptionScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase-2 production-realism pins: a day-scale parked {@code SubscriptionRenewalWorkflow} under crash/recovery,
 * churn, duplicate signals, start storms and window elapse (see {@link ParkedSubscriptionScenario} for each run's
 * mechanism). The headline expected-gap pin is the <strong>lost wake</strong>: a crash between a wait's live match
 * and its COMPLETED commit permanently loses the wake — the durably-committed signal is re-delivered on recovery in
 * REPLAY mode, which never evaluates wait conditions, and the re-registered wait never sees it; only a brand-new
 * delivery (producer retry) or the wait's own timeout moves the instance again.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class ParkedSubscriptionTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void lostWake_crashBetweenMatchAndCommit_recoveryNeverRedeliversTheWake_asExpectedGap() {
        var outcome = ParkedSubscriptionScenario.lostWakeOnCrashBetweenMatchAndCommit(0L, "L1");

        // The producer's signal IS durable — only the wait step's COMPLETED vanished in the crash window.
        assertThat(outcome.signalCommittedBeforeCrash())
                .as("the matching RenewalDecidedEvent was durably committed before the crash")
                .isTrue();

        // EXPECTED GAP (lost wake): recovery re-delivers the committed signal in REPLAY mode (state evolve only —
        // wait conditions are evaluated exclusively in live mode), and the wait re-registers only when the body
        // re-runs at live-switch. Nothing ever re-matches the signal: the instance stays parked.
        assertThat(outcome.wokeAfterRecoveryAlone())
                .as("EXPECTED GAP: recovery alone never re-delivers the wake — the committed signal is consumed in "
                            + "replay mode and the re-registered wait never sees it")
                .isFalse();
        assertThat(outcome.renewalEffectsAfterRecovery())
                .as("the renewal was never processed after recovery alone")
                .isEqualTo(0);

        // The production mitigation: a brand-new delivery (producer retry) is evaluated LIVE and wakes the instance.
        assertThat(outcome.completedAfterRedelivery())
                .as("a fresh redelivery of the same signal rescues the instance")
                .isTrue();
        assertThat(outcome.waitCompletedRecords())
                .as("exactly one COMPLETED for the wait step (the redelivery wake; the vanished one never committed)")
                .isEqualTo(1);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void parkedInstance_survivesChurnRestartsAndDuplicateSignals_wakesExactlyOnce() {
        var outcome = ParkedSubscriptionScenario.parkedSurvivesChurnRestartsAndDuplicateSignals(0L, "P1");

        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.waitCompletedRecords())
                .as("the duplicated decision wakes the parked instance exactly once")
                .isEqualTo(1);
        assertThat(outcome.renewalEffects()).as("the renewal processed exactly once").isEqualTo(1);
        assertThat(outcome.registerEffects()).as("registration ran exactly once across all recoveries").isEqualTo(1);
        assertThat(outcome.expireEffects()).as("the expiry path never ran").isEqualTo(0);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void waitTimeout_firesExactlyOnceAcrossRestarts_drivingTheExpiryPath() {
        var outcome = ParkedSubscriptionScenario.timeoutFiresOnceAcrossRestarts(0L, "T1");

        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.CANCELLED);
        assertThat(outcome.waitTimedOutRecords())
                .as("the re-registered wait's timeout fires exactly once across two recoveries")
                .isEqualTo(1);
        assertThat(outcome.waitCompletedRecords()).isEqualTo(0);
        assertThat(outcome.expireEffects()).as("the expiry compensation ran exactly once").isEqualTo(1);
        assertThat(outcome.renewalEffects()).isEqualTo(0);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void duplicateStartStorm_onLiveParkedId_neverRespawnsOrReruns() {
        var outcome = ParkedSubscriptionScenario.duplicateStartStormOnLiveParkedId(0L, "S1");

        assertThat(outcome.startedRecords())
                .as("five redelivered start events for a LIVE parked id never spawn a second instance (INV-10)")
                .isEqualTo(1);
        assertThat(outcome.registerEffects()).as("registration never re-ran under the storm").isEqualTo(1);
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.waitCompletedRecords()).isEqualTo(1);
    }
}
