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
import io.axoniq.framework.workflow.simulation.scenarios.RecoveryAfterNonTerminalExitScenario;
import io.axoniq.framework.workflow.simulation.scenarios.RecoveryAfterNonTerminalExitScenario.Outcome;
import io.axoniq.framework.workflow.simulation.workflow.OrderWorkflow;
import org.junit.jupiter.api.*;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Restarts the engine after a workflow body stopped without a terminal status, and checks the instance stays
 * registered while paused and is re-driven to its terminal status after the restart, with one terminal record and no
 * step side effect run twice. See {@link RecoveryAfterNonTerminalExitScenario} for each arm's mechanism and evidence.
 *
 * @author Stefan Dragisic
 */
class RecoveryAfterNonTerminalExitTest {

    @Nested
    class GracefulShutdown {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void parkedInstanceIsRestoredAfterRestartAndALateWakeCompletesIt() {
            // given / when
            Outcome outcome = RecoveryAfterNonTerminalExitScenario.gracefulShutdown(0L);

            // then
                assertThat(outcome.faultLanded())
                    .as("the shutdown interrupted the parked driver, or this run proves nothing")
                    .isTrue();
            assertRecoveredAndCompleted(outcome);
        }
    }

    @Nested
    class ReplayDriftPause {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void pausedInstanceStaysRegisteredAndCompletesAfterRestartUnderTheMatchingBody() {
            // given / when
            Outcome outcome = RecoveryAfterNonTerminalExitScenario.driftPause(0L, false);

            // then
            assertThat(outcome.faultLanded()).as("the divergent body paused on replay drift").isTrue();
            assertThat(outcome.liveAfterPause()).as("the drift pause keeps the instance registered").isTrue();
            assertRecoveredAndCompleted(outcome);
        }

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void wakeDeliveredDuringThePauseIsNotReEvaluatedAfterRestart_asExpectedGap() {
            // given / when
            Outcome outcome = RecoveryAfterNonTerminalExitScenario.driftPause(0L, true);

            // then
            assertThat(outcome.faultLanded()).as("the divergent body paused on replay drift").isTrue();
            assertThat(outcome.tokenBeforeRestart())
                    .as("the checkpoint passed the wake while the instance was paused")
                    .isGreaterThan(outcome.instanceHeadIndex());
            assertThat(outcome.restoredAfterRestart()).as("the restart restores the paused instance").isTrue();
            // EXPECTED GAP (F-16 family): the wake reached the engine while no driver ran for the instance. The paused
            // execution only evolves its own events, the checkpoint moves past the wake, and the restored wait matches
            // live deliveries only, so the instance stays parked until the producer sends the wake again.
            assertThat(outcome.terminalStatus())
                    .as("EXPECTED GAP: a wake delivered during a pause is lost to the restored wait")
                    .isNull();
            assertThat(outcome.terminalRecords()).isZero();
        }
    }

    @Nested
    class RecoverableExceptionPause {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void pausedInstanceCompletesAfterGracefulRestart() {
            // given / when
            Outcome outcome = RecoveryAfterNonTerminalExitScenario.recoverableExceptionPause(0L, true);

            // then
            assertPausedThenCompleted(outcome);
        }

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void pausedInstanceCompletesAfterCrashRecovery() {
            // given / when
            Outcome outcome = RecoveryAfterNonTerminalExitScenario.recoverableExceptionPause(0L, false);

            // then
            assertPausedThenCompleted(outcome);
        }

        private void assertPausedThenCompleted(Outcome outcome) {
            assertThat(outcome.faultLanded()).as("the body threw its I/O failure once").isTrue();
            assertThat(outcome.liveAfterPause()).as("a recoverable exception keeps the instance registered").isTrue();
            assertRecoveredAndCompleted(outcome);
        }
    }

    @Nested
    class AppendRejection {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void rejectedInstanceStaysRegisteredAndCompletesAfterRestartWithTheOwnerUnchanged() {
            // given / when
            Outcome outcome = RecoveryAfterNonTerminalExitScenario.appendRejection(0L);

            // then
            assertThat(outcome.faultLanded()).as("the foreign write fenced the instance's append").isTrue();
            assertThat(outcome.liveAfterPause()).as("a rejected append keeps the instance registered").isTrue();
            assertRecoveredAndCompleted(outcome, OrderWorkflow.SHIP_ORDER_MAX_RETRIES + 1);
        }
    }

    private static void assertRecoveredAndCompleted(Outcome outcome) {
        assertRecoveredAndCompleted(outcome, 1);
    }

    private static void assertRecoveredAndCompleted(Outcome outcome, int expectedStepAfterPauseRuns) {
        assertThat(outcome.restoredAfterRestart()).as("the restarted engine restores the instance").isTrue();
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.terminalRecords()).as("exactly one terminal record").isEqualTo(1);
        assertThat(outcome.stepBeforePauseRuns()).as("a step done before the pause is replayed, not re-run")
                                                .isEqualTo(1);
        assertThat(outcome.stepAfterPauseRuns()).as("a step after the pause runs once per defined attempt")
                                               .isEqualTo(expectedStepAfterPauseRuns);
    }
}
