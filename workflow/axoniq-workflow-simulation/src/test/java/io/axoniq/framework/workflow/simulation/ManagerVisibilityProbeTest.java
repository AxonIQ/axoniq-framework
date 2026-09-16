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
import io.axoniq.framework.workflow.simulation.invariants.InvariantViolation;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.ManagerVisibilityProbeScenario;
import io.axoniq.framework.workflow.simulation.scenarios.ManagerVisibilityProbeScenario.Mode;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-31 ({@code ManagerVisibilityMonotonic}) and INV-32 ({@code ManagerStatusMonotonic})
 * through {@link ManagerVisibilityProbeScenario}: the Workflow Manager merges the live execution repository with the
 * history projection, and the two move at different speeds. Bridges TLA+ counterexamples {@code MC_managerview.cfg}
 * and {@code MC_managerview_status.cfg} ({@code ManagerView.tla}, F-42).
 * <p>
 * The held arms were expected-gap pins for F-42 and flipped when the manager started sourcing a non-live id from the
 * store instead of trusting a lagging projection (the {@code READ_LOG_ON_HISTORY_MISS} toggle of the model,
 * {@code MC_managerview_fixed.cfg}); they now guard that fix. Each is paired with the neighbouring guarantee — the
 * answer converges once the projection catches up. The pure-assertion arms prove the two invariant assertions can
 * fail.
 *
 * @author Stefan Dragisic
 */
class ManagerVisibilityProbeTest {

    private static final Logger logger = LoggerFactory.getLogger(ManagerVisibilityProbeTest.class);

    @Nested
    class HistoryBehindTheLiveRemoval {

        @Test
        @Timeout(value = 90, unit = TimeUnit.SECONDS)
        void aFinishedIdStaysAnsweredWhileTheProjectionLags_F42Closed() {
            var outcome = ManagerVisibilityProbeScenario.run(3L, "A", Mode.VISIBILITY);
            logger.info("Manager visibility probe (VISIBILITY): {}", outcome);

            assertThat(outcome.whileLive())
                    .as("the manager answered the live, parked instance — the answer it must never take back")
                    .isEqualTo(WorkflowStatus.STARTED);
            assertThat(outcome.logStatus())
                    .as("the committed log holds the terminal status when the second read is taken")
                    .isEqualTo(WorkflowStatus.COMPLETED);
            // INV-31: the id the manager already answered stays answered once the execution left the live set,
            // even though history has not yet applied its STARTED — the manager sources a non-live id from the store.
            // Before that fix this read returned null (F-42).
            assertThat(outcome.afterRemoval())
                    .as("INV-31: an id the manager answered before is still answered, from the store")
                    .isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(outcome.afterRelease())
                    .as("complementary guarantee: the answer converges once the projection catches up")
                    .isEqualTo(WorkflowStatus.COMPLETED);
        }

        @Test
        @Timeout(value = 90, unit = TimeUnit.SECONDS)
        void aCompletedIdIsAnsweredCompletedWhileTheProjectionLags_F42Closed() {
            var outcome = ManagerVisibilityProbeScenario.run(5L, "B", Mode.STATUS);
            logger.info("Manager visibility probe (STATUS): {}", outcome);

            assertThat(outcome.whileLive()).as("the live, parked instance").isEqualTo(WorkflowStatus.STARTED);
            assertThat(outcome.logStatus()).as("the log is terminal").isEqualTo(WorkflowStatus.COMPLETED);
            // INV-32: the live COMPLETED state is gone and the projection still says STARTED; the manager answers the
            // store, so the terminal status is never taken back. Before that fix this read returned STARTED (F-42).
            assertThat(outcome.afterRemoval())
                    .as("INV-32: a completed id is answered COMPLETED although the projection still says STARTED")
                    .isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(outcome.afterRelease())
                    .as("complementary guarantee: the answer converges once the projection catches up")
                    .isEqualTo(WorkflowStatus.COMPLETED);
        }

        @Test
        @Timeout(value = 90, unit = TimeUnit.SECONDS)
        void withoutAnInducedLagTheAnswerConverges_windowMeasuredNotAsserted() {
            var outcome = ManagerVisibilityProbeScenario.run(7L, "C", Mode.UNHELD);
            logger.info("Manager visibility probe (UNHELD): {} — {} stale answer(s) observed in the real window",
                        outcome, outcome.staleAnswersUnheld());

            assertThat(outcome.whileLive()).as("the live, parked instance").isEqualTo(WorkflowStatus.STARTED);
            assertThat(outcome.afterRelease())
                    .as("the manager converges on the terminal status without any induced lag")
                    .isEqualTo(WorkflowStatus.COMPLETED);
        }
    }

    @Nested
    class TheAssertionsCanFail {

        @Test
        void managerVisibilityMonotonic_detectsAForgottenId() {
            assertThatThrownBy(() -> Invariants.assertManagerVisibilityMonotonic(Set.of("extcancel-A"), Map.of()))
                    .isInstanceOf(InvariantViolation.class)
                    .hasMessageContaining("ManagerVisibilityMonotonic")
                    .hasMessageContaining("extcancel-A");
        }

        @Test
        void managerVisibilityMonotonic_toleratesAnIdNeverAnswered() {
            assertThatCode(() -> Invariants.assertManagerVisibilityMonotonic(Set.of(), Map.of()))
                    .doesNotThrowAnyException();
        }

        @Test
        void managerStatusMonotonic_detectsATerminalAnswerTakenBack() {
            assertThatThrownBy(() -> Invariants.assertManagerStatusMonotonic(
                    Map.of("extcancel-A", WorkflowStatus.COMPLETED), Map.of()))
                    .isInstanceOf(InvariantViolation.class)
                    .hasMessageContaining("ManagerStatusMonotonic")
                    .hasMessageContaining("COMPLETED");
        }

        @Test
        void managerStatusMonotonic_toleratesANonTerminalEarlierAnswer() {
            assertThatCode(() -> Invariants.assertManagerStatusMonotonic(
                    Map.of("extcancel-A", WorkflowStatus.STARTED), Map.of()))
                    .doesNotThrowAnyException();
        }
    }
}
