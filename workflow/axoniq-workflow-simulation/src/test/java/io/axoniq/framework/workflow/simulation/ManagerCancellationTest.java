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
import io.axoniq.framework.workflow.runtime.api.manager.NonUniqueWorkflowInstanceMatchException;
import io.axoniq.framework.workflow.simulation.scenarios.ManagerCancellationScenario;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link ManagerCancellationScenario}: every cancellation operation of the {@code WorkflowManager} on a live
 * instance reaches the body the same way the in-engine {@code cancelRunningStep} surface does
 * ({@link ExternalStepCancellationTest}); on a historic id the requests answer negatively and append nothing
 * (INVARIANTS.md INV-35 {@code ManagerCancelTargetsLiveOnly}); on an ambiguous query {@code findOne} refuses to
 * pick and {@code findMany} reaches every match.
 *
 * @author Stefan Dragisic
 */
class ManagerCancellationTest {

    private static final Logger logger = LoggerFactory.getLogger(ManagerCancellationTest.class);

    @Nested
    class OnALiveInstance {

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        void requestStepCancellationIsCaughtAndCompensated() {
            var outcome = ManagerCancellationScenario.cancelStep(0L, "A");
            logger.info("Manager step cancellation: {}", outcome);

            assertThat(outcome.awaitCancelled()).as("the wait step commits a terminal CANCELLED record").isTrue();
            assertThat(outcome.compensateCompleted()).as("the body compensates to COMPLETED").isTrue();
            assertThat(outcome.terminalStatus()).as("the workflow completes").isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(outcome.compensateEffects()).as("the compensation side effect runs exactly once").isEqualTo(1);
            assertThat(outcome.lastStepAnswer())
                    .as("the request that recorded the cancellation answers true")
                    .isTrue();
        }

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        void requestCancellationOfAllStepsReportsTheOneRunningStep() {
            var outcome = ManagerCancellationScenario.cancelAllSteps(1L, "B");
            logger.info("Manager cancel-all-steps: {}", outcome);

            assertThat(outcome.awaitCancelled()).as("the wait step commits a terminal CANCELLED record").isTrue();
            assertThat(outcome.compensateCompleted()).as("the body compensates to COMPLETED").isTrue();
            assertThat(outcome.terminalStatus()).as("the workflow completes").isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(outcome.compensateEffects()).as("the compensation side effect runs exactly once").isEqualTo(1);
            assertThat(outcome.lastAllStepsAnswer())
                    .as("the request that recorded the cancellation reports one cancelled step")
                    .isEqualTo(1);
        }

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        void requestWorkflowCancellationTerminatesAndNothingIsRecordedAfterwards() {
            var outcome = ManagerCancellationScenario.cancelWorkflow(2L, "C");
            logger.info("Manager workflow cancellation: {}", outcome);

            assertThat(outcome.terminalStatus())
                    .as("the workflow records one terminal CANCELLED status")
                    .isEqualTo(WorkflowStatus.CANCELLED);
            assertThat(outcome.terminalIsFinal())
                    .as("INV-7: whatever the woken body attempts after the terminal record, nothing more is recorded")
                    .isTrue();
            assertThat(outcome.compensateCompleted())
                    .as("the compensation cannot be recorded after the workflow's terminal record")
                    .isFalse();
        }
    }

    @Nested
    class OnAHistoricInstance {

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        void theThreeRequestsAnswerNegativelyAndAppendNothing() {
            var outcome = ManagerCancellationScenario.cancelTerminalTarget(3L, "D");
            logger.info("Manager cancellation on a historic id: {}", outcome);

            assertThat(outcome.stateStatus())
                    .as("the manager still answers the historic id, from history")
                    .isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(outcome.stepAnswer()).as("step cancel on a historic id answers false").isFalse();
            assertThat(outcome.allStepsAnswer()).as("cancel-all-steps on a historic id answers 0").isZero();
            assertThat(outcome.workflowAnswered())
                    .as("workflow cancel on a historic id completes normally rather than failing")
                    .isTrue();
            // INV-35 was asserted inside the scenario; this pins the same observable in plain numbers.
            assertThat(outcome.appendedEvents())
                    .as("INV-35: no event is appended for an id that was not live at the request")
                    .isZero();
        }
    }

    @Nested
    class OnAnAmbiguousQuery {

        @Test
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        void findOneRefusesToPickAndFindManyReachesBoth() {
            var outcome = ManagerCancellationScenario.nonUnique(4L);
            logger.info("Manager non-unique query: {}", outcome);

            assertThat(outcome.singleStateFailure())
                    .as("singleState() on two matches fails with NonUniqueWorkflowInstanceMatchException")
                    .isEqualTo(NonUniqueWorkflowInstanceMatchException.class);
            assertThat(outcome.manySize()).as("findMany sees both instances").isEqualTo(2);
            assertThat(outcome.batchStepAnswer()).as("the batch request reports at least one live target").isTrue();
            assertThat(outcome.cancelledInstances()).as("both wait steps are cancelled").isEqualTo(2);
            assertThat(outcome.compensatedInstances()).as("both bodies compensate").isEqualTo(2);
        }
    }
}
