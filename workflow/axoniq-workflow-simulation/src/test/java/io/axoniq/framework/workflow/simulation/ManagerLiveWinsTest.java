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
import io.axoniq.framework.workflow.simulation.scenarios.ManagerLiveWinsScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.simulation.workflow.ManagerLiveWinsWorkflow.STEP_AWAIT_APPROVAL;
import static io.axoniq.framework.workflow.simulation.workflow.ManagerLiveWinsWorkflow.STEP_AWAIT_RESUME;
import static io.axoniq.framework.workflow.simulation.workflow.ManagerLiveWinsWorkflow.STEP_COMPENSATE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises INVARIANTS.md INV-36 ({@code ManagerLiveWins}) through {@link ManagerLiveWinsScenario}: with the history
 * projection held behind a live instance whose state has moved, the {@code WorkflowManager} answers the live state.
 * Added because a merge that preferred history passed every settle-time arm of the P7 campaign.
 *
 * @author Stefan Dragisic
 */
class ManagerLiveWinsTest {

    private static final Logger logger = LoggerFactory.getLogger(ManagerLiveWinsTest.class);

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void theManagerAnswersTheLiveStateWhileHistoryIsBehind() {
        var outcome = ManagerLiveWinsScenario.run(9L, "L");
        logger.info("Manager live wins: {}", outcome);

        assertThat(outcome.heldHistorySteps())
                .as("the held projection is behind, or this run proves nothing")
                .containsOnly(java.util.Map.entry(STEP_AWAIT_APPROVAL, StepStatus.STARTED));
        assertThat(outcome.liveSteps())
                .as("the live state has moved past the projection")
                .containsEntry(STEP_AWAIT_APPROVAL, StepStatus.CANCELLED)
                .containsEntry(STEP_COMPENSATE, StepStatus.COMPLETED)
                .containsEntry(STEP_AWAIT_RESUME, StepStatus.STARTED);
        assertThat(outcome.liveWinsHeld())
                .as("INV-36: the manager answers the live state, not the held projection")
                .isTrue();
        assertThat(outcome.managerSteps())
                .as("the manager's steps are the live steps")
                .isEqualTo(outcome.liveSteps());
        assertThat(outcome.afterRelease())
                .as("complementary guarantee: the answer converges once the instance completed and the freeze lifted")
                .isEqualTo(WorkflowStatus.COMPLETED);
    }
}
