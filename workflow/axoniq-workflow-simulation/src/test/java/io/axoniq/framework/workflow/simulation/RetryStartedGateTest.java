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
import io.axoniq.framework.workflow.simulation.scenarios.RetryStartedGateScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The retry-attempt gate (issue #408) under the two faults that hit a running retry attempt: a crash and an external
 * cancellation. Sibling of {@code FencedRetryBackoffTest} (the claim-loss fault) and {@code AtMostOnceRetryResumeTest}
 * (a crash during the first attempt).
 *
 * @author Stefan Dragisic
 */
class RetryStartedGateTest {

    private static final Logger logger = LoggerFactory.getLogger(RetryStartedGateTest.class);

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void aCrashDuringARetryAttemptResumesItAsIndeterminateWithItsOwnAttemptNumber() {
        var outcome = RetryStartedGateScenario.runCrashDuringRetryAttempt(21L, "c1");
        logger.info("Crash during retry attempt: {}", outcome);

        assertThat(outcome.lastRecordBeforeCrash())
                .as("the fault landed: the durable log ends with attempt 2 in flight; outcome %s", outcome)
                .isEqualTo(StepStatus.RETRY_STARTED);
        assertThat(outcome.effectsBeforeCrash())
                .as("attempts 1 and 2 each ran once before the crash")
                .isEqualTo(2);
        assertThat(outcome.retryingAttempts())
                .as("the interrupted attempt 2 is decided as RETRYING(2), not restarted at 1")
                .containsExactly(1, 2);
        assertThat(outcome.retryStartedAttempts())
                .as("attempt 2 started once before the crash, attempt 3 once after it")
                .containsExactly(2, 3);
        assertThat(outcome.effectsAfterRecovery())
                .as("at-most-once per attempt: attempt 2 is not re-run, attempt 3 runs once")
                .isEqualTo(3);
        assertThat(outcome.completedRecords()).isEqualTo(1);
        assertThat(outcome.workflowReachedTerminal()).isTrue();
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void anExternalCancelDuringARetryAttemptEndsTheStepWithoutAnotherAttempt() {
        var outcome = RetryStartedGateScenario.runExternalCancelDuringRetryAttempt(22L, "x1");
        logger.info("External cancel during retry attempt: {}", outcome);

        assertThat(outcome.retryStartedBeforeCancel())
                .as("the fault landed on a running retry attempt: RETRY_STARTED(2) was committed")
                .isEqualTo(1);
        assertThat(outcome.effectsBeforeCancel())
                .as("attempt 1 timed out and attempt 2 was running")
                .isEqualTo(2);
        assertThat(outcome.stepCancelRecorded())
                .as("the cancelled retry attempt records CANCELLED; outcome %s", outcome)
                .isTrue();
        assertThat(outcome.effectsAfterCancel())
                .as("no attempt runs after the cancel")
                .isEqualTo(2);
        assertThat(outcome.retryingRecords())
                .as("the cancelled attempt is not decided as another retry")
                .isEqualTo(1);
        assertThat(outcome.retryStartedRecords())
                .as("no further attempt starts after the cancel")
                .isEqualTo(1);
        assertThat(outcome.timedOutRecords())
                .as("the cancelled attempt's timeout window does not record TIMED_OUT")
                .isZero();
    }
}
