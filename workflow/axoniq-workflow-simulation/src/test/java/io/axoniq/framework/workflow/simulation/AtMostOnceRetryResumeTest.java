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
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import io.axoniq.framework.workflow.simulation.harness.EngineInstance;
import io.axoniq.framework.workflow.simulation.harness.Polling;
import io.axoniq.framework.workflow.simulation.harness.SimulationWorld;
import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.workflow.CountingEffects;
import io.axoniq.framework.workflow.simulation.workflow.RetryResumeWorkflow;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.RetryRequestedEvent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the at-most-once <strong>retry</strong> branch of the F-0 fix (INVARIANTS.md INV-6): when a step that
 * carries a {@link io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy RetryPolicy} has its first attempt
 * interrupted by a crash in the apply→commit window, the engine must <strong>not</strong> re-run the action in place;
 * it turns the interrupted attempt into a {@code RETRYING} transition + a fresh attempt (the regular error flow), which
 * then drives the step to {@code COMPLETED}.
 * <p>
 * Sibling of {@code F0EffectDuplicationTest} (which covers the <em>no-retry</em> branch: a crash-interrupted attempt
 * resolves to {@code FAILED}). Together they pin both arms of the user-specified rule "no retry → FAILED, retry →
 * RETRYING". This test is the safety check for the retry arm specifically: the random fuzz/chaos never lands in the
 * at-most-once window, so this deterministic scenario is what exercises (and proves non-hanging) the
 * {@code handleAttemptFailure}-from-the-guard path.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class AtMostOnceRetryResumeTest {

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void crashInterruptedRetriedStep_goesRetrying_thenCompletes_atMostOncePerAttempt() {
        CountingEffects effects = new CountingEffects();
        var registration = EngineInstance.retryResumeWorkflow(effects);
        try (var world = new SimulationWorld(0L, registration)) {
            String orderId = "A";
            String workflowId = "retryresume-" + orderId;

            // Arm the F-0 window on the retried step's first-attempt COMPLETED.
            world.eventStore().armVanishCommitFor(RetryResumeWorkflow.STEP_CHARGE, StepStatus.COMPLETED);

            world.engine().publish(new RetryRequestedEvent(orderId));
            Polling.awaitOrFail(Duration.ofSeconds(10), "first attempt action to run once",
                                () -> effects.count(workflowId, RetryResumeWorkflow.STEP_CHARGE) >= 1);
            Polling.awaitOrFail(Duration.ofSeconds(10), "first-attempt COMPLETED commit to vanish",
                                () -> !world.eventStore().isVanishArmed());

            // Crash + recover: the interrupted STARTED attempt must NOT be re-run in place. The retry policy turns it
            // into a RETRYING transition + a fresh attempt that completes the step.
            world.crashAndRecover();
            Polling.awaitOrFail(Duration.ofSeconds(15),
                                "step to reach COMPLETED via the retry (path must not hang)",
                                () -> recordCount(world.committedLog(), RetryResumeWorkflow.STEP_CHARGE,
                                                  StepStatus.COMPLETED) >= 1);

            List<EventMessage> log = world.committedLog();

            // The interrupted attempt became a RETRYING transition (not an in-place re-run, which would have NO RETRYING
            // record) — the defining signature of the retry branch.
            assertThat(recordCount(log, RetryResumeWorkflow.STEP_CHARGE, StepStatus.RETRYING))
                    .as("retry branch: the crash-interrupted first attempt is recorded as RETRYING, then re-attempted")
                    .isGreaterThanOrEqualTo(1);

            // The step then completed (the path did not hang or wedge).
            assertThat(recordCount(log, RetryResumeWorkflow.STEP_CHARGE, StepStatus.COMPLETED))
                    .as("the retried step reaches a terminal COMPLETED record")
                    .isEqualTo(1);

            // At-most-once PER ATTEMPT: the action ran once before the crash and once on the fresh retry attempt — the
            // interrupted attempt was NOT re-run in place (which is what at-most-once forbids).
            assertThat(effects.count(workflowId, RetryResumeWorkflow.STEP_CHARGE))
                    .as("at-most-once per attempt: effect runs once per attempt (pre-crash attempt + one retry)")
                    .isEqualTo(2);

            // INV-2 still holds: at most one terminal COMPLETED record for the step.
            Invariants.assertAtMostOnceRecording(log);
        }
    }

    private static int recordCount(List<EventMessage> log, String stepName, StepStatus status) {
        return (int) log.stream()
                .filter(e -> stepName.equals(MetadataUtils.getStepName(e.metadata()))
                        && MetadataUtils.getStepStatus(e.metadata()).map(status::equals).orElse(false))
                .count();
    }
}
