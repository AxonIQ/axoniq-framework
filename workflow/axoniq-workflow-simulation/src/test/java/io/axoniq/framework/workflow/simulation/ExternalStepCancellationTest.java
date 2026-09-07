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

import io.axoniq.framework.workflow.simulation.scenarios.ExternalStepCancellationScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link ExternalStepCancellationScenario}: cancelling a running blocking wait step from a thread other than
 * the workflow's own control thread must let the body catch the {@code StepCancellationException} and compensate to a
 * terminal COMPLETED workflow — the cross-thread half of the {@code cancelRunningStep} surface.
 * <p>
 * On an engine whose {@code cancelRunningStep} pumps the single-consumer task queue on the CALLER's thread, this test
 * fails at the scenario's compensation wait: the external caller races the control thread for its own queued tasks and
 * leaks an interrupt into it, so the compensation step starts but never completes (and the workflow never terminates).
 * On an engine whose external cancellation is applied by enqueueing for the control thread, the full
 * catch-and-compensate path completes and this test is green.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class ExternalStepCancellationTest {

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void externallyCancelledStepIsCaughtAndCompensated() {
        ExternalStepCancellationScenario.Outcome outcome = ExternalStepCancellationScenario.run(0L, "A");

        assertThat(outcome.awaitStepCancelled())
                .as("the externally cancelled wait step commits a terminal CANCELLED record")
                .isTrue();
        assertThat(outcome.compensateCompleted())
                .as("the body catches the cancellation and its compensation step commits COMPLETED")
                .isTrue();
        assertThat(outcome.workflowCompleted())
                .as("the workflow reaches a terminal COMPLETED status after compensating")
                .isTrue();
        // Complementary guarantee: compensation ran its side effect exactly once — the external cancel neither
        // suppressed the compensation body nor re-ran it.
        assertThat(outcome.compensateEffects())
                .as("the compensation side effect runs exactly once")
                .isEqualTo(1);
    }
}
