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

import io.axoniq.framework.workflow.runtime.api.execution.state.StepIndeterminateException;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.simulation.scenarios.WriteThenVanishScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bridges TLA+ {@code MC_effect_fixed.cfg} / {@code EffectAtMostOnce} (INVARIANTS.md INV-6, finding F-0) — the
 * <strong>fixed</strong> behaviour. This is the acceptance pin for the F-0 fix: it was previously a documented
 * expected-violation (effect ran twice), and it now asserts the engine's at-most-once guarantee.
 * <p>
 * The write-then-vanish scenario drives the real engine through the crash window where a {@code chargePayment} action
 * has run but its {@code COMPLETED} event has not committed. On recovery the engine finds the step {@code STARTED} and,
 * under at-most-once, does <strong>not</strong> re-run the action — the external effect runs <strong>exactly once</strong>
 * — and resolves the interrupted attempt to a terminal {@code FAILED} record (the regular error flow, since
 * {@code chargePayment} has no retry policy). This is the implementation-level confirmation of the TLA+
 * {@code MC_effect_fixed.cfg} result ({@code APPEND_CONDITION=TRUE} ⇒ {@code No error}).
 * <p>
 * It also asserts at-most-once <em>recording</em> (INV-2) still holds — at most one terminal record for the step.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class F0EffectDuplicationTest {

    @ParameterizedTest(name = "seed {0}: write-then-vanish does NOT re-run the charge effect (F-0 fixed, at-most-once)")
    @ValueSource(longs = {1L, 2L, 3L})
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void writeThenVanish_doesNotReExecuteEffect_atMostOnce(long seed) {
        WriteThenVanishScenario.Outcome outcome = WriteThenVanishScenario.run(seed, "A");

        // FIXED (F-0): the external effect ran exactly once — the crash-interrupted attempt is NOT re-run on recovery.
        assertThat(outcome.chargeEffectCount())
                .as("F-0 fixed: chargePayment effect must run at most once (effect at-most-once across crash/replay)")
                .isEqualTo(1);

        // The interrupted attempt is resolved through the regular error flow; chargePayment has no retry policy, so it
        // becomes a terminal FAILED record (cause: StepIndeterminateException).
        assertThat(outcome.chargeTerminalStatus())
                .as("F-0 fixed: the not-re-run chargePayment attempt resolves to a terminal FAILED record")
                .isEqualTo(StepStatus.FAILED);

        // The dedicated cause is durably recorded: re-reaching a previously-STARTED step surfaces a
        // StepIndeterminateException (the FAILED step event's WorkflowError carries its type), NOT a re-run.
        assertThat(outcome.chargeFailureCauseType())
                .as("F-0 fixed: the not-re-run attempt's FAILED record carries StepIndeterminateException as its cause")
                .isEqualTo(StepIndeterminateException.class.getName());

        // INV-2 still holds: at most one terminal record for the step.
        assertThat(outcome.chargeTerminalRecordCount())
                .as("AtMostOnceRecording: chargePayment has at most one terminal record")
                .isLessThanOrEqualTo(1);
    }
}
