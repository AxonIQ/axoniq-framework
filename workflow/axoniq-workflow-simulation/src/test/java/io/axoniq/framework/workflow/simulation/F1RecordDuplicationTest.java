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

import io.axoniq.framework.workflow.simulation.invariants.Invariants;
import io.axoniq.framework.workflow.simulation.scenarios.SplitBrainScenario;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Bridges TLA+ counterexample {@code MC_record.cfg} / {@code AtMostOnceRecording} (INVARIANTS.md INV-2, finding F-1
 * consequence).
 * <p>
 * {@code AtMostOnceRecording} (INV-2) holds only <em>given</em> {@code AtMostOneOwner} (INV-1). The TLA+
 * {@code MC_record.cfg} run ({@code DURABLE_LEASE=FALSE}, {@code APPEND_CONDITION=FALSE}) shows the dependency the
 * {@code ExecuteDelegate} FIXMEs warn about: two split-brain owners each decide from their own in-memory view that a
 * step is still open and both append {@code COMPLETED} for it, so the durable log ends
 * {@code ⟨STARTED, COMPLETED, COMPLETED⟩} — two terminal records for one step.
 * <p>
 * The {@link SplitBrainScenario} drives this against the real engine: two engine instances over one shared durable
 * event store, both racing the first {@code execute} step ({@code reserveInventory}). With DCB append conditions the
 * second writer's append is rejected, so the log holds at most one terminal record per step
 * ({@code MC_record_fixed.cfg}). The former F-1 expectation of a duplicate record was flipped to this assertion when the
 * fencing landed.
 * <p>
 * This is the deliberate contrast with the single-owner crash path ({@link F0EffectDuplicationTest}): there INV-2 still
 * holds (≤1 terminal record) even though the effect duplicates (F-0); here, with the lease broken, INV-2 itself is
 * violable.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class F1RecordDuplicationTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void splitBrainOwners_appendAtMostOneTerminalRecordPerStep() {
        SplitBrainScenario.Outcome outcome = SplitBrainScenario.run(0L, "A");
        // F-1 consequence closed: the append condition fences the second writer, MC_record_fixed.cfg holds.
        assertThat(outcome.maxTerminalRecordsForAStep())
                .as("AtMostOnceRecording: at most one terminal record per step under split-brain")
                .isEqualTo(1);
    }
}
