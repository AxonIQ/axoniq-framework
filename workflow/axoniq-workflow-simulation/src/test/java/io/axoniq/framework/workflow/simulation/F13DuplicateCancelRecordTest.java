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
import io.axoniq.framework.workflow.simulation.scenarios.DuplicateCancelTerminalRecordScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins finding <strong>F-13</strong> ({@code DuplicateCancelTerminalRecord}): the engine's cancel path re-publishes a
 * <strong>duplicate, identical</strong> {@code <workflow>:CANCELLED} terminal record for one {@code workflowId} when the
 * recovered body is re-driven — a duplicate durable <em>workflow-status</em> terminal record (corruption class), the
 * cancel-path / workflow-terminal analogue of F-7.
 * <p>
 * Mirrors {@code F7DuplicatePayloadRecordTest} (which pins the F-7 step-terminal duplicate so the build stays green): this
 * test pins the F-13 gap is <strong>present</strong> — the duplicate {@code <workflow>:CANCELLED} (per-{@code workflowId}
 * count reaches 2) IS committed when the recovered cancelled body re-reaches {@code ctx.cancel()}. The duplicate is
 * produced by the engine's <strong>ungated</strong> workflow-terminal publish ({@code TerminateDelegate.cancelled}'s
 * direct {@code eventSink.publish} / {@code SimpleWorkflowExecution.handleWorkflowException}, guarded only by the upstream
 * {@code execute()} short-circuit + the {@code switchToLiveMode} terminal-eviction filter — see
 * {@link DuplicateCancelTerminalRecordScenario} for the verified mechanism). The same ungated publish produces this
 * duplicate two ways: intermittently via the crash/replay re-drive race (the root of the {@code Inv7TerminalIsFinalTest}
 * cancel flake), and <strong>deterministically</strong> via the F-3 start-event redelivery restart used here — so the
 * pin is green every run, with no rerun-masking and without touching the shared {@code crashAndRecover()} seam.
 * <p>
 * A candidate fix — gating the workflow-terminal ({@code cancel}/{@code fail}/{@code timeout}) publish on "already
 * terminal" / routing it through a guarded path (the F-7-class fix), and/or extending
 * {@link Invariants#assertAtMostOnceRecording} to also count workflow-status terminal duplicates (closing the INV-2
 * coverage gap) — would make the count stay 1, the intended signal to re-evaluate this expectation. The complementary
 * assertion proves the claim is not over-stated: before the body is re-driven (after the cancel + a crash/replay alone)
 * the count is exactly 1, so the second record is a genuine re-publish on the re-drive, not a double-count of the
 * original.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class F13DuplicateCancelRecordTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void cancelPath_recordsOneWorkflowTerminalRecordAcrossReDrive_F13Closed() {
        DuplicateCancelTerminalRecordScenario.Outcome outcome =
                DuplicateCancelTerminalRecordScenario.run(0L, "A");

        // Before the recovered body is re-driven: the single, correct terminal record. A crash/replay ALONE appends
        // nothing after terminal (INV-7 holds for the in-scope crash/replay) — exactly one <workflow>:CANCELLED.
        assertThat(outcome.cancelledRecordsBeforeReDrive())
                .as("after the cancel + a crash/replay alone, exactly one <workflow>:CANCELLED (INV-7 holds)")
                .isEqualTo(1);

        // F-13 closed: the re-drive (the F-3 start-event redelivery) spawns from ORIGIN, so its first append is
        // rejected by the events the terminated instance already holds. No second <workflow>:CANCELLED can land.
        assertThat(outcome.cancelledRecordsAfterReDrive())
                .as("F-13 closed: the re-driven cancel path's second <workflow>:CANCELLED is rejected by the append "
                            + "re-driven (2 terminal workflow-status records for one workflowId), bypassing any "
                            + "already-terminal guard at the publish site")
                .isEqualTo(1);
    }
}
