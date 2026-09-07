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
import io.axoniq.framework.workflow.simulation.scenarios.StatusHookFiresOncePerStatusScenario;
import io.axoniq.framework.workflow.simulation.workflow.StatusHookFires;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises INVARIANTS.md INV-17 ({@code StatusHookFiresOncePerStatus}): a registered workflow status-change hook fires
 * <strong>at most once</strong> per status for an instance and is <strong>NOT re-fired on a crash + replay</strong> —
 * the lifecycle-hook analogue of INV-6 ({@code EffectAtMostOnce}/F-0).
 * <p>
 * The first test drives the real engine through {@code HookWorkflow} (reaches STARTED then a terminal COMPLETED), with a
 * counting status-change listener registered on both statuses, and (i) the STARTED hook fires exactly once, (ii) no
 * status's hook ever fires more than once — including across two crash + replay cycles that re-evolve the committed
 * STARTED/COMPLETED status events (the no-re-fire guarantee — empirically HOLDS), and (iii) the COMPLETED hook fires
 * exactly once (<strong>F-4 FIXED</strong> — the engine's happy completion path awaits the terminal state change before
 * {@code finishWorkflow}, so the COMPLETED evolution and its hook run before the per-instance task queue is cleared; the
 * hook is no longer dropped). The remaining tests are assertion pins proving the checks are not trivial: a status whose
 * hook fired twice throws (the re-fire break the invariant guards); a STARTED hook that fired zero times (dropped) throws
 * via the reliable at-least-once facet; a COMPLETED hook that fired zero times throws via
 * {@code assertCompletedHookFiredExactlyOnce} (the now-enforced at-least-once facet — F-4 FIXED); a COMPLETED hook that
 * fired twice throws via the document helper (the re-fire break even for the terminal hook); and an out-of-scope instance
 * is skipped (the check reads only the asked-for {@code workflowId}).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Inv17StatusHookFiresOncePerStatusTest {

    private static final String WF = "hook-A";

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void statusHooksFireAtMostOncePerStatus_andAreNotReFiredAcrossCrashReplay() {
        StatusHookFiresOncePerStatusScenario.Outcome outcome = StatusHookFiresOncePerStatusScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("the hook workflow must reach a terminal COMPLETED status")
                .isTrue();
        // (a) The reliable at-least-once facet: the STARTED hook fired exactly once (the start path awaits the STARTED
        // evolution before proceeding).
        assertThat(outcome.startedFiresAtTerminal())
                .as("StatusHookFiresOncePerStatus: the STARTED hook fires exactly once")
                .isEqualTo(1);
        // (a) The no-re-fire guarantee (the F-0 analogue, which HOLDS): no status's hook count increases across the two
        // crash + replay cycles — the recovered engine does not re-invoke listeners while re-evolving committed status
        // events.
        assertThat(outcome.startedFiresAfterCrash())
                .as("StatusHookFiresOncePerStatus: the STARTED hook is not re-fired on crash/replay")
                .isEqualTo(outcome.startedFiresAtTerminal())
                .isEqualTo(1);
        assertThat(outcome.completedFiresAfterCrash())
                .as("StatusHookFiresOncePerStatus: the COMPLETED hook is not re-fired on crash/replay (F-4 FIXED — "
                            + "stays exactly 1)")
                .isEqualTo(outcome.completedFiresAtTerminal())
                .isEqualTo(1);
        // The COMPLETED hook now fires exactly once (F-4 FIXED — the engine's happy completion path awaits the terminal
        // state change before finishWorkflow, so the COMPLETED evolution and its hook run before the queue is cleared; it
        // is no longer dropped).
        assertThat(outcome.completedFiresAtTerminal())
                .as("StatusHookFiresOncePerStatus: the COMPLETED hook fires exactly once (F-4 FIXED — no longer dropped)")
                .isEqualTo(1);
        // F-4 FIXED observability: `completedHookDropped` is now always false (the terminal COMPLETED hook is no longer
        // dropped on the happy completion path).
        assertThat(outcome.completedHookDropped())
                .as("F-4 FIXED: the terminal COMPLETED hook is no longer dropped (completedHookDropped is false)")
                .isFalse();
    }

    @Test
    void assertStatusHookFiresOncePerStatus_passesWhenEachStatusFiredOnce() {
        StatusHookFires fires = new StatusHookFires();
        fires.record(WF, WorkflowStatus.STARTED);
        fires.record(WF, WorkflowStatus.COMPLETED);

        assertThatCode(() -> Invariants.assertStatusHookFiresOncePerStatus(
                fires, WF, WorkflowStatus.STARTED, WorkflowStatus.COMPLETED))
                .as("each registered status firing exactly once is sound")
                .doesNotThrowAnyException();
    }

    @Test
    void assertStatusHookFiresOncePerStatus_throwsWhenAStatusFiredTwice() {
        // The re-fire break the invariant guards (the lifecycle-hook F-0 analogue): a status's hook fired twice — e.g. a
        // replay that re-invoked the listener.
        StatusHookFires fires = new StatusHookFires();
        fires.record(WF, WorkflowStatus.STARTED);
        fires.record(WF, WorkflowStatus.COMPLETED);
        fires.record(WF, WorkflowStatus.COMPLETED); // re-fired

        assertThatThrownBy(() -> Invariants.assertStatusHookFiresOncePerStatus(
                fires, WF, WorkflowStatus.STARTED, WorkflowStatus.COMPLETED))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("StatusHookFiresOncePerStatus")
                .hasMessageContaining(WF)
                .hasMessageContaining("fired 2 times");
    }

    @Test
    void assertStartedHookFiredExactlyOnce_throwsWhenStartedHookNeverFired() {
        // The break: the STARTED hook fired ZERO times (a dropped hook). INV-17's reliable at-least-once facet requires
        // STARTED to fire exactly once, so a never-fired STARTED hook throws.
        StatusHookFires fires = new StatusHookFires();
        // (no STARTED fire recorded)
        fires.record(WF, WorkflowStatus.COMPLETED);

        assertThatThrownBy(() -> Invariants.assertStartedHookFiredExactlyOnce(fires, WF))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("StatusHookFiresOncePerStatus")
                .hasMessageContaining("STARTED")
                .hasMessageContaining("fired 0 times");
    }

    @Test
    void assertCompletedHookFiredExactlyOnce_throwsWhenCompletedHookNeverFired() {
        // The break: the COMPLETED hook fired ZERO times (a dropped hook). After F-4 is fixed INV-17's at-least-once
        // facet for the terminal hook is enforced, so a never-fired COMPLETED hook throws.
        StatusHookFires fires = new StatusHookFires();
        fires.record(WF, WorkflowStatus.STARTED);
        // (no COMPLETED fire recorded)

        assertThatThrownBy(() -> Invariants.assertCompletedHookFiredExactlyOnce(fires, WF))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("StatusHookFiresOncePerStatus")
                .hasMessageContaining("COMPLETED")
                .hasMessageContaining("fired 0 times");
    }

    @Test
    void documentTerminalHookMayBeDropped_reportsDropAndToleratesSingleFire_butThrowsOnReFire() {
        // Drop case: COMPLETED hook fired 0 times -> documented as dropped (F-4), no throw (at-most-once holds).
        StatusHookFires droppedFires = new StatusHookFires();
        droppedFires.record(WF, WorkflowStatus.STARTED);
        assertThat(Invariants.documentTerminalHookMayBeDropped(droppedFires, WF))
                .as("a dropped COMPLETED hook (count 0) is the documented F-4 finding, reported (not thrown)")
                .isTrue();

        // Single-fire case: COMPLETED hook fired once -> not dropped, no throw.
        StatusHookFires onceFires = new StatusHookFires();
        onceFires.record(WF, WorkflowStatus.COMPLETED);
        assertThat(Invariants.documentTerminalHookMayBeDropped(onceFires, WF))
                .as("a COMPLETED hook that fired exactly once is not dropped")
                .isFalse();

        // Re-fire case: COMPLETED hook fired twice -> the no-re-fire guarantee must hold even for the terminal hook,
        // so this throws (it would be the lifecycle-hook re-fire finding, the F-0 analogue).
        StatusHookFires twiceFires = new StatusHookFires();
        twiceFires.record(WF, WorkflowStatus.COMPLETED);
        twiceFires.record(WF, WorkflowStatus.COMPLETED);
        assertThatThrownBy(() -> Invariants.documentTerminalHookMayBeDropped(twiceFires, WF))
                .isInstanceOf(InvariantViolation.class)
                .hasMessageContaining("StatusHookFiresOncePerStatus")
                .hasMessageContaining("fired 2 times");
    }

    @Test
    void assertStatusHookFiresOncePerStatus_skipsOutOfScopeInstance() {
        // The check reads only the asked-for workflowId; another instance's fires (e.g. a re-fired foreign hook) must not
        // affect this instance's check — an out-of-scope instance is effectively skipped (count 0 for the asked id).
        StatusHookFires fires = new StatusHookFires();
        fires.record("order-wf0", WorkflowStatus.STARTED);
        fires.record("order-wf0", WorkflowStatus.STARTED); // a foreign instance even firing twice is irrelevant here

        assertThatCode(() -> Invariants.assertStatusHookFiresOncePerStatus(
                fires, WF, WorkflowStatus.STARTED, WorkflowStatus.COMPLETED))
                .as("the check is per workflowId; a different instance's fires are not counted")
                .doesNotThrowAnyException();
    }
}
