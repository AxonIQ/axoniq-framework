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
import io.axoniq.framework.workflow.simulation.scenarios.LoopAndStormScenario;
import io.axoniq.framework.workflow.simulation.workflow.LoopingPollWorkflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase-4 production-realism pins: the documented retry-loop recipe under both authorings, and broadcast fan-out at
 * small scale (see {@link LoopAndStormScenario}). The headline expected-gap pin characterizes the
 * <strong>reused-step-names loop</strong> — the authoring the canonical {@code PaymentWorkflow} example ships — whose
 * retry path degenerates into a hot, record-less cached-result spin: the wait never re-registers (blind to fresh
 * signals), nothing new is committed (invisible to log-based liveness), and only the probe body's own iteration bound
 * surfaces it.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class LoopAndStormTest {

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void reusedNamesLoop_spinsOnCachedResults_appendsNothing_asExpectedGap() {
        var outcome = LoopAndStormScenario.reusedNamesLiveLock(0L, "L1");

        // EXPECTED GAP: the body looped MAX_SPINS times...
        assertThat(outcome.bodyIterations())
                .as("the body iterated to its bound — every retry after the first was an instant cached read")
                .isEqualTo(LoopingPollWorkflow.MAX_SPINS);
        // ...but the durable log holds exactly ONE iteration: the wait registered once and never re-registered
        // (cached TIMED_OUT every later pass — the loop is blind to any fresh signal), and the sleep never slept
        // again (cached terminal).
        assertThat(outcome.pollStartedRecords())
                .as("the reused-name wait registered exactly once — iterations 2+ never re-registered")
                .isEqualTo(1);
        assertThat(outcome.pollTerminalRecords()).isEqualTo(1);

        // F-20 HISTORY (INV-2 violation, surfaced BY this probe's first run, pre-Phase-1'): ctx.sleep is
        // NON-blocking (F-19), so each loop pass re-entered the reused-name sleep while the step was still
        // STARTED — WaitForDelegate's past-deadline branch then queued ANOTHER timedOut publish, gated only on
        // the IN-MEMORY step status, which lagged the first TIMED_OUT's durably-async apply. Observed 2..25
        // duplicate terminals across runs — the count was timing-dependent because the duplication REQUIRED the
        // real-time JDK timer thread racing the body's spin loop.
        //
        // PHASE-1' RE-PIN: with the per-attempt timeout armed through WorkflowScheduler.withTimeout the count
        // is now BIMODAL, and both modes express the F-19/F-20 pathology:
        //   0  — the virtual-time sleep timer never fires before the spinning body exhausts MAX_SPINS and the
        //        workflow goes terminal (the F-19 no-op sleep never slept NOR terminated), or
        //   ≥2 — the body's re-entry still races the timedOut evolve on real thread scheduling and the ungated
        //        past-deadline publish records duplicates (the original F-20 corruption, load-dependent).
        // A FIXED engine (F-19 blocking sleep + F-20 durable-state-gated publish) yields exactly ONE terminal
        // for the single registration — so the pin is "anything but 1": it stays green across both gap modes
        // and flips exactly when the fixes land. The remaining nondeterminism (0 vs ≥2) is real thread
        // scheduling — the dimension Phase 2 (deterministic scheduler / interleaving fuzz) makes seeded and
        // explorable; the strict F-20 acceptance then moves to that probe.
        assertThat(outcome.retryDelayTerminalRecords())
                .as("EXPECTED GAP (Phase-1' re-pin): reused-name sleep records 0 (never fired before exhaustion) "
                            + "or ≥2 (duplicate terminals) — never the fixed-engine 1; see comment")
                .isNotEqualTo(1);

        // The probe body bounds the spin and fails explicitly; a production body (the canonical example has no
        // bound) would spin forever, non-terminal and invisible.
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.FAILED);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void counterNamesLoop_completesOnSignal() {
        var outcome = LoopAndStormScenario.counterNamesCompletesOnSignal(0L, "L2");

        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.bodyIterations()).as("the signal arrived during iteration 1").isEqualTo(1);
        assertThat(outcome.processEffects()).isEqualTo(1);
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void alignedClock_counterNamesLoop_pacesMultipleGenuineIterations_deterministically() {
        var outcome = LoopAndStormScenario.alignedCounterNamesMultiIterationPacing(0L, "L4");

        // The unlock the aligned-clock seam provides: MULTIPLE genuine loop iterations, each paced by its own exact
        // virtual advance (10s -> poll#i TIMED_OUT, 2s -> retryDelay#i resolves), previously undrivable under the D5
        // era residual (post-advance wall-stamped deadlines raced the advanced virtual clock).
        assertThat(outcome.bodyIterations())
                .as("the body genuinely iterated three times: two timed-out polls, then the signalled third")
                .isEqualTo(LoopingPollWorkflow.COUNTER_TIMEOUT_ITERATIONS + 1);
        assertThat(outcome.poll1TimedOutRecords())
                .as("iteration 1's poll timed out exactly once on a plain 10s virtual advance")
                .isEqualTo(1);
        assertThat(outcome.poll2TimedOutRecords())
                .as("iteration 2's poll re-registered and timed out exactly once the same way")
                .isEqualTo(1);
        assertThat(outcome.sleep1TerminalRecords())
                .as("iteration 1's 2s retry sleep genuinely paced (resolved on its own 2s advance), recorded once")
                .isEqualTo(1);
        assertThat(outcome.sleep2TerminalRecords())
                .as("iteration 2's retry sleep likewise, recorded once")
                .isEqualTo(1);
        assertThat(outcome.poll3CompletedRecords())
                .as("iteration 3's poll completed exactly once on the delivered signal")
                .isEqualTo(1);
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(outcome.processEffects()).isEqualTo(1);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void broadcastSignal_wakesEveryWaiterOnTheSharedKey_exactlyOnce() {
        var outcome = LoopAndStormScenario.broadcastWakesAllWaiters(0L, 5);

        assertThat(outcome.completedInstances())
                .as("one broadcast signal wakes every waiter parked on the shared key")
                .isEqualTo(outcome.totalWaiters());
        assertThat(outcome.perInstanceMatches())
                .as("each waiter woke exactly once")
                .isEqualTo(outcome.totalWaiters());
    }
}
