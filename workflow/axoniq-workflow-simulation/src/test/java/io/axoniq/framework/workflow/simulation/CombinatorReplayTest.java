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
import io.axoniq.framework.workflow.simulation.scenarios.CombinatorReplayScenario;
import io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow.EFFECT_MATCHED_PREFIX;
import static io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow.EFFECT_UNMATCHED_PREFIX;
import static io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow.EFFECT_VERDICT_PREFIX;
import static io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow.EFFECT_WINNER_PREFIX;
import static io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow.STEP_BRANCH_A;
import static io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow.STEP_BRANCH_B;
import static io.axoniq.framework.workflow.simulation.workflow.CombinatorReplayWorkflow.STEP_BRANCH_C;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Combinator replay-determinism pins (see {@link CombinatorReplayScenario}): what a body observes from
 * {@code anyMatch}/{@code allMatch}/{@code noneMatch} — the verdict, the winner, and the
 * {@code matched()}/{@code unmatched()} categorization — compared between the live pass and the recovered re-run.
 * A snapshot counter at 2 was observed by BOTH passes (replay-stable); at 1, by exactly one pass (the divergence).
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class CombinatorReplayTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void allMatch_lateBranchCompletionBeforeCrash_reCategorizesOnReplay_asExpectedGap() {
        var outcome = CombinatorReplayScenario.allMatchLateCompletionAcrossCrash(0L, "AM1");

        assertThat(outcome.runs()).as("live pass + one recovered re-run").isEqualTo(2);
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);

        // The DECISION is replay-stable: both passes saw the allMatch fail (branchB FAILED).
        assertThat(outcome.counters().getOrDefault(EFFECT_VERDICT_PREFIX + "false", 0))
                .as("the allMatch verdict (failure) is replay-stable")
                .isEqualTo(2);
        // branchA matched on both passes; branchB unmatched on both passes.
        assertThat(outcome.counters().getOrDefault(EFFECT_MATCHED_PREFIX + STEP_BRANCH_A, 0)).isEqualTo(2);
        assertThat(outcome.counters().getOrDefault(EFFECT_UNMATCHED_PREFIX + STEP_BRANCH_B, 0)).isEqualTo(2);

        // EXPECTED GAP: the CATEGORIZATION is NOT replay-stable. Live, branchC was still in flight at the
        // short-circuit and sat in unmatched() (the documented C-5 shape); it completed before the crash, so the
        // recovered re-run recomputed it into matched() (categories are computed per body run from CURRENT branch
        // states — CombinatorSupport.computeCategories — never recorded). Any body logic keyed on matched()/
        // unmatched() membership (e.g. "compensate exactly the unmatched branches") diverges between the live run
        // and the replay.
        assertThat(outcome.counters().getOrDefault(EFFECT_UNMATCHED_PREFIX + STEP_BRANCH_C, 0))
                .as("EXPECTED GAP: branchC was unmatched on the LIVE pass only")
                .isEqualTo(1);
        assertThat(outcome.counters().getOrDefault(EFFECT_MATCHED_PREFIX + STEP_BRANCH_C, 0))
                .as("EXPECTED GAP: branchC re-categorized into matched on the RECOVERED pass only")
                .isEqualTo(1);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void anyMatch_winnerIdentity_isReplayStable_evenAfterBothBranchesCached() {
        var outcome = CombinatorReplayScenario.anyMatchWinnerAcrossCrash(0L, "AW1");

        assertThat(outcome.runs()).isEqualTo(2);
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);

        // The winner is derived from the DURABLE first-completed order (firstCompletedAmong over event-sourced
        // timestamps), so the recovered re-run — with BOTH branches cached COMPLETED — re-resolves the SAME winner
        // the live race produced.
        assertThat(outcome.counters().getOrDefault(EFFECT_WINNER_PREFIX + STEP_BRANCH_B, 0))
                .as("the live winner (branchB, first completed) is re-resolved identically on replay")
                .isEqualTo(2);
        assertThat(outcome.counters().getOrDefault(EFFECT_WINNER_PREFIX + STEP_BRANCH_A, 0))
                .as("the later-completed branch never becomes the winner")
                .isEqualTo(0);
        // The categorization gap applies here too: branchA completed AFTER the live race resolved, so it was
        // unmatched live and matched on the recovered pass — same root as the allMatch pin.
        assertThat(outcome.counters().getOrDefault(EFFECT_MATCHED_PREFIX + STEP_BRANCH_B, 0)).isEqualTo(2);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void noneMatch_timedOutBranchPassesTheGuard_andVerdictIsReplayStable() {
        var outcome = CombinatorReplayScenario.noneMatchTimedOutBranchAcrossCrash(0L, "NM1");

        assertThat(outcome.runs()).isEqualTo(2);
        assertThat(outcome.terminalStatus()).isEqualTo(WorkflowStatus.COMPLETED);

        // EXPECTED GAP (the characterized C-2 foot-gun, now pinned): failure() is true only for FAILED, so a guard
        // meant to catch "no branch failed" PASSES over a TIMED_OUT branch — on both passes (the verdict itself is
        // replay-stable, because both branch states are durable terminals by snapshot time).
        assertThat(outcome.counters().getOrDefault(EFFECT_VERDICT_PREFIX + "true", 0))
                .as("EXPECTED GAP (C-2): noneMatch(failure) passes over a TIMED_OUT branch, on both passes")
                .isEqualTo(2);
        assertThat(outcome.counters().getOrDefault(EFFECT_VERDICT_PREFIX + "false", 0)).isEqualTo(0);
        // Categorization: neither branch matches the failure predicate, so both sit in unmatched() on both passes.
        assertThat(outcome.counters().getOrDefault(EFFECT_UNMATCHED_PREFIX + STEP_BRANCH_A, 0)).isEqualTo(2);
        assertThat(outcome.counters().getOrDefault(EFFECT_UNMATCHED_PREFIX + STEP_BRANCH_B, 0)).isEqualTo(2);
    }
}
