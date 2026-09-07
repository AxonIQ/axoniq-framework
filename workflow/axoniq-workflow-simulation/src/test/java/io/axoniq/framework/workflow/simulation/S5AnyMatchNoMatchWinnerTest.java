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

import io.axoniq.framework.workflow.simulation.scenarios.AnyMatchNoMatchWinnerScenario;
import io.axoniq.framework.workflow.simulation.workflow.AnyMatchNoMatchWorkflow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Settles the candidate finding <strong>S-5</strong> and extends INVARIANTS.md INV-14 ({@code CombinatorConsistency})
 * coverage: the {@code anyMatch} winner-result semantics on the no-predicate-match-but-all-branches-completed path. The
 * engine's {@code AnyMatchCombinatorDelegate.resolveWinner()} (runtime {@code AnyMatchCombinatorDelegate.java:114-116})
 * sets {@code fallback.orElse(results[0])} (the first completed branch) as the "winner" when no branch satisfies the
 * predicate but all completed, so the winner-derived accessors ({@code success()}/{@code result()}/{@code resultAs()})
 * read that branch even though no branch matched.
 * <p>
 * Drives the real engine through {@link AnyMatchNoMatchWorkflow}: three {@code execute} branches that all complete voting
 * {@code no} (none satisfies {@code VOTED_YES}). It characterizes BOTH sides:
 * <ul>
 *   <li><strong>the predicate-level decision is CORRECT</strong> (consistent with INV-14): {@code matched()} is empty and
 *       {@code unmatched()} holds all three branches — the categorized lists ARE the documented, correct way to read a
 *       no-match result, and they report it soundly;</li>
 *   <li><strong>the winner-derived accessors are the gap (S-5)</strong>: {@code success()} returns {@code true} (the
 *       fallback branch's COMPLETED status) and {@code result()} returns {@link AnyMatchNoMatchWorkflow#BRANCH_A branch
 *       A}'s payload — even though NO branch matched the predicate. An author reading {@code anyMatch(...).result()} on
 *       this path silently reads branch A's data as if it were "the" result.</li>
 * </ul>
 * Verdict: <strong>CONFIRMED</strong> (minor) — an INV-14 winner-result-semantics edge that only bites if an author reads
 * the winner-derived accessors on the no-match path (rather than {@code matched()}/{@code unmatched()}). Engine left
 * unchanged (POC test/docs-only rule); candidate fix: make the winner empty / the result accessors
 * {@code Optional.empty()} explicitly on the no-match path.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class S5AnyMatchNoMatchWinnerTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void anyMatch_noPredicateMatchButAllCompleted_winnerAccessorsReadFirstBranch_candidateFindingS5() {
        AnyMatchNoMatchWinnerScenario.Outcome outcome = AnyMatchNoMatchWinnerScenario.run(0L, "A");

        assertThat(outcome.reachedTerminal())
                .as("AnyMatchNoMatchWorkflow must reach a terminal (COMPLETED) workflow status")
                .isTrue();

        // THE CORRECT SIDE (consistent with INV-14): the predicate-level decision is sound — matched() is empty (no branch
        // satisfied the predicate) and all three completed-non-matching branches are in unmatched().
        assertThat(outcome.matchedEmpty())
                .as("INV-14 (sound): anyMatch.matched() is empty on the no-predicate-match path — the categorized "
                            + "decision is correct")
                .isTrue();
        assertThat(outcome.unmatchedSize())
                .as("all three completed-but-non-matching branches are in unmatched()")
                .isEqualTo(3);

        // THE GAP (S-5): the winner-derived accessors read the fallback (first completed) branch even though no branch
        // matched the predicate.
        assertThat(outcome.anySuccess())
                .as("S-5: anyMatch.success() returns true on the no-match path — it delegates to the fallback (first "
                            + "completed) branch's COMPLETED status, NOT to a predicate match")
                .isTrue();
        assertThat(outcome.winnerResultPresent())
                .as("S-5: anyMatch.result() is present (non-empty) on the no-match path — it returns the fallback "
                            + "branch's payload rather than Optional.empty()")
                .isTrue();
        assertThat(outcome.winnerBranchId())
                .as("S-5: anyMatch.result()/resultAs() read BRANCH_A's payload (the fallback.orElse(results[0]) "
                            + "first-completed branch) as if it were the winner, on the no-match-all-completed path")
                .isEqualTo(AnyMatchNoMatchWorkflow.BRANCH_A);
    }
}
