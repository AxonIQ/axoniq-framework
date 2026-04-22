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
package io.axoniq.workflow.runtime.api.execution.state;

import jakarta.annotation.Nonnull;

import java.util.function.Predicate;

/**
 * Guard combinator that succeeds when all completed results match the predicate.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface AllMatchCombinator {

    /**
     * Guard combinator — ensures <b>all</b> completed results match the given predicate. Short-circuits on the first
     * non-match.
     *
     * <h3>Short-circuit (non-match found)</h3>
     * <p>When a completed result does <b>not</b> match the predicate, it becomes the "violator". The composite
     * delegates all state queries to the violating result. Remaining results are <b>not</b> automatically cancelled —
     * the caller is responsible for cancelling if desired.</p>
     *
     * <h3>Success (all complete, all matched)</h3>
     * <p>When all results complete and all match the predicate:
     * {@code isSuccess()=true}, {@code result()=empty}, {@code isFailure()=false}.</p>
     *
     * <h3>Completion</h3>
     * <p>{@code isCompleted()} is non-blocking and returns {@code true} when any completed result
     * does <b>not</b> match the predicate <b>or</b> when all results have reached a terminal state.</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to <b>all</b> results.</p>
     *
     * <h3>Summary</h3>
     * <p>The structural inverse of {@code .noneMatch()}: succeeds when <b>all</b> match, but
     * short-circuits on first non-match to avoid cascading errors.</p>
     *
     * <h3>Comparison with other combinators</h3>
     * <table>
     *   <tr><th>Combinator</th><th>Resolves when</th><th>Cancels losers?</th></tr>
     *   <tr><td><b>allMatch</b></td><td>All complete and all match, or short-circuit on first non-match</td><td>No</td></tr>
     *   <tr><td>anyMatch</td><td>First predicate match, or all complete</td><td>No (by default)</td></tr>
     *   <tr><td>noneMatch</td><td>All complete without a match, or short-circuit on first match</td><td>No (by default)</td></tr>
     * </table>
     *
     * <h3>Result categorization</h3>
     * <p>The returned {@link CombinatorWorkflowStepResult} provides {@code matched()} and {@code unmatched()}
     * to access categorized sub-results. For {@code allMatch(WorkflowStepResult::success, successStep, failingStep, slowStep)}
     * where successStep succeeded, failingStep failed (short-circuits), and slowStep is still running:</p>
     * <ul>
     *   <li>{@code matched()}   → [successStep]          — results that satisfied {@code success()}</li>
     *   <li>{@code unmatched()} → [failingStep, slowStep] — all other results, including violators and not-yet-completed</li>
     * </ul>
     *
     * @param predicate the predicate that all results should match.
     * @param results   the step results to guard.
     * @return a composite result which succeeds when all results match, or short-circuits on the first non-match.
     */
    @Nonnull
    CombinatorWorkflowStepResult allMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                          WorkflowStepResult... results);
}
