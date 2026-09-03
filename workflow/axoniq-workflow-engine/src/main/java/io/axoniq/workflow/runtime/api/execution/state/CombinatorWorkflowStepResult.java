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


import java.util.List;

/**
 * Result type that all combinator methods ({@link AnyMatchCombinator#anyMatch},
 * {@link NoneMatchCombinator#noneMatch}, {@link AllMatchCombinator#allMatch})
 * are required to return.
 *
 * <p>In addition to the standard {@link WorkflowStepResult} queries ({@code success()},
 * {@code failure()}, {@code result()}, etc.), this interface exposes the categorized
 * sub-results split by the combinator's predicate.</p>
 *
 * <p>Both {@code matched()} and {@code unmatched()} block until the combinator has resolved
 * (consistent with {@code success()}/{@code failure()}). The returned lists are unmodifiable.
 * Together they always cover <b>all</b> input results:
 * {@code matched().size() + unmatched().size() == all input results}.</p>
 *
 * <p>{@code matched()} is sorted by event-sourced timestamps (earliest first) and contains only
 * completed sub-results. {@code unmatched()} lists completed results (sorted by timestamp) first,
 * followed by not-yet-completed results in their original array order.</p>
 *
 * <h3>How {@code matched()} and {@code unmatched()} apply to each combinator</h3>
 *
 * <h4>{@code anyMatch(WorkflowStepResult::success, stepA, stepB, stepC)}</h4>
 * <p>Predicate: {@code success()}. Suppose stepA succeeded, stepB failed, stepC succeeded.
 * The combinator resolves on the first success (the winner), but {@code matched()} returns
 * <b>all</b> completed results that satisfy the predicate at the time of the call.
 * The winner is always the first element.</p>
 * <ul>
 *   <li>{@code matched()}   → [stepA, stepC] — all completed successes (winner first)</li>
 *   <li>{@code unmatched()} → [stepB]         — the unsuccessful steps</li>
 * </ul>
 *
 * <h4>{@code noneMatch(WorkflowStepResult::failure, failingStep, slowStep)}</h4>
 * <p>Predicate: {@code failure()}. failingStep fails in 500ms, slowStep is still running.</p>
 * <ul>
 *   <li>{@code matched()}   → [failingStep] — triggered the failure predicate (violator!)</li>
 *   <li>{@code unmatched()} → [slowStep]    — not yet completed, still in the list</li>
 * </ul>
 *
 * <h4>{@code allMatch(WorkflowStepResult::success, stepA, stepB, stepC)}</h4>
 * <p>Predicate: {@code success()}. Suppose stepA succeeded, stepB failed (short-circuits), stepC still running.</p>
 * <ul>
 *   <li>{@code matched()}   → [stepA]        — the successful steps</li>
 *   <li>{@code unmatched()} → [stepB, stepC] — the failed step (violator!) and the still-running step</li>
 * </ul>
 *
 * <h3>Short-circuit and non-completed results</h3>
 * <p>{@code matched()} and {@code unmatched()} reflect a <b>snapshot</b> of each result's state
 * at the time they are called — not the final outcome. Categorization is purely based on
 * {@code isCompleted() && predicate.test()} at that moment.</p>
 *
 * <p>Because combinators short-circuit, some results may not have completed yet when
 * {@code matched()}/{@code unmatched()} is called. Those not-yet-completed results always
 * appear in {@code unmatched()}, even if they would satisfy the predicate once they complete.</p>
 *
 * <p>Conversely, for {@code anyMatch}, the combinator resolves on the <em>first</em> match,
 * but other results may complete and satisfy the predicate before {@code matched()} is called.
 * In that case, {@code matched()} will contain multiple results — not just the winner.
 * The winner (used for {@code success()}/{@code failure()}/{@code result()} delegation) is
 * always the first element of {@code matched()}.</p>
 *
 * <p>Callers can always query individual results directly
 * (e.g. {@code r.failure()}, {@code r.isCompleted()}) regardless of which list they are in.</p>
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public interface CombinatorWorkflowStepResult extends WorkflowStepResult {

    /**
     * Returns all completed sub-results that satisfy the combinator's predicate at the time of
     * this call. For {@code anyMatch}, this may include more than just the winner if other results
     * completed and matched between resolution and this call — the winner is always the first element.
     * <p>Blocks until the combinator has resolved. The returned list is unmodifiable
     * and sorted by event-sourced timestamps (earliest first).</p>
     *
     * @return an unmodifiable list of matching sub-results, sorted by event-sourced timestamp.
     */
    List<WorkflowStepResult> matched();

    /**
     * Returns all other sub-results that are not in {@link #matched()}.
     * This may include results that have not yet completed, as well as completed results
     * that did not satisfy the predicate. In short-circuit scenarios, a result may appear here
     * even if its status would technically satisfy the predicate — it simply was not the one
     * that triggered the combinator's resolution.
     * <p>Blocks until the combinator has resolved. The returned list is unmodifiable.
     * Completed results appear first (sorted by event-sourced timestamps), followed by
     * not-yet-completed results in their original array order.</p>
     *
     * @return an unmodifiable list of non-matching sub-results.
     */
    List<WorkflowStepResult> unmatched();
}
