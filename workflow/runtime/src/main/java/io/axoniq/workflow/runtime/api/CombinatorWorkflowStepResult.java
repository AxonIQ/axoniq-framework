/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api;

import jakarta.annotation.Nonnull;

import java.util.List;

/**
 * Extended result type returned by all three combinator methods
 * ({@link AnyMatchCombinator#anyMatch}, {@link NoneMatchCombinator#noneMatch},
 * {@link AllMatchCombinator#allMatch}).
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
 * <p>Predicate: {@code success()}. Suppose stepA succeeded, stepB failed, stepC succeeded.</p>
 * <ul>
 *   <li>{@code matched()}   → [stepA, stepC] — the successful steps (winners)</li>
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
 * <h3>Short-circuit and race conditions</h3>
 * <p>{@code matched()} contains only the result(s) that <em>triggered</em> the predicate match.
 * Results in {@code unmatched()} may have any status — including a status that would satisfy the
 * predicate — if they were not the ones that triggered the combinator's resolution.</p>
 *
 * <p>Example: {@code noneMatch(WorkflowStepResult::failure, failingStep1, failingStep2)} where both
 * steps fail, but failingStep1 had the earlier event-sourced timestamp:</p>
 * <ul>
 *   <li>{@code matched()}   → [failingStep1] — triggered the short-circuit</li>
 *   <li>{@code unmatched()} → [failingStep2] — also failed, but was not the trigger.
 *       {@code failingStep2.failure()} still returns {@code true}.</li>
 * </ul>
 *
 * <p>This is not a bug — the combinator short-circuits on the <em>first</em> match. Subsequent
 * results end up in {@code unmatched()} regardless of their actual status. Callers can always
 * query individual results directly (e.g. {@code r.failure()}, {@code r.isCompleted()}).</p>
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface CombinatorWorkflowStepResult extends WorkflowStepResult {

    /**
     * Returns sub-results that triggered/satisfied the combinator's predicate.
     * Only completed results can appear here.
     * <p>Blocks until the combinator has resolved. The returned list is unmodifiable
     * and sorted by event-sourced timestamps (earliest first).</p>
     *
     * @return an unmodifiable list of matching sub-results, sorted by event-sourced timestamp.
     */
    @Nonnull
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
    @Nonnull
    List<WorkflowStepResult> unmatched();
}
