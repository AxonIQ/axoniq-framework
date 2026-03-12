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
 * (consistent with {@code success()}/{@code failure()}). The returned lists are unmodifiable,
 * contain only completed sub-results, and are sorted by event-sourced timestamps (earliest first).</p>
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
 * <h4>{@code noneMatch(WorkflowStepResult::failure, stepA, stepB, stepC)}</h4>
 * <p>Predicate: {@code failure()}. Suppose stepA succeeded, stepB failed (short-circuits).</p>
 * <ul>
 *   <li>{@code matched()}   → [stepB]  — the failed step (violator!)</li>
 *   <li>{@code unmatched()} → [stepA]  — clean steps completed before short-circuit</li>
 * </ul>
 *
 * <h4>{@code allMatch(WorkflowStepResult::success, stepA, stepB, stepC)}</h4>
 * <p>Predicate: {@code success()}. Suppose stepA succeeded, stepB failed (short-circuits).</p>
 * <ul>
 *   <li>{@code matched()}   → [stepA]  — the successful steps</li>
 *   <li>{@code unmatched()} → [stepB]  — the failed step (violator!)</li>
 * </ul>
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface CombinatorWorkflowStepResult extends WorkflowStepResult {

    /**
     * Returns completed sub-results that satisfy the combinator's predicate.
     * <p>Blocks until the combinator has resolved. The returned list is unmodifiable
     * and sorted by event-sourced timestamps (earliest first).</p>
     *
     * @return an unmodifiable list of matching sub-results, sorted by event-sourced timestamp.
     */
    @Nonnull
    List<WorkflowStepResult> matched();

    /**
     * Returns completed sub-results that do <b>not</b> satisfy the combinator's predicate.
     * <p>Blocks until the combinator has resolved. The returned list is unmodifiable
     * and sorted by event-sourced timestamps (earliest first).</p>
     *
     * @return an unmodifiable list of non-matching sub-results, sorted by event-sourced timestamp.
     */
    @Nonnull
    List<WorkflowStepResult> unmatched();
}
