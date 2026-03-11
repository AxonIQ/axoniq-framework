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

import java.util.Optional;

/**
 * Combinator that waits for all results to complete before resolving.
 *
 * @see io.axoniq.workflow.runtime.engine.impl.AllCompletedCombinatorDelegate
 */
public interface AllCompletedCombinator {

    /**
     * Barrier semantics — waits for <b>every</b> result to reach a terminal state before the composite itself is
     * considered completed.
     *
     * <h3>Completion</h3>
     * <p>{@link WorkflowStepResult#isCompleted() isCompleted()} returns {@code true} only when
     * <b>all</b> results have completed (succeeded, failed, timed out, or been cancelled).
     * Until that point, blocking queries ({@code isSuccess()}, {@code isFailure()}, etc.) will block the calling
     * thread.</p>
     *
     * <h3>Success &amp; failure</h3>
     * <ul>
     *   <li>{@code isSuccess()} — {@code true} only when <b>every</b> result succeeded.</li>
     *   <li>{@code isFailure()} — {@code true} when <b>at least one</b> result failed.
     *       {@code error()} returns the error of the first failed result (array order).</li>
     *   <li>{@code isCanceled()} / {@code isTimeout()} — {@code true} when at least one result
     *       was cancelled / timed out.</li>
     * </ul>
     *
     * <h3>Result payload</h3>
     * <p>Because the composite represents multiple results, {@code result()} always returns
     * {@link Optional#empty()}. Access individual payloads through the original result references.</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to every result. There is no
     * automatic cancellation — if one result fails, the remaining results continue to run.</p>
     *
     * <h3>Comparison with other combinators</h3>
     * <table>
     *   <tr><th>Combinator</th><th>Resolves when</th><th>Cancels losers?</th></tr>
     *   <tr><td><b>all</b></td><td>All results complete</td><td>No</td></tr>
     *   <tr><td>anyMatch</td><td>First predicate match, or all complete</td><td>No (by default)</td></tr>
     *   <tr><td>noneMatch</td><td>All complete without a match, or short-circuit</td><td>No (by default)</td></tr>
     * </table>
     *
     * @param results the step results to combine.
     * @return a composite result that completes when all underlying results have completed.
     */
    @Nonnull
    WorkflowStepResult all(WorkflowStepResult... results);
}
