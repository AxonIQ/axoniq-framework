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


import java.util.function.Predicate;

/**
 * Combinator that resolves when the first completed result matches a given predicate.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface AnyMatchCombinator {

    /**
     * Predicate-based combinator — resolves when the <b>first</b> completed result matches the given predicate.
     * Replaces both the former {@code race()} and {@code anySuccessful()} methods:
     * <ul>
     *   <li>{@code anyMatch(state, WorkflowStepResult::isCompleted, ...)} — any terminal wins (old {@code race})</li>
     *   <li>{@code anyMatch(state, WorkflowStepResult::isSuccess, ...)} — first success wins (old {@code anySuccessful})</li>
     *   <li>Also works with {@code ::isFailure}, {@code ::isTimeout}, {@code ::isCanceled}</li>
     * </ul>
     *
     * <h3>Winner selection</h3>
     * <p>The composite blocks until at least one <em>completed</em> result matches the predicate.
     * That result becomes the winner and the composite delegates every state query
     * ({@code isSuccess()}, {@code isFailure()}, {@code result()}, {@code error()}, etc.) to it.</p>
     *
     * <h3>Fallback</h3>
     * <p>When all results complete but none matched the predicate, the first completed result
     * (by event-sourced timestamp) becomes the consolation winner.</p>
     *
     * <h3>Loser handling</h3>
     * <p>Losers are <b>not</b> automatically cancelled. The caller is responsible for cancelling
     * remaining results if desired.</p>
     *
     * <h3>Event-sourcing replay safety</h3>
     * <p>When multiple steps match the predicate before cancellation takes effect
     * (e.g. during event replay), the winner is determined by <b>event-sourced timestamps</b>, not by array order.</p>
     *
     * <h3>Completion</h3>
     * <p>{@code isCompleted()} is non-blocking and returns {@code true} when any completed result
     * matches the predicate <b>or</b> when all results have reached a terminal state.</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to <b>all</b> results.</p>
     *
     * <h3>Result categorization</h3>
     * <p>The returned {@link CombinatorWorkflowStepResult} provides {@code matched()} and {@code unmatched()}
     * to access categorized sub-results. {@code matched()} returns <b>all</b> completed results that
     * satisfy the predicate at the time of the call — not just the winner. Because other steps may
     * complete between resolution and calling {@code matched()}, the list can contain more than one
     * result. The winner (used for {@code success()}/{@code failure()}/{@code result()} delegation)
     * is always the first element.</p>
     *
     * <p>For {@code anyMatch(WorkflowStepResult::success, fastStep, slowStep)}
     * where fastStep succeeds and slowStep is still running:</p>
     * <ul>
     *   <li>{@code matched()}   → [fastStep] — completed results that satisfied {@code success()}</li>
     *   <li>{@code unmatched()} → [slowStep] — all other results, including those not yet completed</li>
     * </ul>
     *
     * @param predicate the predicate to match against completed results.
     * @param results   the competing step results.
     * @return a composite result that resolves to the first matching result, or fallback to first completed.
     */
    CombinatorWorkflowStepResult anyMatch(Predicate<WorkflowStepResult> predicate, WorkflowStepResult... results);

}
