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
 * Guard combinator that succeeds when no completed result matches the predicate.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public interface NoneMatchCombinator {

    /**
     * Guard combinator — ensures <b>no</b> completed result matches the given predicate. Short-circuits on the first
     * match.
     *
     * <h3>Short-circuit (match found)</h3>
     * <p>When a completed result matches the predicate, it becomes the "violator". The composite
     * delegates all state queries to the violating result. Remaining results are <b>not</b> automatically cancelled —
     * the caller is responsible for cancelling if desired.</p>
     *
     * <h3>Success (all complete, none matched)</h3>
     * <p>When all results complete without any matching the predicate:
     * {@code isSuccess()=true}, {@code result()=empty}, {@code isFailure()=false}.</p>
     *
     * <h3>Completion</h3>
     * <p>{@code isCompleted()} is non-blocking and returns {@code true} when any completed result
     * matches the predicate <b>or</b> when all results have reached a terminal state.</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to <b>all</b> results.</p>
     *
     * <h3>Summary</h3>
     * <p>Similar to {@code .allMatch()} but with guard semantics: succeeds when <b>none</b> match, but
     * short-circuits on first match to avoid cascading errors.</p>
     *
     * <h3>Result categorization</h3>
     * <p>The returned {@link CombinatorWorkflowStepResult} provides {@code matched()} and {@code unmatched()}
     * to access categorized sub-results. For {@code noneMatch(WorkflowStepResult::failure, failingStep, slowStep)}
     * where failingStep fails (short-circuits) and slowStep is still running:</p>
     * <ul>
     *   <li>{@code matched()}   → [failingStep] — results that satisfied {@code failure()} (violators!)</li>
     *   <li>{@code unmatched()} → [slowStep]    — all other results, including those not yet completed</li>
     * </ul>
     *
     * @param predicate the predicate that no result should match.
     * @param results   the step results to guard.
     * @return a composite result which succeeds when no result matches, or short-circuits on the first match.
     */
    CombinatorWorkflowStepResult noneMatch(Predicate<WorkflowStepResult> predicate,
                                           WorkflowStepResult... results);
}
