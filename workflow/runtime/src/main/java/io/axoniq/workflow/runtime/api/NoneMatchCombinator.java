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

import java.util.function.Predicate;

/**
 * Guard combinator that succeeds when no completed result matches the predicate.
 *
 * @see io.axoniq.workflow.runtime.engine.impl.NoneMatchCombinatorDelegate
 */
public interface NoneMatchCombinator {

    /**
     * Combines the given results into a composite that succeeds when no result matches, or short-circuits on the first
     * match. When a violating result is found, all remaining results are cancelled. If all results complete without
     * a match, the composite succeeds.
     *
     * @param predicate the predicate that no result should match
     * @param results   the step results to guard
     * @return a composite {@link WorkflowStepResult} that succeeds when none match, or delegates to the violating result
     * @see io.axoniq.workflow.runtime.engine.impl.NoneMatchCombinatorDelegate#noneMatch(Predicate, WorkflowStepResult...) for detailed semantics
     */
    @Nonnull
    WorkflowStepResult noneMatch(@Nonnull Predicate<WorkflowStepResult> predicate, WorkflowStepResult... results);

}
