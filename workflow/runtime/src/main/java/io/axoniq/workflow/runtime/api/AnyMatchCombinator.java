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
 * Combinator that resolves when the first completed result matches a given predicate.
 *
 * @see io.axoniq.workflow.runtime.engine.impl.AnyMatchCombinatorDelegate
 */
public interface AnyMatchCombinator {

    /**
     * Combines the given results into a composite that resolves to the first completed result matching the predicate.
     * When a match is found, all other (losing) results are automatically cancelled. If no result matches but all
     * complete, falls back to the first completed result without cancelling any.
     *
     * @param predicate the predicate to match against completed results
     * @param results   the competing step results
     * @return a composite {@link WorkflowStepResult} that resolves to the winning match, or falls back to the first completed result
     * @see io.axoniq.workflow.runtime.engine.impl.AnyMatchCombinatorDelegate#anyMatch(Predicate, WorkflowStepResult...) for detailed semantics
     */
    @Nonnull
    WorkflowStepResult anyMatch(@Nonnull Predicate<WorkflowStepResult> predicate, WorkflowStepResult... results);

}
