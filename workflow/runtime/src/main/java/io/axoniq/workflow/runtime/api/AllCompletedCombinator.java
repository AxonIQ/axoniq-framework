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


/**
 * Combinator that waits for all results to complete before resolving.
 *
 * @see io.axoniq.workflow.runtime.engine.impl.AllCompletedCombinatorDelegate
 */
public interface AllCompletedCombinator {

    /**
     * Combines the given results into a single composite that completes when all underlying results have completed.
     *
     * @param results the step results to combine
     * @return a composite {@link WorkflowStepResult} that completes when every result has reached a terminal state
     * @see io.axoniq.workflow.runtime.engine.impl.AllCompletedCombinatorDelegate#all(WorkflowStepResult...) for detailed semantics
     */
    @Nonnull
    WorkflowStepResult all(WorkflowStepResult... results);
}
