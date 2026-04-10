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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.state.NoneMatchCombinator;
import io.axoniq.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;


/**
 * Default implementation of {@link NoneMatchCombinator}.
 *
 * @author Stefan Dragisic
 * @see NoneMatchCombinator
 * @since 1.0.0
 */
@Internal
public class NoneMatchCombinatorDelegate implements NoneMatchCombinator {

    private final WorkflowExecution workflowExecution;

    /**
     * Creates a new delegate backed by the given workflow state.
     *
     * @param workflowExecution the workflow execution.
     */
    @Internal
    public NoneMatchCombinatorDelegate(@Nonnull WorkflowExecution workflowExecution) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "Workflow Execution must not be null");
    }

    @Nonnull
    public CombinatorWorkflowStepResult noneMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return new CombinatorWorkflowStepResult() {

            private WorkflowStepResult violator;
            private boolean allCompletedNoneMatched;
            private CombinatorSupport.Categories categories;

            private void resolveViolator() {
                if (violator != null || allCompletedNoneMatched) {
                    return;
                }

                var matched = CombinatorSupport.findFirstByPredicate(results, predicate, workflowExecution.state());
                if (matched.isPresent()) {
                    violator = matched.get();
                    return;
                }

                if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                    allCompletedNoneMatched = true;
                    return;
                }

                awaitAndResolve();
            }

            private void awaitAndResolve() {
                try {
                    workflowExecution.awaitStateChange(s ->
                                                               Arrays.stream(results)
                                                                     .filter(WorkflowStepResult::isCompleted)
                                                                     .anyMatch(predicate)
                                                                       || Arrays.stream(results)
                                                                                .allMatch(WorkflowStepResult::isCompleted)
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while awaiting noneMatch result", e);
                }

                var matchAfterWait = CombinatorSupport.findFirstByPredicate(results,
                                                                            predicate,
                                                                            workflowExecution.state());
                if (matchAfterWait.isPresent()) {
                    violator = matchAfterWait.get();
                } else {
                    allCompletedNoneMatched = true;
                }
            }

            private CombinatorSupport.Categories categories() {
                if (categories == null) {
                    categories = CombinatorSupport.computeCategories(results, predicate, workflowExecution.state());
                }
                return categories;
            }

            @Override
            @Nonnull
            public List<WorkflowStepResult> matched() {
                resolveViolator();
                return categories().matched();
            }

            @Override
            @Nonnull
            public List<WorkflowStepResult> unmatched() {
                resolveViolator();
                return categories().unmatched();
            }

            @Override
            @Nonnull
            public String getStepName() {
                return "noneMatch(" + String.join(", ",
                                                  Arrays.stream(results).map(WorkflowStepResult::getStepName).toList())
                        + ")";
            }

            @Override
            public boolean isCompleted() {
                return Arrays.stream(results)
                             .filter(WorkflowStepResult::isCompleted)
                             .anyMatch(predicate)
                        || Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted);
            }

            @Override
            @Nonnull
            public <T> Optional<T> result() {
                resolveViolator();
                if (violator != null) {
                    return violator.result();
                }
                return Optional.empty();
            }

            @Override
            @Nonnull
            public Optional<StepFailedException> error() {
                resolveViolator();
                if (violator != null) {
                    return violator.error();
                }
                return Optional.empty();
            }

            @Override
            public boolean success() {
                resolveViolator();
                return violator == null && allCompletedNoneMatched;
            }

            @Override
            public boolean failure() {
                resolveViolator();
                return violator != null;
            }

            @Override
            public boolean canceled() {
                resolveViolator();
                if (violator != null) {
                    return violator.canceled();
                }
                return false;
            }

            @Override
            public boolean timeout() {
                resolveViolator();
                if (violator != null) {
                    return violator.timeout();
                }
                return false;
            }

            @Override
            public void await() {
                resolveViolator();
            }

            @Override
            public void cancel() {
                Arrays.stream(results).forEach(WorkflowStepResult::cancel);
            }

            @Override
            public void cancel(@Nonnull String reason) {
                Arrays.stream(results).forEach(r -> r.cancel(reason));
            }
        };
    }
}
