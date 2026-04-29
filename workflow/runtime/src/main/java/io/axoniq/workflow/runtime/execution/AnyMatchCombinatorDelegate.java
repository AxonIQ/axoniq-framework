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

package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.AnyMatchCombinator;
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult;
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
 * Default implementation of {@link AnyMatchCombinator}.
 *
 * @author Stefan Dragisic
 * @see AnyMatchCombinator
 * @since 1.0.0
 */
@Internal
public class AnyMatchCombinatorDelegate implements AnyMatchCombinator {

    private final WorkflowExecution workflowExecution;

    /**
     * Creates a new delegate backed by the given workflow state.
     *
     * @param workflowExecution the workflow execution.
     */
    @Internal
    public AnyMatchCombinatorDelegate(@Nonnull WorkflowExecution workflowExecution) {
        this.workflowExecution = Objects.requireNonNull(workflowExecution, "state must not be null");
    }

    @Override
    @Nonnull
    public CombinatorWorkflowStepResult anyMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                 @Nonnull WorkflowStepResult... results) {

        return new CombinatorWorkflowStepResult() {

            private WorkflowStepResult winner;
            private CombinatorSupport.Categories categories;

            private WorkflowStepResult resolveWinner() {
                if (winner != null) {
                    return winner;
                }

                var matching = CombinatorSupport.findFirstByPredicate(results, predicate, workflowExecution.state());
                if (matching.isPresent()) {
                    return setWinner(matching.get());
                }

                if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                    var fallback = CombinatorSupport.findFirstByPredicate(
                            results, WorkflowStepResult::isCompleted, workflowExecution.state());
                    if (fallback.isPresent()) {
                        return setWinner(fallback.get());
                    }
                }

                return awaitAndResolve();
            }

            private WorkflowStepResult awaitAndResolve() {
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
                    throw new IllegalStateException("Interrupted while awaiting anyMatch result", e);
                }

                var matchAfterWait = CombinatorSupport.findFirstByPredicate(results,
                                                                            predicate,
                                                                            workflowExecution.state());
                if (matchAfterWait.isPresent()) {
                    return setWinner(matchAfterWait.get());
                }
                var fallback = CombinatorSupport.findFirstByPredicate(
                        results, WorkflowStepResult::isCompleted, workflowExecution.state());
                return setWinner(fallback.orElse(results[0]));
            }

            private WorkflowStepResult setWinner(WorkflowStepResult winner) {
                this.winner = winner;
                return winner;
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
                resolveWinner();
                return categories().matched();
            }

            @Override
            @Nonnull
            public List<WorkflowStepResult> unmatched() {
                resolveWinner();
                return categories().unmatched();
            }

            @Override
            @Nonnull
            public String getStepName() {
                return "anyMatch(" + String.join(", ",
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
                return resolveWinner().result();
            }

            @Override
            @Nonnull
            public Optional<StepFailedException> error() {
                return resolveWinner().error();
            }

            @Override
            public boolean success() {
                return resolveWinner().success();
            }

            @Override
            public boolean failure() {
                return resolveWinner().failure();
            }

            @Override
            public boolean canceled() {
                return resolveWinner().canceled();
            }

            @Override
            public boolean timeout() {
                return resolveWinner().timeout();
            }

            @Override
            public void await() {
                resolveWinner().await();
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
