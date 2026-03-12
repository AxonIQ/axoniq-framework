package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.AnyMatchCombinator;
import io.axoniq.workflow.runtime.api.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;


/**
 * Default implementation of {@link AnyMatchCombinator}.
 *
 * @see AnyMatchCombinator
 */
public class AnyMatchCombinatorDelegate implements AnyMatchCombinator {

    private final WorkflowState workflowState;

    /**
     * Creates a new delegate backed by the given workflow state.
     *
     * @param workflowState the workflow state used for event-sourced timestamp resolution
     */
    public AnyMatchCombinatorDelegate(@Nonnull WorkflowState workflowState) {
        this.workflowState = Objects.requireNonNull(workflowState, "workflowState must not be null");
    }

    /** {@inheritDoc} */
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

                var matching = CombinatorSupport.findFirstByPredicate(results, predicate, workflowState);
                if (matching.isPresent()) {
                    return setWinner(matching.get(), false);
                }

                if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                    var fallback = CombinatorSupport.findFirstByPredicate(
                            results, WorkflowStepResult::isCompleted, workflowState);
                    if (fallback.isPresent()) {
                        return setWinner(fallback.get(), false);
                    }
                }

                return awaitAndResolve();
            }

            private WorkflowStepResult awaitAndResolve() {
                try {
                    workflowState.awaitStateChange(s ->
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

                var matchAfterWait = CombinatorSupport.findFirstByPredicate(results, predicate, workflowState);
                if (matchAfterWait.isPresent()) {
                    return setWinner(matchAfterWait.get(), false);
                }
                var fallback = CombinatorSupport.findFirstByPredicate(
                        results, WorkflowStepResult::isCompleted, workflowState);
                return setWinner(fallback.orElse(results[0]), false);
            }

            private WorkflowStepResult setWinner(WorkflowStepResult winner, boolean cancelLosers) {
                this.winner = winner;
                if (cancelLosers) {
                    for (WorkflowStepResult r : results) {
                        if (r != winner) {
                            r.cancel("Superseded by " + winner.getStepName());
                        }
                    }
                }
                return winner;
            }

            private CombinatorSupport.Categories categories() {
                if (categories == null) {
                    categories = CombinatorSupport.computeCategories(results, predicate, workflowState);
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
