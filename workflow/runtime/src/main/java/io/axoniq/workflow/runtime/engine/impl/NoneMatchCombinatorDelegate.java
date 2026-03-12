package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.NoneMatchCombinator;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

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
public class NoneMatchCombinatorDelegate implements NoneMatchCombinator {

    private final WorkflowExecution workflowExecution;

    /**
     * Creates a new delegate backed by the given workflow state.
     *
     * @param workflowExecution the workflow execution.
     */
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
