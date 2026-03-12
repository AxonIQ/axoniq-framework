package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.AllMatchCombinator;
import io.axoniq.workflow.runtime.api.CombinatorWorkflowStepResult;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Default implementation of {@link AllMatchCombinator}.
 *
 * @see AllMatchCombinator
 */
public class AllMatchCombinatorDelegate implements AllMatchCombinator {

    private final WorkflowState workflowState;

    /**
     * Creates a new delegate backed by the given workflow state.
     *
     * @param workflowState the workflow state used for event-sourced timestamp resolution
     */
    public AllMatchCombinatorDelegate(@Nonnull WorkflowState workflowState) {
        this.workflowState = Objects.requireNonNull(workflowState, "workflowState must not be null");
    }

    /** {@inheritDoc} */
    @Nonnull
    public CombinatorWorkflowStepResult allMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                                  WorkflowStepResult... results) {
        return new CombinatorWorkflowStepResult() {

            private WorkflowStepResult violator;
            private boolean allCompletedAllMatched;
            private List<WorkflowStepResult> cachedMatched;
            private List<WorkflowStepResult> cachedUnmatched;

            private void resolveViolator() {
                if (violator != null || allCompletedAllMatched) {
                    return;
                }

                var matched = findFirstViolator();
                if (matched.isPresent()) {
                    setViolator(matched.get());
                    return;
                }

                if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                    allCompletedAllMatched = true;
                    return;
                }

                awaitAndResolve();
            }

            private void awaitAndResolve() {
                try {
                    workflowState.awaitStateChange(s ->
                                                           Arrays.stream(results)
                                                                 .filter(WorkflowStepResult::isCompleted)
                                                                 .anyMatch(predicate.negate())
                                                                   || Arrays.stream(results)
                                                                            .allMatch(WorkflowStepResult::isCompleted)
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while awaiting allMatch result", e);
                }

                var matchAfterWait = findFirstViolator();
                if (matchAfterWait.isPresent()) {
                    setViolator(matchAfterWait.get());
                } else {
                    allCompletedAllMatched = true;
                }
            }

            private void setViolator(WorkflowStepResult v) {
                violator = v;
            }

            private Optional<WorkflowStepResult> findFirstViolator() {
                var violatorNames = Arrays.stream(results)
                                          .filter(WorkflowStepResult::isCompleted)
                                          .filter(predicate.negate())
                                          .map(WorkflowStepResult::getStepName)
                                          .collect(Collectors.toSet());
                if (violatorNames.isEmpty()) {
                    return Optional.empty();
                }
                return workflowState.firstCompletedAmong(violatorNames)
                                    .flatMap(name -> Arrays.stream(results)
                                                           .filter(r -> r.getStepName().equals(name))
                                                           .findFirst())
                                    .or(() -> Arrays.stream(results)
                                                    .filter(WorkflowStepResult::isCompleted)
                                                    .filter(predicate.negate())
                                                    .findFirst());
            }

            private void computeCategories() {
                if (cachedMatched != null) {
                    return;
                }
                var completed = Arrays.stream(results)
                                      .filter(WorkflowStepResult::isCompleted)
                                      .toList();

                var matchedResults = completed.stream().filter(predicate).toList();
                var unmatchedResults = completed.stream().filter(predicate.negate()).toList();

                cachedMatched = sortByEventSourcedTimestamp(matchedResults);
                cachedUnmatched = sortByEventSourcedTimestamp(unmatchedResults);
            }

            private List<WorkflowStepResult> sortByEventSourcedTimestamp(List<WorkflowStepResult> items) {
                if (items.isEmpty()) {
                    return List.of();
                }
                var names = items.stream()
                                 .map(WorkflowStepResult::getStepName)
                                 .collect(Collectors.toSet());
                var sortedNames = workflowState.sortedCompletedAmong(names);
                if (!sortedNames.isEmpty()) {
                    return Collections.unmodifiableList(
                            sortedNames.stream()
                                       .flatMap(name -> items.stream()
                                                             .filter(r -> r.getStepName().equals(name)))
                                       .toList());
                }
                return Collections.unmodifiableList(items);
            }

            @Override
            @Nonnull
            public List<WorkflowStepResult> matched() {
                resolveViolator();
                computeCategories();
                return cachedMatched;
            }

            @Override
            @Nonnull
            public List<WorkflowStepResult> unmatched() {
                resolveViolator();
                computeCategories();
                return cachedUnmatched;
            }

            @Override
            @Nonnull
            public String getStepName() {
                return "allMatch(" + String.join(", ",
                                                  Arrays.stream(results).map(WorkflowStepResult::getStepName).toList())
                        + ")";
            }

            @Override
            public boolean isCompleted() {
                return Arrays.stream(results)
                             .filter(WorkflowStepResult::isCompleted)
                             .anyMatch(predicate.negate())
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
                return violator == null && allCompletedAllMatched;
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
