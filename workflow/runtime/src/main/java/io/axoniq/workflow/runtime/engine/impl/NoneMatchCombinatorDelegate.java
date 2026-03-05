package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.NoneMatchCombinator;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * Default implementation of {@link NoneMatchCombinator}.
 *
 * @see NoneMatchCombinator
 */
public class NoneMatchCombinatorDelegate implements NoneMatchCombinator {

    private final WorkflowState workflowState;

    /**
     * Creates a new delegate backed by the given workflow state.
     *
     * @param workflowState the workflow state used for event-sourced timestamp resolution
     */
    public NoneMatchCombinatorDelegate(@Nonnull WorkflowState workflowState) {
        this.workflowState = workflowState;
    }

    /**
     * Guard combinator — ensures <b>no</b> completed result matches the given predicate. Short-circuits on the first
     * match.
     *
     * <h3>Short-circuit (match found)</h3>
     * <p>When a completed result matches the predicate, it becomes the "violator". The composite
     * delegates all state queries to the violating result and cancels remaining results with
     * {@code cancel("Disqualified by <violatorStepName>")}.</p>
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
     * @param predicate the predicate that no result should match.
     * @param results   the step results to guard.
     * @return a composite result that succeeds when no result matches, or short-circuits on first match.
     */
    @Nonnull
    public WorkflowStepResult noneMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                        WorkflowStepResult... results) {
        return new WorkflowStepResult() {

            private WorkflowStepResult violator;
            private boolean allCompletedNoneMatched;

            private void resolve() {
                if (violator != null || allCompletedNoneMatched) {
                    return;
                }

                do {
                    var matched = findFirstViolator();
                    if (matched.isPresent()) {
                        setViolator(matched.get());
                        return;
                    }

                    if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                        allCompletedNoneMatched = true;
                        return;
                    }

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
                        break;
                    }
                } while (true);
            }

            private void setViolator(WorkflowStepResult v) {
                violator = v;
                for (WorkflowStepResult r : results) {
                    if (r != v) {
                        r.cancel("Disqualified by " + v.getStepName());
                    }
                }
            }

            private Optional<WorkflowStepResult> findFirstViolator() {
                var violatorNames = Arrays.stream(results)
                                          .filter(WorkflowStepResult::isCompleted)
                                          .filter(predicate)
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
                                                    .filter(predicate)
                                                    .findFirst());
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
                resolve();
                if (violator != null) {
                    return violator.result();
                }
                return Optional.empty();
            }

            @Override
            @Nonnull
            public Optional<StepFailedException> error() {
                resolve();
                if (violator != null) {
                    return violator.error();
                }
                return Optional.empty();
            }

            @Override
            public boolean isSuccess() {
                resolve();
                return violator == null && allCompletedNoneMatched;
            }

            @Override
            public boolean isFailure() {
                resolve();
                return violator != null;
            }

            @Override
            public boolean isCanceled() {
                resolve();
                if (violator != null) {
                    return violator.isCanceled();
                }
                return false;
            }

            @Override
            public boolean isTimeout() {
                resolve();
                if (violator != null) {
                    return violator.isTimeout();
                }
                return false;
            }

            @Override
            public boolean await() {
                resolve();
                return violator == null;
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
