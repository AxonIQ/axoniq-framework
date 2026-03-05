package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.AnyMatchCombinator;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;


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
        this.workflowState = workflowState;
    }

    /**
     * Predicate-based combinator — resolves when the <b>first</b> completed result matches the given predicate.
     * Replaces both the former {@code race()} and {@code anySuccessful()} methods:
     * <ul>
     *   <li>{@code anyMatch(state, WorkflowStepResult::isCompleted, ...)} — any terminal wins (old {@code race})</li>
     *   <li>{@code anyMatch(state, WorkflowStepResult::isSuccess, ...)} — first success wins (old {@code anySuccessful})</li>
     *   <li>Also works with {@code ::isFailure}, {@code ::isTimeout}, {@code ::isCanceled}</li>
     * </ul>
     *
     * <h3>Winner selection</h3>
     * <p>The composite blocks until at least one <em>completed</em> result matches the predicate.
     * That result becomes the winner and the composite delegates every state query
     * ({@code isSuccess()}, {@code isFailure()}, {@code result()}, {@code error()}, etc.) to it.</p>
     *
     * <h3>Fallback</h3>
     * <p>When all results complete but none matched the predicate, the first completed result
     * (by event-sourced timestamp) becomes the consolation winner. No loser cancellation occurs
     * in this fallback path since all results are already terminal.</p>
     *
     * <h3>Loser cancellation</h3>
     * <p>When a predicate match is found, every other result receives
     * {@code cancel("Superseded by <winnerStepName>")}.</p>
     *
     * <h3>Event-sourcing replay safety</h3>
     * <p>When multiple steps match the predicate before cancellation takes effect
     * (e.g. during event replay), the winner is determined by <b>event-sourced timestamps</b>
     * via {@link WorkflowState#firstCompletedAmong(Set)}, not by array order.</p>
     *
     * <h3>Completion</h3>
     * <p>{@code isCompleted()} is non-blocking and returns {@code true} when any completed result
     * matches the predicate <b>or</b> when all results have reached a terminal state.</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to <b>all</b> results.</p>
     *
     * @param predicate the predicate to match against completed results.
     * @param results   the competing step results.
     * @return a composite result that resolves to the first matching result, or fallback to first completed.
     */
    @Override
    @Nonnull
    public WorkflowStepResult anyMatch(@Nonnull Predicate<WorkflowStepResult> predicate,
                                       @Nonnull WorkflowStepResult... results) {

        return new WorkflowStepResult() {

            private WorkflowStepResult winner;

            private WorkflowStepResult resolveWinner() {
                if (winner != null) {
                    return winner;
                }

                do {
                    var matching = findFirstMatching(predicate);
                    if (matching.isPresent()) {
                        return setWinner(matching.get(), true);
                    }

                    if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                        var fallback = findFirstMatching(WorkflowStepResult::isCompleted);
                        if (fallback.isPresent()) {
                            return setWinner(fallback.get(), false);
                        }
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

                return winner;
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

            private Optional<WorkflowStepResult> findFirstMatching(Predicate<WorkflowStepResult> matchPredicate) {
                var matchedNames = Arrays.stream(results)
                                         .filter(WorkflowStepResult::isCompleted)
                                         .filter(matchPredicate)
                                         .map(WorkflowStepResult::getStepName)
                                         .collect(Collectors.toSet());
                if (matchedNames.isEmpty()) {
                    return Optional.empty();
                }
                return workflowState.firstCompletedAmong(matchedNames)
                                    .flatMap(name -> Arrays.stream(results)
                                                           .filter(r -> r.getStepName().equals(name))
                                                           .findFirst())
                                    .or(() -> Arrays.stream(results)
                                                    .filter(WorkflowStepResult::isCompleted)
                                                    .filter(matchPredicate)
                                                    .findFirst());
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
            public boolean isSuccess() {
                return resolveWinner().isSuccess();
            }

            @Override
            public boolean isFailure() {
                return resolveWinner().isFailure();
            }

            @Override
            public boolean isCanceled() {
                return resolveWinner().isCanceled();
            }

            @Override
            public boolean isTimeout() {
                return resolveWinner().isTimeout();
            }

            @Override
            public boolean await() {
                return resolveWinner().await();
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
