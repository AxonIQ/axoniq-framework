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
package io.axoniq.workflow.runtime.engine.result;

import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.execution.WorkflowState;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility containing {@link WorkflowStepResult} factory methods.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowStepResults {

    private WorkflowStepResults() {
        // util class
    }

    public static WorkflowStepResult stateBased(@Nonnull String stepName, WorkflowState workflowState) {
        return new StateBasedWorkflowStepResult(stepName, () -> {
            workflowState.awaitStateChange(s -> true);
            return null;
        }, workflowState);
    }

    /**
     * Constructs completed result.
     *
     * @param payload payload of the result, might be null.
     * @return completed step result.
     */
    @Nonnull
    public static WorkflowStepResult completed(@Nonnull String stepName, @Nullable Object payload) {
        return new CompletedWorkflowStepResult(stepName, payload, null, null, false);
    }

    /**
     * Constructs failed result.
     *
     * @param error failure causing error.
     * @return failed result.
     */
    @Nonnull
    public static WorkflowStepResult failed(@Nonnull String stepName, @Nonnull Throwable error) {
        return new CompletedWorkflowStepResult(stepName,
                                               null,
                                               Objects.requireNonNull(error, "Error must be provided"),
                                               null,
                                               false);
    }

    /**
     * Constructs cancelled result.
     *
     * @return cancelled result.
     */
    @Nonnull
    public static WorkflowStepResult cancelled(@Nonnull String stepName) {
        return new CompletedWorkflowStepResult(stepName, null, null, null, true);
    }

    /**
     * Constructs timed out result.
     *
     * @param timeout timeout duration.
     * @return timed out result.
     */
    @Nonnull
    public static WorkflowStepResult timeout(@Nonnull String stepName, @Nonnull Duration timeout) {
        return new CompletedWorkflowStepResult(stepName,
                                               null,
                                               null,
                                               Objects.requireNonNull(timeout, "Timeout must be provided"),
                                               false);
    }

    /**
     * Barrier semantics — waits for <b>every</b> result to reach a terminal state before the
     * composite itself is considered completed. Equivalent to JS {@code Promise.all()}.
     *
     * <h3>Completion</h3>
     * <p>{@link WorkflowStepResult#isCompleted() isCompleted()} returns {@code true} only when
     * <b>all</b> results have completed (succeeded, failed, timed out, or been cancelled).
     * Until that point, blocking queries ({@code isSuccess()}, {@code isFailure()}, etc.) will
     * block the calling thread.</p>
     *
     * <h3>Success &amp; failure</h3>
     * <ul>
     *   <li>{@code isSuccess()} — {@code true} only when <b>every</b> result succeeded.</li>
     *   <li>{@code isFailure()} — {@code true} when <b>at least one</b> result failed.
     *       {@code error()} returns the error of the first failed result (array order).</li>
     *   <li>{@code isCanceled()} / {@code isTimeout()} — {@code true} when at least one result
     *       was cancelled / timed out.</li>
     * </ul>
     *
     * <h3>Result payload</h3>
     * <p>Because the composite represents multiple results, {@code result()} always returns
     * {@link Optional#empty()}. Access individual payloads through the original result references.</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to every result. There is no
     * automatic cancellation — if one result fails, the remaining results continue to run.</p>
     *
     * <h3>Comparison with other combinators</h3>
     * <table>
     *   <tr><th>Combinator</th><th>Resolves when</th><th>Cancels losers?</th></tr>
     *   <tr><td><b>all</b></td><td>All results complete</td><td>No</td></tr>
     *   <tr><td>{@link #race}</td><td>First terminal result</td><td>Yes</td></tr>
     *   <tr><td>{@link #anySuccessful}</td><td>First success, or all fail</td><td>Yes (on success)</td></tr>
     * </table>
     *
     * @param results the step results to combine.
     * @return a composite result that completes when all underlying results have completed.
     * @see #race(WorkflowState, WorkflowStepResult...)
     * @see #anySuccessful(WorkflowState, WorkflowStepResult...)
     */
    @Nonnull
    public static WorkflowStepResult all(WorkflowStepResult... results) {
        return new WorkflowStepResult() {

            @Override
            @Nonnull
            public String getStepName() {
                return "all(" + String.join(", ", Arrays.stream(results).map(WorkflowStepResult::getStepName).toList())
                        + ")";
            }

            @Override
            public boolean isCompleted() {
                return Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted);
            }

            @Override
            @Nonnull
            public <T> Optional<T> result() {
                return Optional.empty();
            }

            @Override
            @Nonnull
            public Optional<StepFailedException> error() {
                return Arrays.stream(results).filter(WorkflowStepResult::isFailure).findFirst().flatMap(
                        WorkflowStepResult::error);
            }

            @Override
            public boolean isSuccess() {
                return Arrays.stream(results).allMatch(WorkflowStepResult::isSuccess);
            }

            @Override
            public boolean isFailure() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::isFailure);
            }

            @Override
            public boolean isCanceled() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::isCanceled);
            }

            @Override
            public boolean isTimeout() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::isTimeout);
            }

            @Override
            public boolean await() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::await);
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

    /**
     * Race semantics — the first result to reach <b>any</b> terminal state (success, failure,
     * timeout, or cancellation) becomes the "winner". All remaining results are automatically
     * cancelled. Equivalent to JS {@code Promise.race()}.
     *
     * <h3>Winner selection</h3>
     * <p>The composite blocks until at least one result reaches a terminal state.
     * That result becomes the winner and the composite delegates every state query
     * ({@code isSuccess()}, {@code isFailure()}, {@code result()}, {@code error()}, etc.)
     * to it. Note that the winner may be a <em>failed</em> or <em>timed-out</em> result —
     * whatever finishes first wins, regardless of outcome.</p>
     *
     * <h3>Loser cancellation</h3>
     * <p>Once the winner is determined, every other result receives
     * {@code cancel("Superseded by <winnerStepName>")}. Results that have already
     * reached a terminal state will ignore the cancellation.</p>
     *
     * <h3>Event-sourcing replay safety</h3>
     * <p>When multiple steps complete before cancellation takes effect (e.g. during event replay),
     * the winner is determined by <b>event-sourced timestamps</b> via
     * {@link WorkflowState#firstCompletedAmong(Set)}, not by array order. This guarantees
     * deterministic winner selection across replays.</p>
     *
     * <h3>Completion</h3>
     * <p>{@code isCompleted()} is non-blocking and returns {@code true} as soon as any result
     * has reached a terminal state.</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to <b>all</b> results,
     * including the winner (if already resolved).</p>
     *
     * <h3>Comparison with other combinators</h3>
     * <table>
     *   <tr><th>Combinator</th><th>Resolves when</th><th>Cancels losers?</th></tr>
     *   <tr><td>{@link #all}</td><td>All results complete</td><td>No</td></tr>
     *   <tr><td><b>race</b></td><td>First terminal result</td><td>Yes</td></tr>
     *   <tr><td>{@link #anySuccessful}</td><td>First success, or all fail</td><td>Yes (on success)</td></tr>
     * </table>
     *
     * @param workflowState shared workflow state used for event-driven awaiting (no polling).
     * @param results       the competing step results.
     * @return a composite result that resolves to the first terminal result.
     * @see #all(WorkflowStepResult...)
     * @see #anySuccessful(WorkflowState, WorkflowStepResult...)
     */
    @Nonnull
    public static WorkflowStepResult race(@Nonnull WorkflowState workflowState, WorkflowStepResult... results) {
        return new WorkflowStepResult() {

            private volatile WorkflowStepResult winner;

            private WorkflowStepResult resolveWinner() {
                WorkflowStepResult w = winner;
                if (w != null) {
                    return w;
                }

                // Block until any step reaches terminal state
                do {
                    var resolved = findFirstCompleted();
                    if (resolved.isPresent()) {
                        return setWinner(resolved.get());
                    }
                    try {
                        workflowState.awaitStateChange(s ->
                                Arrays.stream(results).anyMatch(WorkflowStepResult::isCompleted)
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } while (true);

                return winner;
            }

            private synchronized WorkflowStepResult setWinner(WorkflowStepResult w) {
                if (winner == null) {
                    winner = w;
                    for (WorkflowStepResult r : results) {
                        if (r != w) {
                            r.cancel("Superseded by " + w.getStepName());
                        }
                    }
                }
                return winner;
            }

            /**
             * Safeguard against race condition during event-sourcing replay:
             * when multiple steps completed before cancellation took effect,
             * use event-sourced timestamps to deterministically select the
             * true first completer — array iteration order is not reliable.
             */
            private Optional<WorkflowStepResult> findFirstCompleted() {
                var completedNames = Arrays.stream(results)
                        .filter(WorkflowStepResult::isCompleted)
                        .map(WorkflowStepResult::getStepName)
                        .collect(Collectors.toSet());
                if (completedNames.isEmpty()) {
                    return Optional.empty();
                }
                return workflowState.firstCompletedAmong(completedNames)
                        .flatMap(name -> Arrays.stream(results)
                                .filter(r -> r.getStepName().equals(name))
                                .findFirst())
                        .or(() -> Arrays.stream(results)
                                .filter(WorkflowStepResult::isCompleted)
                                .findFirst());
            }

            @Override
            @Nonnull
            public String getStepName() {
                return "race(" + String.join(", ",
                        Arrays.stream(results).map(WorkflowStepResult::getStepName).toList()) + ")";
            }

            @Override
            public boolean isCompleted() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::isCompleted);
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

    /**
     * First-successful semantics — resolves on the first result that <b>succeeds</b>, ignoring
     * failures along the way. Only fails when <b>every</b> result has failed.
     * Equivalent to JS {@code Promise.any()}.
     *
     * <h3>Winner selection</h3>
     * <p>The composite blocks until at least one result succeeds, or all results have completed.
     * Unlike {@link #race}, failures, timeouts, and cancellations do <em>not</em> resolve the
     * composite — they are silently ignored as long as at least one result is still running.</p>
     *
     * <h3>Success path</h3>
     * <p>When a result succeeds it becomes the winner. The composite delegates every state query
     * ({@code isSuccess()}, {@code result()}, {@code error()}, etc.) to it. All remaining
     * results receive {@code cancel("Superseded by successful <winnerStepName>")}.</p>
     *
     * <h3>All-failed path</h3>
     * <p>If every result reaches a terminal state without a single success, the composite
     * selects the <b>first failure</b> (by event-sourced timestamp) as the winner.
     * In this case no loser cancellation occurs (all are already terminal). The composite
     * will report {@code isFailure() == true} and {@code error()} will return the winner's
     * error.</p>
     *
     * <h3>Event-sourcing replay safety</h3>
     * <p>When multiple steps succeed before cancellation takes effect (e.g. during event replay),
     * the winner is determined by <b>event-sourced timestamps</b> via
     * {@link WorkflowState#anySuccessfulAmong(Set)}, not by array order. This guarantees
     * deterministic winner selection across replays. The all-failed fallback uses
     * {@link WorkflowState#firstCompletedAmong(Set)} for the same guarantee.</p>
     *
     * <h3>Completion</h3>
     * <p>{@code isCompleted()} is non-blocking and returns {@code true} when any result has
     * succeeded <b>or</b> when all results have reached a terminal state (all failed).</p>
     *
     * <h3>Cancellation</h3>
     * <p>{@code cancel()} and {@code cancel(reason)} propagate to <b>all</b> results.</p>
     *
     * <h3>Comparison with other combinators</h3>
     * <table>
     *   <tr><th>Combinator</th><th>Resolves when</th><th>Cancels losers?</th></tr>
     *   <tr><td>{@link #all}</td><td>All results complete</td><td>No</td></tr>
     *   <tr><td>{@link #race}</td><td>First terminal result</td><td>Yes</td></tr>
     *   <tr><td><b>anySuccessful</b></td><td>First success, or all fail</td><td>Yes (on success)</td></tr>
     * </table>
     *
     * @param workflowState shared workflow state used for event-driven awaiting (no polling).
     * @param results       the competing step results.
     * @return a composite result that resolves to the first successful result, or the first failure if all fail.
     * @see #all(WorkflowStepResult...)
     * @see #race(WorkflowState, WorkflowStepResult...)
     */
    @Nonnull
    public static WorkflowStepResult anySuccessful(@Nonnull WorkflowState workflowState,
                                                     WorkflowStepResult... results) {
        return new WorkflowStepResult() {

            private volatile WorkflowStepResult winner;

            private WorkflowStepResult resolveWinner() {
                WorkflowStepResult w = winner;
                if (w != null) {
                    return w;
                }

                do {
                    var successful = findFirstSuccessful();
                    if (successful.isPresent()) {
                        return setWinner(successful.get(), true);
                    }

                    // All completed but none successful → all failed
                    if (Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)) {
                        var firstFailed = findFirstCompleted();
                        if (firstFailed.isPresent()) {
                            return setWinner(firstFailed.get(), false);
                        }
                    }

                    try {
                        workflowState.awaitStateChange(s ->
                                Arrays.stream(results).anyMatch(WorkflowStepResult::isSuccess)
                                        || Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } while (true);

                return winner;
            }

            private synchronized WorkflowStepResult setWinner(WorkflowStepResult w, boolean cancelLosers) {
                if (winner == null) {
                    winner = w;
                    if (cancelLosers) {
                        for (WorkflowStepResult r : results) {
                            if (r != w) {
                                r.cancel("Superseded by successful " + w.getStepName());
                            }
                        }
                    }
                }
                return winner;
            }

            private Optional<WorkflowStepResult> findFirstSuccessful() {
                var successfulNames = Arrays.stream(results)
                        .filter(WorkflowStepResult::isSuccess)
                        .map(WorkflowStepResult::getStepName)
                        .collect(Collectors.toSet());
                if (successfulNames.isEmpty()) {
                    return Optional.empty();
                }
                return workflowState.anySuccessfulAmong(successfulNames)
                        .flatMap(name -> Arrays.stream(results)
                                .filter(r -> r.getStepName().equals(name))
                                .findFirst())
                        .or(() -> Arrays.stream(results)
                                .filter(WorkflowStepResult::isSuccess)
                                .findFirst());
            }

            private Optional<WorkflowStepResult> findFirstCompleted() {
                var completedNames = Arrays.stream(results)
                        .filter(WorkflowStepResult::isCompleted)
                        .map(WorkflowStepResult::getStepName)
                        .collect(Collectors.toSet());
                if (completedNames.isEmpty()) {
                    return Optional.empty();
                }
                return workflowState.firstCompletedAmong(completedNames)
                        .flatMap(name -> Arrays.stream(results)
                                .filter(r -> r.getStepName().equals(name))
                                .findFirst())
                        .or(() -> Arrays.stream(results)
                                .filter(WorkflowStepResult::isCompleted)
                                .findFirst());
            }

            @Override
            @Nonnull
            public String getStepName() {
                return "anySuccessful(" + String.join(", ",
                        Arrays.stream(results).map(WorkflowStepResult::getStepName).toList()) + ")";
            }

            @Override
            public boolean isCompleted() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::isSuccess)
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

    // TODO: allSettled — waits for ALL results regardless of outcome.
    //  Unlike all(), does not short-circuit. Equivalent to JS Promise.allSettled().

    // TODO: nOf(int n, ...) — quorum/majority pattern.
    //  Waits for N out of M to succeed, cancels the rest.
}
