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
import java.util.function.Predicate;
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
     * composite itself is considered completed.
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
     *   <tr><td>{@link #anyMatch}</td><td>First predicate match, or all complete</td><td>Yes (on match)</td></tr>
     *   <tr><td>{@link #noneMatch}</td><td>All complete without match, or short-circuit</td><td>Yes (on short-circuit)</td></tr>
     * </table>
     *
     * @param results the step results to combine.
     * @return a composite result that completes when all underlying results have completed.
     * @see #anyMatch(WorkflowState, Predicate, WorkflowStepResult...)
     * @see #noneMatch(WorkflowState, Predicate, WorkflowStepResult...)
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
     * Predicate-based combinator — resolves when the <b>first</b> completed result matches the
     * given predicate. Replaces both the former {@code race()} and {@code anySuccessful()} methods:
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
     * @param workflowState shared workflow state used for event-driven awaiting (no polling).
     * @param predicate     the predicate to match against completed results.
     * @param results       the competing step results.
     * @return a composite result that resolves to the first matching result, or fallback to first completed.
     * @see #all(WorkflowStepResult...)
     * @see #noneMatch(WorkflowState, Predicate, WorkflowStepResult...)
     */
    @Nonnull
    public static WorkflowStepResult anyMatch(@Nonnull WorkflowState workflowState,
                                              @Nonnull Predicate<WorkflowStepResult> predicate,
                                              WorkflowStepResult... results) {
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
                                        || Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)
                        );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } while (true);

                return winner;
            }

            private WorkflowStepResult setWinner(WorkflowStepResult w, boolean cancelLosers) {
                winner = w;
                if (cancelLosers) {
                    for (WorkflowStepResult r : results) {
                        if (r != w) {
                            r.cancel("Superseded by " + w.getStepName());
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
                        Arrays.stream(results).map(WorkflowStepResult::getStepName).toList()) + ")";
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

    /**
     * Guard combinator — ensures <b>no</b> completed result matches the given predicate.
     * Short-circuits on the first match.
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
     * @param workflowState shared workflow state used for event-driven awaiting (no polling).
     * @param predicate     the predicate that no result should match.
     * @param results       the step results to guard.
     * @return a composite result that succeeds when no result matches, or short-circuits on first match.
     * @see #anyMatch(WorkflowState, Predicate, WorkflowStepResult...)
     */
    @Nonnull
    public static WorkflowStepResult noneMatch(@Nonnull WorkflowState workflowState,
                                               @Nonnull Predicate<WorkflowStepResult> predicate,
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
                                        || Arrays.stream(results).allMatch(WorkflowStepResult::isCompleted)
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
                        Arrays.stream(results).map(WorkflowStepResult::getStepName).toList()) + ")";
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
