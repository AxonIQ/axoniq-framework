package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.AllCompletedCombinator;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Arrays;
import java.util.Optional;

/**
 * Default implementation of {@link AllCompletedCombinator}.
 *
 * @see AllCompletedCombinator
 */
public class AllCompletedCombinatorDelegate implements AllCompletedCombinator {

    /**
     * Barrier semantics — waits for <b>every</b> result to reach a terminal state before the composite itself is
     * considered completed.
     *
     * <h3>Completion</h3>
     * <p>{@link WorkflowStepResult#isCompleted() isCompleted()} returns {@code true} only when
     * <b>all</b> results have completed (succeeded, failed, timed out, or been cancelled).
     * Until that point, blocking queries ({@code isSuccess()}, {@code isFailure()}, etc.) will block the calling
     * thread.</p>
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
     */
    public WorkflowStepResult all(WorkflowStepResult... results) {
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
}
