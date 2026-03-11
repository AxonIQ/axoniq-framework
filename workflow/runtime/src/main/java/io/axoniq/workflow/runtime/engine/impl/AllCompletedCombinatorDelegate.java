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

    /** {@inheritDoc} */
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
                return Arrays.stream(results).filter(WorkflowStepResult::failure).findFirst().flatMap(
                        WorkflowStepResult::error);
            }

            @Override
            public boolean success() {
                return Arrays.stream(results).allMatch(WorkflowStepResult::success);
            }

            @Override
            public boolean failure() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::failure);
            }

            @Override
            public boolean canceled() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::canceled);
            }

            @Override
            public boolean timeout() {
                return Arrays.stream(results).anyMatch(WorkflowStepResult::timeout);
            }

            @Override
            public void await() {
                Arrays.stream(results).forEach(WorkflowStepResult::await);
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
