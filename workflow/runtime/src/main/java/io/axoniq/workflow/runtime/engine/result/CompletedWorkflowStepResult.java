package io.axoniq.workflow.runtime.engine.result;

import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;

/**
 * A step execution result that already has completed.
 */
public class CompletedWorkflowStepResult implements WorkflowStepResult {

    private final String stepName;
    private final Object payload;
    private final StepFailedException error;
    private final Duration timeout;
    private final boolean cancelled;

    CompletedWorkflowStepResult(@Nonnull String stepName,
                                @Nullable Object payload,
                                @Nullable Throwable error,
                                @Nullable Duration timeout,
                                boolean cancelled) {
        this.stepName = stepName;
        this.payload = payload;
        if (error != null) {
            if (error instanceof StepFailedException) {
                this.error = (StepFailedException) error;
            } else {
                this.error = new StepFailedException(error);
            }
        } else {
            this.error = null;
        }
        this.timeout = timeout;
        this.cancelled = cancelled;
    }

    @Override
    @Nonnull
    public String getStepName() {
        return stepName;
    }

    @Override
    public boolean isCompleted() {
        return true;
    }

    @Override
    @Nonnull
    public <T> Optional<T> payload() {
        //noinspection unchecked
        return Optional.ofNullable((T) payload);
    }

    @Override
    @Nonnull
    public Optional<StepFailedException> error() {
        return Optional.ofNullable(error);
    }

    @Override
    public boolean isSuccess() {
        return !cancelled && timeout == null && error == null;
    }

    @Override
    public boolean isFailure() {
        return error != null;
    }

    @Override
    public boolean isCanceled() {
        return cancelled;
    }

    @Override
    public boolean isTimeout() {
        return timeout != null;
    }
}
