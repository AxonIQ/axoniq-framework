/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.state.StepFailedException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;

import java.time.Duration;
import java.util.Optional;

/**
 * A step execution result that already has completed.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
class CompletedWorkflowStepResult implements WorkflowStepResult {

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
    public <T> Optional<T> result() {
        //noinspection unchecked
        return Optional.ofNullable((T) payload);
    }

    @Override
    @Nonnull
    public Optional<StepFailedException> error() {
        return Optional.ofNullable(error);
    }

    @Override
    public boolean success() {
        return !cancelled && timeout == null && error == null;
    }

    @Override
    public boolean failure() {
        return error != null;
    }

    @Override
    public boolean canceled() {
        return cancelled;
    }

    @Override
    public boolean timeout() {
        return timeout != null;
    }

    @Override
    public void await() {
    }

    @Override
    public void cancel() {
        // no-op: already in terminal state
    }

    @Override
    public void cancel(@Nonnull String reason) {
        // no-op: already in terminal state
    }
}
