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
package io.axoniq.workflow.runtime.api.execution.state;

import jakarta.annotation.Nonnull;

import java.util.Optional;

/**
 * Represents a result of a step execution.
 */
public interface WorkflowStepResult {

    @Nonnull
    String getStepName();

    /**
     * Check (non-blocking) if the step has finished (reached a terminal state).
     *
     * @return true, if the state is terminal.
     */
    boolean isCompleted();

    /**
     * Retrieves optional result.
     *
     * @param <T> type of result.
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished.
     */
    @Nonnull
    <T> Optional<T> result();

    /**
     * Retrieves an optional error.
     *
     * @return error if the step has finished with an error. Will return empty optional, if the step is not finished.
     */
    @Nonnull
    Optional<StepFailedException> error();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation completed successfully.
     */
    boolean success();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation completed with an exception.
     */
    boolean failure();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation was interrupted by external party.
     */
    boolean canceled();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation was interrupted by timeout specified on start.
     */
    boolean timeout();

    /**
     * Blocks until step execution reaches a terminal state.
     */
    void await();

    /**
     * Cancels this step if it is still running. No-op if already in a terminal state.
     */
    void cancel();

    /**
     * Cancels this step with a reason if it is still running. No-op if already in a terminal state.
     *
     * @param reason human-readable cancellation reason.
     */
    void cancel(@Nonnull String reason);
}
