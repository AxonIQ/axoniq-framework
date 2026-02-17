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
package io.axoniq.workflow.runtime.api;

import io.axoniq.workflow.runtime.engine.step.StepFailedException;
import jakarta.annotation.Nonnull;

import java.util.Optional;

/**
 * Represents a typed result of a step execution.
 */
public interface WorkflowStepResult {

    @Nonnull
    String getStepName();

    /**
     * Check (non-blocking)
     *
     * @return
     */
    boolean isCompleted();

    @Nonnull
    <T> Optional<T> payload();

    @Nonnull
    Optional<StepFailedException> error();

    /**
     * Blocks until step execution is finished.
     *
     * @return true, if the operation completed successfully.
     */
    boolean isSuccess();

    /**
     * Blocks until step execution is finished.
     *
     * @return true, if the operation completed with an exception.
     */
    boolean isFailure();

    /**
     * Blocks until step execution is finished.
     *
     * @return true, if the operation was interrupted by external party.
     */
    boolean isCanceled();

    /**
     * Blocks until step execution is finished.
     *
     * @return true, if the operation was interrupted by timeout specified on start.
     */
    boolean isTimeout();
}
