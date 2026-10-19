/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

/**
 * Step status.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public enum StepStatus {
    /**
     * The first attempt of the step was accepted by the store and is running.
     */
    STARTED,
    /**
     * An attempt failed and the retry policy allows another one. The step waits for the backoff before the next attempt
     * starts.
     */
    RETRYING,
    /**
     * A retry attempt was accepted by the store and is running. Recorded once per retry attempt, after the
     * {@link #RETRYING} record of the attempt that failed.
     */
    RETRY_STARTED,
    /**
     * The step finished successfully. Terminal.
     */
    COMPLETED,
    /**
     * The step finished with an error and no retry is left. Terminal.
     */
    FAILED,
    /**
     * The step did not finish within its timeout. Terminal.
     */
    TIMED_OUT,
    /**
     * The step was cancelled, either directly or through a terminal transition of the workflow. Terminal.
     */
    CANCELLED;

    /**
     * Checks if the status is terminal.
     *
     * @return true, if terminal.
     */
    public boolean isTerminal() {
        return switch (this) {
            case STARTED, RETRYING, RETRY_STARTED -> false;
            case COMPLETED, FAILED, CANCELLED, TIMED_OUT -> true;
        };
    }
}
