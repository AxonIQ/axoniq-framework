/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

/**
 * Status of the workflow execution.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public enum WorkflowStatus {
    NONE,
    STARTED,
    COMPLETED,
    FAILED,
    CANCELLED,
    TIMED_OUT;

    /**
     * Checks if the status a terminal.
     *
     * @return true, if terminal.
     */
    public boolean isTerminal() {
        return switch (this) {
            case NONE, STARTED -> false;
            case COMPLETED, FAILED, CANCELLED, TIMED_OUT -> true;
        };
    }
}
