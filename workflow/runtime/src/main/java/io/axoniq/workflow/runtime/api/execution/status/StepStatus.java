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
package io.axoniq.workflow.runtime.api.execution.status;

/**
 * Step status.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 0.1.0
 */
public enum StepStatus {
    STARTED,
    RETRYING,
    COMPLETED,
    FAILED,
    TIMED_OUT,
    CANCELLED;

    /**
     * Checks if the status is terminal.
     *
     * @return true, if terminal.
     */
    public boolean isTerminal() {
        return switch (this) {
            case STARTED, RETRYING -> false;
            case COMPLETED, FAILED, CANCELLED, TIMED_OUT -> true;
        };
    }
}
