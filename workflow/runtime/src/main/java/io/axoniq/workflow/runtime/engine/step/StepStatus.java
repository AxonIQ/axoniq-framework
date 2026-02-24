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
package io.axoniq.workflow.runtime.engine.step;

/**
 * Step status.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public enum StepStatus {
    STARTED,
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
            case STARTED -> false;
            case COMPLETED, FAILED, CANCELLED, TIMED_OUT -> true;
        };
    }
}
