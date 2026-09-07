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
package io.axoniq.framework.workflow.simulation.invariants;


/**
 * Thrown when a protocol invariant (INVARIANTS.md) is broken during a simulation step. Carries the offending
 * invariant's {@code MachineName} and a message; the harness enriches it with the seed and the full committed event
 * log before failing the run, so any break is reproducible.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public class InvariantViolation extends RuntimeException {

    private final String machineName;

    /**
     * Creates a violation for the named invariant.
     *
     * @param machineName the invariant's stable {@code MachineName} (e.g. {@code AtMostOnceRecording}).
     * @param message     human-readable description of what broke.
     */
    public InvariantViolation(String machineName, String message) {
        super("[" + machineName + "] " + message);
        this.machineName = machineName;
    }

    /**
     * Returns the {@code MachineName} of the broken invariant.
     *
     * @return the invariant machine name.
     */
        public String machineName() {
        return machineName;
    }
}
