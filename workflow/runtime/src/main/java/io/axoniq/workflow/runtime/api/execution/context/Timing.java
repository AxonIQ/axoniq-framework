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
package io.axoniq.workflow.runtime.api.execution.context;


import java.time.Duration;

/**
 * Reusable timing configuration shared by primitive specs.
 *
 * @param timeout timeout for primitive completion
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public record Timing(Duration timeout) {

    /**
     * Returns a copy of this timing specification with the provided timeout.
     *
     * @param timeout timeout for primitive completion
     * @return copied timing specification with updated timeout
     */
    public Timing timeout(Duration timeout) {
        return new Timing(timeout);
    }

    /**
     * Returns a copy of this timing specification with the provided timeout in seconds.
     *
     * @param timeoutSeconds timeout in seconds
     * @return copied timing specification with updated timeout
     */
    public Timing timeout(long timeoutSeconds) {
        return timeout(Duration.ofSeconds(timeoutSeconds));
    }
}
