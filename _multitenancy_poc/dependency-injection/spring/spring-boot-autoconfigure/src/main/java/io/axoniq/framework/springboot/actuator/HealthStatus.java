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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.springboot.actuator;

import org.springframework.boot.actuate.health.Status;

/**
 * Utility class holding additional {@link Status} instances.
 *
 * @author Steven van Beelen
 * @author Marc Gathier
 * @since 4.6.0
 */
public abstract class HealthStatus {

    /**
     * A {@link Status} suggesting the connection is still working but not at full capacity.
     */
    public static final Status WARN = new Status("WARN");

    private HealthStatus() {
        // Utility class
    }
}
