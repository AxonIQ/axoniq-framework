/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
