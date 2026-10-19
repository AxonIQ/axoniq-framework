/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.actuator.axonserver;

import io.axoniq.framework.springboot.actuator.HealthStatus;
import org.springframework.boot.actuate.health.SimpleStatusAggregator;
import org.springframework.boot.actuate.health.Status;

/**
 * A {@link SimpleStatusAggregator} implementation determining the overall health of an Axon Framework application using
 * Axon Server. Adds the {@link HealthStatus#WARN} status to the regular set of {@link Status statuses}.
 *
 * @author Steven van Beelen
 * @author Marc Gathier
 * @since 4.6.0
 */
public class AxonServerStatusAggregator extends SimpleStatusAggregator {

    /**
     * Constructs a default Axon Server specific {@link SimpleStatusAggregator}. Adds the {@link HealthStatus#WARN}
     * after {@link Status#OUT_OF_SERVICE} and before {@link Status#UP}.
     */
    public AxonServerStatusAggregator() {
        super(Status.DOWN, Status.OUT_OF_SERVICE, HealthStatus.WARN, Status.UP, Status.UNKNOWN);
    }
}
