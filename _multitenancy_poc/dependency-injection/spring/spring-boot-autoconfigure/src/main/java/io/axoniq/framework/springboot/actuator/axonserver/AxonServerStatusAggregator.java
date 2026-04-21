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
