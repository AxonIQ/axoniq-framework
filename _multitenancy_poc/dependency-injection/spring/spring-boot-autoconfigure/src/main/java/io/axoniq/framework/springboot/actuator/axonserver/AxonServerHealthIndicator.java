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

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.springboot.actuator.HealthStatus;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@link AbstractHealthIndicator} implementation exposing the health of the connections made through the {@link
 * AxonServerConnectionManager}. This status is exposed through the {@code "axonServer"} component.
 * <p>
 * The status is regarded as {@link Status#UP} if <b>all</b> {@link AxonServerConnectionManager#connections()} are up.
 * If one of them is down, the status is {@link HealthStatus#WARN}. If all of them are down the status will be {@link
 * Status#DOWN}. This {@link org.springframework.boot.actuate.health.HealthIndicator} also shares connection details per
 * context under {@code "{context-name}.connection.active"}.
 *
 * @author Steven van Beelen
 * @since 4.6.0
 */
public class AxonServerHealthIndicator extends AbstractHealthIndicator {

    private static final String CONNECTION = "%s.connection.active";

    private final AxonServerConnectionManager connectionManager;

    /**
     * Constructs this health indicator, extracting health information from the given {@code connectionManager}.
     *
     * @param connectionManager The Axon Server connection manager to extract health information from.
     */
    public AxonServerHealthIndicator(AxonServerConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        builder.up();

        AtomicBoolean anyConnectionUp = new AtomicBoolean(false);
        Map<String, Boolean> connections = connectionManager.connections();

        connections.forEach((context, connectionStatus) -> {
            String contextStatusCode;
            if (Boolean.FALSE.equals(connectionStatus)) {
                contextStatusCode = Status.DOWN.getCode();
                builder.status(HealthStatus.WARN);
            } else {
                contextStatusCode = Status.UP.getCode();
                anyConnectionUp.compareAndSet(false, true);
            }
            builder.withDetail(String.format(CONNECTION, context), contextStatusCode);
        });

        if (!connections.isEmpty() && !anyConnectionUp.get()) {
            builder.down();
        }
    }
}
