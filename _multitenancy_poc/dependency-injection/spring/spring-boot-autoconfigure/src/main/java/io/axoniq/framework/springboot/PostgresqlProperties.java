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

package io.axoniq.framework.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties for configuring the Axon Framework PostgreSQL extension.
 * <p>
 * These properties are bound to the {@code axon.postgresql} prefix.
 *
 * @author Steven van Beelen
 * @since 1.0.0
 */
@ConfigurationProperties("axon.postgresql")
public class PostgresqlProperties {

    /**
     * Whether the PostgreSQL extension is enabled.
     * <p>
     * When set to {@code false}, the service-loaded
     * {@link io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer} is disabled, preventing the
     * {@link io.axoniq.framework.postgresql.PostgresqlEventStorageEngine} from being registered as the
     * {@link org.axonframework.eventsourcing.eventstore.EventStorageEngine}. Defaults to {@code true}.
     */
    private boolean enabled = true;

    /**
     * Returns whether the PostgreSQL extension is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether the PostgreSQL extension is enabled.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}