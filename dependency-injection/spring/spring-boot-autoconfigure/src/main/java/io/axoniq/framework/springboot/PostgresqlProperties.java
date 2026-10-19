/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

import io.axoniq.framework.postgresql.SchemaInitialization;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties for configuring the Axon Framework PostgreSQL extension.
 * <p>
 * These properties are bound to the {@code axon.postgresql} prefix.
 *
 * @author Steven van Beelen
 * @author John Hendrikx
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
     * How the {@link io.axoniq.framework.postgresql.PostgresqlEventStorageEngine} should handle a missing or
     * incomplete schema on startup. Defaults to {@link SchemaInitialization#CREATE_IF_MISSING}.
     */
    private SchemaInitialization schemaInitialization = SchemaInitialization.CREATE_IF_MISSING;

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

    /**
     * Returns how the engine should handle a missing or incomplete schema on startup.
     *
     * @return the configured {@link SchemaInitialization}
     */
    public SchemaInitialization getSchemaInitialization() {
        return schemaInitialization;
    }

    /**
     * Sets how the engine should handle a missing or incomplete schema on startup.
     *
     * @param schemaInitialization the {@link SchemaInitialization} to use, cannot be {@code null}
     */
    public void setSchemaInitialization(SchemaInitialization schemaInitialization) {
        this.schemaInitialization = schemaInitialization;
    }
}