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

package io.axoniq.framework.extension.postgresql;

import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import javax.sql.DataSource;

/**
 * A {@link ConfigurationEnhancer} that is auto-loadable by the {@link ApplicationConfigurer}, setting the
 * {@link PostgresqlEventStorageEngine} as the {@link EventStorageEngine} to use when no other is present.
 * <p>
 * The {@link #ENHANCER_ORDER} is set such that this {@code ConfigurationEnhancer} will follow after the
 * {@code AxonServerConfigurationEnhancer}, thus giving precedence over to the Axon Server {@code EventStorageEngine}.
 *
 * @author Steven van Beelen
 * @since 1.0.0
 */
public class PostgresqlConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The {@link #order()} of this enhancer. Positioned after the Axon Server {@code ConfigurationEnhancer}
     * ({@code Integer.MIN_VALUE + 10}).
     */
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 20;

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(
                EventStorageEngine.class,
                configuration -> new PostgresqlEventStorageEngine(
                        configuration.getComponent(DataSource.class),
                        configuration.getComponent(EventConverter.class),
                        configuration.getComponent(EntitlementManager.class)
                ),
                SearchScope.ALL
        );
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}