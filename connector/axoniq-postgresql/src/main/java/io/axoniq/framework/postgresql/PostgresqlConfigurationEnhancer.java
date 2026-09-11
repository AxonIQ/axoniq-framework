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

package io.axoniq.framework.postgresql;

import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * A {@link ConfigurationEnhancer} that is auto-loadable by the {@link ApplicationConfigurer}, setting the
 * {@link PostgresqlEventStorageEngine} as the {@link EventStorageEngine} to use when no other is present. The engine is
 * also made available as a {@link SnapshotStore}.
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
        if (!registry.hasComponent(DataSource.class, SearchScope.ALL)) {
            return;
        }

        ComponentBuilder<PostgresqlEventStorageEngine> shared = eventStorageEngineBuilder();

        registry.registerIfNotPresent(EventStorageEngine.class, shared, SearchScope.ALL);
        registry.registerIfNotPresent(SnapshotStore.class, shared, SearchScope.ALL);
    }

    private static ComponentBuilder<PostgresqlEventStorageEngine> eventStorageEngineBuilder() {
        // An AtomicReference would seem to suffice, but its contract isn't optimal for what this builder does.
        // There's a none-zero chance that an updateAndGet call would be invoked several times under contention.
        // As we open a connection, the contention isn't unlikely to happen.
        // AtomicReference use would thus potentially accidentally open a ghost connection we'd never be able to close cleanly.
        // Hence, ConcurrentHashMap#computeIfAbsent is used as it does guarantee the mapping function runs at most once per key.
        ConcurrentHashMap<String, PostgresqlEventStorageEngine> instance = new ConcurrentHashMap<>();
        return config -> instance.computeIfAbsent(
                "shared",
                ignored -> new PostgresqlEventStorageEngine(
                        config.getComponent(DataSource.class),
                        config.getComponent(EventConverter.class)
                )
        );
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}