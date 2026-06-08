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
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/**
 * A {@link ConfigurationEnhancer} that is auto-loadable by the {@link ApplicationConfigurer}, setting the
 * {@link PostgresqlEventStorageEngine} as the {@link EventStorageEngine} to use when no other is present.
 * The engine is also made available as a {@link SnapshotStore}.
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

        boolean engineAlreadyPresent = registry.hasComponent(EventStorageEngine.class, SearchScope.ALL);

        // Capture the engine instance so the SnapshotStore builder can return it directly,
        // avoiding a circular dependency: resolving SnapshotStore must not trigger EventStorageEngine
        // resolution, because the framework resolves SnapshotStore during EventStorageEngine decoration.
        AtomicReference<PostgresqlEventStorageEngine> engineRef = new AtomicReference<>();
        // TODO #4699 this is a bit too hackish, better solution is being able to register an alias for a component

        registry.registerIfNotPresent(
                EventStorageEngine.class,
                configuration -> {
                    PostgresqlEventStorageEngine engine = new PostgresqlEventStorageEngine(
                            configuration.getComponent(DataSource.class),
                            configuration.getComponent(EventConverter.class)
                    );
                    engineRef.set(engine);
                    return engine;
                },
                SearchScope.ALL
        );

        if (!engineAlreadyPresent) {
            registry.registerIfNotPresent(
                    SnapshotStore.class,
                    configuration -> {
                        PostgresqlEventStorageEngine engine = engineRef.get();
                        if (engine == null) {
                            // SnapshotStore resolved before EventStorageEngine; trigger engine creation first
                            configuration.getComponent(EventStorageEngine.class);
                            engine = engineRef.get();
                        }
                        return Objects.requireNonNull(engine);
                    },
                    SearchScope.ALL
            );
        }
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}