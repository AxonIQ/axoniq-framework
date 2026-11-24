/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots.configuration;

import io.axoniq.framework.messaging.transformation.snapshots.SnapshotTransformerChain;
import io.axoniq.framework.messaging.transformation.snapshots.SnapshotTransformingEventStore;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.eventstore.EventStore;

/**
 * A {@link ConfigurationEnhancer} decorating the {@link EventStore} with a {@link SnapshotTransformingEventStore} when
 * a {@link SnapshotTransformerChain} is registered as a component.
 * <p>
 * Snapshots stored in the application's current format are read with the {@link GeneralConverter}, the converter
 * entities use to read their own snapshots.
 * <p>
 * Discovered through the {@link java.util.ServiceLoader} mechanism; not intended for direct use.
 *
 * @author John Hendrikx
 * @since 5.5.0
 */
public final class SnapshotTransformationConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * Instantiated by the {@link java.util.ServiceLoader}; not intended for direct use.
     */
    public SnapshotTransformationConfigurationEnhancer() {
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(
                EventStore.class,
                SnapshotTransformingEventStore.DECORATION_ORDER,
                (config, name, delegate) -> config.getOptionalComponent(SnapshotTransformerChain.class)
                                                  .<EventStore>map(chain -> new SnapshotTransformingEventStore(
                                                          delegate,
                                                          chain,
                                                          config.getComponent(GeneralConverter.class)))
                                                  .orElse(delegate)
        );
    }
}
