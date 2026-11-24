/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.snapshots.configuration;

import io.axoniq.framework.messaging.transformation.snapshots.SnapshotTransformation;
import io.axoniq.framework.messaging.transformation.snapshots.SnapshotTransformerChain;
import io.axoniq.framework.messaging.transformation.snapshots.SnapshotTransformingEventStore;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.VersionedType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link SnapshotTransformationConfigurationEnhancer} is discovered, and installs the
 * {@link SnapshotTransformingEventStore} only when a {@link SnapshotTransformerChain} is registered.
 *
 * @author John Hendrikx
 */
final class SnapshotTransformationConfigurationEnhancerTest {

    @Test
    void shouldBeRegisteredAsAServiceLoaderProvider() throws Exception {
        // when
        URL registration = getClass().getClassLoader()
                                     .getResource("META-INF/services/" + ConfigurationEnhancer.class.getName());

        // then
        assertThat(registration).isNotNull();

        try (InputStream stream = registration.openStream()) {
            assertThat(new String(stream.readAllBytes(), StandardCharsets.UTF_8))
                    .contains(SnapshotTransformationConfigurationEnhancer.class.getName());
        }
    }

    @Test
    void shouldNotDecorateTheEventStoreWhenNoChainIsRegistered() {
        // when
        EventStore resolved = EventSourcingConfigurer.create()
                                                     .registerEventStore(config -> Mockito.mock(EventStore.class))
                                                     .build()
                                                     .getComponent(EventStore.class);

        // then
        assertThat(resolved).isNotInstanceOf(SnapshotTransformingEventStore.class);
    }

    @Test
    void shouldDecorateTheEventStoreWhenAChainIsRegistered() {
        // given
        SnapshotTransformerChain chain = SnapshotTransformerChain.builder()
                .register(SnapshotTransformation.pathFrom(VersionedType.of("account", "1.0.0"))
                                                .renameTo(VersionedType.of("ledger", "1.0.0"))
                                                .build())
                .build();

        // when
        EventStore resolved = EventSourcingConfigurer.create()
                                                     .registerEventStore(config -> Mockito.mock(EventStore.class))
                                                     .componentRegistry(registry -> registry.registerComponent(
                                                             SnapshotTransformerChain.class, config -> chain
                                                     ))
                                                     .build()
                                                     .getComponent(EventStore.class);

        // then
        assertThat(resolved).isInstanceOf(SnapshotTransformingEventStore.class);
    }
}
