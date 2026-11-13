/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.axonserverconnector;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.snapshot.AxonServerSnapshotStore;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.eventsourcing.SnapshottingEntityLifecycleHandlerTestSuite;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.junit.jupiter.api.*;

import java.io.IOException;

/**
 * Tests the {@link org.axonframework.eventsourcing.handler.SnapshottingEntityLifecycleHandler} with an {@link AxonServerSnapshotStore}.
 *
 * @author John Hendrikx
 */
public class AxonServerBackedSnapshotterIT extends SnapshottingEntityLifecycleHandlerTestSuite {

    /*
     * A context of its own, rather than the shared container's default context, so this suite's events don't
     * mix with other suites sharing the container.
     */
    private static final String CONTEXT = "axon-server-backed-snapshotter-it";

    private static final AxonServerContainer CONTAINER = SharedAxonServerContainer.INSTANCE;

    @BeforeAll
    static void startContainer() throws IOException {
        SharedAxonServerContainer.ensureStarted();

        try {
            AxonServerContainerUtils.deleteContext(CONTAINER.getHost(), CONTAINER.getHttpPort(), CONTEXT);
        } catch (IOException ignored) {
            // Context didn't exist yet.
        }
        AxonServerContainerUtils.createContext(CONTAINER.getHost(),
                                               CONTAINER.getHttpPort(),
                                               CONTEXT,
                                               AxonServerContainerUtils.DCB_CONTEXT);
    }

    @Override
    protected void registerComponents(ComponentRegistry registry) {
        registry.registerComponent(
                AxonServerConfiguration.class,
                c -> AxonServerConfiguration.builder()
                                            .componentName("AxonServerBackedSnapshotterIT")
                                            .servers(CONTAINER.getAxonServerAddress())
                                            .context(CONTEXT)
                                            .build()
        );

        registry.registerComponent(SnapshotStore.class, c -> {
            AxonServerConnectionManager component = c.getComponent(AxonServerConnectionManager.class);

            return new AxonServerSnapshotStore(component.getConnection(), c.getComponent(GeneralConverter.class));
        });
    }
}
