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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStoreTestSuite;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test suite implementation validating the {@link AxonServerEventStorageEngine}.
 *
 * @author John Hendrikx
 */
@Testcontainers
class AxonServerStorageEngineBackedEventStoreIT
        extends StorageEngineBackedEventStoreTestSuite<AxonServerEventStorageEngine> {

    private static final UnitOfWorkFactory FACTORY = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
    private static final String CONTEXT = "default";

    @SuppressWarnings("resource")
    @Container
    private static final AxonServerContainer container =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:2025.2.0")
                    .withDevMode(true)
                    .withDcbContext(true);

    private static AxonServerConnection connection;

    private static AxonServerEventStorageEngine engine;

    @BeforeAll
    static void buildEngine() {
        container.start();
        ServerAddress address = new ServerAddress(container.getHost(), container.getGrpcPort());
        connection = AxonServerConnectionFactory.forClient("AxonServerEventStorageEngineTest")
                                                .routingServers(address)
                                                .build()
                                                .connect(CONTEXT);
    }

    @AfterAll
    static void afterAll() {
        connection.disconnect();
        container.stop();
    }

    @NonNull
    @Override
    protected AxonServerEventStorageEngine getStorageEngine(@NonNull EventConverter converter) {
        if (engine == null) {
            engine = new AxonServerEventStorageEngine(connection, converter);
        }

        return engine;
    }

    @NonNull
    @Override
    protected UnitOfWork unitOfWork() {
        return FACTORY.create();
    }

    @Test
    void describeTo() {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        engine.describeTo(descriptor);

        Map<String, Object> describedProperties = descriptor.getDescribedProperties();
        assertThat(describedProperties)
                .hasSize(2)
                .containsKey("connection")
                .containsKey("converter");
    }
}