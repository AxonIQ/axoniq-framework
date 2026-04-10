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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.eventsourcing.eventstore.StorageEngineTestSuite;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.test.server.AxonServerContainer;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test suite implementation validating the {@link AxonServerEventStorageEngine}.
 *
 * @author Steven van Beelen
 */
@Testcontainers
class AxonServerEventStorageEngineIT extends StorageEngineTestSuite<AxonServerEventStorageEngine> {

    private static final String CONTEXT = "default";

    @SuppressWarnings("resource")
    @Container
    private static final AxonServerContainer container = new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:2025.2.0")
            .withDevMode(true)
            .withDcbContext(true);

    private static AxonServerConnection connection;

    @AfterAll
    static void afterAll() {
        connection.disconnect();
        container.stop();
    }

    @Override
    protected AxonServerEventStorageEngine createStorageEngine() throws IOException {
        container.start();
        ServerAddress address = new ServerAddress(container.getHost(), container.getGrpcPort());
        connection = AxonServerConnectionFactory.forClient("AxonServerEventStorageEngineTest")
                                                .routingServers(address)
                                                .build()
                                                .connect(CONTEXT);

        EventConverter eventConverter = new DelegatingEventConverter(new ChainingContentTypeConverter());
        return new AxonServerEventStorageEngine(connection, eventConverter);
    }

    @Override
    protected ProcessingContext processingContext() {
        return null;
    }

    @Test
    void describeTo() {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();

        testSubject.describeTo(descriptor);

        Map<String, Object> describedProperties = descriptor.getDescribedProperties();
        assertThat(describedProperties).hasSize(2);
        assertThat(describedProperties).containsKey("connection");
        assertThat(describedProperties).containsKey("converter");
    }
}