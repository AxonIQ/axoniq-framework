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

package io.axoniq.framework.axonserver.connector.api;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;
import io.axoniq.axonserver.connector.impl.ServerAddress;
import io.axoniq.axonserver.connector.query.QueryChannel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A real {@link AxonServerConnectionManager} test double that never talks to Axon Server. It hands out an inert
 * connection per context and records which contexts were requested, so tests can assert per-tenant routing without
 * mocking. The connection's channels are not backed by a server, so only construction-level behavior can be exercised
 * through it.
 */
public class RecordingAxonServerConnectionManager extends AxonServerConnectionManager {

    private final List<String> requestedContexts = new CopyOnWriteArrayList<>();
    private final Map<String, AxonServerConnection> connections = new ConcurrentHashMap<>();

    /**
     * Constructs a {@code RecordingAxonServerConnectionManager} handing out inert connections, so no test reaches a
     * real Axon Server.
     */
    public RecordingAxonServerConnectionManager() {
        super(managerBuilder(), new InertConnectionFactory());
    }

    /**
     * Returns the contexts a connection was requested for, so a test can assert which tenants were connected to and in
     * what order.
     *
     * @return the contexts passed to {@link #getConnection(String)}, in call order
     */
    public List<String> requestedContexts() {
        return List.copyOf(requestedContexts);
    }

    @Override
    public AxonServerConnection getConnection(String context) {
        requestedContexts.add(context);
        return connections.computeIfAbsent(context, ignored -> new InertConnection());
    }

    private static Builder managerBuilder() {
        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setClientId("client-id");
        configuration.setComponentName("component-name");
        return AxonServerConnectionManager.builder()
                                          .axonServerConfiguration(configuration)
                                          .routingServers("localhost:8124");
    }

    private static final class InertConnectionFactory extends AxonServerConnectionFactory {

        private InertConnectionFactory() {
            super(new InertConnectionFactoryBuilder());
        }

        @Override
        public AxonServerConnection connect(String context) {
            return new InertConnection();
        }

        @Override
        public void shutdown() {
            // no-op
        }
    }

    private static final class InertConnectionFactoryBuilder extends AxonServerConnectionFactory.Builder {

        private InertConnectionFactoryBuilder() {
            super("component-name", "client-id");
            routingServers(new ServerAddress("localhost", 8124));
        }
    }

    private static final class InertConnection implements AxonServerConnection {

        @Override
        public boolean isConnectionFailed() {
            return false;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public void disconnect() {
            // no-op
        }

        @Override
        public ControlChannel controlChannel() {
            throw noChannels();
        }

        @Override
        public CommandChannel commandChannel() {
            throw noChannels();
        }

        @Override
        public EventChannel eventChannel() {
            throw noChannels();
        }

        @Override
        public DcbEventChannel dcbEventChannel() {
            throw noChannels();
        }

        @Override
        public QueryChannel queryChannel() {
            throw noChannels();
        }

        @Override
        public SnapshotChannel snapshotChannel() {
            throw noChannels();
        }

        @Override
        public EventTransformationChannel eventTransformationChannel() {
            throw noChannels();
        }

        @Override
        public AdminChannel adminChannel() {
            throw noChannels();
        }

        private static UnsupportedOperationException noChannels() {
            return new UnsupportedOperationException(
                    "The inert test connection exposes no channels and supports construction-level behavior only");
        }
    }
}
