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

package io.axoniq.framework.messaging.multitenancy.util;

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
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptors;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.messaging.core.Message;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_REPLICATION_GROUP;
import static io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor.tenantWithId;

/**
 * Utility class providing test fixtures for multi-tenancy related tests.
 */
public enum TestFixtures {
    ;

    public static final TenantDescriptor TENANT_A = new TenantDescriptor(
            "foo-a",
            Map.of("replicationGroup", DEFAULT_REPLICATION_GROUP)
    );

    public static final TenantDescriptor TENANT_B = new TenantDescriptor(
            "foo-b",
            Map.of("replicationGroup", DEFAULT_REPLICATION_GROUP)
    );

    public static final List<TenantDescriptor> TENANT_LIST = List.of(TENANT_A, TENANT_B);
    public static final TenantDescriptors TENANT_DESCRIPTORS = () -> TENANT_LIST;

    /**
     * Creates a {@link TenantResolver} that always resolves to the given tenant ID.
     *
     * @param tenantId the tenant ID to resolve to
     * @return a {@link TenantResolver} that always resolves to the given tenant ID
     * @see #alwaysTenant(TenantDescriptor)
     */
    public static TenantResolver alwaysTenant(String tenantId) {
        return alwaysTenant(tenantWithId(tenantId));
    }

    /**
     * Creates a {@link TenantResolver} that always resolves to the given {@link TenantDescriptor}.
     *
     * @param tenantDescriptor the {@link TenantDescriptor} to resolve to
     * @return a {@link TenantResolver} that always resolves to the given {@link TenantDescriptor}
     */
    public static TenantResolver alwaysTenant(TenantDescriptor tenantDescriptor) {
        return new AlwaysTenant(tenantDescriptor);
    }

    private record AlwaysTenant(TenantDescriptor tenantDescriptor) implements TenantResolver {

        @Override
        public TenantDescriptor resolveTenant(Message message, Collection<TenantDescriptor> tenants) {
            return tenantDescriptor;
        }

        @Override
        public Message attachTenant(Message message, TenantDescriptor tenant) {
            return message.andMetadata(Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId()));
        }
    }

    /**
     * A real {@link AxonServerConnectionManager} test double that never talks to Axon Server. It hands out an inert
     * connection per context and records which contexts were requested, so tests can assert per-tenant routing without
     * mocking. The connection's channels are not backed by a server, so only construction-level behavior can be
     * exercised through it.
     */
    public static class RecordingAxonServerConnectionManager extends AxonServerConnectionManager {

        private final List<String> requestedContexts = new CopyOnWriteArrayList<>();
        private final Map<String, AxonServerConnection> connections = new ConcurrentHashMap<>();
        private final boolean fixedConnections;

        /**
         * Creates a connection manager that returns an inert connection for every requested context.
         */
        public RecordingAxonServerConnectionManager() {
            super(managerBuilder(), new InertConnectionFactory());
            fixedConnections = false;
        }

        /**
         * Creates a connection manager that returns the supplied recording connection for each configured context.
         *
         * @param configuration the Axon Server configuration used to construct the manager
         * @param connections   the connections returned by context
         */
        public RecordingAxonServerConnectionManager(AxonServerConfiguration configuration,
                                                    Map<String, ? extends AxonServerConnection> connections) {
            super(managerBuilder(configuration), new FixedConnectionFactory(connections));
            this.connections.putAll(connections);
            fixedConnections = true;
        }

        public List<String> requestedContexts() {
            return List.copyOf(requestedContexts);
        }

        @Override
        public AxonServerConnection getConnection(String context) {
            requestedContexts.add(context);
            if (fixedConnections && !connections.containsKey(context)) {
                throw new IllegalArgumentException("Unknown context " + context);
            }
            return connections.computeIfAbsent(context, ignored -> new InertConnection());
        }

        private static Builder managerBuilder() {
            AxonServerConfiguration configuration = new AxonServerConfiguration();
            configuration.setClientId("client-id");
            configuration.setComponentName("component-name");
            return managerBuilder(configuration);
        }

        private static Builder managerBuilder(AxonServerConfiguration configuration) {
            return AxonServerConnectionManager.builder().axonServerConfiguration(configuration)
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

        private static final class FixedConnectionFactory extends AxonServerConnectionFactory {

            private final Map<String, ? extends AxonServerConnection> connections;

            private FixedConnectionFactory(Map<String, ? extends AxonServerConnection> connections) {
                super(new InertConnectionFactoryBuilder());
                this.connections = Map.copyOf(connections);
            }

            @Override
            public AxonServerConnection connect(String context) {
                AxonServerConnection connection = connections.get(context);
                if (connection == null) {
                    throw new IllegalArgumentException("Unknown context " + context);
                }
                return connection;
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
}
