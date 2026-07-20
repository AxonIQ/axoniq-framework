/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the
 * specific language governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.multitenancy.query;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.Registration;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.configuration.MetadataBasedTenantResolver;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

class MultiTenantAxonServerQueryBusConnectorTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant-1");
    private static final TenantDescriptor TENANT_2 = new TenantDescriptor("tenant-2", Map.of("replicationGroup", "rg-2"));
    private static final QualifiedName QUERY_ONE = new QualifiedName("query-one");
    private static final QualifiedName QUERY_TWO = new QualifiedName("query-two");

    @Test
    void subscribeRegistersQueryHandlersOnAllKnownTenants() {
        TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
        AxonServerConnection connection1 = connection();
        AxonServerConnection connection2 = connection();
        MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                          Map.of(TENANT_1.tenantId(),
                                                                                 connection1,
                                                                                 TENANT_2.tenantId(),
                                                                                 connection2));

        testSubject.subscribe(QUERY_ONE).join();
        testSubject.subscribe(QUERY_TWO).join();

        verify(connection1.queryChannel(), times(2)).registerQueryHandler(any(), any(QueryDefinition.class));
        verify(connection2.queryChannel(), times(2)).registerQueryHandler(any(), any(QueryDefinition.class));
    }

    @Test
    void queryRoutesToResolvedTenant() {
        TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
        AxonServerConnection connection1 = connection();
        AxonServerConnection connection2 = connection();
        MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                          Map.of(TENANT_1.tenantId(),
                                                                                 connection1,
                                                                                 TENANT_2.tenantId(),
                                                                                 connection2));

        QueryMessage query = queryFor(TENANT_2.tenantId());
        testSubject.query(query, null);

        verify(connection2.queryChannel()).query(any());
        verify(connection1.queryChannel(), never()).query(any());
    }

    @Test
    void registerAndStartTenantReplaysKnownQueries() {
        TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
        AxonServerConnection connection1 = connection();
        AxonServerConnection connection2 = connection();
        MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                          Map.of(TENANT_1.tenantId(),
                                                                                 connection1,
                                                                                 TENANT_2.tenantId(),
                                                                                 connection2));

        testSubject.subscribe(QUERY_ONE).join();

        tenantProvider.addTenant(TENANT_2);

        verify(connection2.queryChannel()).registerQueryHandler(any(), any(QueryDefinition.class));
    }

    @Test
    void unsubscribeRemovesQueryFromAllTenants() {
        TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
        AxonServerConnection connection1 = connection();
        AxonServerConnection connection2 = connection();
        Registration reg1 = mock(Registration.class);
        Registration reg2 = mock(Registration.class);
        when(reg1.onAck(any(Runnable.class))).thenAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        });
        when(reg2.onAck(any(Runnable.class))).thenAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        });
        when(connection1.queryChannel().registerQueryHandler(any(), any(QueryDefinition.class))).thenReturn(reg1);
        when(connection2.queryChannel().registerQueryHandler(any(), any(QueryDefinition.class))).thenReturn(reg2);
        MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                          Map.of(TENANT_1.tenantId(),
                                                                                 connection1,
                                                                                 TENANT_2.tenantId(),
                                                                                 connection2));

        testSubject.subscribe(QUERY_ONE).join();
        boolean unsubscribed = testSubject.unsubscribe(QUERY_ONE);

        assertThat(unsubscribed).isTrue();
        verify(reg1).cancel();
        verify(reg2).cancel();
    }

    @Test
    void disconnectDisconnectsAllTenantConnections() {
        TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
        AxonServerConnection connection1 = connection();
        AxonServerConnection connection2 = connection();
        MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                          Map.of(TENANT_1.tenantId(),
                                                                                 connection1,
                                                                                 TENANT_2.tenantId(),
                                                                                 connection2));

        testSubject.subscribe(QUERY_ONE).join();
        testSubject.disconnect().join();

        verify(connection1.queryChannel()).prepareDisconnect();
        verify(connection2.queryChannel()).prepareDisconnect();
        verify(connection1).disconnect();
        verify(connection2).disconnect();
    }

    @Test
    void dispatchingWithUnknownTenantFails() {
        TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
        AxonServerConnection connection1 = connection();
        MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                          Map.of(TENANT_1.tenantId(),
                                                                                 connection1));

        assertThatThrownBy(() -> testSubject.query(queryFor(TENANT_2.tenantId()), null))
                .isInstanceOf(io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException.class);
    }

    private static MultiTenantAxonServerQueryBusConnector createSubject(TestTenantProvider tenantProvider,
                                                                        Map<String, AxonServerConnection> connections) {
        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setClientId("client-id");
        configuration.setComponentName("component-name");

        AxonServerConnectionManager connectionManager = mock(AxonServerConnectionManager.class);
        connections.forEach((tenantId, connection) -> when(connectionManager.getConnection(tenantId)).thenReturn(connection));

        MultiTenantAxonServerQueryBusConnector connector = new MultiTenantAxonServerQueryBusConnector(
                tenantProvider,
                new MetadataBasedTenantResolver(),
                connectionManager,
                configuration,
                null
        );
        tenantProvider.subscribe(connector);
        return connector;
    }

    private static QueryMessage queryFor(String tenantId) {
        return new GenericQueryMessage(
                new GenericMessage("message-id",
                                   new MessageType(QUERY_ONE.name()),
                                   "payload".getBytes(),
                                   Map.of("tenantId", tenantId))
        );
    }

    private static AxonServerConnection connection() {
        AxonServerConnection connection = mock(AxonServerConnection.class, RETURNS_DEEP_STUBS);
        QueryChannel queryChannel = mock(QueryChannel.class);
        Registration registration = mock(Registration.class);
        when(registration.onAck(any(Runnable.class))).thenAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        });
        when(queryChannel.registerQueryHandler(any(), any(QueryDefinition.class))).thenReturn(registration);
        when(queryChannel.query(any())).thenReturn(mock(ResultStream.class));
        when(connection.queryChannel()).thenReturn(queryChannel);
        when(connection.isConnected()).thenReturn(true);
        doNothing().when(connection).disconnect();
        return connection;
    }

    private static final class TestTenantProvider implements TenantProvider {

        private final List<TenantDescriptor> tenants = new ArrayList<>();
        private final List<MultiTenantAwareComponent> components = new ArrayList<>();

        private TestTenantProvider(Collection<TenantDescriptor> tenants) {
            this.tenants.addAll(tenants);
        }

        @Override
        public org.axonframework.common.Registration subscribe(MultiTenantAwareComponent component) {
            components.add(component);
            tenants.forEach(component::registerAndStartTenant);
            return () -> components.remove(component);
        }

        @Override
        public List<TenantDescriptor> getTenants() {
            return List.copyOf(tenants);
        }

        private void addTenant(TenantDescriptor tenantDescriptor) {
            tenants.add(tenantDescriptor);
            components.forEach(component -> component.registerAndStartTenant(tenantDescriptor));
        }
    }
}
