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

package io.axoniq.framework.messaging.multitenancy.axonserver.queryhandling;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.AxonServerConnectionFactory;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.FlowControl;
import io.axoniq.axonserver.connector.ReplyChannel;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.connector.query.QueryDefinition;
import io.axoniq.axonserver.connector.query.QueryHandler;
import io.axoniq.axonserver.connector.query.SubscriptionQueryResult;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.common.lifecycle.ShutdownInProgressException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class MultiTenantAxonServerQueryBusConnectorTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant-1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant-2");
    private static final QualifiedName QUERY_ONE = new QualifiedName("query-one");
    private static final QualifiedName QUERY_TWO = new QualifiedName("query-two");

    @Nested
    class ConstructorValidation {

        private final AxonServerConfiguration configuration = defaultConfiguration();
        private final AxonServerConnectionManager connectionManager =
                new RecordingConnectionManager(configuration, Map.of());
        private final MessageConverter converter = Mockito.mock(MessageConverter.class);
        private final TenantRouter tenantResolver = new TenantRouter(
                new TenantResolver() {
                    @Override
                    public TenantDescriptor resolveTenant(Message message, Collection<TenantDescriptor> tenants) {
                        return tenants.stream()
                                      .findFirst()
                                      .orElseThrow(() -> new TenantNotResolvedException("no tenant"));
                    }

                    @Override
                    public Message attachTenant(Message message, TenantDescriptor tenant) {
                        return message.andMetadata(Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId()));
                    }
                },
                List::of);

        @Test
        void rejectsNullTenantResolver() {
            assertThatThrownBy(() -> new MultiTenantAxonServerQueryBusConnector(
                    null, connectionManager, configuration, converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullConnectionManager() {
            assertThatThrownBy(() -> new MultiTenantAxonServerQueryBusConnector(
                    tenantResolver, null, configuration, converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullConfiguration() {
            assertThatThrownBy(() -> new MultiTenantAxonServerQueryBusConnector(
                    tenantResolver, connectionManager, null, converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullConverter() {
            assertThatThrownBy(() -> new MultiTenantAxonServerQueryBusConnector(
                    tenantResolver, connectionManager, configuration, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Start {

        @Test
        void startReopensDispatchingAfterShutdown() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            testSubject.shutdownDispatching().join();

            // when / then: querying while still shut down fails
            assertThatThrownBy(() -> testSubject.query(queryFor(TENANT_1.tenantId()), null))
                    .isInstanceOf(ShutdownInProgressException.class);

            // when: the connector is started again
            testSubject.start();

            // then: querying is possible again
            assertThatCode(() -> testSubject.query(queryFor(TENANT_1.tenantId()), null))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    class Query {

        @Test
        void queryRoutesQueryToResolvedTenant() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            QueryMessage query = queryFor(TENANT_2.tenantId());
            MessageStream<QueryResponseMessage> result = testSubject.query(query, null);

            assertThat(result).isNotNull();
            assertThat(connection1.recordingQueryChannel().sentQueries()).isEmpty();
            assertThat(connection2.recordingQueryChannel().sentQueries()).hasSize(1);
            assertThat(connection2.recordingQueryChannel().sentQueries().get(0).getMessageIdentifier())
                    .isEqualTo(query.identifier());
        }

        @Test
        void queryingWithUnknownTenantFails() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            assertThatThrownBy(() -> testSubject.query(queryFor(TENANT_2.tenantId()), null))
                    .isInstanceOf(TenantNotResolvedException.class);
        }
    }

    @Nested
    class SubscriptionQuery {

        @Test
        void subscriptionQueryRoutesQueryToResolvedTenant() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            QueryMessage query = queryFor(TENANT_2.tenantId());
            MessageStream<QueryResponseMessage> result = testSubject.subscriptionQuery(query, null, 64);

            assertThat(result).isNotNull();
            assertThat(connection1.recordingQueryChannel().sentSubscriptionQueries()).isEmpty();
            assertThat(connection2.recordingQueryChannel().sentSubscriptionQueries()).hasSize(1);
            assertThat(connection2.recordingQueryChannel().sentSubscriptionQueries().get(0).getMessageIdentifier())
                    .isEqualTo(query.identifier());
        }

        @Test
        void subscriptionQueryingWithUnknownTenantFails() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            assertThatThrownBy(() -> testSubject.subscriptionQuery(queryFor(TENANT_2.tenantId()), null, 64))
                    .isInstanceOf(TenantNotResolvedException.class);
        }
    }

    @Nested
    class Subscribe {

        @Test
        void subscribeRegistersQueryHandlersOnAllKnownTenants() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(QUERY_ONE).join();
            testSubject.subscribe(QUERY_TWO).join();

            assertThat(connection1.recordingQueryChannel().registeredQueryNames())
                    .containsExactlyInAnyOrder(QUERY_ONE.name(), QUERY_TWO.name());
            assertThat(connection2.recordingQueryChannel().registeredQueryNames())
                    .containsExactlyInAnyOrder(QUERY_ONE.name(), QUERY_TWO.name());
        }

        @Test
        void resubscribingSameQueryIsANoOpAndDoesNotCreateNewRegistration() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            // when
            testSubject.subscribe(QUERY_ONE).join();
            testSubject.subscribe(QUERY_ONE).join();

            // then
            List<RecordingRegistration> registrationsForQuery =
                    connection1.recordingQueryChannel().allRegistrationsFor(QUERY_ONE.name());
            assertThat(registrationsForQuery).hasSize(1);
        }

        @Test
        void subscribingWithNoTenantsCompletesImmediately() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of());
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider, Map.of());

            // when
            CompletableFuture<Void> result = testSubject.subscribe(QUERY_ONE);

            // then
            assertThat(result).isCompleted();
        }
    }

    @Nested
    class Unsubscribe {

        @Test
        void unsubscribeRemovesQueryFromAllTenants() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(QUERY_ONE).join();
            boolean unsubscribed = testSubject.unsubscribe(QUERY_ONE);

            assertThat(unsubscribed).isTrue();
            assertThat(connection1.recordingQueryChannel().registeredQueryNames())
                    .doesNotContain(QUERY_ONE.name());
            assertThat(connection2.recordingQueryChannel().registeredQueryNames())
                    .doesNotContain(QUERY_ONE.name());
        }

        @Test
        void unsubscribingUnknownQueryReturnsFalse() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            // when
            boolean result = testSubject.unsubscribe(QUERY_ONE);

            // then
            assertThat(result).isFalse();
        }
    }

    @Nested
    class OnIncomingQuery {

        @Test
        void onIncomingQueryIsPropagatedToTenantsRegisteredAfterHandlerIsSet() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                 Map.of(TENANT_1.tenantId(),
                                                                                        connection1,
                                                                                        TENANT_2.tenantId(),
                                                                                        connection2));

            testSubject.subscribe(QUERY_ONE).join();

            List<QueryMessage> receivedQueries = new ArrayList<>();
            testSubject.onIncomingQuery(incomingQueryHandler(receivedQueries));

            // TENANT_2 registers after both subscribe() and onIncomingQuery() were already called
            tenantProvider.addTenant(TENANT_2);

            connection2.recordingQueryChannel().simulateIncomingQuery(incomingQuery(), new NoOpReplyChannel());

            assertThat(receivedQueries)
                    .as("Query on the tenant registered after onIncomingQuery() was not received")
                    .hasSize(1);
        }

        @Test
        void onIncomingQueryReachesTenantsRegisteredBeforeHandlerIsSet() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            testSubject.subscribe(QUERY_ONE).join();

            // when
            List<QueryMessage> receivedQueries = new ArrayList<>();
            testSubject.onIncomingQuery(incomingQueryHandler(receivedQueries));

            connection1.recordingQueryChannel().simulateIncomingQuery(incomingQuery(), new NoOpReplyChannel());

            // then
            assertThat(receivedQueries)
                    .as("Query on the tenant registered before onIncomingQuery() was not received")
                    .hasSize(1);
        }

        @Test
        void onIncomingQueryReplacesPreviouslySetHandler() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            testSubject.subscribe(QUERY_ONE).join();

            List<QueryMessage> firstHandlerQueries = new ArrayList<>();
            testSubject.onIncomingQuery(incomingQueryHandler(firstHandlerQueries));

            // when
            List<QueryMessage> secondHandlerQueries = new ArrayList<>();
            testSubject.onIncomingQuery(incomingQueryHandler(secondHandlerQueries));

            connection1.recordingQueryChannel().simulateIncomingQuery(incomingQuery(), new NoOpReplyChannel());

            // then
            assertThat(secondHandlerQueries)
                    .as("The replacing handler should receive the query")
                    .hasSize(1);
            assertThat(firstHandlerQueries)
                    .as("The replaced handler should no longer receive queries")
                    .isEmpty();
        }
    }

    @Nested
    class ShutdownDispatching {

        @Test
        void shutdownDispatchingCompletesForAllTenantConnectors() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            // when
            CompletableFuture<Void> result = testSubject.shutdownDispatching();

            // then
            assertThat(result).isCompleted();
        }

        @Test
        void shutdownDispatchingWithNoTenantsCompletesImmediately() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of());
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider, Map.of());

            // when
            CompletableFuture<Void> result = testSubject.shutdownDispatching();

            // then
            assertThat(result).isCompleted();
        }
    }

    @Nested
    class Disconnect {

        @Test
        void disconnectDisconnectsAllTenantConnections() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(QUERY_ONE).join();

            testSubject.disconnect().join();

            assertThat(connection1.recordingQueryChannel().prepareDisconnectCalled()).isTrue();
            assertThat(connection2.recordingQueryChannel().prepareDisconnectCalled()).isTrue();
            assertThat(connection1.disconnectCalls()).isEqualTo(1);
            assertThat(connection2.disconnectCalls()).isEqualTo(1);
        }
    }

    @Nested
    class TenantRegistration {

        @Test
        void registerAndStartTenantReplaysKnownQueries() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(QUERY_ONE).join();

            tenantProvider.addTenant(TENANT_2);

            assertThat(connection2.recordingQueryChannel().registeredQueryNames())
                    .contains(QUERY_ONE.name());
        }

        @Test
        void duplicateTenantRegistrationDoesNotLeakOrphanedQueryRegistration() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            testSubject.subscribe(QUERY_ONE).join();

            // TENANT_1 already has a connector; re-registering it must not replay known subscriptions onto that
            // already-in-sync connector, since AxonServerQueryBusConnector#subscribe(QualifiedName) does not cancel a
            // prior registration for the same query name and would otherwise leak one.
            testSubject.registerTenant(TENANT_1);

            List<RecordingRegistration> registrationsForQuery =
                    connection1.recordingQueryChannel().allRegistrationsFor(QUERY_ONE.name());
            long activeRegistrations = registrationsForQuery.stream()
                                                             .filter(registration -> !registration.isCancelled())
                                                             .count();

            assertThat(activeRegistrations)
                    .as("Re-registering an already-known tenant must not leave an orphaned, uncancelled query "
                                + "registration behind")
                    .isEqualTo(1);
        }
    }

    @Nested
    class TenantRemoval {

        @Test
        void cancellingRegistrationRemovesTenantAndDisconnectsItsConnector() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));
            Registration registration = testSubject.registerTenant(TENANT_1);

            // when
            boolean cancelled = registration.cancel();

            // then
            assertThat(cancelled).isTrue();
            assertThat(connection1.disconnectCalls()).isEqualTo(1);
            assertThatThrownBy(() -> testSubject.query(queryFor(TENANT_1.tenantId()), null))
                    .isInstanceOf(TenantNotResolvedException.class);
            assertThatCode(() -> testSubject.query(queryFor(TENANT_2.tenantId()), null))
                    .doesNotThrowAnyException();
        }

        @Test
        void cancellingAlreadyRemovedTenantRegistrationReturnsFalse() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                               Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            Registration registration = testSubject.registerTenant(TENANT_1);
            registration.cancel();

            // when
            boolean secondCancel = registration.cancel();

            // then
            assertThat(secondCancel).isFalse();
        }
    }

    @Nested
    class DescribeTo {

        @Test
        void describeToExposesTenantsSubscriptionsAndConnectors() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerQueryBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));
            testSubject.subscribe(QUERY_ONE).join();

            // when
            MockComponentDescriptor descriptor = new MockComponentDescriptor();
            testSubject.describeTo(descriptor);

            // then
            Set<TenantDescriptor> describedTenants = descriptor.getProperty("tenants");
            assertThat(describedTenants).containsExactlyInAnyOrder(TENANT_1, TENANT_2);

            Set<QualifiedName> describedSubscriptions = descriptor.getProperty("subscribedQueries");
            assertThat(describedSubscriptions).containsExactly(QUERY_ONE);

            Map<String, ?> describedConnectors = descriptor.getProperty("tenantConnectors");
            assertThat(describedConnectors).containsOnlyKeys(TENANT_1.tenantId(), TENANT_2.tenantId());
        }
    }

    private static AxonServerConfiguration defaultConfiguration() {
        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setClientId("client-id");
        configuration.setComponentName("component-name");
        return configuration;
    }

    private static MultiTenantAxonServerQueryBusConnector createSubject(TestTenantProvider tenantProvider,
                                                                         Map<String, RecordingConnection> connections) {
        AxonServerConfiguration configuration = defaultConfiguration();
        MessageConverter converter = Mockito.mock(MessageConverter.class);
        MultiTenantAxonServerQueryBusConnector connector = new MultiTenantAxonServerQueryBusConnector(
                routerFor(tenantProvider),
                new RecordingConnectionManager(configuration, connections),
                configuration,
                converter
        );
        tenantProvider.subscribe(connector);
        return connector;
    }

    /**
     * A router resolving on the tenant named in the command's metadata, against the tenants the provider knows.
     */
    private static TenantRouter routerFor(TenantProvider tenantProvider) {
        return new TenantRouter(
                new TenantResolver() {
                    @Override
                    public TenantDescriptor resolveTenant(Message message, Collection<TenantDescriptor> tenants) {
                        return tenants.stream()
                                      .filter(tenant -> tenant.tenantId().equals(message.metadata().get("tenantId")))
                                      .findFirst()
                                      .orElseThrow(() -> new TenantNotResolvedException(
                                              "No tenant found in metadata"));
                    }

                    @Override
                    public Message attachTenant(Message message, TenantDescriptor tenant) {
                        return message.andMetadata(Map.of("tenantId", tenant.tenantId()));
                    }
                },
                tenantProvider);
    }

    private static QueryMessage queryFor(String tenantId) {
        return new GenericQueryMessage(
                new GenericMessage("message-id",
                                   new MessageType(QUERY_ONE.name()),
                                   "payload".getBytes(),
                                   Map.of("tenantId", tenantId))
        );
    }

    private static QueryBusConnector.Handler incomingQueryHandler(List<QueryMessage> receivedQueries) {
        return new QueryBusConnector.Handler() {
            @Override
            public MessageStream<QueryResponseMessage> query(QueryMessage query) {
                receivedQueries.add(query);
                return MessageStream.empty().cast();
            }

            @Override
            public Registration registerUpdateHandler(QueryMessage subscriptionQueryMessage,
                                                       QueryBusConnector.UpdateCallback updateCallback) {
                return () -> true;
            }
        };
    }

    private static QueryRequest incomingQuery() {
        return QueryRequest.newBuilder()
                           .setMessageIdentifier("incoming-message-id")
                           .setQuery(QUERY_ONE.name())
                           .setPayload(SerializedObject.newBuilder()
                                                       .setType(QUERY_ONE.name())
                                                       .setRevision("0")
                                                       .setData(com.google.protobuf.ByteString.EMPTY)
                                                       .build())
                           .build();
    }

    private static final class TestTenantProvider implements TenantProvider {

        private final List<TenantDescriptor> tenants = new ArrayList<>();
        private final List<io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent> components =
                new ArrayList<>();

        private TestTenantProvider(Collection<TenantDescriptor> tenants) {
            this.tenants.addAll(tenants);
        }

        @Override
        public Registration subscribe(
                io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent component) {
            components.add(component);
            tenants.forEach(component::registerAndStartTenant);
            return () -> components.remove(component);
        }

        @Override
        public List<TenantDescriptor> tenants() {
            return List.copyOf(tenants);
        }

        private void addTenant(TenantDescriptor tenantDescriptor) {
            tenants.add(tenantDescriptor);
            components.forEach(component -> component.registerAndStartTenant(tenantDescriptor));
        }
    }

    private static final class RecordingConnectionManager extends AxonServerConnectionManager {

        private final Map<String, RecordingConnection> connections;

        private RecordingConnectionManager(AxonServerConfiguration configuration,
                                           Map<String, RecordingConnection> connections) {
            super(builder(configuration), new RecordingConnectionFactory(builder(configuration), connections));
            this.connections = connections;
        }

        @Override
        public AxonServerConnection getConnection(String context) {
            RecordingConnection connection = connections.get(context);
            if (connection == null) {
                throw new IllegalArgumentException("Unknown context " + context);
            }
            return connection;
        }

        private static AxonServerConnectionManager.Builder builder(AxonServerConfiguration configuration) {
            return AxonServerConnectionManager.builder()
                                              .axonServerConfiguration(configuration)
                                              .routingServers("localhost:8124");
        }
    }

    private static final class RecordingConnectionFactory extends AxonServerConnectionFactory {

        private final Map<String, RecordingConnection> connections;

        private RecordingConnectionFactory(AxonServerConnectionManager.Builder builder,
                                           Map<String, RecordingConnection> connections) {
            super(new AxonServerConnectionFactoryBuilder(builder));
            this.connections = connections;
        }

        @Override
        public AxonServerConnection connect(String context) {
            RecordingConnection connection = connections.get(context);
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

    private static final class AxonServerConnectionFactoryBuilder
            extends AxonServerConnectionFactory.Builder {

        private AxonServerConnectionFactoryBuilder(AxonServerConnectionManager.Builder builder) {
            super("component-name", "client-id");
            routingServers(new io.axoniq.axonserver.connector.impl.ServerAddress("localhost", 8124));
        }

        @Override
        public AxonServerConnectionFactory build() {
            throw new UnsupportedOperationException("Not used in tests");
        }
    }

    private static final class RecordingConnection implements AxonServerConnection {

        private final RecordingQueryChannel queryChannel = new RecordingQueryChannel();
        private int disconnectCalls;
        private boolean connected = true;

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
            return connected;
        }

        @Override
        public void disconnect() {
            connected = false;
            disconnectCalls++;
        }

        @Override
        public ControlChannel controlChannel() {
            return null;
        }

        @Override
        public CommandChannel commandChannel() {
            return null;
        }

        @Override
        public EventChannel eventChannel() {
            return null;
        }

        @Override
        public DcbEventChannel dcbEventChannel() {
            return null;
        }

        @Override
        public QueryChannel queryChannel() {
            return queryChannel;
        }

        @Override
        public SnapshotChannel snapshotChannel() {
            return null;
        }

        @Override
        public EventTransformationChannel eventTransformationChannel() {
            return null;
        }

        @Override
        public AdminChannel adminChannel() {
            return null;
        }

        private RecordingQueryChannel recordingQueryChannel() {
            return queryChannel;
        }

        private int disconnectCalls() {
            return disconnectCalls;
        }
    }

    private static final class RecordingQueryChannel implements QueryChannel {

        private final List<QueryRequest> sentQueries = new ArrayList<>();
        private final List<QueryRequest> sentSubscriptionQueries = new ArrayList<>();
        private final Map<String, RecordingRegistration> registrations = new LinkedHashMap<>();
        private final Map<String, List<RecordingRegistration>> allRegistrationsByQuery = new LinkedHashMap<>();
        private final Map<String, QueryHandler> handlers = new LinkedHashMap<>();
        private boolean prepareDisconnectCalled;

        @Override
        public io.axoniq.axonserver.connector.Registration registerQueryHandler(QueryHandler handler,
                                                                                  QueryDefinition... queryTypes) {
            String[] queryNames = Arrays.stream(queryTypes).map(QueryDefinition::getQueryName).toArray(String[]::new);
            RecordingRegistration registration = new RecordingRegistration(registrations, queryNames);
            for (String queryName : queryNames) {
                registrations.put(queryName, registration);
                handlers.put(queryName, handler);
                allRegistrationsByQuery.computeIfAbsent(queryName, key -> new ArrayList<>()).add(registration);
            }
            return registration;
        }

        private List<RecordingRegistration> allRegistrationsFor(String queryName) {
            return List.copyOf(allRegistrationsByQuery.getOrDefault(queryName, List.of()));
        }

        private FlowControl simulateIncomingQuery(QueryRequest query, ReplyChannel<QueryResponse> replyChannel) {
            return handlers.get(query.getQuery()).stream(query, replyChannel);
        }

        @Override
        public ResultStream<QueryResponse> query(QueryRequest query) {
            sentQueries.add(query);
            return new EmptyResultStream<>();
        }

        @Override
        public SubscriptionQueryResult subscriptionQuery(QueryRequest query, int bufferSize, int fetchSize) {
            sentSubscriptionQueries.add(query);
            return new RecordingSubscriptionQueryResult();
        }

        @Override
        public CompletableFuture<Void> prepareDisconnect() {
            prepareDisconnectCalled = true;
            return CompletableFuture.completedFuture(null);
        }

        private List<QueryRequest> sentQueries() {
            return sentQueries;
        }

        private List<QueryRequest> sentSubscriptionQueries() {
            return sentSubscriptionQueries;
        }

        private List<String> registeredQueryNames() {
            return List.copyOf(registrations.keySet());
        }

        private boolean prepareDisconnectCalled() {
            return prepareDisconnectCalled;
        }
    }

    private static final class RecordingRegistration implements io.axoniq.axonserver.connector.Registration {

        private final Map<String, RecordingRegistration> registrations;
        private final List<String> queryNames;
        private boolean cancelled;

        private RecordingRegistration(Map<String, RecordingRegistration> registrations, String... queryNames) {
            this.registrations = registrations;
            this.queryNames = List.of(queryNames);
        }

        @Override
        public CompletableFuture<Void> cancel() {
            cancelled = true;
            queryNames.forEach(registrations::remove);
            return CompletableFuture.completedFuture(null);
        }

        private boolean isCancelled() {
            return cancelled;
        }
    }

    private static final class EmptyResultStream<T> implements ResultStream<T> {

        @Override
        public T peek() {
            return null;
        }

        @Override
        public T nextIfAvailable() {
            return null;
        }

        @Override
        public T nextIfAvailable(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public T next() {
            return null;
        }

        @Override
        public void onAvailable(Runnable callback) {
            // no-op: never has anything available
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public Optional<Throwable> getError() {
            return Optional.empty();
        }
    }

    private static final class RecordingSubscriptionQueryResult implements SubscriptionQueryResult {

        @Override
        public CompletableFuture<QueryResponse> initialResult() {
            return CompletableFuture.completedFuture(QueryResponse.getDefaultInstance());
        }

        @Override
        public ResultStream<QueryResponse> initialResults() {
            return new EmptyResultStream<>();
        }

        @Override
        public ResultStream<QueryUpdate> updates() {
            return new EmptyResultStream<>();
        }
    }

    private static final class NoOpReplyChannel implements ReplyChannel<QueryResponse> {

        @Override
        public void send(QueryResponse outboundMessage) {
            // no-op
        }

        @Override
        public void complete() {
            // no-op
        }

        @Override
        public void completeWithError(ErrorMessage errorMessage) {
            // no-op
        }

        @Override
        public void completeWithError(ErrorCategory errorCategory, String message) {
            // no-op
        }
    }
}
