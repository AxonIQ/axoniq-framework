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

package io.axoniq.framework.messaging.multitenancy.axonserver;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Verifies the {@link AxonServerMultiTenancyConfigurationDefaults} against a real {@link MessagingConfigurer}: the
 * Axon Server-backed multi-tenancy components are wired for a given configuration, and the enhancer only acts when
 * multi-tenancy is enabled.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 */
class AxonServerMultiTenancyConfigurationDefaultsTest {

    @Test
    void orderEqualsEnhancerOrderConstant() {
        assertThat(new AxonServerMultiTenancyConfigurationDefaults().order())
                .isEqualTo(AxonServerMultiTenancyConfigurationDefaults.ENHANCER_ORDER);
    }

    @Test
    void enhanceIsANoOpWhenMultiTenancyIsNotEnabled() {
        // when
        AxonConfiguration configuration = MessagingConfigurer.create().build();

        // then none of the Axon Server-backed multi-tenancy defaults were registered
        assertThat(configuration.hasComponent(TenantProvider.class)).isFalse();
        assertThat(configuration.getComponent(CommandBusConnector.class))
                .extracting("delegate")
                .isNotInstanceOf(MultiTenantAxonServerCommandBusConnector.class);
    }

    @Nested
    class DefaultComponentRegistration {

        private final TenantComponentProvider<CourseRepository> componentProvider =
                TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

        private AxonConfiguration configuration;

        @BeforeEach
        void buildConfiguration() {
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                                               .componentRegistry(registry -> registry.registerComponent(
                                                       TenantComponentProvider.class,
                                                       config -> componentProvider))
                                               .build();
        }

        @Test
        void registersTheDefaultAxonServerTenantProvider() {
            assertThat(configuration.getComponent(TenantProvider.class))
                    .isInstanceOf(AxonServerTenantProvider.class);
        }

        @Test
        void registersTheDefaultMultiTenantAxonServerCommandBusConnector() {
            assertThat(configuration.getComponent(CommandBusConnector.class))
                    .extracting("delegate")
                    .isInstanceOf(MultiTenantAxonServerCommandBusConnector.class);
        }

        @Test
        void registersTheMultiTenantEventStorageEngineAsTheEventStorageEngine() {
            // the routing engine resolves snapshots itself, so the framework registers it undecorated
            assertThat(configuration.getComponent(EventStorageEngine.class))
                    .isInstanceOf(MultiTenantEventStorageEngine.class);
        }

        @Test
        void registersTheSameRoutingEngineInstanceAsTheSnapshotStore() {
            // the same instance under both types, so the framework does not complement the routing engine with a
            // snapshot store above the fan-out
            assertThat(configuration.getComponent(SnapshotStore.class))
                    .isInstanceOf(MultiTenantEventStorageEngine.class)
                    .isSameAs(configuration.getComponent(EventStorageEngine.class));
        }
    }

    @Nested
    class ForeignSnapshotStoreRejection {

        // A SnapshotStore registered elsewhere would be complemented onto the routing engine above the tenant fan-out,
        // resolving snapshots before a tenant is known. That has to fail loudly rather than degrade isolation silently.
        // Verified at startup rather than while enhancing, so it holds whichever order the registrations happen in.
        @Test
        void rejectsASnapshotStoreRegisteredByTheApplication() {
            SnapshotStore singleTenantStore = new RecordingSnapshotStore();
            AxonConfiguration configuration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry.registerComponent(
                                               SnapshotStore.class, config -> singleTenantStore))
                                       .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                                       .build();

            assertThatThrownBy(configuration::start)
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining("SnapshotStore")
                    .hasMessageContaining("TenantSnapshotStoreFactory");
        }

        // The mirror image: replacing the EventStorageEngine leaves the routing engine as the SnapshotStore only, which
        // pairs snapshot positions from a tenant's store with a tail sourced from a non-tenant engine.
        @Test
        void rejectsAnEventStorageEngineRegisteredByTheApplication() {
            EventStorageEngine singleTenantEngine = new RecordingEventStorageEngine();
            AxonConfiguration configuration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry.registerComponent(
                                               EventStorageEngine.class, config -> singleTenantEngine))
                                       .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                                       .build();

            assertThatThrownBy(configuration::start)
                    .hasRootCauseInstanceOf(AxonConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining("EventStorageEngine")
                    .hasMessageContaining("TenantEventStorageEngineFactory");
        }
    }

    @Nested
    class TenantLifecycleWiring {

        private final StubTenantProvider tenantProvider = new StubTenantProvider();
        private final TenantComponentProvider<CourseRepository> componentProvider =
                TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

        private AxonConfiguration configuration;

        @BeforeEach
        void buildAndStartConfiguration() {
            tenantProvider.addTenant(TENANT_A);
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                                               .componentRegistry(registry -> registry
                                                       .registerComponent(TenantProvider.class,
                                                                          config -> tenantProvider)
                                                       .registerComponent(TenantComponentProvider.class,
                                                                          config -> componentProvider))
                                               .build();
            configuration.start();
        }

        @AfterEach
        void shutdownConfiguration() {
            configuration.shutdown();
        }

        @Test
        void subscribesTheCommandBusConnectorToTheTenantProviderAtStartup() throws Exception {
            // given
            MultiTenantAwareComponent connector = commandBusConnectorDelegate(configuration);

            // then
            assertThat(tenantProvider.subscribedComponents()).contains(connector);
        }

        @Test
        void cancelsTheCommandBusConnectorSubscriptionOnShutdown() throws Exception {
            // given
            MultiTenantAwareComponent connector = commandBusConnectorDelegate(configuration);

            // when
            configuration.shutdown();

            // then
            assertThat(tenantProvider.subscribedComponents()).doesNotContain(connector);
        }

        @Test
        void subscribesTheEventStorageEngineFactoryToTheTenantProviderAtStartup() {
            MultiTenantAwareComponent factory =
                    (MultiTenantAwareComponent) configuration.getComponent(TenantEventStorageEngineFactory.class);

            assertThat(tenantProvider.subscribedComponents()).contains(factory);
        }

        @Test
        void cancelsTheEventStorageEngineFactorySubscriptionOnShutdown() {
            MultiTenantAwareComponent factory =
                    (MultiTenantAwareComponent) configuration.getComponent(TenantEventStorageEngineFactory.class);

            configuration.shutdown();

            assertThat(tenantProvider.subscribedComponents()).doesNotContain(factory);
        }

        @Test
        void subscribesTheSnapshotStoreFactoryToTheTenantProviderAtStartup() {
            MultiTenantAwareComponent factory =
                    (MultiTenantAwareComponent) configuration.getComponent(TenantSnapshotStoreFactory.class);

            assertThat(tenantProvider.subscribedComponents()).contains(factory);
        }

        @Test
        void cancelsTheSnapshotStoreFactorySubscriptionOnShutdown() {
            MultiTenantAwareComponent factory =
                    (MultiTenantAwareComponent) configuration.getComponent(TenantSnapshotStoreFactory.class);

            configuration.shutdown();

            assertThat(tenantProvider.subscribedComponents()).doesNotContain(factory);
        }
    }

    @Nested
    @ExtendWith(MockitoExtension.class)
    class TenantConnectorPopulationAtStartup {

        private final String tenantId = TENANT_A.tenantId();

        @Mock
        private AxonServerConnectionManager connectionManager;
        @Mock
        private AxonServerConnection connection;
        @Mock
        private AdminChannel adminChannel;
        @Mock
        private ResultStream<ContextUpdate> contextUpdates;
        // AxonServerConfigurationEnhancer wires a real, network-connecting QueryBusConnector on top of the (mocked)
        // AxonServerConnectionManager; overriding it here keeps that enhancer's other real defaults (AxonServerConfiguration,
        // MessageConverter) while short-circuiting the one component that would otherwise fail configuration.start().
        // that will probably be removed once we implement the query handling part
        @Mock
        private QueryBusConnector queryBusConnector;

        private AxonConfiguration configuration;

        @BeforeEach
        void buildAndStartConfiguration() {
            // The real AxonServerTenantProvider talks to Axon Server's admin API to discover tenants, so the
            // connection manager backing it is faked here instead of using the StubTenantProvider the other nested
            // classes rely on: only a real provider proves the phase-ordering wiring in
            // AxonServerMultiTenancyConfigurationDefaults.
            when(connectionManager.getConnection(ADMIN_CONTEXT)).thenReturn(connection);
            when(connectionManager.getConnection(tenantId)).thenReturn(connection);
            // EventProcessorControlService (wired by the real AxonServerConfigurationEnhancer, since scanning stays
            // enabled) resolves the default context connection at startup regardless of this test's tenant.
            when(connectionManager.getConnection(DEFAULT_CONTEXT)).thenReturn(connection);
            when(connection.adminChannel()).thenReturn(adminChannel);
            when(adminChannel.getAllContexts()).thenReturn(
                    CompletableFuture.completedFuture(List.of(contextOverview(tenantId))));
            when(adminChannel.subscribeToContextUpdates()).thenReturn(contextUpdates);

            configuration = MessagingConfigurer.create()
                                               .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                                               .componentRegistry(registry -> registry
                                                       .registerComponent(AxonServerConnectionManager.class,
                                                                          config -> connectionManager)
                                                       .registerComponent(QueryBusConnector.class,
                                                                          config -> queryBusConnector))
                                               .build();
            configuration.start();
        }

        @AfterEach
        void shutdownConfiguration() {
            configuration.shutdown();
        }

        @Test
        void tenantConnectorsArePopulatedForEveryTenantTheRealProviderDiscoveredAtStartup() throws Exception {
            // AxonServerTenantProvider#start() runs at TENANT_PROVIDER_PHASE and the connector subscribes one phase
            // later, so by the time this runs, the tenant discovered during start() must already have a connector.
            MultiTenantAxonServerCommandBusConnector connector =
                    (MultiTenantAxonServerCommandBusConnector) commandBusConnectorDelegate(configuration);

            MockComponentDescriptor descriptor = new MockComponentDescriptor();
            connector.describeTo(descriptor);
            Map<String, ?> tenantConnectors = descriptor.getProperty("tenantConnectors");

            assertThat(tenantConnectors).containsOnlyKeys(tenantId);
        }

        private ContextOverview contextOverview(String contextName) {
            return ContextOverview.newBuilder()
                                  .setName(contextName)
                                  .setReplicationGroup(
                                          ReplicationGroupOverview.newBuilder()
                                                                  .setName("default-rg")
                                                                  .build())
                                  .build();
        }
    }

    private static MultiTenantAwareComponent commandBusConnectorDelegate(AxonConfiguration configuration)
            throws Exception {
        CommandBusConnector connector = configuration.getComponent(CommandBusConnector.class);
        Field delegateField = delegateField(connector.getClass());
        if (delegateField == null) {
            // No decorator in front of it (e.g. PayloadConvertingCommandBusConnector, wired by the AxonServerConnector
            // module's own enhancer): the resolved component already is the multi-tenant connector itself.
            return (MultiTenantAwareComponent) connector;
        }
        delegateField.setAccessible(true);
        return (MultiTenantAwareComponent) delegateField.get(connector);
    }

    // The PayloadConvertingCommandBusConnector decorator declares "delegate" on a superclass, not on itself.
    // Returns null when no such field exists anywhere in the hierarchy, i.e. connector isn't wrapped at all.
    private static Field delegateField(Class<?> type) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            try {
                return current.getDeclaredField("delegate");
            } catch (NoSuchFieldException ignored) {
                // Keep searching up the hierarchy.
            }
        }
        return null;
    }

    private static final class CourseRepository implements AutoCloseable {

        private final TenantDescriptor tenant;
        private boolean closed;

        private CourseRepository(TenantDescriptor tenant) {
            this.tenant = tenant;
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}
