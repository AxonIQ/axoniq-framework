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

package io.axoniq.framework.messaging.multitenancy.axonserver.configuration;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.axonserver.api.AxonServerTenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.commandhandling.MultiTenantAxonServerCommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing.AxonServerTenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventsourcing.AxonServerTenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.axonserver.queryhandling.MultiTenantAxonServerQueryBusConnector;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantEventStorageEngineFactory;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import io.axoniq.framework.messaging.multitenancy.util.RecordingSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.SnapshotCapableEventStorageEngine;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.jspecify.annotations.Nullable;
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
 * Verifies the {@link AxonServerMultiTenancyConfigurationDefaults} against a real {@link MessagingConfigurer}: the Axon
 * Server-backed multi-tenancy components are wired for a given configuration out of the box, and stay away once
 * multi-tenancy is disabled.
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
    void enhanceIsANoOpWhenMultiTenancyIsDisabled() {
        // when
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(MultiTenancyUtils::disable)
                                   .build();

        // then none of the Axon Server-backed multi-tenancy defaults were registered
        assertThat(configuration.hasComponent(TenantProvider.class)).isFalse();
        assertThat(configuration.hasComponent(TenantRouter.class)).isFalse();
        assertThat(configuration.getComponent(CommandBusConnector.class))
                .extracting("delegate")
                .isNotInstanceOf(MultiTenantAxonServerCommandBusConnector.class);
        assertThat(configuration.getComponent(QueryBusConnector.class))
                .extracting("delegate")
                .isNotInstanceOf(MultiTenantAxonServerQueryBusConnector.class);
    }

    @Nested
    class MissingAxonServerConnection {

        private AxonConfiguration configuration;

        @BeforeEach
        void buildConfigurationWithoutAxonServer() {
            // multi-tenancy left active while the Axon Server connector is disabled, so nothing registers a
            // connection manager for the tenants to be contexts of
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(registry -> registry.disableEnhancer(
                                                       AxonServerConfigurationEnhancer.class))
                                               .build();
        }

        @Test
        void resolvingTheTenantProviderFailsWithAnActionableMessage() {
            // when resolving the tenant provider
            // then the failure names the cause and both ways out, rather than a bare missing-component message
            assertThatThrownBy(() -> configuration.getComponent(TenantProvider.class))
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("no AxonServerConnectionManager is configured")
                    .hasMessageContaining("MultiTenancyUtils#disable");
        }

        @Test
        void resolvingTheCommandBusConnectorFailsWithAnActionableMessage() {
            // when resolving the multi-tenant command bus connector, which needs the same connection
            // then it fails the same diagnosable way
            assertThatThrownBy(() -> configuration.getComponent(CommandBusConnector.class))
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("no AxonServerConnectionManager is configured")
                    .hasMessageContaining("MultiTenancyUtils#disable");
        }
    }

    private static AxonServerConnectionManager stubConnectionManager() {
        AxonServerConnectionManager connectionManager = mock(AxonServerConnectionManager.class);
        AxonServerConnection connection = mock(AxonServerConnection.class);
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connectionManager.getConnection(anyString())).thenReturn(connection);
        return connectionManager;
    }

    @Nested
    class ForeignStorageComponentRejection {

        // An EventStorageEngine registered elsewhere serves every tenant from one place, so no tenant keeps its events
        // to itself and streamed events carry no tenant, which is what a tenant-scoped component is resolved from.
        @Test
        void rejectsAnEventStorageEngineRegisteredByTheApplication() {
            EventStorageEngine singleTenantEngine = new InMemoryEventStorageEngine();
            MessagingConfigurer configurer =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry.registerComponent(
                                               EventStorageEngine.class, config -> singleTenantEngine));

            assertThatThrownBy(configurer::build)
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("EventStorageEngine")
                    .hasMessageContaining("TenantEventStorageEngineFactory");
        }

        // The rejection must not trip on the framework's own default engine: EventSourcingConfigurationDefaults
        // registers an InMemoryEventStorageEngine, but at an order far after this enhancer, so it backs off instead.
        @Test
        void acceptsTheDefaultEventSourcingSetupAndYieldsTheRoutingEngine() {
            AxonConfiguration defaultSetup =
                    EventSourcingConfigurer.create()
                                           .build();

            assertThat(defaultSetup.getComponent(EventStorageEngine.class))
                    .isInstanceOf(MultiTenantEventStorageEngine.class);
        }

        // A SnapshotStore registered elsewhere serves every tenant from one place, while sourcing keeps reading each
        // tenant's snapshots from that tenant's own store. That has to fail loudly rather than write and read snapshots
        // in different places.
        @Test
        void rejectsASnapshotStoreRegisteredByTheApplication() {
            SnapshotStore singleTenantStore = new RecordingSnapshotStore();
            MessagingConfigurer configurer =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry.registerComponent(
                                               SnapshotStore.class, config -> singleTenantStore));

            assertThatThrownBy(configurer::build)
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining("SnapshotStore")
                    .hasMessageContaining("TenantSnapshotStoreFactory");
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
                                               .componentRegistry(registry -> registry
                                                       .registerComponent(AxonServerConnectionManager.class,
                                                                          config -> connectionManager))
                                               .build();
            configuration.start();
        }

        @AfterEach
        void shutdownConfiguration() {
            configuration.shutdown();
        }

        @Test
        void tenantConnectorsArePopulatedForEveryTenantTheRealProviderDiscoveredAtStartupForCommandConnector() throws Exception {
            // AxonServerTenantProvider#start() runs at TENANT_PROVIDER_PHASE and the connector subscribes one phase
            // later, so by the time this runs, the tenant discovered during start() must already have a connector.
            MultiTenantAxonServerCommandBusConnector connector =
                    (MultiTenantAxonServerCommandBusConnector) commandBusConnectorDelegate(configuration);

            MockComponentDescriptor descriptor = new MockComponentDescriptor();
            connector.describeTo(descriptor);
            Map<String, ?> tenantConnectors = descriptor.getProperty("tenantConnectors");

            assertThat(tenantConnectors).containsOnlyKeys(tenantId);
        }

        @Test
        void tenantConnectorsArePopulatedForEveryTenantTheRealProviderDiscoveredAtStartupForQueryConnector()
                throws Exception {
            // Same phase-ordering guarantee as the command bus connector, verified for its query bus counterpart.
            MultiTenantAxonServerQueryBusConnector connector =
                    (MultiTenantAxonServerQueryBusConnector) queryBusConnectorDelegate(configuration);

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

    @Nested
    class DefaultComponentRegistration {

        private final TenantComponentProvider<CourseRepository> componentProvider =
                TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

        private AxonConfiguration configuration;

        @BeforeEach
        void buildConfiguration() {
            configuration = MessagingConfigurer.create()
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
        void registersTheDefaultMultiTenantAxonServerCommandBusConnector() throws Exception {
            assertThat(commandBusConnectorDelegate(configuration))
                    .isInstanceOf(MultiTenantAxonServerCommandBusConnector.class);
        }

        @Test
        void registersTheMultiTenantEventStorageEngineAsTheEventStorageEngine() {
            assertThat(configuration.getComponent(EventStorageEngine.class))
                    .isInstanceOf(MultiTenantEventStorageEngine.class);
        }

        @Test
        void subscribesTheFactoriesBeforeTheRoutingEngineThatComposesFromThem() {
            // The engine announces a tenant only once it holds it, and whatever acts on that announcement composes
            // through the factories, so a factory that has not been told yet fails the merged stream for every tenant.
            StubTenantProvider orderedProvider = new StubTenantProvider();
            AxonConfiguration orderedConfiguration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry
                                               .registerComponent(TenantProvider.class, config -> orderedProvider)
                                               .registerComponent(TenantComponentProvider.class,
                                                                  config -> componentProvider)
                                               .registerComponent(AxonServerConnectionManager.class,
                                                                  config -> stubConnectionManager()))
                                       .build();
            orderedConfiguration.start();
            try {
                List<Class<?>> subscriptionOrder = orderedProvider.subscribedComponents()
                                                                  .stream()
                                                                  .<Class<?>>map(Object::getClass)
                                                                  .toList();

                assertThat(subscriptionOrder).containsSubsequence(AxonServerTenantSnapshotStoreFactory.class,
                                                                  MultiTenantEventStorageEngine.class);
                assertThat(subscriptionOrder).containsSubsequence(AxonServerTenantEventStorageEngineFactory.class,
                                                                  MultiTenantEventStorageEngine.class);
            } finally {
                orderedConfiguration.shutdown();
            }
        }

        @Test
        void subscribesAndFollowsTheRoutingEngineItselfWhenTheEventStorageEngineIsDecorated() {
            // A decorator returns the decorated type, so resolving the event storage engine no longer yields the
            // tenant-routing engine. Both the tenant lifecycle subscription and the restarter's listener still have to
            // reach the engine itself, which they do because a start handler is bound to its own component rather than
            // to the decorated one. Resolving instead would follow a decorator that announces no tenant change.
            StubTenantProvider decoratedProvider = new StubTenantProvider();
            AxonConfiguration decoratedConfiguration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry
                                               .registerComponent(TenantProvider.class, config -> decoratedProvider)
                                               .registerComponent(TenantComponentProvider.class,
                                                                  config -> componentProvider)
                                               .registerComponent(AxonServerConnectionManager.class,
                                                                  config -> stubConnectionManager())
                                               .registerDecorator(EventStorageEngine.class, 0,
                                                                  (config, name, delegate) ->
                                                                          SnapshotCapableEventStorageEngine.decorate(
                                                                                  delegate,
                                                                                  new InMemorySnapshotStore())))
                                       .build();
            decoratedConfiguration.start();
            try {
                MultiTenantEventStorageEngine routingEngine =
                        decoratedProvider.subscribedComponents()
                                         .stream()
                                         .filter(MultiTenantEventStorageEngine.class::isInstance)
                                         .map(MultiTenantEventStorageEngine.class::cast)
                                         .findFirst()
                                         .orElseThrow();
                MockComponentDescriptor descriptor = new MockComponentDescriptor();
                routingEngine.describeTo(descriptor);

                assertThat(decoratedConfiguration.getComponent(EventStorageEngine.class))
                        .isInstanceOf(SnapshotCapableEventStorageEngine.class);
                assertThat(decoratedProvider.subscribedComponents())
                        .filteredOn(MultiTenantEventStorageEngine.class::isInstance)
                        .hasSize(1);
                // The restarter's listener sits on the routing engine, not on the decorator wrapping it.
                assertThat(descriptor.getDescribedProperties()).containsEntry("tenantChangeListenerCount", 1);
            } finally {
                decoratedConfiguration.shutdown();
            }
        }

        @Test
        void subscribesTheRoutingEngineToTheTenantProviderExactlyOnce() {
            // Registering the engine twice, or wrapping a second registration in a subscribed component, would register
            // every tenant with it twice and recompose each tenant's engine.
            StubTenantProvider countingProvider = new StubTenantProvider();
            AxonConfiguration countedConfiguration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry
                                               .registerComponent(TenantProvider.class, config -> countingProvider)
                                               .registerComponent(TenantComponentProvider.class,
                                                                  config -> componentProvider)
                                               .registerComponent(AxonServerConnectionManager.class,
                                                                  config -> stubConnectionManager()))
                                       .build();
            countedConfiguration.start();
            try {
                assertThat(countingProvider.subscribedComponents())
                        .filteredOn(MultiTenantEventStorageEngine.class::isInstance)
                        .hasSize(1);
            } finally {
                countedConfiguration.shutdown();
            }
        }

        @Test
        void registersTheMultiTenantSnapshotStoreAsTheSnapshotStore() {
            assertThat(configuration.getComponent(SnapshotStore.class))
                    .isInstanceOf(MultiTenantSnapshotStore.class);
        }

        @Test
        void leavesTheRoutingEngineUndecoratedSoSnapshotSourcingReachesEachTenantsOwnEngine() {
            // the application-wide snapshot composition is disabled, so the framework does not decorate the routing
            // engine with the snapshot store above the tenant fan-out
            assertThat(configuration.getComponent(EventStorageEngine.class))
                    .isNotInstanceOf(SnapshotCapableEventStorageEngine.class);
        }

        @Test
        void routesWithTheTenantRouterFromTheConfiguration() {
            MockComponentDescriptor descriptor = new MockComponentDescriptor();

            configuration.getComponent(EventStorageEngine.class).describeTo(descriptor);

            assertThat(descriptor.getDescribedProperties())
                    .containsEntry("tenantRouter", configuration.getComponent(TenantRouter.class));
        }

        @Test
        void registersTheDefaultMultiTenantAxonServerQueryBusConnector() {
            assertThat(configuration.getComponent(QueryBusConnector.class))
                    .extracting("delegate")
                    .isInstanceOf(MultiTenantAxonServerQueryBusConnector.class);
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
                                               .componentRegistry(registry -> registry
                                                       .registerComponent(TenantProvider.class,
                                                                          config -> tenantProvider)
                                                       .registerComponent(TenantComponentProvider.class,
                                                                          config -> componentProvider)
                                                       .registerComponent(AxonServerConnectionManager.class,
                                                                          config -> stubConnectionManager()))
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
        void subscribesTheQueryBusConnectorToTheTenantProviderAtStartup() throws Exception {
            // given
            MultiTenantAwareComponent connector = queryBusConnectorDelegate(configuration);

            // then
            assertThat(tenantProvider.subscribedComponents()).contains(connector);
        }

        @Test
        void cancelsTheQueryBusConnectorSubscriptionOnShutdown() throws Exception {
            // given
            MultiTenantAwareComponent connector = queryBusConnectorDelegate(configuration);

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
        void subscribesAReplacementEventStorageEngineFactoryToTheTenantProviderAtStartup() {
            TenantEventStorageEngineFactory replacement = mock(TenantEventStorageEngineFactory.class,
                                                               withSettings().extraInterfaces(
                                                                       MultiTenantAwareComponent.class));
            MultiTenantAwareComponent awareReplacement = (MultiTenantAwareComponent) replacement;
            when(awareReplacement.registerTenant(any())).thenReturn(() -> true);

            AxonConfiguration replacementConfiguration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry
                                               .registerComponent(TenantProvider.class, config -> tenantProvider)
                                               .registerComponent(TenantEventStorageEngineFactory.class,
                                                                  config -> replacement))
                                       .build();
            replacementConfiguration.start();
            try {
                assertThat(tenantProvider.subscribedComponents()).contains(awareReplacement);
            } finally {
                replacementConfiguration.shutdown();
            }

            assertThat(tenantProvider.subscribedComponents()).doesNotContain(awareReplacement);
        }

        @Test
        void subscribesTheRoutingEngineToTheTenantProviderAtStartup() {
            MultiTenantAwareComponent routingEngine =
                    (MultiTenantAwareComponent) configuration.getComponent(EventStorageEngine.class);

            assertThat(tenantProvider.subscribedComponents()).contains(routingEngine);
        }

        @Test
        void cancelsTheRoutingEngineSubscriptionOnShutdown() {
            MultiTenantAwareComponent routingEngine =
                    (MultiTenantAwareComponent) configuration.getComponent(EventStorageEngine.class);

            configuration.shutdown();

            assertThat(tenantProvider.subscribedComponents()).doesNotContain(routingEngine);
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

    private static MultiTenantAwareComponent commandBusConnectorDelegate(AxonConfiguration configuration)
            throws Exception {
        return multiTenantDelegate(configuration.getComponent(CommandBusConnector.class));
    }

    private static MultiTenantAwareComponent queryBusConnectorDelegate(AxonConfiguration configuration)
            throws Exception {
        return multiTenantDelegate(configuration.getComponent(QueryBusConnector.class));
    }

    private static MultiTenantAwareComponent multiTenantDelegate(Object connector) throws Exception {
        Field delegateField = delegateField(connector.getClass());
        if (delegateField == null) {
            // No decorator in front of it (e.g. PayloadConvertingCommandBusConnector or
            // PayloadConvertingQueryBusConnector, wired by the AxonServerConnector module's own enhancer): the
            // resolved component already is the multi-tenant connector itself.
            return (MultiTenantAwareComponent) connector;
        }
        delegateField.setAccessible(true);
        return (MultiTenantAwareComponent) delegateField.get(connector);
    }

    // The PayloadConvertingCommandBusConnector decorator declares "delegate" on a superclass, not on itself.
    // Returns null when no such field exists anywhere in the hierarchy, i.e. connector isn't wrapped at all.
    @Nullable
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

    private record CourseRepository(TenantDescriptor tenant) implements AutoCloseable {

        @Override
        public void close() {
            // unused, but the component provider requires AutoCloseable to be able to close all tenant components on shutdown
        }
    }
}
