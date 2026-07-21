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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.RegisterTenantDescriptorHandlerInterceptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.axonserver.AxonServerTenantProvider;
import io.axoniq.framework.messaging.multitenancy.axonserver.MultiTenantAxonServerCommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import io.axoniq.framework.messaging.queryhandling.distributed.QueryBusConnector;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.interception.HandlerInterceptorRegistry;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Verifies the {@link MultiTenancyConfigurationDefaults} against a real {@link MessagingConfigurer}: the right
 * components are wired for a given configuration, and the enhancer only acts when multi-tenancy is enabled.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 */
class MultiTenancyConfigurationDefaultsTest {

    @Test
    void orderEqualsEnhancerOrderConstant() {
        assertThat(new MultiTenancyConfigurationDefaults().order())
                .isEqualTo(MultiTenancyConfigurationDefaults.ENHANCER_ORDER);
    }

    @Test
    void enhanceIsANoOpWhenMultiTenancyIsNotEnabled() {
        // when
        AxonConfiguration configuration = MessagingConfigurer.create().build();

        // then none of the multi-tenancy defaults were registered
        assertThat(configuration.hasComponent(TenantResolver.class)).isFalse();
        assertThat(configuration.hasComponent(TenantProvider.class)).isFalse();
        assertThat(configuration.hasComponent(TenantComponentProviderSubscriber.class)).isFalse();
        assertThat(configuration.getComponent(CommandBusConnector.class))
                .extracting("delegate")
                .isNotInstanceOf(MultiTenantAxonServerCommandBusConnector.class);
        HandlerInterceptorRegistry interceptorRegistry = configuration.getComponent(HandlerInterceptorRegistry.class);
        assertThat(interceptorRegistry.commandInterceptors(configuration, TenantAwareCommandHandler.class, "handle"))
                .noneMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
        assertThat(interceptorRegistry.queryInterceptors(configuration, TenantAwareQueryHandler.class, "handle"))
                .noneMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
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
        void registersTheDefaultMetadataBasedTenantResolver() {
            assertThat(configuration.getComponent(TenantResolver.class))
                    .isInstanceOf(MetadataBasedTenantResolver.class);
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
        void registersTheTenantComponentProviderSubscriber() {
            assertThat(configuration.getComponent(TenantComponentProviderSubscriber.class)).isNotNull();
        }

        @Test
        void registersAParameterResolverFactoryThatResolvesTenantScopedComponentParameters() throws Exception {
            // given
            ParameterResolverFactory factory = configuration.getComponent(ParameterResolverFactory.class);
            Method handler = SampleHandlers.class.getDeclaredMethod("handle", CourseRepository.class);

            // when
            ParameterResolver<?> resolver = factory.createInstance(handler, handler.getParameters(), 0);

            // then
            assertThat(resolver)
                    .extracting("provider")
                    .isInstanceOf(TenantComponentProvider.class);
        }
    }

    @Nested
    class HandlerInterceptorWiring {

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
        void registersTheTenantDescriptorInterceptorForCommandAndQueryHandlersOnly() {
            // given
            HandlerInterceptorRegistry registry = configuration.getComponent(HandlerInterceptorRegistry.class);

            // when
            var commandInterceptors = registry.commandInterceptors(configuration,
                                                                   TenantAwareCommandHandler.class,
                                                                   "handle");
            var queryInterceptors = registry.queryInterceptors(configuration,
                                                               TenantAwareQueryHandler.class,
                                                               "handle");
            var eventInterceptors = registry.eventInterceptors(configuration,
                                                               SampleHandlers.class,
                                                               "handle");

            // then
            assertThat(commandInterceptors)
                    .anyMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
            assertThat(queryInterceptors)
                    .anyMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
            assertThat(eventInterceptors)
                    .noneMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
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
        void shuttingDownTheConfigurationDestroysAllComponentInstances() {
            // given instances for a replayed tenant and for a tenant added at runtime
            tenantProvider.addTenant(TENANT_B);
            CourseRepository repositoryA = componentProvider.componentFor(TENANT_A);
            CourseRepository repositoryB = componentProvider.componentFor(TENANT_B);

            // when the configuration shuts down, cancelling the retained provider subscriptions
            configuration.shutdown();

            // then both instances are destroyed, without relying on the tenant provider's own shutdown
            assertThat(repositoryA.closed).isTrue();
            assertThat(repositoryB.closed).isTrue();
            assertThat(componentProvider.tenants()).isEmpty();
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
        // that will probably remove once we implemented the query handling part
        @Mock
        private QueryBusConnector queryBusConnector;

        private AxonConfiguration configuration;

        @BeforeEach
        void buildAndStartConfiguration() {
            // The real AxonServerTenantProvider talks to Axon Server's admin API to discover tenants, so the
            // connection manager backing it is faked here instead of using the StubTenantProvider the other nested
            // classes rely on: only a real provider proves the phase-ordering wiring in MultiTenancyConfigurationDefaults.
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

    @SuppressWarnings("unused")
    private static final class SampleHandlers {
        void handle(@TenantScoped CourseRepository repository) {
            // Reflection target only. The parameter type drives the matching under test.
        }
    }

    @SuppressWarnings("unused")
    private static final class TenantAwareCommandHandler {

        @CommandHandler(commandName = "tenant-aware-command")
        void handle(String command, @TenantScoped CourseRepository repository) {
            // Reflection target only. The interceptor lookup does not invoke this method.
        }
    }

    @SuppressWarnings("unused")
    private static final class TenantAwareQueryHandler {

        @QueryHandler(queryName = "tenant-aware-query")
        void handle(String query, @TenantScoped CourseRepository repository) {
            // Reflection target only. The interceptor lookup does not invoke this method.
        }
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
