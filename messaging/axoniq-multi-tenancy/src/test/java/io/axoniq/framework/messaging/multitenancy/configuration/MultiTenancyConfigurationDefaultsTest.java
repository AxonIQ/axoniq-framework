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

import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.RegisterTenantDescriptorHandlerInterceptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantChangeSource;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.interception.HandlerInterceptorRegistry;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.time.Duration;

import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_A;
import static io.axoniq.framework.messaging.multitenancy.util.TestFixtures.TENANT_B;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link MultiTenancyConfigurationDefaults} against a real {@link MessagingConfigurer}: the generic,
 * backend-agnostic multi-tenancy components are wired for a given configuration, and the enhancer only acts when
 * multi-tenancy is enabled.
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
        assertThat(configuration.hasComponent(TenantComponentProviderSubscriber.class)).isFalse();
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
        void registersTheTenantRouterForTenantRoutingComponentsToShare() {
            assertThat(configuration.getComponent(TenantRouter.class)).isNotNull();
        }

        @Test
        void registersTheTenantComponentProviderSubscriber() {
            assertThat(configuration.getComponent(TenantComponentProviderSubscriber.class)).isNotNull();
        }

        @Test
        void registersTheStreamingProcessorRestarter() {
            assertThat(configuration.getComponent(MultiTenantStreamingProcessorRestarter.class)).isNotNull();
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
        void wiresTheStreamingProcessorRestarterToTheRoutingEnginesTenantsUntilShutdown() {
            // Asserted through what the wiring does, rather than through the restarter reporting itself as running: a
            // tenant registered with the routing engine reaches the restarter while the configuration runs, and stops
            // reaching it afterwards.
            MultiTenantStreamingProcessorRestarter restarter =
                    configuration.getComponent(MultiTenantStreamingProcessorRestarter.class);
            MultiTenantEventStorageEngine routingEngine =
                    (MultiTenantEventStorageEngine) configuration.getComponent(TenantChangeSource.class);

            routingEngine.registerTenant(TENANT_B);

            await().atMost(Duration.ofSeconds(2))
                   .untilAsserted(() -> assertThat(restartCount(restarter)).isPositive());

            configuration.shutdown();

            // Asserted on the engine, since a stopped restarter ignores a restart request either way, so counting
            // restarts cannot tell a cancelled listener from a still-registered one.
            MockComponentDescriptor engineDescriptor = new MockComponentDescriptor();
            routingEngine.describeTo(engineDescriptor);
            assertThat(engineDescriptor.getDescribedProperties()).containsEntry("tenantChangeListenerCount", 0);
        }

        private static long restartCount(MultiTenantStreamingProcessorRestarter restarter) {
            MockComponentDescriptor descriptor = new MockComponentDescriptor();
            restarter.describeTo(descriptor);
            return (long) descriptor.getDescribedProperties().get("restartCount");
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
