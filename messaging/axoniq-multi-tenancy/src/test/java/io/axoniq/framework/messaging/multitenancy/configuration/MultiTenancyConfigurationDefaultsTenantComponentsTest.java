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

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the tenant-component wiring performed by {@link MultiTenancyConfigurationDefaults} against a real
 * {@link MessagingConfigurer}: providers are subscribed to the {@link TenantProvider}, and message handler parameters
 * resolve to the tenant-scoped component of the tenant carried by the message.
 */
class MultiTenancyConfigurationDefaultsTenantComponentsTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");
    private static final TenantDescriptor TENANT_B = TenantDescriptor.tenantWithId("tenant-b");

    private final StubTenantProvider tenantProvider = new StubTenantProvider();
    private final TenantComponentProvider<CourseRepository> componentProvider =
            TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

    private Configuration configuration;

    @BeforeEach
    void buildConfiguration() {
        tenantProvider.addTenant(TENANT_A);
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(registry -> registry
                                                   // Scanning would pull in the Axon Server enhancer, which needs
                                                   // eventsourcing classes absent from this module's test classpath.
                                                   .disableEnhancerScanning()
                                                   .registerEnhancer(new MultiTenancyConfigurationDefaults())
                                                   .registerComponent(TenantProvider.class, config -> tenantProvider)
                                                   .registerComponent(TenantComponentProvider.class,
                                                                      config -> componentProvider))
                                           .build();
    }

    @Nested
    class TenantLifecycleWiring {

        @Test
        void subscribesTheProviderToTheTenantProviderAndReplaysKnownTenants() {
            // when the provider component is built by the configuration
            configuration.getComponent(TenantComponentProvider.class);

            // then
            assertThat(tenantProvider.subscribedComponents()).contains(componentProvider);
            assertThat(componentProvider.tenants()).containsExactly(TENANT_A);
        }

        @Test
        void propagatesTenantsAddedAtRuntimeToTheProvider() {
            // given
            configuration.getComponent(TenantComponentProvider.class);

            // when
            tenantProvider.addTenant(TENANT_B);

            // then
            assertThat(componentProvider.tenants()).containsExactlyInAnyOrder(TENANT_A, TENANT_B);
        }

        @Test
        void removingATenantDestroysItsComponentInstance() {
            // given
            configuration.getComponent(TenantComponentProvider.class);
            tenantProvider.addTenant(TENANT_B);
            CourseRepository repository = componentProvider.componentFor(TENANT_B);

            // when
            tenantProvider.removeTenant(TENANT_B);

            // then
            assertThat(componentProvider.tenants()).containsExactly(TENANT_A);
            assertThat(repository.closed).isTrue();
        }

        @Test
        void shuttingDownTheTenantProviderDestroysAllComponentInstances() {
            // given
            configuration.getComponent(TenantComponentProvider.class);
            CourseRepository repository = componentProvider.componentFor(TENANT_A);

            // when the tenant provider shuts down, deregistering all subscribed components
            tenantProvider.shutdown();

            // then
            assertThat(repository.closed).isTrue();
            assertThat(componentProvider.tenants()).isEmpty();
        }
    }

    @Nested
    class HandlerParameterResolution {

        @Test
        void resolvesTheTenantScopedComponentThroughTheConfiguredParameterResolverFactory() throws Exception {
            // given
            ParameterResolverFactory factory = configuration.getComponent(ParameterResolverFactory.class);
            Method handler = SampleHandlers.class.getDeclaredMethod("handle", CourseRepository.class);
            ParameterResolver<?> resolver = factory.createInstance(handler, handler.getParameters(), 0);
            assertThat(resolver).isNotNull();
            Message message = new GenericMessage(
                    "message-id",
                    new MessageType("TestEvent"),
                    "payload".getBytes(),
                    Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY, TENANT_A.tenantId())
            );

            // when
            Object resolved = resolver.resolveParameterValue(StubProcessingContext.forMessage(message)).join();

            // then
            assertThat(resolved).isSameAs(componentProvider.componentFor(TENANT_A));
            assertThat(((CourseRepository) resolved).tenant).isEqualTo(TENANT_A);
        }
    }

    @SuppressWarnings("unused")
    private static final class SampleHandlers {

        void handle(CourseRepository repository) {
            // Reflection target only. The parameter type drives the matching under test.
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
