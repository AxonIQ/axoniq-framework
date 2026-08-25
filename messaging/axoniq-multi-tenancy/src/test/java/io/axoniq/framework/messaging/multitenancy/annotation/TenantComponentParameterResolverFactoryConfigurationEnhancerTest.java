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

package io.axoniq.framework.messaging.multitenancy.annotation;

import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.RecordingAxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link TenantComponentParameterResolverFactoryConfigurationEnhancer} against a real
 * {@link MessagingConfigurer}: the {@link TenantComponentParameterResolverFactory} resolving
 * {@link TenantScoped}-annotated handler parameters is wired out of the box, and stays wired even once multi-tenancy
 * is disabled, so handlers declaring such parameters can still be inspected.
 *
 * @author Jakob Hatzl
 */
class TenantComponentParameterResolverFactoryConfigurationEnhancerTest {

    private final TenantComponentProvider<CourseRepository> componentProvider =
            TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

    @Nested
    class DefaultComponentRegistration {

        private AxonConfiguration configuration;

        @BeforeEach
        void buildConfiguration() {
            configuration = MessagingConfigurer.create()
                                               .componentRegistry(registry -> registry
                                                       .registerComponent(AxonServerConnectionManager.class,
                                                                          config -> new RecordingAxonServerConnectionManager())
                                                       .registerComponent(TenantComponentProvider.class,
                                                                          config -> componentProvider))
                                               .build();
        }

        @Test
        void registersAParameterResolverFactoryThatResolvesTenantScopedComponentParameters() throws Exception {
            // given a handler whose tenant-scoped parameter is not the payload, so only the tenant-scoped
            // factory can claim it
            ParameterResolverFactory factory = configuration.getComponent(ParameterResolverFactory.class);
            Method handler = tenantScopedHandler();

            // when
            ParameterResolver<?> resolver = factory.createInstance(handler, handler.getParameters(), 1);

            // then the parameter resolves. What it resolves to is covered by
            // TenantComponentParameterResolverFactoryTest.
            assertThat(resolver).isNotNull();
        }
    }

    @Test
    void keepsResolvingTenantScopedParametersWhenMultiTenancyIsDisabled() throws Exception {
        // given a configuration that opted out of multi-tenancy
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(registry -> {
                                       registry.registerComponent(AxonServerConnectionManager.class,
                                                                  config -> new RecordingAxonServerConnectionManager());
                                       MultiTenancyUtils.disable(registry);
                                   })
                                   .componentRegistry(registry -> registry.registerComponent(
                                           TenantComponentProvider.class,
                                           config -> componentProvider))
                                   .build();
        ParameterResolverFactory factory = configuration.getComponent(ParameterResolverFactory.class);
        Method handler = tenantScopedHandler();

        // when
        ParameterResolver<?> resolver = factory.createInstance(handler, handler.getParameters(), 1);

        // then the parameter still resolves. Disabling multi-tenancy must not stop a handler that declares a
        // tenant-scoped parameter from being inspected, which would fail the whole configuration at startup.
        assertThat(resolver).isNotNull();
    }

    private static Method tenantScopedHandler() throws NoSuchMethodException {
        return SampleHandlers.class.getDeclaredMethod("handle", String.class, CourseRepository.class);
    }

    @SuppressWarnings("unused")
    private static final class SampleHandlers {

        void handle(String command, @TenantScoped CourseRepository repository) {
            // Reflection target only. The tenant-scoped parameter sits at index 1, so the payload resolver
            // cannot claim it and the assertions speak only about the tenant-scoped factory.
        }
    }

    private static final class CourseRepository {

        private CourseRepository(TenantDescriptor tenant) {
            // The tenant is not read back. This test is about the parameter being resolvable at all.
        }
    }
}
