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
 * {@link TenantScoped}-annotated handler parameters is wired out of the box.
 * <p>
 * That the factory stays away once multi-tenancy is disabled is covered by
 * {@code MultiTenancyConfigurationDefaultsTest}, which owns the disabling utility.
 *
 * @author Jakob Hatzl
 */
class TenantComponentParameterResolverFactoryConfigurationEnhancerTest {

    private final TenantComponentProvider<CourseRepository> componentProvider =
            TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

    @Test
    void orderEqualsEnhancerOrderConstant() {
        assertThat(new TenantComponentParameterResolverFactoryConfigurationEnhancer().order())
                .isEqualTo(TenantComponentParameterResolverFactoryConfigurationEnhancer.ENHANCER_ORDER);
    }

    @Nested
    class DefaultComponentRegistration {

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

    @SuppressWarnings("unused")
    private static final class SampleHandlers {

        void handle(@TenantScoped CourseRepository repository) {
            // Reflection target only. The parameter type drives the matching under test.
        }
    }

    private static final class CourseRepository {

        private final TenantDescriptor tenant;

        private CourseRepository(TenantDescriptor tenant) {
            this.tenant = tenant;
        }
    }
}
