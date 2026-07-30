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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.postgresql.PostgresqlConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.junit.jupiter.api.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies what {@link MultiTenancyAutoConfiguration} does to a real Spring Boot context, rather than to a
 * {@code MessagingConfigurer} built by hand.
 * <p>
 * The disabling enhancer relies on running before the multi-tenancy enhancers the {@code ServiceLoader} contributes,
 * and Spring collects and orders those enhancers itself. Asserting the bean is present therefore says nothing about
 * whether disabling took effect, which is what these tests cover.
 *
 * @author Laura Devriendt
 */
class MultiTenancyAutoConfigurationSpringContextTest {

    @Nested
    class MultiTenancyDisabled {

        @Test
        void explicitlyDisablingLeavesNoMultiTenancyComponents() {
            // given a context started with multi-tenancy explicitly disabled
            try (ConfigurableApplicationContext context = contextWith("--axon.multitenancy.enabled=false",
                                                                     "--axon.axonserver.enabled=false")) {
                // when the resulting Axon configuration is inspected
                AxonConfiguration configuration = context.getBean(AxonConfiguration.class);

                // then none of the multi-tenancy enhancers registered their components
                assertThat(configuration.hasComponent(TenantResolver.class)).isFalse();
                assertThat(configuration.hasComponent(TenantRouter.class)).isFalse();
                assertThat(configuration.hasComponent(TenantProvider.class)).isFalse();
            }
        }

        @Test
        void explicitlyDisablingStillAllowsTenantScopedHandlersToBeInspected() throws Exception {
            // given a context started with multi-tenancy explicitly disabled
            try (ConfigurableApplicationContext context = contextWith("--axon.multitenancy.enabled=false",
                                                                     "--axon.axonserver.enabled=false")) {
                ParameterResolverFactory factory = context.getBean(AxonConfiguration.class)
                                                          .getComponent(ParameterResolverFactory.class);
                Method handler = SampleHandler.class.getDeclaredMethod("handle", String.class, TenantScopedResource.class);

                // when resolving a tenant-scoped handler parameter
                // then it still resolves, so a handler declaring one does not fail the whole configuration at
                // startup. It fails per message instead, once no tenant can be resolved.
                assertThat(factory.createInstance(handler, handler.getParameters(), 1)).isNotNull();
            }
        }

        @Test
        void disablingAxonServerLeavesNoMultiTenancyComponents() {
            // given a context started without Axon Server, the backend tenants are contexts of
            try (ConfigurableApplicationContext context = contextWith("--axon.axonserver.enabled=false")) {
                // when the resulting Axon configuration is inspected
                AxonConfiguration configuration = context.getBean(AxonConfiguration.class);

                // then multi-tenancy stepped aside, rather than registering components it cannot back
                assertThat(configuration.hasComponent(TenantResolver.class)).isFalse();
                assertThat(configuration.hasComponent(TenantRouter.class)).isFalse();
                assertThat(configuration.hasComponent(TenantProvider.class)).isFalse();
            }
        }
    }

    @SuppressWarnings("unused")
    private static final class SampleHandler {

        void handle(String payload, @TenantScoped TenantScopedResource tenantScoped) {
            // Reflection target only. The tenant-scoped parameter sits at index 1, so the payload resolver cannot
            // claim it, and no bean of its type exists, so Spring's bean resolver cannot either.
        }
    }

    static final class TenantScopedResource {

    }

    /**
     * Mirrors the shape the demo uses: an annotated handler bean the framework autodetects into a query handling
     * module, whose tenant-scoped parameter is resolved during handler inspection at startup.
     */
    static final class TenantScopedQueryHandler {

        @QueryHandler
        String handle(TenantScopedQuery query, @TenantScoped TenantScopedResource resource) {
            return "handled";
        }
    }

    record TenantScopedQuery(String id) {

    }

    private static ConfigurableApplicationContext contextWith(String... properties) {
        return new SpringApplicationBuilder(TestApplication.class).web(WebApplicationType.NONE)
                                                                 .run(properties);
    }

    /**
     * Boots the real autoconfiguration chain, so the multi-tenancy enhancers reach the configuration exactly as they
     * do in an application.
     */
    @Configuration
    @EnableAutoConfiguration
    static class TestApplication {

        /**
         * Disables the Postgresql enhancer, which the {@code ServiceLoader} contributes because
         * {@code axoniq-postgresql} is on this module's test classpath. It initialises a {@code PostgresqlSnapshotStore}
         * against the in-memory database used here and fails, which has nothing to do with multi-tenancy. Ordered
         * first, so the disable is in place before that enhancer would run.
         *
         * @return a configuration enhancer disabling the Postgresql enhancer
         */
        @Bean
        TenantScopedQueryHandler tenantScopedQueryHandler() {
            return new TenantScopedQueryHandler();
        }

        @Bean
        TenantComponentProvider<TenantScopedResource> tenantScopedResourceProvider() {
            return TenantComponentProvider.withFactory(TenantScopedResource.class,
                                                       tenant -> new TenantScopedResource());
        }

        @Bean
        ConfigurationEnhancer disablePostgresqlConfigurationEnhancer() {
            return new ConfigurationEnhancer() {
                @Override
                public void enhance(ComponentRegistry registry) {
                    registry.disableEnhancer(PostgresqlConfigurationEnhancer.class);
                }

                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }
            };
        }
    }
}
