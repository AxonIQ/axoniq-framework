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

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviderUtil;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MultiTenancyAutoConfiguration}.
 *
 * @author Jan Galinski
 * @author Laura Devriendt
 * @since 5.3.0
 */
class MultiTenancyAutoConfigurationTest {

    private static final String DISABLE_ENHANCER = "disableMultiTenancyConfigurationEnhancer";
    private static final String AXON_SERVER_WARNING = "multiTenancyRequiresAxonServerWarning";
    private static final String STATIC_TENANT_CONNECT_PREDICATE = "staticTenantConnectPredicate";
    private static final String LEGACY_TENANTS_PROPERTY = "axon.axonserver.contexts";

    /**
     * Verifies which enhancer beans the auto-configuration contributes for each combination of the
     * {@code axon.multitenancy.enabled} and {@code axon.axonserver.enabled} properties.
     */
    @Nested
    class BeanWiring {

        private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MultiTenancyAutoConfiguration.class));

        @Test
        void contributesNoDisablingEnhancerByDefault() {
            // when no multi-tenancy or Axon Server property is set
            contextRunner.run(context -> {
                // then multi-tenancy is left active and no warning is contributed
                assertThat(context).doesNotHaveBean(DISABLE_ENHANCER);
                assertThat(context).doesNotHaveBean(AXON_SERVER_WARNING);
            });
        }

        @Test
        void contributesNoDisablingEnhancerWhenExplicitlyEnabled() {
            // given multi-tenancy is explicitly enabled
            contextRunner.withPropertyValues("axon.multitenancy.enabled=true")
                         // when the context starts
                         .run(context -> {
                             // then multi-tenancy is left active
                             assertThat(context).doesNotHaveBean(DISABLE_ENHANCER);
                         });
        }

        @Test
        void contributesTheDisablingEnhancerWhenExplicitlyDisabled() {
            // given multi-tenancy is explicitly disabled
            contextRunner.withPropertyValues("axon.multitenancy.enabled=false")
                         // when the context starts
                         .run(context -> {
                             // then the disabling enhancer is contributed, without a warning about Axon Server
                             assertThat(context).hasBean(DISABLE_ENHANCER);
                             assertThat(context).doesNotHaveBean(AXON_SERVER_WARNING);
                         });
        }

        @Test
        void disablesMultiTenancyWhenAxonServerIsDisabled() {
            // given Axon Server is disabled and multi-tenancy is left at its default
            contextRunner.withPropertyValues("axon.axonserver.enabled=false")
                         // when the context starts
                         .run(context -> {
                             // then multi-tenancy is disabled for lack of a backend, without warning about it
                             assertThat(context).hasBean(DISABLE_ENHANCER);
                             assertThat(context).doesNotHaveBean(AXON_SERVER_WARNING);
                         });
        }

        @Test
        void warnsAndDisablesWhenMultiTenancyEnabledWhileAxonServerDisabled() {
            // given multi-tenancy is explicitly enabled while Axon Server is disabled
            contextRunner.withPropertyValues("axon.multitenancy.enabled=true", "axon.axonserver.enabled=false")
                         // when the context starts
                         .run(context -> {
                             // then multi-tenancy is disabled anyway, and the misconfiguration is warned about
                             assertThat(context).hasBean(DISABLE_ENHANCER);
                             assertThat(context).hasBean(AXON_SERVER_WARNING);
                         });
        }

        @Test
        void contributesTheDisablingEnhancerOnceWhenMultiTenancyAndAxonServerAreDisabled() {
            // given both reasons to disable multi-tenancy apply at once
            contextRunner.withPropertyValues("axon.multitenancy.enabled=false", "axon.axonserver.enabled=false")
                         // when the context starts
                         .run(context -> {
                             // then the single disabling enhancer is contributed exactly once
                             assertThat(context).hasSingleBean(ConfigurationEnhancer.class);
                             assertThat(context).hasBean(DISABLE_ENHANCER);
                         });
        }
    }

    @Nested
    class StaticTenantConfiguration {

        private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MultiTenancyAutoConfiguration.class));

        @Test
        void createsAPredicateForConfiguredTenants() {
            // given a comma-separated set of static tenant identifiers
            contextRunner.withPropertyValues("axoniq.multitenancy.tenants=tenant-a, tenant-b")
                         // when the context starts
                         .run(context -> {
                             // then its predicate accepts precisely those tenant identifiers
                             TenantConnectPredicate predicate = context.getBean(TenantConnectPredicate.class);
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("tenant-a"))).isTrue();
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("tenant-b"))).isTrue();
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("tenant-c"))).isFalse();
                         });
        }

        @Test
        void createsAPredicateForLegacyConfiguredTenants() {
            // given the legacy comma-separated static tenant property
            contextRunner.withPropertyValues(LEGACY_TENANTS_PROPERTY + "=legacy-tenant")
                         // when the context starts
                         .run(context -> {
                             // then its predicate accepts the legacy tenant
                             TenantConnectPredicate predicate = context.getBean(TenantConnectPredicate.class);
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("legacy-tenant"))).isTrue();
                         });
        }

        @Test
        void currentPropertyTakesPrecedenceOverLegacyProperty() {
            // given both property names with different tenant sets
            contextRunner.withPropertyValues(
                                 "axoniq.multitenancy.tenants=current-tenant",
                                 LEGACY_TENANTS_PROPERTY + "=legacy-tenant")
                         // when the context starts
                         .run(context -> {
                             // then the current property determines the predicate
                             TenantConnectPredicate predicate = context.getBean(TenantConnectPredicate.class);
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("current-tenant"))).isTrue();
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("legacy-tenant"))).isFalse();
                         });
        }

        @Test
        void doesNotCreateAPredicateWithoutConfiguredTenants() {
            // when no static tenant identifiers are configured
            contextRunner.run(context -> {
                // then discovery retains the default predicate
                assertThat(context).doesNotHaveBean(STATIC_TENANT_CONNECT_PREDICATE);
            });
        }

        @Test
        void applicationPredicateTakesPrecedenceOverStaticTenantConfiguration() {
            // given an application predicate and configured static tenant identifiers
            contextRunner.withUserConfiguration(CustomTenantConnectPredicateConfiguration.class)
                         .withPropertyValues("axoniq.multitenancy.tenants=tenant-a")
                         // when the context starts
                         .run(context -> {
                             // then the application predicate is left in control
                             TenantConnectPredicate predicate = context.getBean(TenantConnectPredicate.class);
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("custom-tenant"))).isTrue();
                             assertThat(predicate.test(TenantDescriptor.tenantWithId("tenant-a"))).isFalse();
                             assertThat(context).doesNotHaveBean(STATIC_TENANT_CONNECT_PREDICATE);
                         });
        }
    }

    /**
     * Verifies the actual effect of the contributed enhancer against a real configuration, including that it runs
     * before {@link MultiTenancyConfigurationDefaults} so disabling takes effect. The
     * {@code MultiTenancyConfigurationDefaults} enhancer is registered explicitly here, standing in for the one the
     * multi-tenancy module contributes through the {@code ServiceLoader} at runtime.
     */
    @Nested
    class EnhancerBehavior {

        private final MultiTenancyAutoConfiguration autoConfiguration = new MultiTenancyAutoConfiguration();

        @Test
        void disablingEnhancerRunsBeforeEveryMultiTenancyEnhancer() {
            // then the disabling enhancer is ordered before the anchor of the multi-tenancy enhancer block, and
            // so before all of them. MultiTenancyUtilsTest guards that the block really is anchored there.
            assertThat(autoConfiguration.disableMultiTenancyConfigurationEnhancer().order())
                    .isLessThan(MultiTenancyConfigurationDefaults.ENHANCER_ORDER);
        }

        @Test
        void multiTenancyDefaultsApplyWithoutAnyDisablingEnhancer() {
            // when the configuration is built with the defaults enhancers and no disabling enhancer
            AxonConfiguration configuration = configurationWith();

            // then the defaults enhancer took effect and registered its default tenant resolver
            assertThat(configuration.getOptionalComponent(TenantResolver.class))
                    .get().isInstanceOf(MetadataBasedTenantResolver.class);
        }

        @Test
        void disablingEnhancerDeactivatesTheMultiTenancyDefaults() {
            // when the configuration is built with the disabling enhancer
            AxonConfiguration configuration = configurationWith(
                    autoConfiguration.disableMultiTenancyConfigurationEnhancer()
            );

            // then the defaults enhancer never ran, so no tenant resolver was registered
            assertThat(configuration.getOptionalComponent(TenantResolver.class)).isEmpty();
        }

        @Test
        void multiTenancyDefaultsSubscribeRegisteredTenantComponentProviders() {
            // given a tenant provider and a tenant-component provider registered as components,
            // mirroring a user-declared Spring bean that the SpringComponentRegistry exposes as a component
            StubTenantProvider tenantProvider = new StubTenantProvider();
            TenantComponentProvider<TenantResource> provider = TenantComponentProvider.withFactory(
                    TenantResource.class, tenant -> new TenantResource(tenant.tenantId())
            );
            AxonConfiguration configuration =
                    MessagingConfigurer.create()
                                       .componentRegistry(registry -> registry
                                               .disableEnhancerScanning()
                                               .registerEnhancer(new MultiTenancyConfigurationDefaults())
                                               .registerComponent(TenantProvider.class, config -> tenantProvider)
                                               .registerComponent(TenantComponentProvider.class, config -> provider))
                                       .build();

            // when the configuration starts
            configuration.start();

            // then the provider is subscribed to the tenant provider, so it follows the tenant lifecycle
            try {
                assertThat(tenantProvider.subscribedComponents()).contains(provider);
            } finally {
                configuration.shutdown();
            }
        }

        private static AxonConfiguration configurationWith(ConfigurationEnhancer... enhancers) {
            return MessagingConfigurer.create()
                                      .componentRegistry(registry -> {
                                          // Scanning would pull in the Axon Server enhancer, whose dependencies are
                                          // out of scope here. The defaults enhancers are registered explicitly.
                                          registry.disableEnhancerScanning()
                                                  .registerEnhancer(new AxonServerMultiTenancyConfigurationDefaults())
                                                  .registerEnhancer(new MultiTenancyConfigurationDefaults());
                                          Stream.of(enhancers).forEach(registry::registerEnhancer);
                                      })
                                      .build();
        }
    }

    /**
     * Verifies the event storage engine wired into a full Spring application context. With multi-tenancy active by
     * default, the engine backing the {@link EventStorageEngine} bean is the tenant-routing
     * {@link MultiTenantEventStorageEngine}, so appends and sources are directed at the store of the message's tenant.
     */
    @Nested
    class StorageEngineWiring {

        private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withUserConfiguration(FullAutoConfigurationContext.class);

        @Test
        void multiTenantEngineIsTheEventStorageEngineWhenAxonServerIsEnabled() {
            // given Axon Server is enabled, so multi-tenancy activates by default
            contextRunner.withPropertyValues("axon.axonserver.enabled=true")
                         // when the context starts
                         .run(context -> {
                             // then the sole event storage engine is the tenant-routing engine, left undecorated so
                             // snapshot sourcing reaches each tenant's own engine
                             assertThat(context).hasSingleBean(EventStorageEngine.class);
                             assertThat(context).getBean(EventStorageEngine.class)
                                                .isInstanceOf(MultiTenantEventStorageEngine.class);
                         });
        }

        @Test
        void multiTenantSnapshotStoreIsTheSnapshotStoreWhenAxonServerIsEnabled() {
            contextRunner.withPropertyValues("axon.axonserver.enabled=true")
                         .run(context -> {
                             // a separate bean of its own type, so injecting an EventStorageEngine stays unambiguous
                             assertThat(context).hasSingleBean(SnapshotStore.class);
                             assertThat(context).getBean(SnapshotStore.class)
                                                .isInstanceOf(MultiTenantSnapshotStore.class);
                         });
        }

        @Test
        void exposesASpringTenantConverterProviderToTheMultiTenantConfiguration() {
            contextRunner.withUserConfiguration(TenantConverterConfiguration.class)
                         .withPropertyValues("axon.axonserver.enabled=true")
                         .run(context -> {
                             TenantComponentProvider provider = context.getBean("tenantConverterProvider",
                                                                                    TenantComponentProvider.class);
                             AxonConfiguration configuration = context.getBean(AxonConfiguration.class);

                             assertThat(TenantComponentProviderUtil.find(configuration, Converter.class))
                                     .containsSame(provider);
                         });
        }

        @Configuration
        @EnableAutoConfiguration
        static class FullAutoConfigurationContext {

        }

        @Configuration
        static class TenantConverterConfiguration {

            @Bean
            TenantComponentProvider<Converter> tenantConverterProvider() {
                return TenantComponentProvider.withFactory(Converter.class, tenant -> new JacksonConverter());
            }
        }
    }

    private record TenantResource(String tenantId) {

    }

    @Configuration
    static class CustomTenantConnectPredicateConfiguration {

        @Bean
        TenantConnectPredicate customTenantConnectPredicate() {
            return tenant -> "custom-tenant".equals(tenant.tenantId());
        }
    }
}
