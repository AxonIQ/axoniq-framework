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
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.axonserver.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantSnapshotStore;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MultiTenancyAutoConfiguration}.
 *
 * @author Jan Galinski
 * @author Laura Devriendt
 * @since 5.3.0
 */
class MultiTenancyAutoConfigurationTest {

    private static final String ENABLE_ENHANCER = "enableMultiTenancyConfigurationEnhancer";
    private static final String DISABLE_ENHANCER = "disableMultiTenancyConfigurationEnhancer";
    private static final String AXON_SERVER_WARNING = "multiTenancyRequiresAxonServerWarning";

    /**
     * Verifies which enhancer beans the auto-configuration contributes for each combination of the
     * {@code axon.multitenancy.enabled} and {@code axon.axonserver.enabled} properties.
     */
    @Nested
    class BeanWiring {

        private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MultiTenancyAutoConfiguration.class));

        @Test
        void contributesTheEnablingEnhancerByDefault() {
            // when no multi-tenancy or Axon Server property is set
            contextRunner.run(context -> {
                // then multi-tenancy is enabled and neither the disabling enhancer nor the warning is contributed
                assertThat(context).hasBean(ENABLE_ENHANCER);
                assertThat(context).doesNotHaveBean(DISABLE_ENHANCER);
                assertThat(context).doesNotHaveBean(AXON_SERVER_WARNING);
            });
        }

        @Test
        void contributesTheEnablingEnhancerWhenExplicitlyEnabled() {
            // given multi-tenancy is explicitly enabled
            contextRunner.withPropertyValues("axon.multitenancy.enabled=true")
                         // when the context starts
                         .run(context -> {
                             // then the enabling enhancer is contributed
                             assertThat(context).hasBean(ENABLE_ENHANCER);
                             assertThat(context).doesNotHaveBean(DISABLE_ENHANCER);
                         });
        }

        @Test
        void doesNotEnableMultiTenancyWhenAxonServerIsDisabled() {
            // given Axon Server is disabled and multi-tenancy is left at its default
            contextRunner.withPropertyValues("axon.axonserver.enabled=false")
                         // when the context starts
                         .run(context -> {
                             // then no enabling, disabling, or warning bean is contributed
                             assertThat(context).doesNotHaveBean(ENABLE_ENHANCER);
                             assertThat(context).doesNotHaveBean(DISABLE_ENHANCER);
                             assertThat(context).doesNotHaveBean(AXON_SERVER_WARNING);
                         });
        }

        @Test
        void warnsButDoesNotEnableWhenMultiTenancyEnabledWhileAxonServerDisabled() {
            // given multi-tenancy is explicitly enabled while Axon Server is disabled
            contextRunner.withPropertyValues("axon.multitenancy.enabled=true", "axon.axonserver.enabled=false")
                         // when the context starts
                         .run(context -> {
                             // then multi-tenancy is not enabled, but the misconfiguration is warned about
                             assertThat(context).doesNotHaveBean(ENABLE_ENHANCER);
                             assertThat(context).hasBean(AXON_SERVER_WARNING);
                         });
        }
    }

    /**
     * Verifies the actual effect of the contributed enhancers against a real configuration, including that they run
     * before {@link MultiTenancyConfigurationDefaults} so enablement and disablement take effect. The
     * {@code MultiTenancyConfigurationDefaults} enhancer is registered explicitly here, standing in for the one the
     * multi-tenancy module contributes through the {@code ServiceLoader} at runtime.
     */
    @Nested
    class EnhancerBehavior {

        private final MultiTenancyAutoConfiguration autoConfiguration = new MultiTenancyAutoConfiguration();

        @Test
        void enablingEnhancerRunsBeforeTheDefaultsEnhancer() {
            // then the enabling enhancer is ordered before the defaults enhancer it activates
            assertThat(autoConfiguration.enableMultiTenancyConfigurationEnhancer().order())
                    .isLessThan(MultiTenancyConfigurationDefaults.ENHANCER_ORDER);
        }

        @Test
        void enablingEnhancerActivatesTheMultiTenancyDefaults() {
            // when the configuration is built with the enabling enhancer
            AxonConfiguration configuration = configurationWith(
                    autoConfiguration.enableMultiTenancyConfigurationEnhancer()
            );

            // then the defaults enhancer took effect and registered its default tenant resolver
            assertThat(configuration.getOptionalComponent(TenantResolver.class))
                    .get().isInstanceOf(MetadataBasedTenantResolver.class);
        }

        @Test
        void enablingEnhancerSubscribesRegisteredTenantComponentProviders() {
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
                                               .registerEnhancer(autoConfiguration.enableMultiTenancyConfigurationEnhancer())
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

        private static AxonConfiguration configurationWith(ConfigurationEnhancer enhancer) {
            return MessagingConfigurer.create()
                                      .componentRegistry(registry -> registry
                                              // Scanning would pull in the Axon Server enhancer, whose dependencies are
                                              // out of scope here. The defaults enhancer is registered explicitly.
                                              .disableEnhancerScanning()
                                              .registerEnhancer(enhancer)
                                              .registerEnhancer(new AxonServerMultiTenancyConfigurationDefaults())
                                              .registerEnhancer(new MultiTenancyConfigurationDefaults()))

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

        @Configuration
        @EnableAutoConfiguration
        static class FullAutoConfigurationContext {

        }
    }

    private record TenantResource(String tenantId) {

    }
}
