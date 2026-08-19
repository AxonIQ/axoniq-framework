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

package io.axoniq.framework.messaging.multitenancy;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.annotation.TenantComponentParameterResolverFactoryConfigurationEnhancer;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.axonserver.configuration.AxonServerMultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import io.axoniq.framework.messaging.multitenancy.deadletter.DeadLetterMultiTenancyConfigurationEnhancer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link MultiTenancyUtils} covers multi-tenancy as a whole: the enhancers it switches off are exactly
 * the ones the module contributes through the {@link java.util.ServiceLoader}, they form one ordered block that
 * runs ahead of Axon Server's own enhancer, and disabling really leaves a configuration without them.
 * <p>
 * What each individual enhancer registers, and what it stops registering once disabled, is covered by that
 * enhancer's own test. This test guards the list and the ordering, which would otherwise silently fall behind
 * when a fourth enhancer is added or one is reordered.
 *
 * @author Jakob Hatzl
 */
class MultiTenancyUtilsTest {

    private static final String MULTI_TENANCY_PACKAGE = "io.axoniq.framework.messaging.multitenancy";

    @Nested
    class EnhancerCoverage {

        @Test
        void disablesEveryEnhancerContributedThroughTheServiceLoader() {
            // given the multi-tenancy enhancers the ServiceLoader actually hands the framework at runtime
            Set<Class<? extends ConfigurationEnhancer>> contributed =
                    ServiceLoader.load(ConfigurationEnhancer.class)
                                 .stream()
                                 .map(ServiceLoader.Provider::type)
                                 .filter(type -> isMultiTenancyPackage(type.getPackageName()))
                                 .collect(Collectors.toSet());

            // then every enhancer it switches off is one the module actually contributes, and the only one left
            // running is the parameter-resolver enhancer, which stays so handlers with tenant-scoped parameters can
            // still be inspected
            assertThat(contributed)
                    .containsAll(MultiTenancyUtils.enhancers())
                    .hasSize(MultiTenancyUtils.enhancers().size() + 1);
            assertThat(MultiTenancyUtils.enhancers())
                    .doesNotContain(TenantComponentParameterResolverFactoryConfigurationEnhancer.class);
        }

        @Test
        void enhancersFormOneBlockAnchoredOnTheGenericDefaults() {
            // given the anchor and the enhancers ordered against it
            int anchor = MultiTenancyConfigurationDefaults.ENHANCER_ORDER;
            List<Integer> siblings = List.of(AxonServerMultiTenancyConfigurationDefaults.ENHANCER_ORDER,
                                             DeadLetterMultiTenancyConfigurationEnhancer.ENHANCER_ORDER);

            // then the generic defaults run strictly first, so ordering below them disables the whole block
            assertThat(siblings).allSatisfy(order -> assertThat(order).isGreaterThan(anchor));
            // and the whole block runs ahead of Axon Server's own enhancer, whose defaults it must pre-empt
            assertThat(siblings).allSatisfy(
                    order -> assertThat(order).isLessThan(AxonServerConfigurationEnhancer.ENHANCER_ORDER)
            );
            assertThat(DeadLetterMultiTenancyConfigurationEnhancer.ENHANCER_ORDER)
                    .isGreaterThan(AxonServerMultiTenancyConfigurationDefaults.ENHANCER_ORDER + 1);
            assertThat(anchor).isLessThan(AxonServerConfigurationEnhancer.ENHANCER_ORDER);
        }
    }

    @Nested
    class Disabling {

        @Test
        void disablingLeavesNoMultiTenancyComponentsInTheConfiguration() {
            // when a configuration opts out, with the enhancers reaching it through the ServiceLoader as at runtime
            AxonConfiguration configuration = MessagingConfigurer.create()
                                                                .componentRegistry(MultiTenancyUtils::disable)
                                                                .build();

            // then neither the generic nor the Axon Server-backed defaults ran. That the parameter-resolver
            // enhancer stands down too is covered by
            // TenantComponentParameterResolverFactoryConfigurationEnhancerTest.
            assertThat(configuration.hasComponent(TenantResolver.class)).isFalse();
            assertThat(configuration.hasComponent(TenantRouter.class)).isFalse();
            assertThat(configuration.hasComponent(TenantProvider.class)).isFalse();
        }

        @Test
        void rejectsANullComponentRegistry() {
            // when disabling without a registry
            // then the guard names the offending parameter
            assertThatThrownBy(() -> MultiTenancyUtils.disable(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("component registry");
        }
    }

    private static boolean isMultiTenancyPackage(String packageName) {
        return packageName.equals(MULTI_TENANCY_PACKAGE) || packageName.startsWith(MULTI_TENANCY_PACKAGE + ".");
    }
}
