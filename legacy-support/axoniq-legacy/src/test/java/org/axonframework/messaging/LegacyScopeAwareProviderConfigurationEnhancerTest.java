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

package org.axonframework.messaging;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating that the {@link LegacyScopeAwareProviderConfigurationEnhancer} gives every configuration a
 * {@link LegacyScopeAwareProvider} that becomes ready with the application.
 *
 * @author Jakob Hatzl
 */
class LegacyScopeAwareProviderConfigurationEnhancerTest {

    private @Nullable AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void theConfigurationsScopeAwareProviderIsALegacyScopeAwareProvider() {
        // given / when
        configuration = MessagingConfigurer.create().start();

        // then
        assertThat(configuration.getComponent(ScopeAwareProvider.class)).isInstanceOf(LegacyScopeAwareProvider.class);
    }

    @Test
    void theProviderProvidesItsComponentsOnceTheApplicationStarted() {
        // given
        configuration = MessagingConfigurer.create().start();
        LegacyScopeAwareProvider provider =
                (LegacyScopeAwareProvider) configuration.getComponent(ScopeAwareProvider.class);
        ScopeAware component = new ScopeAwareStub();

        // when
        provider.register(component);

        // then
        assertThat(provider.provideScopeAwareStream(NoScopeDescriptor.INSTANCE)).containsExactly(component);
    }

    @Test
    void theProviderIsNotReadyBeforeTheApplicationStarted() {
        // given
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(cr -> cr.registerComponent(
                                                   ScopeAwareProviderSettings.class,
                                                   c -> new ScopeAwareProviderSettings(Duration.ZERO)
                                           ))
                                           .build();

        // when
        ScopeAwareProvider provider = configuration.getComponent(ScopeAwareProvider.class);

        // then
        assertThatThrownBy(() -> provider.provideScopeAwareStream(NoScopeDescriptor.INSTANCE))
                .isInstanceOf(ScopeAwareProviderNotReadyException.class);
    }

    @Test
    void theProviderWaitsForTheConfiguredReadinessTimeout() {
        // given
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(cr -> cr.registerComponent(
                                                   ScopeAwareProviderSettings.class,
                                                   c -> new ScopeAwareProviderSettings(Duration.ofMillis(300))
                                           ))
                                           .build();
        ScopeAwareProvider provider = configuration.getComponent(ScopeAwareProvider.class);
        long start = System.nanoTime();

        // when
        assertThatThrownBy(() -> provider.provideScopeAwareStream(NoScopeDescriptor.INSTANCE))
                .isInstanceOf(ScopeAwareProviderNotReadyException.class);

        // then
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(300));
    }

    @Test
    void aScopeAwareProviderOfTheApplicationReplacesTheLegacyProvider() {
        // given
        ScopeAwareProvider applicationProvider = scope -> Stream.empty();

        // when
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(cr -> cr.registerComponent(
                                                   ScopeAwareProvider.class, c -> applicationProvider
                                           ))
                                           .start();

        // then
        assertThat(configuration.getComponent(ScopeAwareProvider.class)).isSameAs(applicationProvider);
    }

    private static class ScopeAwareStub implements ScopeAware {

        @Override
        public void send(Message message, ProcessingContext context, ScopeDescriptor scopeDescription) {
        }

        @Override
        public boolean canResolve(ScopeDescriptor scopeDescription) {
            return true;
        }
    }
}
