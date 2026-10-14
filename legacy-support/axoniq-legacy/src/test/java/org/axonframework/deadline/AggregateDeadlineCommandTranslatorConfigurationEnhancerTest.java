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

package org.axonframework.deadline;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.LegacyScopeAwareProvider;
import org.axonframework.messaging.NoScopeDescriptor;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link AggregateDeadlineCommandTranslatorConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 */
class AggregateDeadlineCommandTranslatorConfigurationEnhancerTest {

    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Test
    void theConfigurationBuildsAnAggregateDeadlineCommandTranslator() {
        // given / when
        configuration = MessagingConfigurer.create().start();

        // then
        assertThat(configuration.getComponent(AggregateDeadlineCommandTranslator.class)).isNotNull();
    }

    @Test
    void theTranslatorRegistersItselfWithTheLegacyScopeAwareProvider() {
        // given / when
        configuration = MessagingConfigurer.create().start();

        // then
        LegacyScopeAwareProvider provider =
                (LegacyScopeAwareProvider) configuration.getComponent(ScopeAwareProvider.class);
        AggregateDeadlineCommandTranslator translator =
                configuration.getComponent(AggregateDeadlineCommandTranslator.class);
        assertThat(provider.provideScopeAwareStream(NoScopeDescriptor.INSTANCE)).contains(translator);
    }

    @Test
    void anApplicationProvidingItsOwnScopeAwareProviderReceivesNoTranslator() {
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
}
