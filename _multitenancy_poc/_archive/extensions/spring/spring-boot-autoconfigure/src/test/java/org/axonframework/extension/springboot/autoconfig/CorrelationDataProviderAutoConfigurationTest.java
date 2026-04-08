/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.messaging.core.correlation.CorrelationDataProvider;
import org.axonframework.messaging.core.correlation.CorrelationDataProviderRegistry;
import org.axonframework.messaging.core.correlation.SimpleCorrelationDataProvider;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link CorrelationDataProviderAutoConfiguration}.
 *
 * @author Steven van Beelen
 */
class CorrelationDataProviderAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withPropertyValues("axon.axonserver.enabled=false", "axon.eventstorage.jpa.polling-interval=0")
                .withUserConfiguration(TestContext.class);
    }

    @Test
    void correlationDataProviderBeansAreRegisteredWithTheCorrelationDataProviderRegistry() {
        org.axonframework.common.configuration.Configuration testConfig =
                mock(org.axonframework.common.configuration.Configuration.class);

        testContext.run(context -> {
            assertThat(context).hasBean("providerOne");
            Object providerOne = context.getBean("providerOne");
            assertThat(providerOne).isInstanceOf(CorrelationDataProvider.class);
            assertThat(context).hasBean("providerTwo");
            Object providerTwo = context.getBean("providerTwo");
            assertThat(providerTwo).isInstanceOf(CorrelationDataProvider.class);

            assertThat(context).hasSingleBean(CorrelationDataProviderRegistry.class);
            CorrelationDataProviderRegistry providerRegistry = context.getBean(CorrelationDataProviderRegistry.class);
            List<CorrelationDataProvider> registeredProviders = providerRegistry.correlationDataProviders(testConfig);
            // Two from the TestContext, and one default MessageOriginProvider
            assertThat(registeredProviders).hasSize(3);
            assertThat(registeredProviders).contains((CorrelationDataProvider) providerOne);
            assertThat(registeredProviders).contains((CorrelationDataProvider) providerTwo);
        });
    }

    @Configuration
    @EnableAutoConfiguration
    public static class TestContext {

        @Bean
        public CorrelationDataProvider providerOne() {
            return new SimpleCorrelationDataProvider("thisMetaDataKey");
        }

        @Bean
        public CorrelationDataProvider providerTwo() {
            return new SimpleCorrelationDataProvider("thatMetaDataKey");
        }
    }
}