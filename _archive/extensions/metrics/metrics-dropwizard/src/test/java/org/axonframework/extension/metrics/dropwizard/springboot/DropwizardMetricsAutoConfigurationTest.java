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

package org.axonframework.extension.metrics.dropwizard.springboot;

import io.dropwizard.metrics5.MetricRegistry;
import org.axonframework.extension.metrics.dropwizard.MetricsConfigurationEnhancer;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link DropwizardMetricsAutoConfiguration}.
 *
 * @author Steven van Beelen
 */
class DropwizardMetricsAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner().withUserConfiguration(TestContext.class);
    }

    @Test
    void defaultMetricAutoConfigSetsMetricRegistryBeanForEnhancer() {
        testContext.withPropertyValues(
                           "axon.axonserver.enabled=false",
                           "axon.metrics.enabled=true"
                   )
                   .run(context -> {
                       assertThat(context).hasSingleBean(MetricRegistry.class);
                       assertThat(context).hasSingleBean(MetricsConfigurationEnhancer.class);
                   });
    }

    @Test
    void defaultMetricAutoConfigSetsMetricRegistryBeanForEnhancerWorkWithoutProperty() {
        testContext.withPropertyValues(
                           "axon.axonserver.enabled=false"
                           // Deliberately not included "axon.metrics.enabled=true" per test!
                   )
                   .run(context -> {
                       assertThat(context).hasSingleBean(MetricRegistry.class);
                       assertThat(context).hasSingleBean(MetricsConfigurationEnhancer.class);
                   });
    }

    @Test
    void disabledMetricsDisablesMetricRegistryAndMetricsConfigurationEnhancer() {
        testContext.withPropertyValues(
                           "axon.axonserver.enabled=false",
                           "axon.metrics.enabled=false"
                   )
                   .run(context -> {
                       assertThat(context).doesNotHaveBean(MetricRegistry.class);
                       assertThat(context).doesNotHaveBean(MetricsConfigurationEnhancer.class);
                   });
    }

    @Configuration
    @EnableAutoConfiguration
    public static class TestContext {

    }
}
