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

import io.axoniq.framework.kafka.configuration.KafkaConnectorStatus;
import io.axoniq.framework.springboot.actuator.kafka.KafkaConnectorHealthIndicator;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link KafkaActuatorAutoConfiguration}.
 *
 * @author Allard Buijze
 */
class KafkaActuatorAutoConfigurationTest {

    private final ApplicationContextRunner testContext =
            new ApplicationContextRunner().withConfiguration(
                    AutoConfigurations.of(KafkaActuatorAutoConfiguration.class)
            );

    @Test
    void registersTheHealthIndicator() {
        testContext.run(context -> {
            assertThat(context).hasSingleBean(KafkaConnectorHealthIndicator.class);
            // Without a connector status, there is nothing to report.
            assertThat(context.getBean(KafkaConnectorHealthIndicator.class).health().getStatus())
                    .isEqualTo(Status.UP);
        });
    }

    @Test
    void healthIndicatorCanBeDisabled() {
        testContext.withPropertyValues("management.health.kafka-connector.enabled=false")
                   .run(context -> assertThat(context).doesNotHaveBean(KafkaConnectorHealthIndicator.class));
    }

    @Test
    void healthIndicatorIsAbsentWhenTheConnectorIsDisabled() {
        testContext.withPropertyValues("axon.kafka.enabled=false")
                   .run(context -> assertThat(context).doesNotHaveBean(KafkaConnectorHealthIndicator.class));
    }

    @Test
    void healthIndicatorIsAbsentWithoutActuator() {
        testContext.withClassLoader(new FilteredClassLoader("org.springframework.boot.actuate"))
                   .run(context -> assertThat(context).doesNotHaveBean(KafkaConnectorHealthIndicator.class));
    }

    @Test
    void applicationHealthIndicatorReplacesTheAutoconfiguredOne() {
        testContext.withUserConfiguration(CustomHealthIndicatorConfiguration.class)
                   .run(context -> {
                       assertThat(context).hasSingleBean(KafkaConnectorHealthIndicator.class);
                       assertThat(context).hasBean("customKafkaConnectorHealthIndicator");
                   });
    }

    @Configuration
    static class CustomHealthIndicatorConfiguration {

        @Bean
        KafkaConnectorHealthIndicator customKafkaConnectorHealthIndicator(
                ObjectProvider<KafkaConnectorStatus> status
        ) {
            return new KafkaConnectorHealthIndicator(status);
        }
    }
}
