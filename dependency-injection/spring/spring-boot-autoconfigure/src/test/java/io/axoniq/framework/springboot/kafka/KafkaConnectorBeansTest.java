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

package io.axoniq.framework.springboot.kafka;

import io.axoniq.framework.kafka.configuration.KafkaConnector;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates how a Spring Boot application treats {@link KafkaConnector} beans: one replaces the autoconfigured
 * connector, and a second one fails the application at startup, as its sinks and sources would otherwise silently use
 * the first connector's client factory.
 *
 * @author Allard Buijze
 */
class KafkaConnectorBeansTest {

    private final ApplicationContextRunner testContext =
            new ApplicationContextRunner().withUserConfiguration(TestApplication.class)
                                          .withPropertyValues("axon.axonserver.enabled=false",
                                                              "axon.postgresql.enabled=false");

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {JmxAutoConfiguration.class, WebClientAutoConfiguration.class})
    static class TestApplication {

    }

    @Configuration
    static class OneConnector {

        @Bean
        KafkaConnector firstConnector() {
            return KafkaConnector.create(Map.of()).sink("first", TopicResolver.fixed("parcels"));
        }
    }

    @Configuration
    static class TwoConnectors {

        @Bean
        KafkaConnector firstConnector() {
            return KafkaConnector.create(Map.of()).sink("first", TopicResolver.fixed("parcels"));
        }

        @Bean
        KafkaConnector secondConnector() {
            return KafkaConnector.create(Map.of()).sink("second", TopicResolver.fixed("parcels"));
        }
    }

    @Test
    void oneConnectorBeanStartsTheApplicationWithItsSinks() {
        // when
        testContext.withUserConfiguration(OneConnector.class).run(context -> {
            // then
            assertThat(context).hasNotFailed();
            assertThat(context).getBean("first", EventSink.class).isNotNull();
        });
    }

    @Test
    void secondConnectorBeanFailsTheApplicationAtStartup() {
        // when
        testContext.withUserConfiguration(TwoConnectors.class).run(context -> {
            // then
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).rootCause().hasMessageContaining("KafkaConnector");
        });
    }
}
