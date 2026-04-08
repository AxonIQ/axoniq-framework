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

import org.axonframework.axonserver.connector.AxonServerConfigurationEnhancer;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating that the
 * {@link org.axonframework.eventsourcing.configuration.EventSourcingConfigurationDefaults} are registered and
 * customizable when using Spring Boot.
 *
 * @author Steven van Beelen
 */
class EventSourcingConfigurationDefaultsAutoconfigTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withUserConfiguration(TestContext.class)
                .withPropertyValues("axon.axonserver.enabled=false", "axon.eventstorage.jpa.polling-interval=0");
    }

    @Test
    void defaultAxonEventSourcingComponentsArePresent() {
        testContext.run(context -> {
            assertThat(context).hasSingleBean(TagResolver.class);
            assertThat(context).hasBean(TagResolver.class.getName());
            assertThat(context).hasSingleBean(EventStorageEngine.class);
            assertThat(context).hasBean(EventStorageEngine.class.getName());
            assertThat(context).hasSingleBean(EventStore.class);
            assertThat(context).hasSingleBean(EventSink.class);
            assertThat(context).hasBean(EventStore.class.getName());
        });
    }

    @Test
    void overrideDefaultAxonEventSourcingComponentsArePresent() {
        testContext.withUserConfiguration(CustomContext.class).run(context -> {
            assertThat(context).hasSingleBean(TagResolver.class);
            assertThat(context).hasBean("customTagResolver");
            assertThat(context).hasSingleBean(EventStorageEngine.class);
            assertThat(context).hasBean("customEventStorageEngine");
            assertThat(context).hasSingleBean(EventStore.class);
            assertThat(context).hasSingleBean(EventSink.class);
            assertThat(context).hasBean("customEventStore");
        });
    }

    @Configuration
    @EnableAutoConfiguration
    public static class TestContext {

        @Bean
        public ConfigurationEnhancer disableServerConnectorEnhancer() {
            return new ConfigurationEnhancer() {
                @Override
                public void enhance(@NonNull ComponentRegistry registry) {
                    registry.disableEnhancer(AxonServerConfigurationEnhancer.class);
                }

                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }
            };
        }
    }

    @Configuration
    @EnableAutoConfiguration
    public static class CustomContext {

        @Bean
        public TagResolver customTagResolver() {
            return mock(TagResolver.class);
        }

        @Bean
        public EventStorageEngine customEventStorageEngine() {
            return mock(EventStorageEngine.class);
        }

        @Bean
        public EventStore customEventStore() {
            return mock(EventStore.class);
        }
    }
}
