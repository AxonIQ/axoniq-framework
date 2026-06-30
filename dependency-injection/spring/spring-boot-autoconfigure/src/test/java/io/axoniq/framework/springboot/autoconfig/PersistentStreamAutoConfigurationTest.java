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

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.event.DefaultPersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamConnection;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSource;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamScheduledExecutorBuilder;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamSequencingPolicy;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.annotation.Namespace;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorConfiguration;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;

import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link PersistentStreamAutoConfiguration}.
 */
class PersistentStreamAutoConfigurationTest {

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withUserConfiguration(TestContext.class)
                .withPropertyValues("axon.axonserver.enabled=false", "axon.postgresql.enabled=false");
    }

    @Nested
    class DefaultEventSourceFactory {

        @Test
        void defaultFactoryIsRegisteredWhenNonePresent() {
            testContext.run(context -> {
                assertThat(context).hasSingleBean(PersistentStreamEventSourceFactory.class);
                assertThat(context.getBean(PersistentStreamEventSourceFactory.class))
                        .isInstanceOf(DefaultPersistentStreamEventSourceFactory.class);
            });
        }

        @Test
        void customFactoryOverridesDefault() {
            PersistentStreamEventSourceFactory custom = mock(PersistentStreamEventSourceFactory.class);
            testContext.withBean(PersistentStreamEventSourceFactory.class, () -> custom)
                       .run(context -> {
                           assertThat(context).hasSingleBean(PersistentStreamEventSourceFactory.class);
                           assertThat(context.getBean(PersistentStreamEventSourceFactory.class)).isSameAs(custom);
                       });
        }
    }

    @Nested
    class DefaultSchedulerBuilder {

        @Test
        void defaultBuilderIsRegisteredWhenNonePresent() {
            testContext.run(context ->
                    assertThat(context).hasSingleBean(PersistentStreamScheduledExecutorBuilder.class)
            );
        }

        @Test
        void customBuilderOverridesDefault() {
            PersistentStreamScheduledExecutorBuilder custom = mock(PersistentStreamScheduledExecutorBuilder.class);
            testContext.withBean(PersistentStreamScheduledExecutorBuilder.class, () -> custom)
                       .run(context -> {
                           assertThat(context).hasSingleBean(PersistentStreamScheduledExecutorBuilder.class);
                           assertThat(context.getBean(PersistentStreamScheduledExecutorBuilder.class)).isSameAs(custom);
                       });
        }
    }

    @Nested
    class StreamSourceRegistration {

        @Test
        void noStreamBeansRegisteredWhenNoneConfigured() {
            testContext.run(context ->
                    assertThat(context).doesNotHaveBean(PersistentStreamEventSource.class)
            );
        }

        @Test
        void streamBeanRegisteredForEachConfiguredStream() {
            testContext.withPropertyValues(
                    "axon.axonserver.persistent-streams.orderStream.initial-segment-count=2",
                    "axon.axonserver.persistent-streams.inventoryStream.initial-segment-count=1"
            ).run(context -> {
                assertThat(context).hasBean("orderStream");
                assertThat(context).hasBean("inventoryStream");
                assertThat(context.getBean("orderStream")).isInstanceOf(PersistentStreamEventSource.class);
                assertThat(context.getBean("inventoryStream")).isInstanceOf(PersistentStreamEventSource.class);
            });
        }

        @Test
        void streamBeanNameEqualsMapKey() {
            testContext.withPropertyValues(
                    "axon.axonserver.persistent-streams.myStream.initial-segment-count=1"
            ).run(context -> assertThat(context).hasBean("myStream"));
        }

        @Test
        void streamBeanUsesSettingDefaults() {
            testContext.withPropertyValues(
                    "axon.axonserver.persistent-streams.myStream.name=stream1"
            ).run(context -> {
                assertThat(context).hasBean("myStream");
                PersistentStreamEventSource source = context.getBean("myStream", PersistentStreamEventSource.class);
                assertThat(source)
                        .isNotNull()
                        .hasFieldOrPropertyWithValue("name", "stream1")
                        .extracting("persistentStreamConnection", as(InstanceOfAssertFactories.type(
                                PersistentStreamConnection.class)))
                        .extracting("persistentStreamProperties",
                                    as(InstanceOfAssertFactories.type(PersistentStreamProperties.class)))
                        .hasFieldOrPropertyWithValue("streamName", "stream1")
                        .hasFieldOrPropertyWithValue("segments", 1)
                        .hasFieldOrPropertyWithValue("sequencingPolicyName",
                                                     PersistentStreamSequencingPolicy.SEQUENTIAL_POLICY)
                        .hasFieldOrPropertyWithValue("initialPosition", "TAIL")
                        .hasFieldOrPropertyWithValue("filter", null);
            });
        }

        @Test
        void customSchedulerBuilderIsUsedForStreamCreation() {
            ScheduledExecutorService customExecutor = mock(ScheduledExecutorService.class);
            PersistentStreamScheduledExecutorBuilder customBuilder =
                    (threadCount, streamName) -> customExecutor;

            testContext.withBean(PersistentStreamScheduledExecutorBuilder.class, () -> customBuilder)
                       .withPropertyValues(
                               "axon.axonserver.persistent-streams.myStream.initial-segment-count=1"
                       )
                       .run(context -> {
                                assertThat(context).hasBean("myStream");
                                PersistentStreamEventSource source = context.getBean("myStream",
                                                                                     PersistentStreamEventSource.class);
                                assertThat(source)
                                        .extracting("persistentStreamConnection", as(InstanceOfAssertFactories.type(
                                                PersistentStreamConnection.class)))
                                        .extracting("scheduler", as(InstanceOfAssertFactories.type(ScheduledExecutorService.class)))
                                        .isSameAs(customExecutor);
                            }


                       );
        }
    }

    @Nested
    class AutoPersistentStreams {

        @Test
        void persistentStreamWiredAsEventSourceForSubscribingProcessorWhenEnabled() {
            testContext
                    .withPropertyValues("axon.axonserver.auto-persistent-streams-enabled=true")
                    .withBean(TestEventHandlingComponent.class)
                    .run(context -> {
                        // given
                        AxonConfiguration axonConfig = context.getBean(AxonConfiguration.class);
                        SubscribingEventProcessor processor = axonConfig
                                .getModuleConfiguration("EventProcessor[test-processor]")
                                .flatMap(m -> m.getOptionalComponent(EventProcessor.class, "test-processor"))
                                .filter(SubscribingEventProcessor.class::isInstance)
                                .map(SubscribingEventProcessor.class::cast)
                                .orElseThrow();

                        // then
                        assertThat(eventSourceOf(processor))
                                .isInstanceOf(PersistentStreamEventSource.class)
                                .hasFieldOrPropertyWithValue("name", "test-processor-stream");
                    });
        }
    }

    private static SubscribableEventSource eventSourceOf(SubscribingEventProcessor processor) {
        try {
            var field = processor.getClass().getDeclaredField("configuration");
            field.setAccessible(true);
            return ((SubscribingEventProcessorConfiguration) field.get(processor)).eventSource();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    static class TestContext {

        @Bean
        AxonServerConnectionManager axonServerConnectionManager() {
            AxonServerConnectionManager mock = mock(AxonServerConnectionManager.class);
            AxonServerConnection mockConnection = mock(AxonServerConnection.class);
            when(mock.getConnection(anyString())).thenReturn(mockConnection);
            EventChannel mockEventChannel = mock(EventChannel.class);
            when(mockConnection.eventChannel()).thenReturn(mockEventChannel);
            return mock;
        }

        @Bean(name = "eventConverter")
        EventConverter eventConverter() {
            return mock(EventConverter.class);
        }

        @Bean
        Converter genericConverter() {
            return mock(Converter.class);
        }
    }

    @Namespace("test-processor")
    static class TestEventHandlingComponent {

        @EventHandler
        void on(String event) {
            // intentionally blank
        }
    }
}
