/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorConfiguration;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessorConfiguration;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PersistentStreamConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 */
class PersistentStreamConfigurationEnhancerTest {

    private AxonServerConfiguration serverConfig;
    private RecordingEventSourceFactory eventSourceFactory;
    private RecordingExecutorBuilder executorBuilder;
    private PersistentStreamConfigurationEnhancer testSubject;

    @BeforeEach
    void setUp() {
        serverConfig = new AxonServerConfiguration();
        eventSourceFactory = new RecordingEventSourceFactory();
        executorBuilder = new RecordingExecutorBuilder();
        testSubject = new PersistentStreamConfigurationEnhancer(
                () -> serverConfig, executorBuilder, eventSourceFactory
        );
    }

    @AfterEach
    void tearDown() {
        executorBuilder.created.forEach(ScheduledExecutorService::shutdownNow);
    }

    @Nested
    class Construction {

        @SuppressWarnings("DataFlowIssue")
        @Test
        void rejectsNullServerConfigSupplier() {
            // when / then
            assertThatThrownBy(() -> new PersistentStreamConfigurationEnhancer(
                    null, executorBuilder, eventSourceFactory
            )).isInstanceOf(NullPointerException.class)
              .hasMessageContaining("axonServerConfigSupplier");
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void rejectsNullSchedulerBuilder() {
            // when / then
            assertThatThrownBy(() -> new PersistentStreamConfigurationEnhancer(
                    () -> serverConfig, null, eventSourceFactory
            )).isInstanceOf(NullPointerException.class)
              .hasMessageContaining("schedulerBuilder");
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void rejectsNullEventSourceFactory() {
            // when / then
            assertThatThrownBy(() -> new PersistentStreamConfigurationEnhancer(
                    () -> serverConfig, executorBuilder, null
            )).isInstanceOf(NullPointerException.class)
              .hasMessageContaining("eventSourceFactory");
        }
    }

    @Nested
    class ExecutorRegistry {

        @Test
        void registersExecutorRegistryWhenNonePresent() {
            // when
            Configuration config = buildConfiguration(registry -> {
            });

            // then
            assertThat(config.getOptionalComponent(PersistentStreamExecutorRegistry.class)).isPresent();
        }

        @Test
        void keepsExistingExecutorRegistry() {
            // given
            PersistentStreamExecutorRegistry existing = new PersistentStreamExecutorRegistry();

            // when
            Configuration config = buildConfiguration(registry -> registry.registerComponent(
                    PersistentStreamExecutorRegistry.class, c -> existing
            ));

            // then
            assertThat(config.getComponent(PersistentStreamExecutorRegistry.class)).isSameAs(existing);
        }

        @Test
        void shutsDownTrackedExecutorsWhenConfigurationShutsDown() {
            // given
            streamSettings("myStream");
            AxonConfiguration config = buildConfiguration(registry -> {
            });
            config.getComponent(SubscribableEventSource.class, "myStream");
            try (ScheduledExecutorService executor = eventSourceFactory.invocations.getFirst()
                    .schedulerFactory.apply("pool")) {
                config.start();
                assertThat(executor.isShutdown()).isFalse();

                // when
                config.shutdown();

                // then
                assertThat(executor.isShutdown()).isTrue();
            }
        }
    }

    @Nested
    class ConfiguredStreams {

        @Test
        void registersNoSourcesWhenNoStreamsAreConfigured() {
            // when
            Configuration config = buildConfiguration(registry -> {
            });

            // then
            assertThat(config.hasComponent(SubscribableEventSource.class, "myStream")).isFalse();
            assertThat(eventSourceFactory.invocations).isEmpty();
        }

        @Test
        void registersSourceUnderTheMapKeyAndBuildsItLazily() {
            // given
            streamSettings("myStream");

            // when
            Configuration config = buildConfiguration(registry -> {
            });

            // then
            assertThat(eventSourceFactory.invocations).isEmpty();
            SubscribableEventSource source = config.getComponent(SubscribableEventSource.class, "myStream");
            assertThat(eventSourceFactory.invocations).hasSize(1);
            assertThat(source).isSameAs(eventSourceFactory.invocations.getFirst().source);
        }

        @Test
        void streamNameDefaultsToTheMapKey() {
            // given
            streamSettings("myStream");

            // when
            Configuration config = buildConfiguration(registry -> {
            });
            config.getComponent(SubscribableEventSource.class, "myStream");

            // then
            assertThat(eventSourceFactory.invocations.getFirst().name).isEqualTo("myStream");
            assertThat(eventSourceFactory.invocations.getFirst().properties.streamName()).isEqualTo("myStream");
        }

        @Test
        void streamNameCanBeOverriddenBySettings() {
            // given
            streamSettings("myStream").setName("serverSideName");

            // when
            Configuration config = buildConfiguration(registry -> {
            });
            config.getComponent(SubscribableEventSource.class, "myStream");

            // then
            assertThat(eventSourceFactory.invocations.getFirst().name).isEqualTo("serverSideName");
            assertThat(eventSourceFactory.invocations.getFirst().properties.streamName())
                    .isEqualTo("serverSideName");
        }

        @Test
        void passesSettingsOnToTheFactory() {
            // given
            AxonServerConfiguration.PersistentStreamSettings settings = streamSettings("myStream");
            settings.setInitialSegmentCount(4);
            settings.setSequencingPolicy("SequentialPerAggregate");
            settings.setSequencingPolicyParameters(List.of("a", "b"));
            settings.setInitialPosition("HEAD");
            settings.setFilter("payloadType == 'x'");
            settings.setBatchSize(25);

            // when
            Configuration config = buildConfiguration(registry -> {
            });
            config.getComponent(SubscribableEventSource.class, "myStream");

            // then
            RecordedBuild build = eventSourceFactory.invocations.getFirst();
            assertThat(build.properties)
                    .usingRecursiveComparison()
                    .isEqualTo(new PersistentStreamProperties("myStream",
                                                              4,
                                                              "SequentialPerAggregate",
                                                              List.of("a", "b"),
                                                              "HEAD",
                                                              "payloadType == 'x'"));
            assertThat(build.batchSize).isEqualTo(25);
            assertThat(build.configuration).isNotNull();
        }

        @Test
        void buildsEachConfiguredStream() {
            // given
            streamSettings("first");
            streamSettings("second");

            // when
            Configuration config = buildConfiguration(registry -> {
            });
            config.getComponent(SubscribableEventSource.class, "first");
            config.getComponent(SubscribableEventSource.class, "second");

            // then
            assertThat(eventSourceFactory.invocations)
                    .extracting(build -> build.name)
                    .containsExactlyInAnyOrder("first", "second");
        }

        @Test
        void schedulerFactoryBuildsPoolsWithConfiguredThreadCountAndRequestedName() {
            // given
            streamSettings("myStream").setThreadCount(3);

            // when
            Configuration config = buildConfiguration(registry -> {
            });
            config.getComponent(SubscribableEventSource.class, "myStream");
            ScheduledExecutorService first = eventSourceFactory.invocations.getFirst().schedulerFactory
                    .apply("pool-one");
            ScheduledExecutorService second = eventSourceFactory.invocations.getFirst().schedulerFactory
                    .apply("pool-two");

            // then
            assertThat(first).isNotSameAs(second);
            assertThat(executorBuilder.requests).containsExactly(new Request(3, "pool-one"),
                                                                 new Request(3, "pool-two"));
        }

        @Test
        void poolsFromTheSchedulerFactoryAreTrackedInTheRegistry() {
            // given
            streamSettings("myStream");
            Configuration config = buildConfiguration(registry -> {
            });
            config.getComponent(SubscribableEventSource.class, "myStream");
            ScheduledExecutorService pool = eventSourceFactory.invocations.getFirst().schedulerFactory.apply("pool");

            // when
            config.getComponent(PersistentStreamExecutorRegistry.class).shutdownAll();

            // then
            assertThat(pool.isShutdown()).isTrue();
        }
    }

    @Nested
    class AutoPersistentStreams {

        @Test
        void leavesProcessorConfigurationUntouchedWhenDisabled() {
            // given
            serverConfig.setAutoPersistentStreamsEnabled(false);

            // when
            Configuration config = buildConfiguration(registerProcessorConfiguration("projection"));
            config.getComponent(SubscribingEventProcessorConfiguration.class, "projection");

            // then
            assertThat(eventSourceFactory.invocations).isEmpty();
            assertThat(config.hasComponent(SubscribableEventSource.class, "projection-stream")).isFalse();
        }

        @Test
        void wiresPersistentStreamAsEventSourceOfSubscribingProcessorWhenEnabled() {
            // given
            serverConfig.setAutoPersistentStreamsEnabled(true);
            serverConfig.getAutoPersistentStreamsSettings().setBatchSize(7);
            serverConfig.getAutoPersistentStreamsSettings().setThreadCount(2);

            // when
            Configuration config = buildConfiguration(registerProcessorConfiguration("projection"));
            SubscribingEventProcessorConfiguration processorConfig =
                    config.getComponent(SubscribingEventProcessorConfiguration.class, "projection");

            // then
            assertThat(eventSourceFactory.invocations).hasSize(1);
            RecordedBuild build = eventSourceFactory.invocations.getFirst();
            assertThat(build.name).isEqualTo("projection-stream");
            assertThat(build.properties.streamName()).isEqualTo("projection-stream");
            assertThat(build.batchSize).isEqualTo(7);
            assertThat(processorConfig.eventSource()).isSameAs(build.source);
        }

        @Test
        void registersTheAutoPersistentStreamSourceAsNamedComponent() {
            // given
            serverConfig.setAutoPersistentStreamsEnabled(true);

            // when
            Configuration config = buildConfiguration(registerProcessorConfiguration("projection"));
            config.getComponent(SubscribingEventProcessorConfiguration.class, "projection");

            // then
            assertThat(config.getComponent(SubscribableEventSource.class, "projection-stream"))
                    .isSameAs(eventSourceFactory.invocations.getFirst().source);
        }

        @Test
        void tracksAutoPersistentStreamPoolsInTheRegistry() {
            // given
            serverConfig.setAutoPersistentStreamsEnabled(true);
            serverConfig.getAutoPersistentStreamsSettings().setThreadCount(2);
            Configuration config = buildConfiguration(registerProcessorConfiguration("projection"));
            config.getComponent(SubscribingEventProcessorConfiguration.class, "projection");
            ScheduledExecutorService pool = eventSourceFactory.invocations.getFirst().schedulerFactory.apply("pool");

            // when
            config.getComponent(PersistentStreamExecutorRegistry.class).shutdownAll();

            // then
            assertThat(executorBuilder.requests).containsExactly(new Request(2, "pool"));
            assertThat(pool.isShutdown()).isTrue();
        }

        @Test
        void reusesExplicitlyConfiguredStreamSourceWithMatchingName() {
            // given
            serverConfig.setAutoPersistentStreamsEnabled(true);
            streamSettings("projection-stream");

            // when
            Configuration config = buildConfiguration(registerProcessorConfiguration("projection"));
            config.getComponent(SubscribingEventProcessorConfiguration.class, "projection");

            // then
            assertThat(eventSourceFactory.invocations).isEmpty();
        }
    }

    private AxonServerConfiguration.PersistentStreamSettings streamSettings(String key) {
        AxonServerConfiguration.PersistentStreamSettings settings = new AxonServerConfiguration.PersistentStreamSettings();
        serverConfig.getPersistentStreams().put(key, settings);
        return settings;
    }

    private Consumer<ComponentRegistry> registerProcessorConfiguration(String processorName) {
        return registry -> registry.registerComponent(
                SubscribingEventProcessorConfiguration.class,
                processorName,
                config -> new SubscribingEventProcessorConfiguration(
                        new EventProcessorConfiguration(processorName, config), config
                )
        );
    }

    private AxonConfiguration buildConfiguration(Consumer<ComponentRegistry> additional) {
        return MessagingConfigurer.create()
                                  .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                  .componentRegistry(additional)
                                  .componentRegistry(testSubject::enhance)
                                  .build();
    }

    private record Request(int threadCount, String poolName) {

    }

    private static class RecordingExecutorBuilder implements PersistentStreamScheduledExecutorBuilder {

        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private final List<ScheduledExecutorService> created = new CopyOnWriteArrayList<>();

        @Override
        public ScheduledExecutorService apply(Integer threadCount, String streamName) {
            requests.add(new Request(threadCount, streamName));
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            created.add(executor);
            return executor;
        }
    }

    private static class RecordedBuild {

        private final String name;
        private final PersistentStreamProperties properties;
        private final Function<String, ScheduledExecutorService> schedulerFactory;
        private final int batchSize;
        private final Configuration configuration;
        private final SubscribableEventSource source;

        private RecordedBuild(String name,
                              PersistentStreamProperties properties,
                              Function<String, ScheduledExecutorService> schedulerFactory,
                              int batchSize,
                              Configuration configuration,
                              SubscribableEventSource source) {
            this.name = name;
            this.properties = properties;
            this.schedulerFactory = schedulerFactory;
            this.batchSize = batchSize;
            this.configuration = configuration;
            this.source = source;
        }
    }

    private static class RecordingEventSourceFactory implements PersistentStreamEventSourceFactory {

        private final List<RecordedBuild> invocations = new CopyOnWriteArrayList<>();

        @Override
        public SubscribableEventSource build(String name,
                                             PersistentStreamProperties properties,
                                             Function<String, ScheduledExecutorService> schedulerFactory,
                                             int batchSize,
                                             Configuration configuration) {
            SubscribableEventSource source = handler -> () -> true;
            invocations.add(new RecordedBuild(name, properties, schedulerFactory, batchSize, configuration, source));
            return source;
        }
    }
}
