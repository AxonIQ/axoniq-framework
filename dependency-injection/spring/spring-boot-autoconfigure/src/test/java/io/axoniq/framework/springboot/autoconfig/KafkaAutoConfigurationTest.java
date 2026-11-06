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

import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.LegacyKafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.configuration.KafkaConnector;
import io.axoniq.framework.kafka.publishing.RecordKeyResolver;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.subscribing.FailedRecordPolicy;
import io.axoniq.framework.kafka.subscribing.InitialPosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.*;

/**
 * Test class validating the {@link KafkaAutoConfiguration}.
 * <p>
 * The autoconfiguration translates properties into a {@link KafkaConnector}. The assertions build a
 * {@link MessagingConfigurer} with the connector bean and inspect the resulting components; creating sinks and
 * sources does not connect to Kafka.
 *
 * @author Allard Buijze
 */
class KafkaAutoConfigurationTest {

    private static final TopicResolver TOPICS = (event, context) -> Optional.of("parcels");
    private static final RecordKeyResolver KEYS = (event, context) -> "key";
    private static final KafkaEventCodec<byte[]> CODEC =
            new DefaultKafkaEventCodec(new DelegatingEventConverter(new JacksonConverter()));

    private final ApplicationContextRunner testContext = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class));

    private static AxonConfiguration configurationFrom(ApplicationContext context) {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> context.getBeansOfType(ConfigurationEnhancer.class)
                                                                        .values()
                                                                        .forEach(registry::registerEnhancer))
                                  .build();
    }

    /**
     * Returns the properties the given {@code component} describes, looking through decorators.
     */
    private static Map<String, Object> describedProperties(Object component) {
        MockComponentDescriptor descriptor = new MockComponentDescriptor();
        ((DescribableComponent) component).describeTo(descriptor);
        Object delegate = descriptor.getDescribedProperties().get("delegate");
        return delegate instanceof DescribableComponent ? describedProperties(delegate)
                : descriptor.getDescribedProperties();
    }

    @Nested
    class Activation {

        @Test
        void registersConnectorByDefault() {
            // when
            testContext.run(context -> {
                // then
                assertThat(context).hasSingleBean(KafkaConnector.class);
            });
        }

        @Test
        void backsOffWhenDisabled() {
            // given
            ApplicationContextRunner disabled = testContext.withPropertyValues("axon.kafka.enabled=false");

            // when
            disabled.run(context -> {
                // then
                assertThat(context).doesNotHaveBean(KafkaConnector.class);
            });
        }

        @Test
        void backsOffWhenConnectorIsNotOnTheClasspath() {
            // given
            ApplicationContextRunner withoutConnector =
                    testContext.withClassLoader(new FilteredClassLoader(KafkaConnector.class));

            // when
            withoutConnector.run(context -> {
                // then
                assertThat(context).doesNotHaveBean(KafkaConnector.class);
            });
        }

        @Test
        void backsOffWhenTheApplicationDefinesAConnector() {
            // given
            KafkaConnector own = KafkaConnector.create(Map.of());
            ApplicationContextRunner withOwnConnector =
                    testContext.withPropertyValues("axon.kafka.sinks.outbound.topic=parcels")
                               .withBean(KafkaConnector.class, () -> own);

            // when
            withOwnConnector.run(context -> {
                // then
                assertThat(context).getBean(KafkaConnector.class).isSameAs(own);
                assertThat(configurationFrom(context).getOptionalComponent(EventSink.class, "outbound")).isEmpty();
            });
        }
    }

    @Nested
    class ApplicationBeans {

        private Logger autoConfigurationLogger;
        private ListAppender appender;

        @BeforeEach
        void attachAppender() {
            autoConfigurationLogger = (Logger) LogManager.getLogger(KafkaAutoConfiguration.class);
            appender = new ListAppender("KafkaAutoConfigurationTest");
            appender.start();
            autoConfigurationLogger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            autoConfigurationLogger.removeAppender(appender);
            appender.stop();
        }

        private void assertWarned(String fragment) {
            assertThat(appender.getEvents()).map(LogEvent::getMessage)
                                            .anySatisfy(m -> assertThat(m.getFormattedMessage()).contains(fragment));
        }

        @Test
        void clientFactoryBeanReplacesTheClientSettings() {
            // given
            ApplicationContextRunner withFactory =
                    testContext.withBean(KafkaClientFactory.class, () -> KafkaClientFactory.create(Map.of()));

            // when
            withFactory.run(context -> {
                // then - the connector registers no client factory of its own, so the bean is used
                assertThat(configurationFrom(context).getOptionalComponent(KafkaClientFactory.class)).isEmpty();
                assertThat(appender.getEvents()).isEmpty();
            });
        }

        @Test
        void clientSettingsNextToAClientFactoryBeanAreReportedAsUnused() {
            // given
            ApplicationContextRunner withFactoryAndSettings =
                    testContext.withPropertyValues("axon.kafka.bootstrap-servers=broker:9092")
                               .withBean(KafkaClientFactory.class, () -> KafkaClientFactory.create(Map.of()));

            // when
            withFactoryAndSettings.run(context -> {
                // then
                assertThat(context).hasNotFailed();
                assertWarned("axon.kafka.bootstrap-servers");
            });
        }

        @Test
        void legacyCodecSettingNextToACodecBeanIsReportedAsUnused() {
            // given
            ApplicationContextRunner withCodecAndSetting =
                    testContext.withPropertyValues("axon.kafka.codec=legacy")
                               .withBean(KafkaEventCodec.class, () -> CODEC);

            // when
            withCodecAndSetting.run(context -> {
                // then
                assertThat(context).hasNotFailed();
                assertWarned("axon.kafka.codec=legacy");
            });
        }

        @Test
        void legacyCodecSettingWrapsTheDefaultCodec() {
            // given
            ApplicationContextRunner legacy = testContext.withPropertyValues("axon.kafka.codec=legacy");

            // when
            legacy.run(context -> {
                // then
                assertThat(configurationFrom(context).getComponent(KafkaEventCodec.class))
                        .isInstanceOf(LegacyKafkaEventCodec.class);
            });
        }

        @Test
        void severalCodecBeansWithoutAPrimaryOneFailTheContext() {
            // given
            ApplicationContextRunner withTwoCodecs =
                    testContext.withBean("firstCodec", KafkaEventCodec.class, () -> CODEC)
                               .withBean("secondCodec", KafkaEventCodec.class, () -> CODEC);

            // when
            withTwoCodecs.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("several KafkaEventCodec beans")
                                   .hasMessageContaining("@Primary");
            });
        }

        @Test
        void severalCodecBeansWithAPrimaryOneAreAccepted() {
            // given
            ApplicationContextRunner withPrimaryCodec = testContext.withUserConfiguration(PrimaryCodecs.class);

            // when
            withPrimaryCodec.run(context -> {
                // then
                assertThat(context).hasNotFailed();
            });
        }

        @Test
        void qualifiedResolverBeansReplaceTheSinkSettings() {
            // given
            ApplicationContextRunner withResolvers =
                    testContext.withPropertyValues("axon.kafka.sinks.outbound.confirmation-mode=none")
                               .withUserConfiguration(OutboundResolvers.class);

            // when
            withResolvers.run(context -> {
                // then
                Map<String, Object> sink = describedProperties(
                        configurationFrom(context).getComponent(EventSink.class, "outbound")
                );
                assertThat(sink.get("topicResolver")).isSameAs(TOPICS);
                assertThat(sink.get("recordKeyResolver")).isSameAs(KEYS);
            });
        }

        @Test
        void sinkSettingsNextToQualifiedResolverBeansAreReportedAsUnused() {
            // given
            ApplicationContextRunner withResolversAndSettings =
                    testContext.withPropertyValues("axon.kafka.sinks.outbound.topic=other",
                                                   "axon.kafka.sinks.outbound.record-key-metadata=parcelId")
                               .withUserConfiguration(OutboundResolvers.class);

            // when
            withResolversAndSettings.run(context -> {
                // then
                assertThat(context).hasNotFailed();
                assertWarned("axon.kafka.sinks.outbound.topic");
                assertWarned("axon.kafka.sinks.outbound.record-key-metadata");
            });
        }

        @Test
        void resolverBeansQualifiedForAnotherSinkAreNotUsed() {
            // given
            ApplicationContextRunner withOtherSink =
                    testContext.withPropertyValues("axon.kafka.sinks.other.topic=parcels")
                               .withUserConfiguration(OutboundResolvers.class);

            // when
            withOtherSink.run(context -> {
                // then
                Map<String, Object> sink = describedProperties(
                        configurationFrom(context).getComponent(EventSink.class, "other")
                );
                assertThat(sink.get("topicResolver")).isNotSameAs(TOPICS);
                assertThat(sink.get("recordKeyResolver")).isNotSameAs(KEYS);
            });
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OutboundResolvers {

        @Bean
        @Qualifier("outbound")
        TopicResolver outboundTopics() {
            return TOPICS;
        }

        @Bean
        @Qualifier("outbound")
        RecordKeyResolver outboundKeys() {
            return KEYS;
        }
    }

    private static final Executor GROUP_EXECUTOR = Runnable::run;

    @Configuration(proxyBeanMethods = false)
    static class GroupExecutor {

        @Bean
        @Qualifier("group")
        Executor groupExecutor() {
            return GROUP_EXECUTOR;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class PrimaryCodecs {

        @Bean
        @Primary
        KafkaEventCodec<byte[]> firstCodec() {
            return CODEC;
        }

        @Bean
        KafkaEventCodec<byte[]> secondCodec() {
            return CODEC;
        }
    }

    @Nested
    class Sinks {

        @Test
        void withoutSinksNoSinkIsRegistered() {
            // when
            testContext.run(context -> {
                // then - only the application's own, unnamed event sink exists
                assertThat(configurationFrom(context).getComponents(EventSink.class))
                        .doesNotContainKey("outbound")
                        .allSatisfy((name, sink) -> assertThat(name).isNull());
            });
        }

        @Test
        void configuredSinkBecomesNamedEventSink() {
            // given
            ApplicationContextRunner withSink =
                    testContext.withPropertyValues("axon.kafka.sinks.outbound.topic=parcels",
                                                   "axon.kafka.sinks.outbound.confirmation-mode=transactional",
                                                   "axon.kafka.sinks.outbound.transactional-id-prefix=parcels-1-");

            // when
            withSink.run(context -> {
                // then
                assertThat(configurationFrom(context).getOptionalComponent(EventSink.class, "outbound"))
                        .isPresent();
            });
        }

        @Test
        void sinkWithoutTopicFailsTheContext() {
            // given
            ApplicationContextRunner withoutTopic =
                    testContext.withPropertyValues("axon.kafka.sinks.outbound.confirmation-mode=none");

            // when
            withoutTopic.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("axon.kafka.sinks.outbound.topic");
            });
        }

        @Test
        void exposedEventsArePublishedByAnExposureProcessor() {
            // given
            ApplicationContextRunner exposing =
                    testContext.withPropertyValues("axon.kafka.sinks.outbound.topic=parcels",
                                                   "axon.kafka.sinks.outbound.expose-events=parcels.ParcelDispatched",
                                                   "axon.kafka.sinks.outbound.expose-from-beginning=true");

            // when
            exposing.run(context -> {
                // then
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(KafkaConnector.class)).isNotNull();
            });
        }
    }

    @Nested
    class Sources {

        @Test
        void configuredSourcesBecomeNamedEventSources() {
            // given
            ApplicationContextRunner withSources =
                    testContext.withPropertyValues("axon.kafka.streaming-sources.stream.topics=parcels",
                                                   "axon.kafka.subscribing-sources.group.topics=parcels",
                                                   "axon.kafka.subscribing-sources.group.group-id=billing",
                                                   "axon.kafka.streaming-sources.pattern.topic-pattern=parcels\\..*");

            // when
            withSources.run(context -> {
                // then
                AxonConfiguration configuration = configurationFrom(context);
                assertThat(configuration.getOptionalComponent(StreamableEventSource.class, "stream")).isPresent();
                assertThat(configuration.getOptionalComponent(StreamableEventSource.class, "pattern")).isPresent();
                assertThat(configuration.getOptionalComponent(SubscribableEventSource.class, "group")).isPresent();
            });
        }

        @Test
        void streamingSourceSettingsAreApplied() {
            // given
            ApplicationContextRunner tuned =
                    testContext.withPropertyValues(
                            "axon.kafka.streaming-sources.stream.topics=parcels",
                            "axon.kafka.streaming-sources.stream.retention-gap-policy=reset-to-earliest",
                            "axon.kafka.streaming-sources.stream.buffer-size=64",
                            "axon.kafka.streaming-sources.stream.poll-timeout=250ms",
                            "axon.kafka.streaming-sources.stream.metadata-refresh-interval=5s"
                    );

            // when
            tuned.run(context -> {
                // then
                Map<String, Object> source = describedProperties(
                        configurationFrom(context).getComponent(StreamableEventSource.class, "stream")
                );
                assertThat(source).containsEntry("retentionGapPolicy", RetentionGapPolicy.RESET_TO_EARLIEST)
                                  .containsEntry("bufferSize", 64)
                                  .containsEntry("pollTimeout", Duration.ofMillis(250))
                                  .containsEntry("metadataRefreshInterval", Duration.ofSeconds(5));
            });
        }

        @Test
        void subscribingSourceSettingsAreApplied() {
            // given
            ApplicationContextRunner tuned =
                    testContext.withPropertyValues(
                            "axon.kafka.subscribing-sources.group.topics=parcels",
                            "axon.kafka.subscribing-sources.group.group-id=billing",
                            "axon.kafka.subscribing-sources.group.consumer-count=3",
                            "axon.kafka.subscribing-sources.group.failed-record-policy=skip",
                            "axon.kafka.subscribing-sources.group.retention-gap-policy=reset-to-earliest",
                            "axon.kafka.subscribing-sources.group.initial-position=latest",
                            "axon.kafka.subscribing-sources.group.retry-initial-backoff=1s",
                            "axon.kafka.subscribing-sources.group.retry-max-backoff=1m",
                            "axon.kafka.subscribing-sources.group.dead-letter-topic-suffix=.failed",
                            "axon.kafka.subscribing-sources.group.poll-timeout=50ms",
                            "axon.kafka.subscribing-sources.group.revocation-timeout=20s",
                            "axon.kafka.subscribing-sources.group.producer-properties.acks=all"
                    );

            // when
            tuned.run(context -> {
                // then
                Map<String, Object> source = describedProperties(
                        configurationFrom(context).getComponent(SubscribableEventSource.class, "group")
                );
                assertThat(source).containsEntry("consumerCount", 3)
                                  .containsEntry("failedRecordPolicy", FailedRecordPolicy.SKIP)
                                  .containsEntry("retentionGapPolicy", RetentionGapPolicy.RESET_TO_EARLIEST)
                                  .containsEntry("initialPosition", InitialPosition.LATEST)
                                  .containsEntry("retryInitialBackoff", Duration.ofSeconds(1))
                                  .containsEntry("retryMaxBackoff", Duration.ofMinutes(1))
                                  .containsEntry("deadLetterTopicSuffix", ".failed")
                                  .containsEntry("pollTimeout", Duration.ofMillis(50))
                                  .containsEntry("revocationTimeout", Duration.ofSeconds(20));
            });
        }

        @Test
        void subscribingSourceKeepsTheConnectorDefaultsForUnsetProperties() {
            // given
            ApplicationContextRunner plain =
                    testContext.withPropertyValues("axon.kafka.subscribing-sources.group.topics=parcels",
                                                   "axon.kafka.subscribing-sources.group.group-id=billing");

            // when
            plain.run(context -> {
                // then
                Map<String, Object> source = describedProperties(
                        configurationFrom(context).getComponent(SubscribableEventSource.class, "group")
                );
                assertThat(source).containsEntry("consumerCount", 1)
                                  .containsEntry("failedRecordPolicy", FailedRecordPolicy.RETRY)
                                  .containsEntry("retentionGapPolicy", RetentionGapPolicy.FAIL)
                                  .containsEntry("initialPosition", InitialPosition.EARLIEST);
            });
        }

        @Test
        void sourceWithTopicsAndATopicPatternFailsTheContext() {
            // given
            ApplicationContextRunner ambiguous =
                    testContext.withPropertyValues("axon.kafka.streaming-sources.stream.topics=parcels",
                                                   "axon.kafka.streaming-sources.stream.topic-pattern=orders\\..*");

            // when
            ambiguous.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("axon.kafka.streaming-sources.stream.topics")
                                   .hasMessageContaining("axon.kafka.streaming-sources.stream.topic-pattern");
            });
        }

        @Test
        void subscribingSourceWithTopicsAndATopicPatternFailsTheContext() {
            // given
            ApplicationContextRunner ambiguous =
                    testContext.withPropertyValues("axon.kafka.subscribing-sources.group.topics=parcels",
                                                   "axon.kafka.subscribing-sources.group.topic-pattern=orders\\..*",
                                                   "axon.kafka.subscribing-sources.group.group-id=billing");

            // when
            ambiguous.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("axon.kafka.subscribing-sources.group.topics")
                                   .hasMessageContaining("axon.kafka.subscribing-sources.group.topic-pattern");
            });
        }

        @Test
        void subscribingSourceWithoutGroupFailsTheContext() {
            // given
            ApplicationContextRunner withoutGroup =
                    testContext.withPropertyValues("axon.kafka.subscribing-sources.group.topics=parcels");

            // when
            withoutGroup.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("group-id");
            });
        }

        @Test
        void sourceNameUsedForBothKindsFailsTheContext() {
            // given
            ApplicationContextRunner duplicate =
                    testContext.withPropertyValues("axon.kafka.streaming-sources.parcels.topics=parcels",
                                                   "axon.kafka.subscribing-sources.parcels.topics=parcels",
                                                   "axon.kafka.subscribing-sources.parcels.group-id=billing");

            // when
            duplicate.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("axon.kafka.streaming-sources.parcels")
                                   .hasMessageContaining("axon.kafka.subscribing-sources.parcels");
            });
        }

        @Test
        void sourceNamedLikeASinkFailsTheContextWithTheProperties() {
            // given
            ApplicationContextRunner duplicate =
                    testContext.withPropertyValues("axon.kafka.sinks.parcels.topic=parcels",
                                                   "axon.kafka.streaming-sources.parcels.topics=parcels");

            // when
            duplicate.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("axon.kafka.sinks.parcels")
                                   .hasMessageContaining("axon.kafka.streaming-sources.parcels");
            });
        }

        @Test
        void subscribingSourceWithAnOffsetResetAndDefaultSettingsStarts() {
            // given - the settings an offset reset replaces are given, but left at their defaults
            ApplicationContextRunner withOffsetReset =
                    testContext.withPropertyValues(
                            "axon.kafka.subscribing-sources.group.topics=parcels",
                            "axon.kafka.subscribing-sources.group.group-id=billing",
                            "axon.kafka.subscribing-sources.group.initial-position=earliest",
                            "axon.kafka.subscribing-sources.group.properties.auto.offset.reset=latest"
                    );

            // when
            withOffsetReset.run(context -> {
                // then
                assertThat(context).hasNotFailed();
            });
        }

        @Test
        void subscribingSourceWithAnOffsetResetAndTheSettingsItReplacesFailsTheContext() {
            // given
            ApplicationContextRunner ambiguous =
                    testContext.withPropertyValues(
                            "axon.kafka.subscribing-sources.group.topics=parcels",
                            "axon.kafka.subscribing-sources.group.group-id=billing",
                            "axon.kafka.subscribing-sources.group.initial-position=latest",
                            "axon.kafka.subscribing-sources.group.retention-gap-policy=reset-to-earliest",
                            "axon.kafka.subscribing-sources.group.properties.auto.offset.reset=latest"
                    );

            // when
            ambiguous.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .hasMessageContaining("auto.offset.reset")
                                   .hasMessageContaining("initial-position")
                                   .hasMessageContaining("retention-gap-policy");
            });
        }

        @Test
        void subscribingSourceWithAnOffsetResetAloneStarts() {
            // given
            ApplicationContextRunner withOffsetReset =
                    testContext.withPropertyValues(
                            "axon.kafka.subscribing-sources.group.topics=parcels",
                            "axon.kafka.subscribing-sources.group.group-id=billing",
                            "axon.kafka.subscribing-sources.group.properties.auto.offset.reset=latest"
                    );

            // when
            withOffsetReset.run(context -> {
                // then
                assertThat(context).hasNotFailed();
            });
        }

        @Test
        void qualifiedExecutorBeanProcessesTheEventsOfTheSubscribingSource() {
            // given
            ApplicationContextRunner withExecutor =
                    testContext.withPropertyValues("axon.kafka.subscribing-sources.group.topics=parcels",
                                                   "axon.kafka.subscribing-sources.group.group-id=billing",
                                                   "axon.kafka.subscribing-sources.other.topics=parcels",
                                                   "axon.kafka.subscribing-sources.other.group-id=auditing")
                               .withUserConfiguration(GroupExecutor.class);

            // when
            withExecutor.run(context -> {
                // then
                AxonConfiguration configuration = configurationFrom(context);
                assertThat(describedProperties(configuration.getComponent(SubscribableEventSource.class, "group")))
                        .containsEntry("executor", GROUP_EXECUTOR);
                assertThat(describedProperties(configuration.getComponent(SubscribableEventSource.class, "other")))
                        .doesNotContainKey("executor");
            });
        }

        @Test
        void invalidTopicPatternFailsTheContextWithTheProperty() {
            // given
            ApplicationContextRunner invalidPattern =
                    testContext.withPropertyValues("axon.kafka.streaming-sources.pattern.topic-pattern=parcels[");

            // when
            invalidPattern.run(context -> {
                // then
                assertThat(context).hasFailed()
                                   .getFailure()
                                   .rootCause()
                                   .isInstanceOf(java.util.regex.PatternSyntaxException.class);
                assertThat(context.getStartupFailure())
                        .hasStackTraceContaining("axon.kafka.streaming-sources.pattern.topic-pattern");
            });
        }
    }
}
