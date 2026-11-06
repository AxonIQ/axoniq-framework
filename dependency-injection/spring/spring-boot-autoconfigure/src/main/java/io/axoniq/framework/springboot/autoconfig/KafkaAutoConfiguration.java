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

import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.configuration.KafkaConnector;
import io.axoniq.framework.kafka.publishing.KafkaEventSink;
import io.axoniq.framework.kafka.publishing.KafkaEventSinkConfiguration;
import io.axoniq.framework.kafka.publishing.RecordKeyResolver;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSourceConfiguration;
import io.axoniq.framework.springboot.KafkaProperties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventSink;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Autoconfiguration of the Kafka connector, translating {@link KafkaProperties} into a {@link KafkaConnector}.
 * <p>
 * Every sink under {@code axon.kafka.sinks} becomes a {@link KafkaEventSink}, registered as an {@link EventSink} under
 * the sink's name, so that components inject it with {@code @Qualifier("<sink>")}, while an unqualified
 * {@link EventSink} remains the application's own. Every source under {@code axon.kafka.streaming-sources} or
 * {@code axon.kafka.subscribing-sources} becomes a named event source that processors select with
 * {@code axon.eventhandling.processors.<processor>.source=<source>}; a name used by more than one sink or source
 * fails the application context. Without sinks configured, nothing is published to Kafka. The connector uses the
 * plain Kafka clients and does not require Spring for Apache Kafka.
 * <p>
 * The connector is active as soon as the {@code axoniq-kafka} module is on the classpath, also without any
 * {@code axon.kafka} properties: it then connects to {@code localhost:9092}, but only once a sink or source is used.
 * Set {@code axon.kafka.enabled=false} to disable it.
 * <p>
 * Beans of the application take part as follows:
 * <ul>
 *     <li>A {@link KafkaClientFactory} bean creates the Kafka clients. The {@code axon.kafka.bootstrap-servers},
 *     {@code axon.kafka.client-id} and {@code axon.kafka.properties} settings are then not used, and a warning says so
 *     when they are set.</li>
 *     <li>A {@link KafkaEventCodec} bean replaces the default codec. The {@code axon.kafka.codec} setting is then not
 *     used, and a warning says so when it is set to {@code legacy}. Several codec beans without a primary one fail
 *     the application context.</li>
 *     <li>A {@link TopicResolver} or {@link RecordKeyResolver} bean qualified with a sink's name, for example with
 *     {@code @Qualifier("outbound")}, replaces the sink's {@code topic} or {@code record-key-metadata} setting, for
 *     topics or keys that properties cannot express.</li>
 *     <li>An {@link Executor} bean qualified with the name of a subscribing source processes that source's events,
 *     instead of virtual threads. The application owns that executor: the source does not shut it down.</li>
 *     <li>A {@link KafkaConnector} bean replaces the autoconfigured connector, and none of the {@code axon.kafka}
 *     properties are used. Declare that bean with return type {@link KafkaConnector}: a bean declared as a
 *     {@link org.axonframework.common.configuration.ConfigurationEnhancer} is not recognized as a connector, and the
 *     autoconfigured connector is registered as well.</li>
 * </ul>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
// Ordered before the event processing autoconfiguration, as the dead-letter queue autoconfiguration is, so that the
// connector's sources are registered before processors are configured. Processors resolve sources lazily, by name, so
// the order is not strictly required.
@AutoConfiguration(beforeName = "org.axonframework.extension.springboot.autoconfig.EventProcessingAutoConfiguration")
@ConditionalOnClass(KafkaConnector.class)
@ConditionalOnProperty(name = "axon.kafka.enabled", matchIfMissing = true)
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAutoConfiguration.class);
    private static final String SINKS = "axon.kafka.sinks.";
    private static final String STREAMING_SOURCES = "axon.kafka.streaming-sources.";
    private static final String SUBSCRIBING_SOURCES = "axon.kafka.subscribing-sources.";

    /**
     * Builds the {@link KafkaConnector} registering the configured sinks and sources.
     *
     * @param properties  the Kafka properties
     * @param beanFactory the bean factory, to find the application's Kafka beans without creating them early
     * @return the connector, as a configuration enhancer
     */
    @Bean
    @ConditionalOnMissingBean(KafkaConnector.class)
    public KafkaConnector kafkaConnector(KafkaProperties properties, ConfigurableListableBeanFactory beanFactory) {
        KafkaConnector connector = clientFactoryBeanDefined(beanFactory)
                ? usingClientFactoryBean(properties)
                : KafkaConnector.create(clientConfiguration(properties));
        boolean codecBeanDefined = codecBeanDefined(beanFactory);
        if (properties.getCodec() == KafkaProperties.Codec.LEGACY) {
            if (codecBeanDefined) {
                logger.warn("The axon.kafka.codec=legacy setting is not used: the application defines a "
                                    + "KafkaEventCodec bean, which encodes and decodes all records.");
            } else {
                connector.withLegacyFormat();
            }
        }
        rejectDuplicateNames(properties);
        properties.getSinks().forEach((name, sink) -> registerSink(connector, beanFactory, name, sink));
        properties.getStreamingSources().forEach((name, source) -> registerStreamingSource(connector, name, source));
        properties.getSubscribingSources()
                  .forEach((name, source) -> registerSubscribingSource(connector, beanFactory, name, source));
        return connector;
    }

    private static boolean clientFactoryBeanDefined(ConfigurableListableBeanFactory beanFactory) {
        return beanFactory.getBeanNamesForType(KafkaClientFactory.class, true, false).length > 0;
    }

    private static KafkaConnector usingClientFactoryBean(KafkaProperties properties) {
        boolean clientSettingsGiven =
                !properties.getBootstrapServers().equals(KafkaProperties.DEFAULT_BOOTSTRAP_SERVERS)
                        || properties.getClientId() != null
                        || !properties.getProperties().isEmpty();
        if (clientSettingsGiven) {
            logger.warn("The axon.kafka.bootstrap-servers, axon.kafka.client-id and axon.kafka.properties settings "
                                + "are not used: the application defines a KafkaClientFactory bean, which creates all "
                                + "Kafka clients. Configure the clients through that bean instead.");
        }
        return KafkaConnector.usingRegisteredClientFactory();
    }

    private static boolean codecBeanDefined(ConfigurableListableBeanFactory beanFactory) {
        String[] codecs = beanFactory.getBeanNamesForType(KafkaEventCodec.class, true, false);
        if (codecs.length > 1) {
            long primaries = Arrays.stream(codecs)
                                   .filter(beanFactory::containsBeanDefinition)
                                   .filter(name -> beanFactory.getBeanDefinition(name).isPrimary())
                                   .count();
            if (primaries != 1) {
                throw new AxonConfigurationException(
                        "The application defines several KafkaEventCodec beans " + Arrays.toString(codecs)
                                + ", and the Kafka connector cannot choose between them. Define one codec bean, or "
                                + "mark the one to use with @Primary."
                );
            }
        }
        return codecs.length > 0;
    }

    private static Map<String, Object> clientConfiguration(KafkaProperties properties) {
        Map<String, Object> configuration = new HashMap<>(properties.getProperties());
        // The dedicated setting takes precedence over a bootstrap.servers entry in axon.kafka.properties.
        configuration.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                          String.join(",", properties.getBootstrapServers()));
        String clientId = properties.getClientId();
        if (clientId != null) {
            configuration.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        }
        return configuration;
    }

    private static void registerSink(KafkaConnector connector,
                                     ConfigurableListableBeanFactory beanFactory,
                                     String name,
                                     KafkaProperties.Sink sink) {
        String topic = sink.getTopic();
        String topicResolverBean = qualifiedBean(beanFactory, TopicResolver.class, name);
        String recordKeyResolverBean = qualifiedBean(beanFactory, RecordKeyResolver.class, name);
        Function<Configuration, TopicResolver> topicResolver;
        if (topicResolverBean != null) {
            if (topic != null) {
                logger.warn("The axon.kafka.sinks.{}.topic setting is not used: the TopicResolver bean [{}] resolves "
                                    + "the topics of sink [{}].", name, topicResolverBean, name);
            }
            // The bean is not created before the sink is.
            topicResolver = config -> beanFactory.getBean(topicResolverBean, TopicResolver.class);
        } else if (topic != null && !topic.isBlank()) {
            TopicResolver fixed = TopicResolver.fixed(topic);
            topicResolver = config -> fixed;
        } else {
            throw new AxonConfigurationException("Kafka sink [" + name + "] needs a topic: axon.kafka.sinks." + name
                                                         + ".topic, or a TopicResolver bean qualified with the "
                                                         + "sink's name.");
        }
        if (recordKeyResolverBean != null && sink.getRecordKeyMetadata() != null) {
            logger.warn("The axon.kafka.sinks.{}.record-key-metadata setting is not used: the RecordKeyResolver bean "
                                + "[{}] resolves the record keys of sink [{}].", name, recordKeyResolverBean, name);
        }
        connector.sink(name, topicResolver,
                       (config, configuration) -> configure(beanFactory, sink, recordKeyResolverBean, configuration));
        if (!sink.getExposeEvents().isEmpty()) {
            connector.exposeEvents(
                    name + "-exposure",
                    name,
                    sink.getExposeEvents().stream().map(QualifiedName::new).collect(Collectors.toSet()),
                    (config, processor) -> sink.isExposeFromBeginning()
                            ? processor.initialToken(source -> source.firstToken(null))
                            : processor
            );
        }
    }

    private static KafkaEventSinkConfiguration configure(ConfigurableListableBeanFactory beanFactory,
                                                         KafkaProperties.Sink sink,
                                                         @Nullable String recordKeyResolverBean,
                                                         KafkaEventSinkConfiguration configuration) {
        KafkaEventSinkConfiguration result =
                configuration.withConfirmationMode(sink.getConfirmationMode())
                             .withAckTimeout(sink.getAckTimeout())
                             .withProducerProperties(Map.copyOf(sink.getProperties()));
        String recordKeyMetadata = sink.getRecordKeyMetadata();
        if (recordKeyResolverBean != null) {
            result = result.withRecordKeyResolver(beanFactory.getBean(recordKeyResolverBean, RecordKeyResolver.class));
        } else if (recordKeyMetadata != null) {
            result = result.withRecordKeyResolver(RecordKeyResolver.fromMetadata(recordKeyMetadata));
        }
        String transactionalIdPrefix = sink.getTransactionalIdPrefix();
        if (transactionalIdPrefix != null) {
            result = result.withTransactionalIdPrefix(transactionalIdPrefix);
        }
        return result;
    }

    /**
     * Returns the name of the single bean of the given {@code type} whose name or {@code @Qualifier} matches the given
     * {@code qualifier}, without creating it, or {@code null} when there is none.
     */
    private static @Nullable String qualifiedBean(ConfigurableListableBeanFactory beanFactory,
                                                  Class<?> type,
                                                  String qualifier) {
        List<String> matches = Arrays.stream(beanFactory.getBeanNamesForType(type, true, false))
                                     .filter(bean -> BeanFactoryAnnotationUtils.isQualifierMatch(
                                             qualifier::equals, bean, beanFactory
                                     ))
                                     .toList();
        if (matches.size() > 1) {
            throw new AxonConfigurationException("Several " + type.getSimpleName() + " beans are qualified with ["
                                                         + qualifier + "]: " + matches + ". Qualify one of them only.");
        }
        return matches.isEmpty() ? null : matches.getFirst();
    }

    /**
     * Rejects a name used for more than one sink or source, naming the properties that use it. The connector registers
     * every sink and source as a component under its name, and processors select their source by name.
     */
    private static void rejectDuplicateNames(KafkaProperties properties) {
        Map<String, List<String>> propertiesByName = new TreeMap<>();
        properties.getSinks().keySet().forEach(
                name -> propertiesByName.computeIfAbsent(name, n -> new ArrayList<>()).add(SINKS + name));
        properties.getStreamingSources().keySet().forEach(
                name -> propertiesByName.computeIfAbsent(name, n -> new ArrayList<>()).add(STREAMING_SOURCES + name));
        properties.getSubscribingSources().keySet().forEach(
                name -> propertiesByName.computeIfAbsent(name, n -> new ArrayList<>()).add(SUBSCRIBING_SOURCES + name));
        propertiesByName.forEach((name, configured) -> {
            if (configured.size() > 1) {
                throw new AxonConfigurationException(
                        "The name [" + name + "] is used by more than one Kafka sink or source: "
                                + String.join(" and ", configured) + ". Sinks and sources are registered by name, "
                                + "and processors select their source by name, so give each a name of its own."
                );
            }
        });
    }

    private static void registerStreamingSource(KafkaConnector connector,
                                                String name,
                                                KafkaProperties.StreamingSource source) {
        TopicSubscription subscription =
                subscription(name, STREAMING_SOURCES + name, source.getTopics(), source.getTopicPattern());
        connector.streamableSource(name, subscription, (config, configuration) ->
                configuration.withUndecodableRecordPolicy(source.getUndecodableRecordPolicy())
                             .withRetentionGapPolicy(source.getRetentionGapPolicy())
                             .withPollTimeout(source.getPollTimeout())
                             .withBufferSize(source.getBufferSize())
                             .withMetadataRefreshInterval(source.getMetadataRefreshInterval())
                             .withConsumerProperties(Map.copyOf(source.getProperties()))
        );
    }

    private static void registerSubscribingSource(KafkaConnector connector,
                                                  ConfigurableListableBeanFactory beanFactory,
                                                  String name,
                                                  KafkaProperties.SubscribingSource source) {
        TopicSubscription subscription =
                subscription(name, SUBSCRIBING_SOURCES + name, source.getTopics(), source.getTopicPattern());
        String groupId = source.getGroupId();
        if (groupId == null || groupId.isBlank()) {
            throw new AxonConfigurationException("Subscribing Kafka source [" + name + "] needs a consumer group: "
                                                         + SUBSCRIBING_SOURCES + name + ".group-id.");
        }
        rejectAmbiguousOffsetReset(name, source);
        String executorBean = qualifiedBean(beanFactory, Executor.class, name);
        connector.subscribableSource(name, subscription, groupId, (config, configuration) -> {
            KafkaSubscribableEventSourceConfiguration result =
                    configuration.withConsumerCount(source.getConsumerCount())
                                 .withFailedRecordPolicy(source.getFailedRecordPolicy())
                                 .withUndecodableRecordPolicy(source.getUndecodableRecordPolicy())
                                 .withRetentionGapPolicy(source.getRetentionGapPolicy())
                                 .withInitialPosition(source.getInitialPosition())
                                 .withPollTimeout(source.getPollTimeout())
                                 .withRetryBackoff(source.getRetryInitialBackoff(), source.getRetryMaxBackoff())
                                 .withDeadLetterTopicSuffix(source.getDeadLetterTopicSuffix())
                                 .withRevocationTimeout(source.getRevocationTimeout())
                                 .withConsumerProperties(Map.copyOf(source.getProperties()))
                                 .withProducerProperties(Map.copyOf(source.getProducerProperties()));
            return executorBean == null
                    ? result
                    : result.withExecutor(beanFactory.getBean(executorBean, Executor.class));
        });
    }

    /**
     * Rejects an {@code auto.offset.reset} consumer setting next to a changed setting it replaces, as only one of them
     * can apply.
     */
    private static void rejectAmbiguousOffsetReset(String name, KafkaProperties.SubscribingSource source) {
        if (!source.getProperties().containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            return;
        }
        List<String> replaced = new ArrayList<>();
        if (source.getInitialPosition() != KafkaSubscribableEventSourceConfiguration.DEFAULT_INITIAL_POSITION) {
            replaced.add("initial-position");
        }
        if (source.getRetentionGapPolicy() != KafkaSubscribableEventSourceConfiguration.DEFAULT_RETENTION_GAP_POLICY) {
            replaced.add("retention-gap-policy");
        }
        if (!replaced.isEmpty()) {
            throw new AxonConfigurationException(
                    "Kafka source [" + name + "] sets " + SUBSCRIBING_SOURCES + name + ".properties."
                            + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + ", which replaces " + replaced + " with "
                            + "Kafka's own offset reset. Remove one or the other; prefer initial-position and "
                            + "retention-gap-policy."
            );
        }
    }

    /**
     * Returns the subscription of the source with the given {@code name}, whose properties start with the given
     * {@code prefix}, from its {@code topics} or its {@code topicPattern}, exactly one of which must be set.
     */
    private static TopicSubscription subscription(String name,
                                                  String prefix,
                                                  List<String> topics,
                                                  @Nullable String topicPattern) {
        if (topicPattern != null && !topicPattern.isBlank()) {
            if (!topics.isEmpty()) {
                throw new AxonConfigurationException("Kafka source [" + name + "] has both topics and "
                                                             + "a topic pattern: " + prefix + ".topics and " + prefix
                                                             + ".topic-pattern. Set one of them only.");
            }
            try {
                return TopicSubscription.pattern(Pattern.compile(topicPattern));
            } catch (PatternSyntaxException e) {
                throw new AxonConfigurationException("Kafka source [" + name + "] has an invalid topic "
                                                             + "pattern: " + prefix + ".topic-pattern. "
                                                             + e.getMessage(), e);
            }
        }
        if (topics.isEmpty()) {
            throw new AxonConfigurationException("Kafka source [" + name + "] needs topics or a topic "
                                                         + "pattern: " + prefix + ".topics.");
        }
        return TopicSubscription.topics(topics.toArray(String[]::new));
    }
}
