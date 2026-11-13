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

package io.axoniq.framework.kafka.streaming;

import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.UndecodableRecordPolicy;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.common.TopicSubscription;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static io.axoniq.framework.kafka.common.KafkaConnectorUtils.requirePositive;

/**
 * The settings of a {@link KafkaStreamableEventSource}: which topics it reads, how it decodes records, and how it
 * reads them.
 * <p>
 * Start from {@link #create(KafkaEventCodec, TopicSubscription)} and adjust the defaults with the {@code with}
 * methods. Each returns a copy, leaving the original unchanged. The defaults are:
 * <ul>
 *     <li>records that carry no event are skipped with a warning ({@link UndecodableRecordPolicy#SKIP_AND_LOG});</li>
 *     <li>a position lost to retention fails the stream ({@link RetentionGapPolicy#FAIL});</li>
 *     <li>a buffer of {@value #DEFAULT_BUFFER_SIZE} events between the Kafka consumer and the processor;</li>
 *     <li>a poll timeout of {@link #DEFAULT_POLL_TIMEOUT};</li>
 *     <li>new partitions, and new topics matching a pattern, are discovered every
 *     {@link #DEFAULT_METADATA_REFRESH_INTERVAL};</li>
 *     <li>no consumer settings besides those of the {@link io.axoniq.framework.kafka.common.KafkaClientFactory}.</li>
 * </ul>
 * <pre>{@code
 * KafkaStreamableEventSourceConfiguration configuration =
 *         KafkaStreamableEventSourceConfiguration.create(new DefaultKafkaEventCodec(eventConverter),
 *                                                        TopicSubscription.topics("parcels"))
 *                                                .withRetentionGapPolicy(RetentionGapPolicy.RESET_TO_EARLIEST);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class KafkaStreamableEventSourceConfiguration {

    /**
     * The default number of events buffered between the Kafka consumer and the processor.
     */
    public static final int DEFAULT_BUFFER_SIZE = 1024;
    /**
     * The default poll timeout: 500 milliseconds.
     */
    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(500);
    /**
     * The default metadata refresh interval: 30 seconds.
     */
    public static final Duration DEFAULT_METADATA_REFRESH_INTERVAL = Duration.ofSeconds(30);
    /**
     * The default undecodable-record policy: {@link UndecodableRecordPolicy#SKIP_AND_LOG}.
     */
    public static final UndecodableRecordPolicy DEFAULT_UNDECODABLE_RECORD_POLICY =
            UndecodableRecordPolicy.SKIP_AND_LOG;
    /**
     * The default retention gap policy: {@link RetentionGapPolicy#FAIL}.
     */
    public static final RetentionGapPolicy DEFAULT_RETENTION_GAP_POLICY = RetentionGapPolicy.FAIL;

    private final TopicSubscription subscription;
    private final KafkaEventCodec<?> codec;
    private final UndecodableRecordPolicy undecodableRecordPolicy;
    private final RetentionGapPolicy retentionGapPolicy;
    private final int bufferSize;
    private final Duration pollTimeout;
    private final Duration metadataRefreshInterval;
    private final Map<String, Object> consumerProperties;

    /**
     * The mutable form of the settings, from which every {@code with} method builds its validated copy.
     */
    private static final class Settings {

        private TopicSubscription subscription;
        private KafkaEventCodec<?> codec;
        private UndecodableRecordPolicy undecodableRecordPolicy = DEFAULT_UNDECODABLE_RECORD_POLICY;
        private RetentionGapPolicy retentionGapPolicy = DEFAULT_RETENTION_GAP_POLICY;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
        private Duration metadataRefreshInterval = DEFAULT_METADATA_REFRESH_INTERVAL;
        private Map<String, Object> consumerProperties = Map.of();

        private Settings(KafkaEventCodec<?> codec, TopicSubscription subscription) {
            this.codec = codec;
            this.subscription = subscription;
        }

        private Settings(KafkaStreamableEventSourceConfiguration source) {
            this.subscription = source.subscription;
            this.codec = source.codec;
            this.undecodableRecordPolicy = source.undecodableRecordPolicy;
            this.retentionGapPolicy = source.retentionGapPolicy;
            this.bufferSize = source.bufferSize;
            this.pollTimeout = source.pollTimeout;
            this.metadataRefreshInterval = source.metadataRefreshInterval;
            this.consumerProperties = source.consumerProperties;
        }
    }

    private KafkaStreamableEventSourceConfiguration(Settings settings) {
        this.subscription = Objects.requireNonNull(settings.subscription, "The subscription must not be null.");
        this.codec = Objects.requireNonNull(settings.codec, "The codec must not be null.");
        this.undecodableRecordPolicy = Objects.requireNonNull(settings.undecodableRecordPolicy,
                                                              "The undecodableRecordPolicy must not be null.");
        this.retentionGapPolicy = Objects.requireNonNull(settings.retentionGapPolicy,
                                                         "The retentionGapPolicy must not be null.");
        if (settings.bufferSize < 1) {
            throw new IllegalArgumentException("The bufferSize must be at least 1.");
        }
        this.bufferSize = settings.bufferSize;
        this.pollTimeout = requirePositive(settings.pollTimeout, "pollTimeout");
        this.metadataRefreshInterval = requirePositive(settings.metadataRefreshInterval, "metadataRefreshInterval");
        this.consumerProperties = Map.copyOf(Objects.requireNonNull(settings.consumerProperties,
                                                                    "The consumerProperties must not be null."));
    }

    private KafkaStreamableEventSourceConfiguration with(Consumer<Settings> change) {
        Settings settings = new Settings(this);
        change.accept(settings);
        return new KafkaStreamableEventSourceConfiguration(settings);
    }

    /**
     * Creates a configuration that reads the topics of the given {@code subscription} and decodes them with the given
     * {@code codec}, with defaults for all other settings.
     *
     * @param codec        the codec that decodes records into events
     * @param subscription the topics to read
     * @return a configuration with the given {@code codec} and {@code subscription}, and defaults otherwise
     */
    public static KafkaStreamableEventSourceConfiguration create(KafkaEventCodec<?> codec,
                                                                 TopicSubscription subscription) {
        return new KafkaStreamableEventSourceConfiguration(new Settings(codec, subscription));
    }

    /**
     * Returns a copy of this configuration with the given {@code undecodableRecordPolicy}.
     *
     * @param undecodableRecordPolicy what to do with records that carry no event
     * @return a copy of this configuration with the given {@code undecodableRecordPolicy}
     */
    public KafkaStreamableEventSourceConfiguration withUndecodableRecordPolicy(
            UndecodableRecordPolicy undecodableRecordPolicy
    ) {
        return with(settings -> settings.undecodableRecordPolicy = undecodableRecordPolicy);
    }

    /**
     * Returns a copy of this configuration with the given {@code retentionGapPolicy}.
     *
     * @param retentionGapPolicy what to do when a position was lost to retention
     * @return a copy of this configuration with the given {@code retentionGapPolicy}
     */
    public KafkaStreamableEventSourceConfiguration withRetentionGapPolicy(RetentionGapPolicy retentionGapPolicy) {
        return with(settings -> settings.retentionGapPolicy = retentionGapPolicy);
    }

    /**
     * Returns a copy of this configuration with the given {@code bufferSize}.
     *
     * @param bufferSize the number of events buffered between the consumer and the processor, at least 1
     * @return a copy of this configuration with the given {@code bufferSize}
     * @throws IllegalArgumentException if the {@code bufferSize} is less than 1
     */
    public KafkaStreamableEventSourceConfiguration withBufferSize(int bufferSize) {
        return with(settings -> settings.bufferSize = bufferSize);
    }

    /**
     * Returns a copy of this configuration with the given {@code pollTimeout}.
     *
     * @param pollTimeout the longest time a single poll of the consumer waits for records, which must be positive
     * @return a copy of this configuration with the given {@code pollTimeout}
     * @throws IllegalArgumentException if the {@code pollTimeout} is zero or negative
     */
    public KafkaStreamableEventSourceConfiguration withPollTimeout(Duration pollTimeout) {
        return with(settings -> settings.pollTimeout = pollTimeout);
    }

    /**
     * Returns a copy of this configuration with the given {@code metadataRefreshInterval}: how often every open stream
     * looks for new partitions of its topics, and, with a topic pattern, for new matching topics. Each lookup
     * describes the named topics, or, with a topic pattern, lists all topics of the cluster.
     *
     * @param metadataRefreshInterval how often to look for new partitions and topics, which must be positive
     * @return a copy of this configuration with the given {@code metadataRefreshInterval}
     * @throws IllegalArgumentException if the {@code metadataRefreshInterval} is zero or negative
     */
    public KafkaStreamableEventSourceConfiguration withMetadataRefreshInterval(Duration metadataRefreshInterval) {
        return with(settings -> settings.metadataRefreshInterval = metadataRefreshInterval);
    }

    /**
     * Returns a copy of this configuration with the given consumer settings, which take precedence over those of the
     * {@link io.axoniq.framework.kafka.common.KafkaClientFactory}. Settings the source relies on cannot be overridden:
     * a {@code group.id}, an {@code enable.auto.commit} other than {@code false} or an {@code auto.offset.reset} other
     * than {@code none} given here is not used, and the source logs a warning.
     *
     * @param consumerProperties the consumer settings specific to this source, using the property names of the
     *                           {@code kafka-clients} library
     * @return a copy of this configuration with the given {@code consumerProperties}
     */
    public KafkaStreamableEventSourceConfiguration withConsumerProperties(Map<String, Object> consumerProperties) {
        return with(settings -> settings.consumerProperties = consumerProperties);
    }

    /**
     * Returns the topics to read.
     *
     * @return the topics to read
     */
    public TopicSubscription subscription() {
        return subscription;
    }

    /**
     * Returns the codec that decodes records into events.
     *
     * @return the codec that decodes records into events
     */
    public KafkaEventCodec<?> codec() {
        return codec;
    }

    /**
     * Returns what to do with records that carry no event.
     *
     * @return the undecodable-record policy
     */
    public UndecodableRecordPolicy undecodableRecordPolicy() {
        return undecodableRecordPolicy;
    }

    /**
     * Returns what to do when a position was lost to retention.
     *
     * @return the retention gap policy
     */
    public RetentionGapPolicy retentionGapPolicy() {
        return retentionGapPolicy;
    }

    /**
     * Returns the number of events buffered between the consumer and the processor.
     *
     * @return the buffer size
     */
    public int bufferSize() {
        return bufferSize;
    }

    /**
     * Returns the longest time a single poll of the consumer waits for records.
     *
     * @return the poll timeout
     */
    public Duration pollTimeout() {
        return pollTimeout;
    }

    /**
     * Returns how often to look for new partitions and topics.
     *
     * @return the metadata refresh interval
     */
    public Duration metadataRefreshInterval() {
        return metadataRefreshInterval;
    }

    /**
     * Returns the consumer settings specific to this source, which take precedence over those of the client factory.
     *
     * @return the consumer settings, as an unmodifiable map
     */
    public Map<String, Object> consumerProperties() {
        return consumerProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaStreamableEventSourceConfiguration that)) {
            return false;
        }
        return bufferSize == that.bufferSize
                && subscription.equals(that.subscription)
                && codec.equals(that.codec)
                && undecodableRecordPolicy.equals(that.undecodableRecordPolicy)
                && retentionGapPolicy == that.retentionGapPolicy
                && pollTimeout.equals(that.pollTimeout)
                && metadataRefreshInterval.equals(that.metadataRefreshInterval)
                && consumerProperties.equals(that.consumerProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscription, codec, undecodableRecordPolicy, retentionGapPolicy, bufferSize, pollTimeout,
                            metadataRefreshInterval, consumerProperties);
    }

    @Override
    public String toString() {
        return "KafkaStreamableEventSourceConfiguration{subscription=" + subscription
                + ", codec=" + codec
                + ", undecodableRecordPolicy=" + undecodableRecordPolicy
                + ", retentionGapPolicy=" + retentionGapPolicy
                + ", bufferSize=" + bufferSize
                + ", pollTimeout=" + pollTimeout
                + ", metadataRefreshInterval=" + metadataRefreshInterval
                + ", consumerProperties=" + consumerProperties.keySet() + "}";
    }
}
