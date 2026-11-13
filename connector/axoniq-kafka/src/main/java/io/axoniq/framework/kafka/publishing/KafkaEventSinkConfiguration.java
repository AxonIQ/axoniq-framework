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

package io.axoniq.framework.kafka.publishing;

import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static io.axoniq.framework.kafka.common.KafkaConnectorUtils.requirePositive;

/**
 * The settings of a {@link KafkaEventSink}: how it encodes events, where it publishes them, and how it confirms
 * that they reached Kafka.
 * <p>
 * Start from {@link #create(KafkaEventCodec, TopicResolver)} and adjust the defaults with the {@code with} methods.
 * Each returns a copy, leaving the original unchanged. The defaults are:
 * <ul>
 *     <li>records are published {@link RecordKeyResolver#none() without a key};</li>
 *     <li>the {@link ConfirmationMode#WAIT_FOR_ACK} confirmation mode, with an acknowledgement timeout of
 *     {@link #DEFAULT_ACK_TIMEOUT};</li>
 *     <li>no producer settings besides those of the {@link io.axoniq.framework.kafka.common.KafkaClientFactory};</li>
 *     <li>no commit executor: a Kafka transaction commits on the thread that commits the processing context.</li>
 * </ul>
 * <pre>{@code
 * KafkaEventSinkConfiguration configuration =
 *         KafkaEventSinkConfiguration.create(new DefaultKafkaEventCodec(eventConverter),
 *                                            TopicResolver.fixed("parcels"))
 *                                    .withRecordKeyResolver(RecordKeyResolver.fromMetadata("parcelId"))
 *                                    .withTransactionalIdPrefix("parcel-service-1-")
 *                                    .withConfirmationMode(ConfirmationMode.TRANSACTIONAL);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class KafkaEventSinkConfiguration {

    /**
     * The default time the {@link ConfirmationMode#WAIT_FOR_ACK} mode waits for Kafka to acknowledge records: 30
     * seconds.
     */
    public static final Duration DEFAULT_ACK_TIMEOUT = Duration.ofSeconds(30);
    /**
     * The default confirmation mode: {@link ConfirmationMode#WAIT_FOR_ACK}.
     */
    public static final ConfirmationMode DEFAULT_CONFIRMATION_MODE = ConfirmationMode.WAIT_FOR_ACK;

    private final KafkaEventCodec<?> codec;
    private final TopicResolver topicResolver;
    private final RecordKeyResolver recordKeyResolver;
    private final ConfirmationMode confirmationMode;
    private final Duration ackTimeout;
    private final @Nullable String transactionalIdPrefix;
    private final Map<String, Object> producerProperties;
    private final @Nullable Executor commitExecutor;

    /**
     * The mutable form of the settings, from which every {@code with} method builds its validated copy.
     */
    private static final class Settings {

        private KafkaEventCodec<?> codec;
        private TopicResolver topicResolver;
        private RecordKeyResolver recordKeyResolver = RecordKeyResolver.none();
        private ConfirmationMode confirmationMode = DEFAULT_CONFIRMATION_MODE;
        private Duration ackTimeout = DEFAULT_ACK_TIMEOUT;
        private @Nullable String transactionalIdPrefix;
        private Map<String, Object> producerProperties = Map.of();
        private @Nullable Executor commitExecutor;

        private Settings(KafkaEventCodec<?> codec, TopicResolver topicResolver) {
            this.codec = codec;
            this.topicResolver = topicResolver;
        }

        private Settings(KafkaEventSinkConfiguration source) {
            this.codec = source.codec;
            this.topicResolver = source.topicResolver;
            this.recordKeyResolver = source.recordKeyResolver;
            this.confirmationMode = source.confirmationMode;
            this.ackTimeout = source.ackTimeout;
            this.transactionalIdPrefix = source.transactionalIdPrefix;
            this.producerProperties = source.producerProperties;
            this.commitExecutor = source.commitExecutor;
        }
    }

    private KafkaEventSinkConfiguration(Settings settings) {
        this.codec = Objects.requireNonNull(settings.codec, "The codec must not be null.");
        this.topicResolver = Objects.requireNonNull(settings.topicResolver, "The topicResolver must not be null.");
        this.recordKeyResolver = Objects.requireNonNull(settings.recordKeyResolver,
                                                        "The recordKeyResolver must not be null.");
        this.confirmationMode = Objects.requireNonNull(settings.confirmationMode,
                                                       "The confirmationMode must not be null.");
        this.ackTimeout = requirePositive(settings.ackTimeout, "ackTimeout");
        if (settings.transactionalIdPrefix != null && settings.transactionalIdPrefix.isBlank()) {
            throw new IllegalArgumentException("The transactionalIdPrefix must not be blank.");
        }
        this.transactionalIdPrefix = settings.transactionalIdPrefix;
        this.producerProperties = Map.copyOf(Objects.requireNonNull(settings.producerProperties,
                                                                    "The producerProperties must not be null."));
        if (producerProperties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
            throw new IllegalArgumentException(
                    "The producerProperties must not set " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + ": the sink sets "
                            + "the transactional identifiers of its producers itself, from the transactional "
                            + "identifier prefix of the TRANSACTIONAL confirmation mode."
            );
        }
        this.commitExecutor = settings.commitExecutor;
    }

    private KafkaEventSinkConfiguration with(Consumer<Settings> change) {
        Settings settings = new Settings(this);
        change.accept(settings);
        return new KafkaEventSinkConfiguration(settings);
    }

    /**
     * Creates a configuration that encodes events with the given {@code codec} and publishes them to the topics the
     * given {@code topicResolver} resolves, with defaults for all other settings.
     *
     * @param codec         the codec that encodes events as records
     * @param topicResolver the resolver of the topic to publish each event to
     * @return a configuration with the given {@code codec} and {@code topicResolver}, and defaults otherwise
     */
    public static KafkaEventSinkConfiguration create(KafkaEventCodec<?> codec, TopicResolver topicResolver) {
        return new KafkaEventSinkConfiguration(new Settings(codec, topicResolver));
    }

    /**
     * Returns a copy of this configuration that resolves the topic of each event with the given
     * {@code topicResolver}.
     *
     * @param topicResolver the resolver of the topic to publish each event to
     * @return a copy of this configuration with the given {@code topicResolver}
     */
    public KafkaEventSinkConfiguration withTopicResolver(TopicResolver topicResolver) {
        return with(settings -> settings.topicResolver = topicResolver);
    }

    /**
     * Returns a copy of this configuration that resolves record keys with the given {@code recordKeyResolver}.
     *
     * @param recordKeyResolver the resolver of the key of each record
     * @return a copy of this configuration with the given {@code recordKeyResolver}
     */
    public KafkaEventSinkConfiguration withRecordKeyResolver(RecordKeyResolver recordKeyResolver) {
        return with(settings -> settings.recordKeyResolver = recordKeyResolver);
    }

    /**
     * Returns a copy of this configuration with the given {@code confirmationMode}. The
     * {@link ConfirmationMode#TRANSACTIONAL} mode also requires a
     * {@link #withTransactionalIdPrefix(String) transactional identifier prefix}.
     *
     * @param confirmationMode how the sink confirms that records reached Kafka
     * @return a copy of this configuration with the given {@code confirmationMode}
     */
    public KafkaEventSinkConfiguration withConfirmationMode(ConfirmationMode confirmationMode) {
        return with(settings -> settings.confirmationMode = confirmationMode);
    }

    /**
     * Returns a copy of this configuration that waits the given {@code ackTimeout} for acknowledgements in the
     * {@link ConfirmationMode#WAIT_FOR_ACK} mode.
     *
     * @param ackTimeout how long to wait for Kafka to acknowledge records, which must be positive
     * @return a copy of this configuration with the given {@code ackTimeout}
     * @throws IllegalArgumentException if the {@code ackTimeout} is zero or negative
     */
    public KafkaEventSinkConfiguration withAckTimeout(Duration ackTimeout) {
        return with(settings -> settings.ackTimeout = ackTimeout);
    }

    /**
     * Returns a copy of this configuration whose transactional producers use identifiers starting with the given
     * {@code transactionalIdPrefix}.
     * <p>
     * The prefix must be unique to each application instance and stable across its restarts: instances that share a
     * prefix fence off each other's producers. After a restart, open transactions under identifiers the instance no
     * longer uses expire after the producer's {@code transaction.timeout.ms}.
     *
     * @param transactionalIdPrefix the prefix of the transactional identifiers of this sink's producers
     * @return a copy of this configuration with the given {@code transactionalIdPrefix}
     * @throws IllegalArgumentException if the {@code transactionalIdPrefix} is blank
     */
    public KafkaEventSinkConfiguration withTransactionalIdPrefix(String transactionalIdPrefix) {
        Objects.requireNonNull(transactionalIdPrefix, "The transactionalIdPrefix must not be null.");
        return with(settings -> settings.transactionalIdPrefix = transactionalIdPrefix);
    }

    /**
     * Returns a copy of this configuration with the given producer settings, which take precedence over those of the
     * {@link io.axoniq.framework.kafka.common.KafkaClientFactory}. They must not set {@code transactional.id}: the
     * sink sets the transactional identifiers of its producers itself, from the
     * {@link #withTransactionalIdPrefix(String) transactional identifier prefix}.
     *
     * @param producerProperties the producer settings specific to this sink, using the property names of the
     *                           {@code kafka-clients} library
     * @return a copy of this configuration with the given {@code producerProperties}
     * @throws IllegalArgumentException if the {@code producerProperties} set {@code transactional.id}
     */
    public KafkaEventSinkConfiguration withProducerProperties(Map<String, Object> producerProperties) {
        return with(settings -> settings.producerProperties = producerProperties);
    }

    /**
     * Returns a copy of this configuration that commits the Kafka transactions of the
     * {@link ConfirmationMode#TRANSACTIONAL} mode on the given {@code commitExecutor}.
     * <p>
     * By default, the commit runs on, and blocks, the thread that commits the processing context, for at most the
     * producer's {@code max.block.ms}. With a commit executor, that thread is released, and the processing context
     * completes its commit once the Kafka transaction has committed. The other confirmation modes ignore this
     * setting.
     *
     * @param commitExecutor the executor on which to commit Kafka transactions
     * @return a copy of this configuration with the given {@code commitExecutor}
     */
    public KafkaEventSinkConfiguration withCommitExecutor(Executor commitExecutor) {
        Objects.requireNonNull(commitExecutor, "The commitExecutor must not be null.");
        return with(settings -> settings.commitExecutor = commitExecutor);
    }

    /**
     * Returns the codec that encodes events as records.
     *
     * @return the codec that encodes events as records
     */
    public KafkaEventCodec<?> codec() {
        return codec;
    }

    /**
     * Returns the resolver of the topic to publish each event to.
     *
     * @return the topic resolver
     */
    public TopicResolver topicResolver() {
        return topicResolver;
    }

    /**
     * Returns the resolver of the key of each record.
     *
     * @return the record key resolver
     */
    public RecordKeyResolver recordKeyResolver() {
        return recordKeyResolver;
    }

    /**
     * Returns how the sink confirms that records reached Kafka.
     *
     * @return the confirmation mode
     */
    public ConfirmationMode confirmationMode() {
        return confirmationMode;
    }

    /**
     * Returns how long the {@link ConfirmationMode#WAIT_FOR_ACK} mode waits for acknowledgements.
     *
     * @return the acknowledgement timeout
     */
    public Duration ackTimeout() {
        return ackTimeout;
    }

    /**
     * Returns the prefix of the transactional identifiers of the producers of the
     * {@link ConfirmationMode#TRANSACTIONAL} mode.
     *
     * @return the transactional identifier prefix, or {@code null} when none is configured
     */
    public @Nullable String transactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    /**
     * Returns the producer settings specific to this sink, which take precedence over those of the client factory.
     *
     * @return the producer settings, as an unmodifiable map
     */
    public Map<String, Object> producerProperties() {
        return producerProperties;
    }

    /**
     * Returns the executor on which the {@link ConfirmationMode#TRANSACTIONAL} mode commits Kafka transactions.
     *
     * @return the commit executor, or {@code null} to commit on the thread that commits the processing context
     */
    public @Nullable Executor commitExecutor() {
        return commitExecutor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaEventSinkConfiguration that)) {
            return false;
        }
        return codec.equals(that.codec)
                && topicResolver.equals(that.topicResolver)
                && recordKeyResolver.equals(that.recordKeyResolver)
                && confirmationMode == that.confirmationMode
                && ackTimeout.equals(that.ackTimeout)
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && producerProperties.equals(that.producerProperties)
                && Objects.equals(commitExecutor, that.commitExecutor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(codec, topicResolver, recordKeyResolver, confirmationMode, ackTimeout,
                            transactionalIdPrefix, producerProperties, commitExecutor);
    }

    @Override
    public String toString() {
        return "KafkaEventSinkConfiguration{codec=" + codec
                + ", topicResolver=" + topicResolver
                + ", recordKeyResolver=" + recordKeyResolver
                + ", confirmationMode=" + confirmationMode
                + ", ackTimeout=" + ackTimeout
                + ", transactionalIdPrefix=" + transactionalIdPrefix
                + ", producerProperties=" + producerProperties.keySet()
                + ", commitExecutor=" + commitExecutor + "}";
    }
}
