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

package io.axoniq.framework.kafka.subscribing;

import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.UndecodableRecordPolicy;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.common.TopicSubscription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static io.axoniq.framework.kafka.common.KafkaConnectorUtils.requirePositive;

/**
 * The settings of a {@link KafkaSubscribableEventSource}: which topics it consumes in which consumer group, how it
 * decodes records, and how it deals with failures.
 * <p>
 * Start from {@link #create(KafkaEventCodec, TopicSubscription, String)} and adjust the defaults with the
 * {@code with} methods. Each returns a copy, leaving the original unchanged. The defaults are:
 * <ul>
 *     <li>one consumer per application instance;</li>
 *     <li>failed records are retried ({@link FailedRecordPolicy#RETRY}), with a delay growing from
 *     {@link #DEFAULT_RETRY_INITIAL_BACKOFF} to {@link #DEFAULT_RETRY_MAX_BACKOFF};</li>
 *     <li>records that carry no event are skipped with a warning ({@link UndecodableRecordPolicy#SKIP_AND_LOG});</li>
 *     <li>dead-letter topics are named after the original topic with the suffix
 *     {@value #DEFAULT_DEAD_LETTER_TOPIC_SUFFIX};</li>
 *     <li>a poll timeout of {@link #DEFAULT_POLL_TIMEOUT}, and a revocation timeout of
 *     {@link #DEFAULT_REVOCATION_TIMEOUT};</li>
 *     <li>a new consumer group starts at the earliest available records ({@link InitialPosition#EARLIEST});</li>
 *     <li>a committed offset lost to retention stops consumption of its partition
 *     ({@link RetentionGapPolicy#FAIL});</li>
 *     <li>events are processed on virtual threads, one per task, which the source creates and stops itself;</li>
 *     <li>no client settings besides those of the {@link io.axoniq.framework.kafka.common.KafkaClientFactory}.</li>
 * </ul>
 * <pre>{@code
 * KafkaSubscribableEventSourceConfiguration configuration =
 *         KafkaSubscribableEventSourceConfiguration.create(new DefaultKafkaEventCodec(eventConverter),
 *                                                          TopicSubscription.topics("parcels"),
 *                                                          "billing")
 *                                                  .withConsumerCount(4)
 *                                                  .withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class KafkaSubscribableEventSourceConfiguration {

    /**
     * The default delay before the first retry of a failed record: 100 milliseconds.
     */
    public static final Duration DEFAULT_RETRY_INITIAL_BACKOFF = Duration.ofMillis(100);
    /**
     * The default longest delay between retries of a failed record: 30 seconds.
     */
    public static final Duration DEFAULT_RETRY_MAX_BACKOFF = Duration.ofSeconds(30);
    /**
     * The default suffix of dead-letter topic names.
     */
    public static final String DEFAULT_DEAD_LETTER_TOPIC_SUFFIX = ".DLT";
    /**
     * The default poll timeout: 100 milliseconds.
     */
    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
    /**
     * The default revocation timeout: 10 seconds.
     */
    public static final Duration DEFAULT_REVOCATION_TIMEOUT = Duration.ofSeconds(10);
    /**
     * The default number of consumers each application instance runs in the consumer group: 1.
     */
    public static final int DEFAULT_CONSUMER_COUNT = 1;
    /**
     * The default failed-record policy: {@link FailedRecordPolicy#RETRY}.
     */
    public static final FailedRecordPolicy DEFAULT_FAILED_RECORD_POLICY = FailedRecordPolicy.RETRY;
    /**
     * The default undecodable-record policy: {@link UndecodableRecordPolicy#SKIP_AND_LOG}.
     */
    public static final UndecodableRecordPolicy DEFAULT_UNDECODABLE_RECORD_POLICY =
            UndecodableRecordPolicy.SKIP_AND_LOG;
    /**
     * The default retention gap policy: {@link RetentionGapPolicy#FAIL}.
     */
    public static final RetentionGapPolicy DEFAULT_RETENTION_GAP_POLICY = RetentionGapPolicy.FAIL;
    /**
     * The default initial position: {@link InitialPosition#EARLIEST}.
     */
    public static final InitialPosition DEFAULT_INITIAL_POSITION = InitialPosition.EARLIEST;

    private final TopicSubscription subscription;
    private final String groupId;
    private final KafkaEventCodec<?> codec;
    private final int consumerCount;
    private final FailedRecordPolicy failedRecordPolicy;
    private final Duration retryInitialBackoff;
    private final Duration retryMaxBackoff;
    private final UndecodableRecordPolicy undecodableRecordPolicy;
    private final String deadLetterTopicSuffix;
    private final Duration pollTimeout;
    private final Duration revocationTimeout;
    private final Map<String, Object> consumerProperties;
    private final Map<String, Object> producerProperties;
    private final RetentionGapPolicy retentionGapPolicy;
    private final InitialPosition initialPosition;
    private final @Nullable Executor executor;

    /**
     * The mutable form of the settings, from which every {@code with} method builds its validated copy.
     */
    private static final class Settings {

        private TopicSubscription subscription;
        private String groupId;
        private KafkaEventCodec<?> codec;
        private int consumerCount = DEFAULT_CONSUMER_COUNT;
        private FailedRecordPolicy failedRecordPolicy = DEFAULT_FAILED_RECORD_POLICY;
        private Duration retryInitialBackoff = DEFAULT_RETRY_INITIAL_BACKOFF;
        private Duration retryMaxBackoff = DEFAULT_RETRY_MAX_BACKOFF;
        private UndecodableRecordPolicy undecodableRecordPolicy = DEFAULT_UNDECODABLE_RECORD_POLICY;
        private String deadLetterTopicSuffix = DEFAULT_DEAD_LETTER_TOPIC_SUFFIX;
        private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;
        private Duration revocationTimeout = DEFAULT_REVOCATION_TIMEOUT;
        private Map<String, Object> consumerProperties = Map.of();
        private Map<String, Object> producerProperties = Map.of();
        private RetentionGapPolicy retentionGapPolicy = DEFAULT_RETENTION_GAP_POLICY;
        private InitialPosition initialPosition = DEFAULT_INITIAL_POSITION;
        private @Nullable Executor executor;

        private Settings(KafkaEventCodec<?> codec, TopicSubscription subscription, String groupId) {
            this.codec = codec;
            this.subscription = subscription;
            this.groupId = groupId;
        }

        private Settings(KafkaSubscribableEventSourceConfiguration source) {
            this.subscription = source.subscription;
            this.groupId = source.groupId;
            this.codec = source.codec;
            this.consumerCount = source.consumerCount;
            this.failedRecordPolicy = source.failedRecordPolicy;
            this.retryInitialBackoff = source.retryInitialBackoff;
            this.retryMaxBackoff = source.retryMaxBackoff;
            this.undecodableRecordPolicy = source.undecodableRecordPolicy;
            this.deadLetterTopicSuffix = source.deadLetterTopicSuffix;
            this.pollTimeout = source.pollTimeout;
            this.revocationTimeout = source.revocationTimeout;
            this.consumerProperties = source.consumerProperties;
            this.producerProperties = source.producerProperties;
            this.retentionGapPolicy = source.retentionGapPolicy;
            this.initialPosition = source.initialPosition;
            this.executor = source.executor;
        }
    }

    private KafkaSubscribableEventSourceConfiguration(Settings settings) {
        this.subscription = Objects.requireNonNull(settings.subscription, "The subscription must not be null.");
        this.groupId = Objects.requireNonNull(settings.groupId, "The groupId must not be null.");
        if (groupId.isBlank()) {
            throw new IllegalArgumentException("The groupId must not be blank.");
        }
        this.codec = Objects.requireNonNull(settings.codec, "The codec must not be null.");
        if (settings.consumerCount < 1) {
            throw new IllegalArgumentException("The consumerCount must be at least 1.");
        }
        this.consumerCount = settings.consumerCount;
        this.failedRecordPolicy = Objects.requireNonNull(settings.failedRecordPolicy,
                                                         "The failedRecordPolicy must not be null.");
        this.retryInitialBackoff = requirePositive(settings.retryInitialBackoff, "retryInitialBackoff");
        this.retryMaxBackoff = requirePositive(settings.retryMaxBackoff, "retryMaxBackoff");
        if (retryInitialBackoff.compareTo(retryMaxBackoff) > 0) {
            throw new IllegalArgumentException(
                    "The retryInitialBackoff (" + retryInitialBackoff + ") must not exceed the retryMaxBackoff ("
                            + retryMaxBackoff + ")."
            );
        }
        this.undecodableRecordPolicy = Objects.requireNonNull(settings.undecodableRecordPolicy,
                                                              "The undecodableRecordPolicy must not be null.");
        this.deadLetterTopicSuffix = Objects.requireNonNull(settings.deadLetterTopicSuffix,
                                                            "The deadLetterTopicSuffix must not be null.");
        if (deadLetterTopicSuffix.isEmpty()) {
            throw new IllegalArgumentException("The deadLetterTopicSuffix must not be empty.");
        }
        this.pollTimeout = requirePositive(settings.pollTimeout, "pollTimeout");
        this.revocationTimeout = requirePositive(settings.revocationTimeout, "revocationTimeout");
        this.consumerProperties = Map.copyOf(Objects.requireNonNull(settings.consumerProperties,
                                                                    "The consumerProperties must not be null."));
        this.producerProperties = Map.copyOf(Objects.requireNonNull(settings.producerProperties,
                                                                    "The producerProperties must not be null."));
        if (producerProperties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
            throw new IllegalArgumentException(
                    "The producerProperties must not set " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + ": the producer "
                            + "that writes to dead-letter topics is not transactional."
            );
        }
        this.retentionGapPolicy = Objects.requireNonNull(settings.retentionGapPolicy,
                                                         "The retentionGapPolicy must not be null.");
        this.initialPosition = Objects.requireNonNull(settings.initialPosition,
                                                      "The initialPosition must not be null.");
        this.executor = settings.executor;
    }

    private KafkaSubscribableEventSourceConfiguration with(Consumer<Settings> change) {
        Settings settings = new Settings(this);
        change.accept(settings);
        return new KafkaSubscribableEventSourceConfiguration(settings);
    }

    /**
     * Creates a configuration that consumes the topics of the given {@code subscription} in the consumer group with
     * the given {@code groupId}, decoding records with the given {@code codec}, with defaults for all other settings.
     *
     * @param codec        the codec that decodes records into events
     * @param subscription the topics to consume
     * @param groupId      the consumer group to join
     * @return a configuration with the given settings, and defaults otherwise
     * @throws IllegalArgumentException if the {@code groupId} is blank
     */
    public static KafkaSubscribableEventSourceConfiguration create(KafkaEventCodec<?> codec,
                                                                   TopicSubscription subscription,
                                                                   String groupId) {
        return new KafkaSubscribableEventSourceConfiguration(new Settings(codec, subscription, groupId));
    }

    /**
     * Returns a copy of this configuration that runs the given number of consumers in each application instance. The
     * consumer group spreads the partitions over all consumers of all instances, so more consumers than partitions
     * leaves consumers idle.
     *
     * @param consumerCount the number of consumers per application instance, at least 1
     * @return a copy of this configuration with the given {@code consumerCount}
     * @throws IllegalArgumentException if the {@code consumerCount} is less than 1
     */
    public KafkaSubscribableEventSourceConfiguration withConsumerCount(int consumerCount) {
        return with(settings -> settings.consumerCount = consumerCount);
    }

    /**
     * Returns a copy of this configuration with the given {@code failedRecordPolicy}.
     *
     * @param failedRecordPolicy what to do with records whose event fails to be processed
     * @return a copy of this configuration with the given {@code failedRecordPolicy}
     */
    public KafkaSubscribableEventSourceConfiguration withFailedRecordPolicy(FailedRecordPolicy failedRecordPolicy) {
        return with(settings -> settings.failedRecordPolicy = failedRecordPolicy);
    }

    /**
     * Returns a copy of this configuration with the given retry delays.
     *
     * @param initialBackoff the delay before the first retry, which must be positive and at most the
     *                       {@code maxBackoff}
     * @param maxBackoff     the longest delay between retries, which must be positive
     * @return a copy of this configuration with the given retry delays
     * @throws IllegalArgumentException if a delay is zero or negative, or the {@code initialBackoff} exceeds the
     *                                  {@code maxBackoff}
     */
    public KafkaSubscribableEventSourceConfiguration withRetryBackoff(Duration initialBackoff, Duration maxBackoff) {
        return with(settings -> {
            settings.retryInitialBackoff = initialBackoff;
            settings.retryMaxBackoff = maxBackoff;
        });
    }

    /**
     * Returns a copy of this configuration with the given {@code undecodableRecordPolicy}. See
     * {@link UndecodableRecordPolicy#FAIL} for how a rejected record stops its partition.
     *
     * @param undecodableRecordPolicy what to do with records that carry no event
     * @return a copy of this configuration with the given {@code undecodableRecordPolicy}
     */
    public KafkaSubscribableEventSourceConfiguration withUndecodableRecordPolicy(
            UndecodableRecordPolicy undecodableRecordPolicy
    ) {
        return with(settings -> settings.undecodableRecordPolicy = undecodableRecordPolicy);
    }

    /**
     * Returns a copy of this configuration with the given {@code retentionGapPolicy}: what to do when the consumer
     * group's committed offset in a partition no longer exists, because Kafka has already deleted the records at that
     * offset under the topic's retention settings.
     * <p>
     * Applies only while {@code auto.offset.reset} is {@code none}, the source's default; see
     * {@link #withConsumerProperties(Map)}.
     *
     * @param retentionGapPolicy what to do when a committed offset was lost to retention
     * @return a copy of this configuration with the given {@code retentionGapPolicy}
     */
    public KafkaSubscribableEventSourceConfiguration withRetentionGapPolicy(RetentionGapPolicy retentionGapPolicy) {
        return with(settings -> settings.retentionGapPolicy = retentionGapPolicy);
    }

    /**
     * Returns a copy of this configuration with the given {@code initialPosition}: where the consumer group starts
     * consuming a partition for which it has no committed offset. That is every partition of a new consumer group, and
     * partitions that are added to a topic later. Partitions with a committed offset continue from there, whatever the
     * initial position.
     * <p>
     * The initial position is independent of the {@link #withRetentionGapPolicy(RetentionGapPolicy) retention gap
     * policy}. Applies only while {@code auto.offset.reset} is {@code none}, the source's default; see
     * {@link #withConsumerProperties(Map)}.
     *
     * @param initialPosition where to start consuming a partition without a committed offset
     * @return a copy of this configuration with the given {@code initialPosition}
     */
    public KafkaSubscribableEventSourceConfiguration withInitialPosition(InitialPosition initialPosition) {
        return with(settings -> settings.initialPosition = initialPosition);
    }

    /**
     * Returns a copy of this configuration with the given {@code deadLetterTopicSuffix}.
     *
     * @param deadLetterTopicSuffix the suffix appended to a topic's name to name its dead-letter topic
     * @return a copy of this configuration with the given {@code deadLetterTopicSuffix}
     * @throws IllegalArgumentException if the {@code deadLetterTopicSuffix} is empty
     */
    public KafkaSubscribableEventSourceConfiguration withDeadLetterTopicSuffix(String deadLetterTopicSuffix) {
        return with(settings -> settings.deadLetterTopicSuffix = deadLetterTopicSuffix);
    }

    /**
     * Returns a copy of this configuration with the given {@code pollTimeout}.
     *
     * @param pollTimeout the longest time a single poll waits for records, which must be positive
     * @return a copy of this configuration with the given {@code pollTimeout}
     * @throws IllegalArgumentException if the {@code pollTimeout} is zero or negative
     */
    public KafkaSubscribableEventSourceConfiguration withPollTimeout(Duration pollTimeout) {
        return with(settings -> settings.pollTimeout = pollTimeout);
    }

    /**
     * Returns a copy of this configuration with the given {@code revocationTimeout}: how long a consumer waits for the
     * events in progress of a partition that Kafka assigns to another consumer. Events that do not finish in time are
     * processed again by the new owner of the partition.
     * <p>
     * The consumer waits inside its {@code poll}, so the revocation timeout, plus the time to commit the offsets,
     * must stay well below the consumer's {@code max.poll.interval.ms} (5 minutes by default).
     * <p>
     * The source also gives events in progress the revocation timeout when it stops; see
     * {@link KafkaSubscribableEventSource#shutdown()} for how long stopping takes.
     *
     * @param revocationTimeout how long to wait for events in progress, which must be positive
     * @return a copy of this configuration with the given {@code revocationTimeout}
     * @throws IllegalArgumentException if the {@code revocationTimeout} is zero or negative
     */
    public KafkaSubscribableEventSourceConfiguration withRevocationTimeout(Duration revocationTimeout) {
        return with(settings -> settings.revocationTimeout = revocationTimeout);
    }

    /**
     * Returns a copy of this configuration with the given consumer settings, which take precedence over those of the
     * {@link io.axoniq.framework.kafka.common.KafkaClientFactory}. The group identifier and disabling automatic offset
     * commits cannot be overridden: a {@code group.id} or {@code enable.auto.commit} given here is not used, and the
     * source logs a warning.
     * <p>
     * A {@code group.instance.id} enables static membership. Every consumer needs an identifier of its own, so with
     * more than one {@link #withConsumerCount(int) consumer}, the source suffixes it with each consumer's index:
     * {@code <group.instance.id>-0}, {@code <group.instance.id>-1}, and so on.
     * <p>
     * An {@code isolation.level} given here replaces the source's default of {@code read_committed}.
     * <p>
     * Setting {@code auto.offset.reset} to another value than {@code none} replaces the
     * {@link #withRetentionGapPolicy(RetentionGapPolicy) retention gap policy} and the
     * {@link #withInitialPosition(InitialPosition) initial position} with Kafka's own reset behavior.
     * With {@code latest}, a retention gap then silently skips to the end of the partition.
     *
     * @param consumerProperties the consumer settings specific to this source, using the property names of the
     *                           {@code kafka-clients} library
     * @return a copy of this configuration with the given {@code consumerProperties}
     */
    public KafkaSubscribableEventSourceConfiguration withConsumerProperties(Map<String, Object> consumerProperties) {
        return with(settings -> settings.consumerProperties = consumerProperties);
    }

    /**
     * Returns a copy of this configuration with the given settings for the producer that writes to dead-letter
     * topics, which take precedence over those of the {@link io.axoniq.framework.kafka.common.KafkaClientFactory}.
     * <p>
     * Dead-letter records are written from the consumer's polling thread, one record between two polls. Writing one
     * blocks that thread for at most the producer's {@code max.block.ms}, which is 10 seconds unless set here, plus 20
     * seconds for the acknowledgement. Together with the {@link #withRevocationTimeout(Duration) revocation timeout},
     * this must stay well below the consumer's {@code max.poll.interval.ms} (5 minutes by default).
     * <p>
     * A {@code max.block.ms} above the revocation timeout plus 10 seconds extends how long stopping may take; see
     * {@link KafkaSubscribableEventSource#shutdown()}. The settings must not set {@code transactional.id}.
     *
     * @param producerProperties the producer settings, using the property names of the {@code kafka-clients} library
     * @return a copy of this configuration with the given {@code producerProperties}
     * @throws IllegalArgumentException if the {@code producerProperties} set {@code transactional.id}
     */
    public KafkaSubscribableEventSourceConfiguration withProducerProperties(Map<String, Object> producerProperties) {
        return with(settings -> settings.producerProperties = producerProperties);
    }

    /**
     * Returns a copy of this configuration that processes events on the given {@code executor}, instead of on virtual
     * threads that the source creates.
     * <p>
     * The source hands the records of each partition to the executor as a chain of tasks, one record after the other,
     * and the records of different partitions concurrently. An executor with fewer threads than the partitions a
     * consumer processes at once makes partitions wait for each other. A record whose task the executor rejects is
     * retried after the retry backoff, without applying the {@link FailedRecordPolicy}: the offsets of the records
     * before it are committed, and processing continues at that record. The executor must run every task it is given,
     * or reject it by throwing a {@link java.util.concurrent.RejectedExecutionException}: an executor that discards
     * tasks silently, such as a {@link java.util.concurrent.ThreadPoolExecutor} with a
     * {@link java.util.concurrent.ThreadPoolExecutor.DiscardPolicy DiscardPolicy} or
     * {@link java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy DiscardOldestPolicy}, leaves the record's
     * partition paused, without reporting it as stopped, until Kafka assigns the partition anew. The polling threads
     * never run on the executor.
     * <p>
     * The application owns the executor: the source does not shut it down when it stops, so shut it down after the
     * source has stopped.
     *
     * @param executor the executor that processes events
     * @return a copy of this configuration with the given {@code executor}
     */
    public KafkaSubscribableEventSourceConfiguration withExecutor(Executor executor) {
        Objects.requireNonNull(executor, "The executor must not be null.");
        return with(settings -> settings.executor = executor);
    }

    /**
     * Returns the topics to consume.
     *
     * @return the topics to consume
     */
    public TopicSubscription subscription() {
        return subscription;
    }

    /**
     * Returns the consumer group to join.
     *
     * @return the identifier of the consumer group
     */
    public String groupId() {
        return groupId;
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
     * Returns the number of consumers each application instance runs in the group.
     *
     * @return the number of consumers per application instance
     */
    public int consumerCount() {
        return consumerCount;
    }

    /**
     * Returns what to do with records whose event fails to be processed.
     *
     * @return the failed-record policy
     */
    public FailedRecordPolicy failedRecordPolicy() {
        return failedRecordPolicy;
    }

    /**
     * Returns the delay before the first retry of a failed record.
     *
     * @return the delay before the first retry
     */
    public Duration retryInitialBackoff() {
        return retryInitialBackoff;
    }

    /**
     * Returns the longest delay between retries of a failed record.
     *
     * @return the longest delay between retries
     */
    public Duration retryMaxBackoff() {
        return retryMaxBackoff;
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
     * Returns the suffix appended to a topic's name to name its dead-letter topic.
     *
     * @return the dead-letter topic suffix
     */
    public String deadLetterTopicSuffix() {
        return deadLetterTopicSuffix;
    }

    /**
     * Returns the longest time a single poll of a consumer waits for records.
     *
     * @return the poll timeout
     */
    public Duration pollTimeout() {
        return pollTimeout;
    }

    /**
     * Returns how long to wait for events in progress when Kafka takes partitions away.
     *
     * @return the revocation timeout
     */
    public Duration revocationTimeout() {
        return revocationTimeout;
    }

    /**
     * Returns the consumer settings specific to this source.
     *
     * @return the consumer settings, as an unmodifiable map
     */
    public Map<String, Object> consumerProperties() {
        return consumerProperties;
    }

    /**
     * Returns the settings of the producer that writes to dead-letter topics.
     *
     * @return the producer settings, as an unmodifiable map
     */
    public Map<String, Object> producerProperties() {
        return producerProperties;
    }

    /**
     * Returns what to do when a committed offset was lost to retention.
     *
     * @return the retention gap policy
     */
    public RetentionGapPolicy retentionGapPolicy() {
        return retentionGapPolicy;
    }

    /**
     * Returns where the consumer group starts consuming a partition for which it has no committed offset.
     *
     * @return the initial position
     */
    public InitialPosition initialPosition() {
        return initialPosition;
    }

    /**
     * Returns the executor that processes events, when the application provides one.
     *
     * @return the executor that processes events, or an empty {@link Optional} when the source processes events on
     * virtual threads it creates itself
     */
    public Optional<Executor> executor() {
        return Optional.ofNullable(executor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaSubscribableEventSourceConfiguration that)) {
            return false;
        }
        return consumerCount == that.consumerCount
                && subscription.equals(that.subscription)
                && groupId.equals(that.groupId)
                && codec.equals(that.codec)
                && failedRecordPolicy == that.failedRecordPolicy
                && retryInitialBackoff.equals(that.retryInitialBackoff)
                && retryMaxBackoff.equals(that.retryMaxBackoff)
                && undecodableRecordPolicy.equals(that.undecodableRecordPolicy)
                && deadLetterTopicSuffix.equals(that.deadLetterTopicSuffix)
                && pollTimeout.equals(that.pollTimeout)
                && revocationTimeout.equals(that.revocationTimeout)
                && consumerProperties.equals(that.consumerProperties)
                && producerProperties.equals(that.producerProperties)
                && retentionGapPolicy == that.retentionGapPolicy
                && initialPosition == that.initialPosition
                && Objects.equals(executor, that.executor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscription, groupId, codec, consumerCount, failedRecordPolicy, retryInitialBackoff,
                            retryMaxBackoff, undecodableRecordPolicy, deadLetterTopicSuffix, pollTimeout,
                            revocationTimeout, consumerProperties, producerProperties, retentionGapPolicy,
                            initialPosition, executor);
    }

    @Override
    public String toString() {
        return "KafkaSubscribableEventSourceConfiguration{subscription=" + subscription
                + ", groupId=" + groupId
                + ", codec=" + codec
                + ", consumerCount=" + consumerCount
                + ", failedRecordPolicy=" + failedRecordPolicy
                + ", retryInitialBackoff=" + retryInitialBackoff
                + ", retryMaxBackoff=" + retryMaxBackoff
                + ", undecodableRecordPolicy=" + undecodableRecordPolicy
                + ", deadLetterTopicSuffix=" + deadLetterTopicSuffix
                + ", pollTimeout=" + pollTimeout
                + ", revocationTimeout=" + revocationTimeout
                + ", consumerProperties=" + consumerProperties.keySet()
                + ", producerProperties=" + producerProperties.keySet()
                + ", retentionGapPolicy=" + retentionGapPolicy
                + ", initialPosition=" + initialPosition
                + ", executor=" + executor + "}";
    }
}
