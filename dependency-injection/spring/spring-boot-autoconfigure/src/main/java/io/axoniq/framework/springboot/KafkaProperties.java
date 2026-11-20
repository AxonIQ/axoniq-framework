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

package io.axoniq.framework.springboot;

import io.axoniq.framework.kafka.codec.UndecodableRecordPolicy;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.publishing.ConfirmationMode;
import io.axoniq.framework.kafka.publishing.KafkaEventSinkConfiguration;
import io.axoniq.framework.kafka.streaming.KafkaStreamableEventSourceConfiguration;
import io.axoniq.framework.kafka.subscribing.FailedRecordPolicy;
import io.axoniq.framework.kafka.subscribing.InitialPosition;
import io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSourceConfiguration;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Properties describing the connection between an application and Kafka, under the {@code axon.kafka} prefix.
 * <p>
 * Every entry under {@code axon.kafka.sinks} becomes a {@link io.axoniq.framework.kafka.publishing.KafkaEventSink},
 * registered as an {@link org.axonframework.messaging.eventhandling.EventSink} under the sink's name, so that
 * components inject it with {@code @Qualifier("<sink>")}. Every entry under {@code axon.kafka.streaming-sources}
 * becomes a named source for pooled streaming event processors, and every entry under
 * {@code axon.kafka.subscribing-sources} a named source for subscribing event processors, consuming in a consumer
 * group. Processors select a source with {@code axon.eventhandling.processors.<processor>.source=<source>}. Names are
 * unique across all sinks and sources. Without sinks, nothing is published to Kafka.
 * <pre>
 * axon.kafka.bootstrap-servers=localhost:9092
 * axon.kafka.sinks.outbound.topic=parcels
 * axon.kafka.sinks.outbound.confirmation-mode=transactional
 * axon.kafka.sinks.outbound.transactional-id-prefix=parcel-service-1-
 * axon.kafka.streaming-sources.parcel-stream.topics=parcels
 * axon.kafka.subscribing-sources.billing.topics=parcels
 * axon.kafka.subscribing-sources.billing.group-id=billing
 * axon.eventhandling.processors.tracking.source=parcel-stream
 * </pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
@ConfigurationProperties("axon.kafka")
public class KafkaProperties {

    /**
     * The bootstrap servers used when none are configured.
     */
    public static final List<String> DEFAULT_BOOTSTRAP_SERVERS = List.of("localhost:9092");

    /**
     * Whether the Kafka connector is enabled. Defaults to {@code true}.
     */
    private boolean enabled = true;

    /**
     * The Kafka brokers to connect to initially. Defaults to {@code localhost:9092}.
     */
    private List<String> bootstrapServers = new ArrayList<>(DEFAULT_BOOTSTRAP_SERVERS);

    /**
     * The client identifier reported to the brokers. Optional. Every client of a sink or source reports it with a
     * suffix of its own: {@code <client-id>-<sink or source name>-<client>}, where {@code <client>} names a long-lived
     * client, such as the index of a subscribable source's consumer, or counts the other clients.
     */
    private @Nullable String clientId;

    /**
     * Further settings for all Kafka clients, using the property names of the {@code kafka-clients} library, such as
     * {@code security.protocol} or {@code ssl.truststore.location}.
     */
    private Map<String, String> properties = new HashMap<>();

    /**
     * The record format: {@code default} for the Axoniq Framework format only, or {@code legacy} to also read and
     * write the headers of Axon Framework 4's Kafka extension. Defaults to {@code default}.
     */
    private Codec codec = Codec.DEFAULT;

    /**
     * The sinks that publish events to Kafka, by name.
     */
    private Map<String, Sink> sinks = new LinkedHashMap<>();

    /**
     * The sources that pooled streaming event processors read from Kafka, by name.
     */
    private Map<String, StreamingSource> streamingSources = new LinkedHashMap<>();

    /**
     * The sources that subscribing event processors consume from Kafka in a consumer group, by name.
     */
    private Map<String, SubscribingSource> subscribingSources = new LinkedHashMap<>();

    /**
     * Returns whether the Kafka connector is enabled.
     *
     * @return whether the Kafka connector is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether the Kafka connector is enabled.
     *
     * @param enabled whether the Kafka connector is enabled
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the Kafka brokers to connect to initially.
     *
     * @return the bootstrap servers
     */
    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Sets the Kafka brokers to connect to initially.
     *
     * @param bootstrapServers the bootstrap servers
     */
    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Returns the client identifier reported to the brokers.
     *
     * @return the client identifier, or {@code null} when not set
     */
    public @Nullable String getClientId() {
        return clientId;
    }

    /**
     * Sets the client identifier reported to the brokers.
     *
     * @param clientId the client identifier
     */
    public void setClientId(@Nullable String clientId) {
        this.clientId = clientId;
    }

    /**
     * Returns the further settings for all Kafka clients.
     *
     * @return the further client settings
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Sets the further settings for all Kafka clients.
     *
     * @param properties the further client settings
     */
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Returns the record format.
     *
     * @return the record format
     */
    public Codec getCodec() {
        return codec;
    }

    /**
     * Sets the record format.
     *
     * @param codec the record format
     */
    public void setCodec(Codec codec) {
        this.codec = codec;
    }

    /**
     * Returns the sinks, by name.
     *
     * @return the sinks, by name
     */
    public Map<String, Sink> getSinks() {
        return sinks;
    }

    /**
     * Sets the sinks, by name.
     *
     * @param sinks the sinks, by name
     */
    public void setSinks(Map<String, Sink> sinks) {
        this.sinks = sinks;
    }

    /**
     * Returns the sources for pooled streaming event processors, by name.
     *
     * @return the streaming sources, by name
     */
    public Map<String, StreamingSource> getStreamingSources() {
        return streamingSources;
    }

    /**
     * Sets the sources for pooled streaming event processors, by name.
     *
     * @param streamingSources the streaming sources, by name
     */
    public void setStreamingSources(Map<String, StreamingSource> streamingSources) {
        this.streamingSources = streamingSources;
    }

    /**
     * Returns the sources for subscribing event processors, by name.
     *
     * @return the subscribing sources, by name
     */
    public Map<String, SubscribingSource> getSubscribingSources() {
        return subscribingSources;
    }

    /**
     * Sets the sources for subscribing event processors, by name.
     *
     * @param subscribingSources the subscribing sources, by name
     */
    public void setSubscribingSources(Map<String, SubscribingSource> subscribingSources) {
        this.subscribingSources = subscribingSources;
    }

    /**
     * The format of the records the connector writes and reads.
     */
    public enum Codec {

        /**
         * The Axoniq Framework format.
         */
        DEFAULT,

        /**
         * The Axoniq Framework format, plus the headers of Axon Framework 4's Kafka extension.
         */
        LEGACY
    }

    /**
     * The settings of a sink that publishes events to Kafka.
     */
    public static class Sink {

        /**
         * The topic to publish events to. Required.
         */
        private @Nullable String topic;

        /**
         * The metadata entry that holds the record key. Without it, records are published without a key.
         */
        private @Nullable String recordKeyMetadata;

        /**
         * How the sink confirms that events reached Kafka. Defaults to {@code wait-for-ack}.
         */
        private ConfirmationMode confirmationMode = KafkaEventSinkConfiguration.DEFAULT_CONFIRMATION_MODE;

        /**
         * The prefix of the transactional identifiers, required for the {@code transactional} confirmation mode.
         * Must be unique to each application instance.
         */
        private @Nullable String transactionalIdPrefix;

        /**
         * How long to wait for Kafka to acknowledge events. Defaults to 30 seconds.
         */
        private Duration ackTimeout = KafkaEventSinkConfiguration.DEFAULT_ACK_TIMEOUT;

        /**
         * The qualified names of events to publish as they are, read from the event store by a processor named
         * {@code <sink>-exposure}, such as {@code parcels.ParcelDispatched}. These are event names, not class names:
         * the name an {@code @Event} annotation gives, or the class name for a payload class without one.
         */
        private List<String> exposeEvents = new ArrayList<>();

        /**
         * Further producer settings for this sink.
         */
        private Map<String, String> properties = new HashMap<>();

        /**
         * Whether the {@code <sink>-exposure} processor starts at the beginning of the event store the first time it
         * starts, publishing the events stored before as well. Defaults to {@code false}: it starts at the end of the
         * event store.
         */
        private boolean exposeFromBeginning;

        /**
         * Returns the topic to publish events to.
         *
         * @return the topic
         */
        public @Nullable String getTopic() {
            return topic;
        }

        /**
         * Sets the topic to publish events to.
         *
         * @param topic the topic
         */
        public void setTopic(@Nullable String topic) {
            this.topic = topic;
        }

        /**
         * Returns the metadata entry that holds the record key.
         *
         * @return the metadata key, or {@code null} to publish without keys
         */
        public @Nullable String getRecordKeyMetadata() {
            return recordKeyMetadata;
        }

        /**
         * Sets the metadata entry that holds the record key.
         *
         * @param recordKeyMetadata the metadata key
         */
        public void setRecordKeyMetadata(@Nullable String recordKeyMetadata) {
            this.recordKeyMetadata = recordKeyMetadata;
        }

        /**
         * Returns how the sink confirms that events reached Kafka.
         *
         * @return the confirmation mode
         */
        public ConfirmationMode getConfirmationMode() {
            return confirmationMode;
        }

        /**
         * Sets how the sink confirms that events reached Kafka.
         *
         * @param confirmationMode the confirmation mode
         */
        public void setConfirmationMode(ConfirmationMode confirmationMode) {
            this.confirmationMode = confirmationMode;
        }

        /**
         * Returns the prefix of the transactional identifiers.
         *
         * @return the prefix, or {@code null} when not set
         */
        public @Nullable String getTransactionalIdPrefix() {
            return transactionalIdPrefix;
        }

        /**
         * Sets the prefix of the transactional identifiers.
         *
         * @param transactionalIdPrefix the prefix
         */
        public void setTransactionalIdPrefix(@Nullable String transactionalIdPrefix) {
            this.transactionalIdPrefix = transactionalIdPrefix;
        }

        /**
         * Returns how long to wait for Kafka to acknowledge events.
         *
         * @return the acknowledgement timeout
         */
        public Duration getAckTimeout() {
            return ackTimeout;
        }

        /**
         * Sets how long to wait for Kafka to acknowledge events.
         *
         * @param ackTimeout the acknowledgement timeout
         */
        public void setAckTimeout(Duration ackTimeout) {
            this.ackTimeout = ackTimeout;
        }

        /**
         * Returns the names of the events to publish as they are.
         *
         * @return the names of the exposed events
         */
        public List<String> getExposeEvents() {
            return exposeEvents;
        }

        /**
         * Sets the names of the events to publish as they are.
         *
         * @param exposeEvents the names of the exposed events
         */
        public void setExposeEvents(List<String> exposeEvents) {
            this.exposeEvents = exposeEvents;
        }

        /**
         * Returns whether the exposure processor starts at the beginning of the event store.
         *
         * @return whether exposure starts at the beginning of the event store
         */
        public boolean isExposeFromBeginning() {
            return exposeFromBeginning;
        }

        /**
         * Sets whether the exposure processor starts at the beginning of the event store.
         *
         * @param exposeFromBeginning whether the exposure processor starts at the beginning of the event store
         */
        public void setExposeFromBeginning(boolean exposeFromBeginning) {
            this.exposeFromBeginning = exposeFromBeginning;
        }

        /**
         * Returns the further producer settings for this sink.
         *
         * @return the further producer settings
         */
        public Map<String, String> getProperties() {
            return properties;
        }

        /**
         * Sets the further producer settings for this sink.
         *
         * @param properties the further producer settings
         */
        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }

    /**
     * The settings of a source that pooled streaming event processors read from Kafka. The source reads every
     * partition of its topics, and the processor tracks its progress in its token store.
     */
    public static class StreamingSource {

        /**
         * The topics to read. Either this or the topic pattern is required.
         */
        private List<String> topics = new ArrayList<>();

        /**
         * A regular expression matching the names of the topics to read. The source reads every matching topic,
         * dead-letter topics included.
         */
        private @Nullable String topicPattern;

        /**
         * What the source does with records that carry no event: {@code skip-and-log} or {@code fail}. Defaults to
         * {@code skip-and-log}.
         */
        private UndecodableRecordPolicy undecodableRecordPolicy =
                KafkaStreamableEventSourceConfiguration.DEFAULT_UNDECODABLE_RECORD_POLICY;

        /**
         * What the source does when Kafka has deleted the records at the position it resumes from, under the topic's
         * retention settings: {@code fail} or {@code reset-to-earliest}. Defaults to {@code fail}.
         */
        private RetentionGapPolicy retentionGapPolicy =
                KafkaStreamableEventSourceConfiguration.DEFAULT_RETENTION_GAP_POLICY;

        /**
         * The longest time a single poll of the consumer waits for records. Defaults to 500 milliseconds.
         */
        private Duration pollTimeout = KafkaStreamableEventSourceConfiguration.DEFAULT_POLL_TIMEOUT;

        /**
         * The number of events the source buffers between the Kafka consumer and the processor. Defaults to 1024.
         */
        private int bufferSize = KafkaStreamableEventSourceConfiguration.DEFAULT_BUFFER_SIZE;

        /**
         * How often the source looks for new partitions and new topics matching its pattern. Defaults to 30 seconds.
         */
        private Duration metadataRefreshInterval =
                KafkaStreamableEventSourceConfiguration.DEFAULT_METADATA_REFRESH_INTERVAL;

        /**
         * Further consumer settings for this source.
         */
        private Map<String, String> properties = new HashMap<>();

        /**
         * Returns the topics to read.
         *
         * @return the topics
         */
        public List<String> getTopics() {
            return topics;
        }

        /**
         * Sets the topics to read.
         *
         * @param topics the topics
         */
        public void setTopics(List<String> topics) {
            this.topics = topics;
        }

        /**
         * Returns the regular expression matching the names of the topics to read.
         *
         * @return the topic pattern, or {@code null} when not set
         */
        public @Nullable String getTopicPattern() {
            return topicPattern;
        }

        /**
         * Sets the regular expression matching the names of the topics to read.
         *
         * @param topicPattern the topic pattern
         */
        public void setTopicPattern(@Nullable String topicPattern) {
            this.topicPattern = topicPattern;
        }

        /**
         * Returns what the source does with records that carry no event.
         *
         * @return the undecodable record policy
         */
        public UndecodableRecordPolicy getUndecodableRecordPolicy() {
            return undecodableRecordPolicy;
        }

        /**
         * Sets what the source does with records that carry no event.
         *
         * @param undecodableRecordPolicy the undecodable record policy
         */
        public void setUndecodableRecordPolicy(UndecodableRecordPolicy undecodableRecordPolicy) {
            this.undecodableRecordPolicy = undecodableRecordPolicy;
        }

        /**
         * Returns what the source does when its position was lost to retention.
         *
         * @return the retention gap policy
         */
        public RetentionGapPolicy getRetentionGapPolicy() {
            return retentionGapPolicy;
        }

        /**
         * Sets what the source does when its position was lost to retention.
         *
         * @param retentionGapPolicy what the source does when its position was lost to retention
         */
        public void setRetentionGapPolicy(RetentionGapPolicy retentionGapPolicy) {
            this.retentionGapPolicy = retentionGapPolicy;
        }

        /**
         * Returns the longest time a single poll waits for records.
         *
         * @return the poll timeout
         */
        public Duration getPollTimeout() {
            return pollTimeout;
        }

        /**
         * Sets the longest time a single poll waits for records.
         *
         * @param pollTimeout the longest time a single poll waits for records
         */
        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        /**
         * Returns the number of events the source buffers.
         *
         * @return the buffer size
         */
        public int getBufferSize() {
            return bufferSize;
        }

        /**
         * Sets the number of events the source buffers.
         *
         * @param bufferSize the number of events the source buffers
         */
        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        /**
         * Returns how often the source looks for new partitions and topics.
         *
         * @return the metadata refresh interval
         */
        public Duration getMetadataRefreshInterval() {
            return metadataRefreshInterval;
        }

        /**
         * Sets how often the source looks for new partitions and topics.
         *
         * @param metadataRefreshInterval how often the source looks for new partitions and topics
         */
        public void setMetadataRefreshInterval(Duration metadataRefreshInterval) {
            this.metadataRefreshInterval = metadataRefreshInterval;
        }

        /**
         * Returns the further consumer settings for this source.
         *
         * @return the further consumer settings
         */
        public Map<String, String> getProperties() {
            return properties;
        }

        /**
         * Sets the further consumer settings for this source.
         *
         * @param properties the further consumer settings
         */
        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }

    /**
     * The settings of a source that subscribing event processors consume from Kafka in a consumer group, committing
     * their progress as Kafka offsets.
     */
    public static class SubscribingSource {

        /**
         * The topics to read. Either this or the topic pattern is required.
         */
        private List<String> topics = new ArrayList<>();

        /**
         * A regular expression matching the names of the topics to read. The source leaves out the topics whose names
         * end in its dead-letter topic suffix.
         */
        private @Nullable String topicPattern;

        /**
         * The consumer group to join. Required.
         */
        private @Nullable String groupId;

        /**
         * The number of consumers each application instance runs in the group. Defaults to 1.
         */
        private int consumerCount = KafkaSubscribableEventSourceConfiguration.DEFAULT_CONSUMER_COUNT;

        /**
         * What the source does with records whose event fails: {@code retry}, {@code skip} or
         * {@code dead-letter-topic}. Defaults to {@code retry}.
         */
        private FailedRecordPolicy failedRecordPolicy =
                KafkaSubscribableEventSourceConfiguration.DEFAULT_FAILED_RECORD_POLICY;

        /**
         * What the source does with records that carry no event: {@code skip-and-log} or {@code fail}. Defaults to
         * {@code skip-and-log}.
         */
        private UndecodableRecordPolicy undecodableRecordPolicy =
                KafkaSubscribableEventSourceConfiguration.DEFAULT_UNDECODABLE_RECORD_POLICY;

        /**
         * What the source does when Kafka has deleted the records at its committed offset, under the topic's
         * retention settings: {@code fail} or {@code reset-to-earliest}. Defaults to {@code fail}. Cannot be changed
         * together with an {@code auto.offset.reset} consumer setting.
         */
        private RetentionGapPolicy retentionGapPolicy =
                KafkaSubscribableEventSourceConfiguration.DEFAULT_RETENTION_GAP_POLICY;

        /**
         * Where the source starts consuming a partition for which its consumer group has no committed offset, such as
         * every partition of a new consumer group: {@code earliest} or {@code latest}. Defaults to {@code earliest}.
         * Cannot be changed together with an {@code auto.offset.reset} consumer setting.
         */
        private InitialPosition initialPosition = KafkaSubscribableEventSourceConfiguration.DEFAULT_INITIAL_POSITION;

        /**
         * The longest time a single poll of the consumer waits for records. Defaults to 100 milliseconds.
         */
        private Duration pollTimeout = KafkaSubscribableEventSourceConfiguration.DEFAULT_POLL_TIMEOUT;

        /**
         * The delay before the first retry of a failed record. Defaults to 100 milliseconds.
         */
        private Duration retryInitialBackoff = KafkaSubscribableEventSourceConfiguration.DEFAULT_RETRY_INITIAL_BACKOFF;

        /**
         * The longest delay between retries of a failed record. Defaults to 30 seconds.
         */
        private Duration retryMaxBackoff = KafkaSubscribableEventSourceConfiguration.DEFAULT_RETRY_MAX_BACKOFF;

        /**
         * The suffix appended to a topic's name to name its dead-letter topic. Defaults to {@code .DLT}.
         */
        private String deadLetterTopicSuffix =
                KafkaSubscribableEventSourceConfiguration.DEFAULT_DEAD_LETTER_TOPIC_SUFFIX;

        /**
         * How long the source waits for events in progress when Kafka takes partitions away, or when it stops.
         * Defaults to 10 seconds. Stopping takes at most this plus 15 seconds, so raising it beyond 15 seconds also
         * requires raising {@code spring.lifecycle.timeout-per-shutdown-phase}.
         */
        private Duration revocationTimeout = KafkaSubscribableEventSourceConfiguration.DEFAULT_REVOCATION_TIMEOUT;

        /**
         * Further consumer settings for this source. An {@code auto.offset.reset} here cannot be combined with a
         * non-default {@code initial-position} or {@code retention-gap-policy}, and a {@code group.instance.id} is
         * suffixed with each consumer's index when {@code consumer-count} is above 1.
         */
        private Map<String, String> properties = new HashMap<>();

        /**
         * Settings of the producer that writes to dead-letter topics.
         */
        private Map<String, String> producerProperties = new HashMap<>();

        /**
         * Returns the topics to read.
         *
         * @return the topics
         */
        public List<String> getTopics() {
            return topics;
        }

        /**
         * Sets the topics to read.
         *
         * @param topics the topics
         */
        public void setTopics(List<String> topics) {
            this.topics = topics;
        }

        /**
         * Returns the regular expression matching the names of the topics to read.
         *
         * @return the topic pattern, or {@code null} when not set
         */
        public @Nullable String getTopicPattern() {
            return topicPattern;
        }

        /**
         * Sets the regular expression matching the names of the topics to read.
         *
         * @param topicPattern the topic pattern
         */
        public void setTopicPattern(@Nullable String topicPattern) {
            this.topicPattern = topicPattern;
        }

        /**
         * Returns the consumer group to join.
         *
         * @return the group identifier, or {@code null} when not set
         */
        public @Nullable String getGroupId() {
            return groupId;
        }

        /**
         * Sets the consumer group to join.
         *
         * @param groupId the group identifier
         */
        public void setGroupId(@Nullable String groupId) {
            this.groupId = groupId;
        }

        /**
         * Returns the number of consumers each application instance runs in the group.
         *
         * @return the consumer count
         */
        public int getConsumerCount() {
            return consumerCount;
        }

        /**
         * Sets the number of consumers each application instance runs in the group.
         *
         * @param consumerCount the consumer count
         */
        public void setConsumerCount(int consumerCount) {
            this.consumerCount = consumerCount;
        }

        /**
         * Returns what the source does with records whose event fails.
         *
         * @return the failed record policy
         */
        public FailedRecordPolicy getFailedRecordPolicy() {
            return failedRecordPolicy;
        }

        /**
         * Sets what the source does with records whose event fails.
         *
         * @param failedRecordPolicy the failed record policy
         */
        public void setFailedRecordPolicy(FailedRecordPolicy failedRecordPolicy) {
            this.failedRecordPolicy = failedRecordPolicy;
        }

        /**
         * Returns what the source does with records that carry no event.
         *
         * @return the undecodable record policy
         */
        public UndecodableRecordPolicy getUndecodableRecordPolicy() {
            return undecodableRecordPolicy;
        }

        /**
         * Sets what the source does with records that carry no event.
         *
         * @param undecodableRecordPolicy the undecodable record policy
         */
        public void setUndecodableRecordPolicy(UndecodableRecordPolicy undecodableRecordPolicy) {
            this.undecodableRecordPolicy = undecodableRecordPolicy;
        }

        /**
         * Returns what the source does when its committed offset was lost to retention.
         *
         * @return the retention gap policy
         */
        public RetentionGapPolicy getRetentionGapPolicy() {
            return retentionGapPolicy;
        }

        /**
         * Sets what the source does when its committed offset was lost to retention.
         *
         * @param retentionGapPolicy what the source does when its committed offset was lost to retention
         */
        public void setRetentionGapPolicy(RetentionGapPolicy retentionGapPolicy) {
            this.retentionGapPolicy = retentionGapPolicy;
        }

        /**
         * Returns where the source starts consuming a partition without a committed offset.
         *
         * @return the initial position
         */
        public InitialPosition getInitialPosition() {
            return initialPosition;
        }

        /**
         * Sets where the source starts consuming a partition without a committed offset.
         *
         * @param initialPosition where to start consuming a partition without a committed offset
         */
        public void setInitialPosition(InitialPosition initialPosition) {
            this.initialPosition = initialPosition;
        }

        /**
         * Returns the longest time a single poll waits for records.
         *
         * @return the poll timeout
         */
        public Duration getPollTimeout() {
            return pollTimeout;
        }

        /**
         * Sets the longest time a single poll waits for records.
         *
         * @param pollTimeout the longest time a single poll waits for records
         */
        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        /**
         * Returns the delay before the first retry of a failed record.
         *
         * @return the initial retry delay
         */
        public Duration getRetryInitialBackoff() {
            return retryInitialBackoff;
        }

        /**
         * Sets the delay before the first retry of a failed record.
         *
         * @param retryInitialBackoff the delay before the first retry of a failed record
         */
        public void setRetryInitialBackoff(Duration retryInitialBackoff) {
            this.retryInitialBackoff = retryInitialBackoff;
        }

        /**
         * Returns the longest delay between retries of a failed record.
         *
         * @return the longest retry delay
         */
        public Duration getRetryMaxBackoff() {
            return retryMaxBackoff;
        }

        /**
         * Sets the longest delay between retries of a failed record.
         *
         * @param retryMaxBackoff the longest delay between retries of a failed record
         */
        public void setRetryMaxBackoff(Duration retryMaxBackoff) {
            this.retryMaxBackoff = retryMaxBackoff;
        }

        /**
         * Returns the suffix that names dead-letter topics.
         *
         * @return the dead-letter topic suffix
         */
        public String getDeadLetterTopicSuffix() {
            return deadLetterTopicSuffix;
        }

        /**
         * Sets the suffix that names dead-letter topics.
         *
         * @param deadLetterTopicSuffix the suffix that names dead-letter topics
         */
        public void setDeadLetterTopicSuffix(String deadLetterTopicSuffix) {
            this.deadLetterTopicSuffix = deadLetterTopicSuffix;
        }

        /**
         * Returns how long the source waits for events in progress when Kafka takes partitions away.
         *
         * @return the revocation timeout
         */
        public Duration getRevocationTimeout() {
            return revocationTimeout;
        }

        /**
         * Sets how long the source waits for events in progress when Kafka takes partitions away.
         *
         * @param revocationTimeout how long to wait for events in progress when Kafka takes partitions away
         */
        public void setRevocationTimeout(Duration revocationTimeout) {
            this.revocationTimeout = revocationTimeout;
        }

        /**
         * Returns the further consumer settings for this source.
         *
         * @return the further consumer settings
         */
        public Map<String, String> getProperties() {
            return properties;
        }

        /**
         * Sets the further consumer settings for this source.
         *
         * @param properties the further consumer settings
         */
        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }

        /**
         * Returns the settings of the producer that writes to dead-letter topics.
         *
         * @return the dead-letter producer settings
         */
        public Map<String, String> getProducerProperties() {
            return producerProperties;
        }

        /**
         * Sets the settings of the producer that writes to dead-letter topics.
         *
         * @param producerProperties the settings of the producer that writes to dead-letter topics
         */
        public void setProducerProperties(Map<String, String> producerProperties) {
            this.producerProperties = producerProperties;
        }
    }
}
