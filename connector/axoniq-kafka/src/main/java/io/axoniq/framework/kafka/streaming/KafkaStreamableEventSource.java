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
import io.axoniq.framework.kafka.common.ConsumerSettings;
import io.axoniq.framework.kafka.common.KafkaAxoniqAddon;
import io.axoniq.framework.kafka.common.KafkaClientFactory;
import io.axoniq.license.entitlement.EntitlementManager;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.WrappedToken;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StreamableEventSource} that reads events from Kafka topics, for use by a pooled streaming event processor.
 * <p>
 * The source reads every partition of the subscribed topics, without joining a consumer group, and the processor
 * stores its progress as a {@link KafkaTrackingToken} in its token store. The processor distributes events over its
 * segments with its sequencing policy, and supports replays, splitting and merging segments,
 * {@link #tokenAt(Instant, ProcessingContext) starting from a point in time}, and dead-letter queues. Events from
 * Kafka carry no aggregate identifier, so configure a sequencing policy for processors that read from Kafka, such as
 * {@link KafkaPartitionSequencingPolicy}.
 * <p>
 * Every processor instance reads every partition, and its segments filter the events after they are fetched. To
 * divide partitions over instances, use a {@link io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource}.
 * <p>
 * The source reads with {@code isolation.level=read_committed} unless its own consumer properties set another level;
 * one in the {@link KafkaClientFactory}'s shared settings does not change this. Tokens from
 * {@link #latestToken(ProcessingContext)} and {@link #tokenAt(Instant, ProcessingContext)} use the same level. Open
 * streams look for new partitions, and new topics matching a pattern, every
 * {@link KafkaStreamableEventSourceConfiguration#withMetadataRefreshInterval(Duration) metadata refresh interval}.
 * Looking up partitions never creates topics.
 * <p>
 * Every event's context carries the {@link io.axoniq.framework.kafka.common.KafkaResources} of the record it came from.
 * Events are delivered in order per partition. Partitions added to a topic later, and topics created later that match
 * a subscription pattern, are read from their beginning once the source discovers them. A source can be combined with
 * other sources, such as the event store, in a
 * {@code io.axoniq.framework.messaging.eventstreaming.MultiStreamableEventSource}.
 * <p>
 * Example usage:
 * <pre>{@code
 * KafkaStreamableEventSource source = new KafkaStreamableEventSource(
 *         KafkaClientFactory.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")),
 *         KafkaStreamableEventSourceConfiguration.create(new DefaultKafkaEventCodec(eventConverter),
 *                                                        TopicSubscription.topics("parcels"))
 * );
 * }</pre>
 * Call {@link #shutdown()} when the application stops, to close the source's metadata client and any open streams.
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class KafkaStreamableEventSource implements StreamableEventSource, DescribableComponent {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamableEventSource.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final KafkaClientFactory clientFactory;
    private final KafkaStreamableEventSourceConfiguration configuration;
    private final EntitlementManager entitlementManager;
    private final Set<KafkaPollingStream<?>> openStreams = ConcurrentHashMap.newKeySet();
    private final TopicMetadata topicMetadata;
    private volatile boolean shutdown;

    /**
     * Constructs a source that reads events as the given {@code configuration} describes, with clients created by the
     * given {@code clientFactory}.
     *
     * @param clientFactory the factory creating the Kafka clients of this source
     * @param configuration the settings of this source
     */
    public KafkaStreamableEventSource(KafkaClientFactory clientFactory,
                                      KafkaStreamableEventSourceConfiguration configuration) {
        this(clientFactory, configuration, EntitlementManager.INSTANCE);
        EntitlementManager.INSTANCE.registerAddon(KafkaAxoniqAddon.class);
    }

    /**
     * Constructs a source that claims consumed events against the given {@code entitlementManager}.
     *
     * @param clientFactory      the factory creating the Kafka clients of this source
     * @param configuration      the settings of this source
     * @param entitlementManager the entitlement manager consumed events are claimed against
     */
    KafkaStreamableEventSource(KafkaClientFactory clientFactory,
                               KafkaStreamableEventSourceConfiguration configuration,
                               EntitlementManager entitlementManager) {
        this.clientFactory = Objects.requireNonNull(clientFactory, "The clientFactory must not be null.");
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null.");
        this.entitlementManager = Objects.requireNonNull(entitlementManager,
                                                         "The entitlementManager must not be null.");
        this.topicMetadata = new TopicMetadata(clientFactory,
                                               configuration.subscription(),
                                               configuration.consumerProperties());
        ConsumerSettings.warnAboutIgnoredStreamingSettings(configuration.consumerProperties(), logger);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException if the condition's position is, or wraps, a token of another type than
     *                                  {@link KafkaTrackingToken} or {@link LegacyKafkaTrackingToken}
     */
    @Override
    public MessageStream<EventMessage> open(StreamingCondition condition, @Nullable ProcessingContext context) {
        Objects.requireNonNull(condition, "The condition must not be null.");
        if (shutdown) {
            return MessageStream.failed(new IllegalStateException("The Kafka event source is shut down."));
        }
        return open(configuration.codec(), startPosition(condition.position()), condition);
    }

    private <V> MessageStream<EventMessage> open(KafkaEventCodec<V> codec,
                                                 KafkaPollingStream.StartPosition startPosition,
                                                 StreamingCondition condition) {
        Consumer<String, V> consumer;
        try {
            consumer = clientFactory.createConsumer(ConsumerSettings.forStreaming(configuration.consumerProperties()),
                                                    new StringDeserializer(),
                                                    codec.valueSerde().deserializer());
        } catch (RuntimeException e) {
            // Reported like any other failure to read, so that the processor handles it as such.
            return MessageStream.failed(e);
        }
        KafkaPollingStream<V> stream = new KafkaPollingStream<>(consumer,
                                                                topicMetadata::partitions,
                                                                configuration,
                                                                codec,
                                                                startPosition,
                                                                condition.criteria(),
                                                                entitlementManager);
        openStreams.add(stream);
        MessageStream<EventMessage> started = stream.start().onClose(() -> openStreams.remove(stream));
        if (shutdown) {
            // Shut down while opening: the shutdown may have missed this stream.
            stream.stop();
            openStreams.remove(stream);
        }
        return started;
    }

    private static KafkaPollingStream.StartPosition startPosition(@Nullable TrackingToken position) {
        if (position == null || TrackingToken.FIRST.equals(position)) {
            return new KafkaPollingStream.StartPosition(KafkaTrackingToken.empty(), false);
        }
        if (TrackingToken.LATEST.equals(position)) {
            return new KafkaPollingStream.StartPosition(KafkaTrackingToken.empty(), true);
        }
        if (position instanceof KafkaTrackingToken kafkaToken) {
            return new KafkaPollingStream.StartPosition(kafkaToken, false);
        }
        if (position instanceof LegacyKafkaTrackingToken legacyToken) {
            return new KafkaPollingStream.StartPosition(legacyToken.toKafkaTrackingToken(), false);
        }
        if (position instanceof WrappedToken wrapped) {
            return startPosition(WrappedToken.unwrapLowerBound(wrapped));
        }
        throw new IllegalArgumentException("A Kafka event source cannot start from a token of type ["
                                                   + position.getClass().getName() + "].");
    }

    @Override
    public CompletableFuture<TrackingToken> firstToken(@Nullable ProcessingContext context) {
        return topicMetadata.offsets(OffsetSpec.earliest()).thenApply(KafkaTrackingToken::of);
    }

    @Override
    public CompletableFuture<TrackingToken> latestToken(@Nullable ProcessingContext context) {
        return topicMetadata.offsets(OffsetSpec.latest()).thenApply(KafkaTrackingToken::of);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Partitions without records at or after the given instant start at their end.
     */
    @Override
    public CompletableFuture<TrackingToken> tokenAt(Instant at, @Nullable ProcessingContext context) {
        Objects.requireNonNull(at, "The instant must not be null.");
        return topicMetadata.offsets(OffsetSpec.forTimestamp(at.toEpochMilli()))
                .thenCombine(topicMetadata.offsets(OffsetSpec.latest()), (atTime, latest) -> {
                    Map<TopicPartition, Long> result = new HashMap<>(latest);
                    // Kafka reports -1 for partitions without records at or after the given time: those have nothing
                    // to replay and start at their end.
                    atTime.forEach((partition, offset) -> {
                        if (offset >= 0) {
                            result.put(partition, offset);
                        }
                    });
                    return KafkaTrackingToken.of(result);
                });
    }

    /**
     * Closes the streams this source has opened, and its metadata client.
     *
     * @return a future that completes when the polling threads of the streams have exited, or after
     * 10 seconds when they have not
     */
    public CompletableFuture<Void> shutdown() {
        shutdown = true;
        List<CompletableFuture<Void>> terminations = new ArrayList<>();
        openStreams.forEach(stream -> {
            stream.stop();
            terminations.add(stream.terminated());
        });
        openStreams.clear();
        topicMetadata.close();
        return CompletableFuture.allOf(terminations.toArray(CompletableFuture[]::new))
                                .completeOnTimeout(null, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("subscription", configuration.subscription());
        descriptor.describeProperty("codec", configuration.codec());
        descriptor.describeProperty("undecodableRecordPolicy", configuration.undecodableRecordPolicy());
        descriptor.describeProperty("retentionGapPolicy", configuration.retentionGapPolicy());
        descriptor.describeProperty("bufferSize", configuration.bufferSize());
        descriptor.describeProperty("pollTimeout", configuration.pollTimeout());
        descriptor.describeProperty("metadataRefreshInterval", configuration.metadataRefreshInterval());
    }
}
