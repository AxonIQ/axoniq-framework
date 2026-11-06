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

import io.axoniq.framework.kafka.codec.DecodedRecord;
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaAxoniqAddon;
import io.axoniq.framework.kafka.common.KafkaConnectorUtils;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QueueMessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * The event stream of one
 * {@link KafkaStreamableEventSource#open(org.axonframework.messaging.eventstreaming.StreamingCondition,
 * org.axonframework.messaging.core.unitofwork.ProcessingContext) open} call: a dedicated thread polls a Kafka consumer
 * that is assigned every partition of the subscribed topics, and offers the decoded events to a bounded buffer that
 * the processor reads from.
 * <p>
 * Kafka consumers are not thread-safe, so the consumer is used by the polling thread only. When the buffer is full,
 * the thread pauses all partitions and keeps polling, which keeps the consumer alive without fetching more records,
 * and resumes them once the processor has made room.
 * <p>
 * Every event carries the cumulative {@link KafkaTrackingToken} in its context: the position after the event in its
 * own partition, and the latest position in all other partitions. Records that are skipped, because they carry no
 * event or do not match the stream's criteria, advance the token of the next event that is delivered.
 * <p>
 * Internal, because it is created by and only reachable through {@link KafkaStreamableEventSource}.
 *
 * @param <V> the type of record value
 * @author Allard Buijze
 * @since 5.5.0
 */
@Internal
class KafkaPollingStream<V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPollingStream.class);
    private static final Duration PAUSED_POLL_TIMEOUT = Duration.ofMillis(20);
    /**
     * The longest time a lookup of partitions or of their earliest offsets may take. It is independent of the poll
     * timeout, as a first lookup may have to set up the connection, including TLS and authentication. Closing the
     * source ends a partition lookup in progress sooner, and stopping the stream a lookup of earliest offsets.
     */
    private static final Duration METADATA_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    private final Consumer<String, V> consumer;
    private final PartitionLookup partitionLookup;
    private final KafkaStreamableEventSourceConfiguration configuration;
    private final KafkaEventCodec<V> codec;
    private final EventCriteria criteria;
    private final EntitlementManager entitlementManager;
    private final QueueMessageStream<EventMessage> buffer;
    private final Deque<PendingEntry> pending = new ArrayDeque<>();
    private final StartPosition startPosition;
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private volatile boolean running = true;
    /**
     * The next offset to read in each partition, kept mutable so that reading a record does not copy all positions;
     * an immutable token is only created for the events that are delivered.
     */
    private final Map<TopicPartition, Long> positions;
    private long nextMetadataRefresh;

    /**
     * Where a stream starts reading.
     *
     * @param token  the position to start from, for partitions it mentions
     * @param latest whether partitions that exist when the stream opens start at their end rather than their
     *               beginning
     */
    record StartPosition(KafkaTrackingToken token, boolean latest) {

    }

    private record PendingEntry(EventMessage event, Context context) {

    }

    /**
     * Looks up the partitions of the subscribed topics, as they are now.
     */
    @FunctionalInterface
    interface PartitionLookup {

        /**
         * Returns the partitions of the subscribed topics that exist, waiting at most the given {@code timeout}.
         *
         * @param timeout the longest time to wait for the lookup
         * @return the partitions of the subscribed topics
         */
        Set<TopicPartition> partitions(Duration timeout);
    }

    /**
     * Constructs a stream reading with the given {@code consumer}, which this stream owns and closes.
     *
     * @param consumer           the consumer to read with, not assigned to any partition yet
     * @param partitionLookup    looks up the partitions of the subscribed topics, as they are now
     * @param configuration      the settings of the owning source
     * @param codec              the codec that decodes records
     * @param startPosition      where to start reading
     * @param criteria           the criteria that events must match to be delivered
     * @param entitlementManager the entitlement manager delivered events are claimed against
     */
    KafkaPollingStream(Consumer<String, V> consumer,
                       PartitionLookup partitionLookup,
                       KafkaStreamableEventSourceConfiguration configuration,
                       KafkaEventCodec<V> codec,
                       StartPosition startPosition,
                       EventCriteria criteria,
                       EntitlementManager entitlementManager) {
        this.consumer = consumer;
        this.partitionLookup = partitionLookup;
        this.configuration = configuration;
        this.codec = codec;
        this.startPosition = startPosition;
        this.positions = new HashMap<>(startPosition.token().positions());
        this.criteria = criteria;
        this.entitlementManager = entitlementManager;
        this.buffer = new QueueMessageStream<>(new ArrayBlockingQueue<>(configuration.bufferSize()));
    }

    /**
     * Starts the polling thread and returns the stream of events it reads. Closing the returned stream stops the
     * thread and closes the consumer.
     *
     * @return the stream of events read from Kafka
     */
    MessageStream<EventMessage> start() {
        Thread.ofPlatform()
              .name("KafkaStreamableEventSource-" + THREAD_COUNTER.getAndIncrement())
              .daemon(true)
              .start(this);
        return buffer.onClose(this::stop);
    }

    /**
     * Stops the polling thread. The consumer is closed by the polling thread itself.
     */
    void stop() {
        running = false;
        consumer.wakeup();
    }

    /**
     * Returns a future that completes when the polling thread has closed the consumer and exited.
     *
     * @return a future that completes when the polling thread has exited
     */
    CompletableFuture<Void> terminated() {
        return terminated;
    }

    @Override
    public void run() {
        try {
            assignPartitions(true);
            while (running) {
                offerPending();
                if (consumer.assignment().isEmpty()) {
                    LockSupport.parkNanos(configuration.pollTimeout().toNanos());
                } else {
                    poll();
                }
                if (System.currentTimeMillis() >= nextMetadataRefresh) {
                    refreshPartitions();
                }
            }
            buffer.seal();
        } catch (WakeupException e) {
            if (running) {
                buffer.sealExceptionally(e);
            } else {
                buffer.seal();
            }
        } catch (Throwable e) {
            logger.warn("Reading events from Kafka failed. The event stream is closed.", e);
            buffer.sealExceptionally(e);
            if (e instanceof VirtualMachineError error) {
                throw error;
            }
        } finally {
            KafkaConnectorUtils.closeQuietly(consumer, CLOSE_TIMEOUT);
            terminated.complete(null);
        }
    }

    private void poll() {
        boolean backPressure = !pending.isEmpty();
        if (backPressure) {
            consumer.pause(consumer.assignment());
        } else if (!consumer.paused().isEmpty()) {
            consumer.resume(consumer.paused());
        }
        ConsumerRecords<String, V> records;
        try {
            records = consumer.poll(backPressure ? PAUSED_POLL_TIMEOUT : configuration.pollTimeout());
        } catch (OffsetOutOfRangeException e) {
            handleRetentionGap(e);
            return;
        } catch (RecordDeserializationException e) {
            skipUndeserializableRecord(e);
            return;
        }
        int delivered = 0;
        for (ConsumerRecord<String, V> record : records) {
            if (handle(record)) {
                delivered++;
            }
        }
        if (delivered > 0) {
            entitlementManager.claimMessage(KafkaAxoniqAddon.IDENTIFIER, EntitlementMessageType.EVENT, delivered);
        }
        offerPending();
    }

    private boolean handle(ConsumerRecord<String, V> record) {
        positions.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
        Optional<DecodedRecord> decoded = configuration.undecodableRecordPolicy().decode(codec, record);
        if (decoded.isEmpty()) {
            return false;
        }
        EventMessage event = decoded.get().event();
        Set<Tag> tags = Tag.fromContext(decoded.get().resources()).orElse(Set.of());
        if (!criteria.matches(event.type().qualifiedName(), tags)) {
            return false;
        }
        Context context = KafkaResources.addToContext(decoded.get().resources(), record);
        pending.add(new PendingEntry(event, TrackingToken.addToContext(context, KafkaTrackingToken.of(positions))));
        return true;
    }

    private void offerPending() {
        PendingEntry next;
        while ((next = pending.peek()) != null && buffer.offer(next.event(), next.context())) {
            pending.poll();
        }
    }

    private void skipUndeserializableRecord(RecordDeserializationException e) {
        // The consumer cannot move past a record it cannot deserialize, so the position is moved explicitly.
        TopicPartition partition = e.topicPartition();
        configuration.undecodableRecordPolicy().handle(partition, e.offset(), e);
        positions.put(partition, e.offset() + 1);
        consumer.seek(partition, e.offset() + 1);
    }

    private void handleRetentionGap(OffsetOutOfRangeException e) {
        Map<TopicPartition, Long> requested = e.offsetOutOfRangePartitions();
        Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(requested.keySet(), METADATA_TIMEOUT);
        Map<TopicPartition, Long> beyondEnd = new HashMap<>();
        requested.forEach((partition, offset) -> {
            Long beginning = beginnings.get(partition);
            if (beginning != null && offset >= beginning) {
                beyondEnd.put(partition, offset);
            }
        });
        if (!beyondEnd.isEmpty()) {
            // Not a retention gap: the partition holds fewer records than the token says were read, for example because
            // the topic was deleted and created again. Continuing would skip records, or read them again.
            throw new IllegalStateException(
                    "The positions " + beyondEnd + " lie beyond the end of their Kafka partitions, which hold fewer "
                            + "records than the processor has read. Was the topic deleted and created again? Reset "
                            + "the processor to read the topic anew.", e
            );
        }
        if (configuration.retentionGapPolicy() == RetentionGapPolicy.FAIL) {
            throw e;
        }
        logger.warn("The positions {} are no longer available in Kafka, as the records were deleted under the topic's "
                            + "retention settings. Continuing from the earliest available records.", requested);
        consumer.seekToBeginning(requested.keySet());
        for (TopicPartition partition : requested.keySet()) {
            positions.put(partition, consumer.position(partition));
        }
    }

    private void refreshPartitions() {
        try {
            assignPartitions(false);
        } catch (WakeupException e) {
            throw e;
        } catch (RuntimeException e) {
            // Partitions already assigned keep being read; the next refresh tries again.
            logger.warn("Refreshing the partitions of the subscribed Kafka topics failed. Retrying in {}.",
                        configuration.metadataRefreshInterval(), e);
        }
    }

    private void assignPartitions(boolean initial) {
        nextMetadataRefresh = System.currentTimeMillis() + configuration.metadataRefreshInterval().toMillis();
        Set<TopicPartition> added =
                new HashSet<>(partitionLookup.partitions(METADATA_TIMEOUT));
        added.removeAll(consumer.assignment());
        if (added.isEmpty()) {
            return;
        }
        Set<TopicPartition> assignment = new HashSet<>(consumer.assignment());
        assignment.addAll(added);
        consumer.assign(assignment);
        boolean atEnd = initial && startPosition.latest();
        for (TopicPartition partition : added) {
            if (atEnd) {
                consumer.seekToEnd(Set.of(partition));
            } else if (positions.containsKey(partition)) {
                // Also at offset 0: records deleted before the token's position are a retention gap, which the
                // retention gap policy deals with.
                consumer.seek(partition, positions.get(partition));
            } else {
                // A partition the token does not mention, such as one added to a topic later, starts at its earliest
                // available record.
                consumer.seekToBeginning(Set.of(partition));
            }
        }
        if (atEnd) {
            // Resolving a position resolves those of all partitions, so every partition must have been sought first.
            for (TopicPartition partition : added) {
                positions.put(partition, consumer.position(partition));
            }
        }
        if (!initial) {
            logger.info("Started reading new Kafka partitions {}.", added);
        }
    }
}
