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

import io.axoniq.framework.kafka.codec.CodecTestFixtures;
import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.publishing.ConfirmationMode;
import io.axoniq.framework.kafka.publishing.KafkaEventSink;
import io.axoniq.framework.kafka.publishing.KafkaEventSinkConfiguration;
import io.axoniq.framework.kafka.publishing.TopicResolver;
import io.axoniq.framework.kafka.utils.KafkaTestBroker;
import io.axoniq.framework.messaging.eventstreaming.MultiStreamableEventSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.AsyncInMemoryStreamableEventSource;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.eventstreaming.StreamableEventSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Runs a {@link PooledStreamingEventProcessor} on a {@link KafkaStreamableEventSource}.
 */
class KafkaStreamableEventSourceProcessingIT {

    private static final String PROCESSOR_NAME = "kafka-processor";

    private final DefaultKafkaEventCodec codec =
            CodecTestFixtures.defaultCodec();
    private final List<AutoCloseable> cleanup = new ArrayList<>();
    private @Nullable AxonConfiguration configuration;
    private String topic;

    public record ParcelDispatched(String parcelId, int sequence) {

    }

    public record OrderShipped(String orderId) {

    }

    @BeforeEach
    void setUp() {
        topic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("processing"), 8);
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            if (configuration != null) {
                configuration.shutdown();
            }
        } finally {
            for (AutoCloseable closeable : cleanup) {
                closeable.close();
            }
        }
    }

    private KafkaStreamableEventSource kafkaSource() {
        KafkaStreamableEventSource source = new KafkaStreamableEventSource(
                KafkaTestBroker.clientFactory(),
                KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
        );
        cleanup.add(() -> source.shutdown().orTimeout(30, TimeUnit.SECONDS).join());
        return source;
    }

    private void publish(String key, int from, int count) {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        for (int i = from; i < from + count; i++) {
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched(key, i));
            records.add(codec.encode(event, topic, key, null));
        }
        KafkaTestBroker.produce(records);
    }

    private void publishAt(String key, int count, Instant timestamp) {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched(key, i));
            ProducerRecord<String, byte[]> encoded = codec.encode(event, topic, key, null);
            records.add(new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), key, encoded.value(),
                                             encoded.headers()));
        }
        KafkaTestBroker.produce(records);
    }

    private void start(StreamableEventSource source, Object handler, int segments) {
        start(source, handler, segments, new InMemoryTokenStore());
    }

    private void start(StreamableEventSource source, Object handler, int segments, TokenStore tokenStore) {
        var module = EventProcessorModule.pooledStreaming(PROCESSOR_NAME)
                                         .eventHandlingComponents(c -> c.autodetected("handler", cfg -> handler))
                                         .customized((cfg, c) -> c.eventSource(source)
                                                                  .tokenStore(tokenStore)
                                                                  .initialSegmentCount(segments));
        configuration = MessagingConfigurer.create()
                                           .eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(module)))
                                           .build();
        configuration.start();
    }

    private PooledStreamingEventProcessor processor() {
        return (PooledStreamingEventProcessor) configuration.getComponents(EventProcessor.class).get(PROCESSOR_NAME);
    }

    /**
     * Records the sequence of every event per partition, and which segments handled events.
     */
    public static class PartitionRecorder {

        final Map<TopicPartition, List<Integer>> sequences = new ConcurrentHashMap<>();
        final List<String> handled = new CopyOnWriteArrayList<>();
        final Set<Integer> segments = ConcurrentHashMap.newKeySet();

        @KafkaPartitionSequencing
        @EventHandler
        public void on(ParcelDispatched event, ProcessingContext context) {
            TopicPartition partition = context.getResource(KafkaResources.TOPIC_PARTITION);
            if (partition != null) {
                sequences.computeIfAbsent(partition, p -> Collections.synchronizedList(new ArrayList<>()))
                         .add(event.sequence());
            }
            handled.add(event.parcelId() + "-" + event.sequence());
            Segment segment = context.getResource(Segment.RESOURCE_KEY);
            if (segment != null) {
                segments.add(segment.getSegmentId());
            }
        }
    }

    @Nested
    class Processing {

        @Test
        void processesPartitionsConcurrentlyKeepingOrderWithinEachPartition() {
            // given
            PartitionRecorder recorder = new PartitionRecorder();
            IntStream.range(0, 16).forEach(k -> publish("parcel-" + k, 0, 10));

            // when
            start(kafkaSource(), recorder, 4);

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 160);
            assertThat(recorder.segments).hasSizeGreaterThan(1);
            // a partition carries several keys; the events of each key, all in one partition, must stay in order
            Map<String, List<Integer>> perKey = new ConcurrentHashMap<>();
            recorder.handled.forEach(h -> {
                int dash = h.lastIndexOf('-');
                perKey.computeIfAbsent(h.substring(0, dash), k -> new ArrayList<>())
                      .add(Integer.parseInt(h.substring(dash + 1)));
            });
            assertThat(perKey.values()).allSatisfy(s -> assertThat(s).isSorted().hasSize(10));
        }

        @Test
        void processorPicksUpAPartitionAddedWhileItRuns() {
            // given
            PartitionRecorder recorder = new PartitionRecorder();
            KafkaStreamableEventSource source = new KafkaStreamableEventSource(
                    KafkaTestBroker.clientFactory(),
                    KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                          .withMetadataRefreshInterval(Duration.ofMillis(200))
            );
            cleanup.add(() -> source.shutdown().orTimeout(30, TimeUnit.SECONDS).join());
            publish("a", 0, 5);
            start(source, recorder, 2);
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 5);

            // when
            KafkaTestBroker.increasePartitions(topic, 9);
            TopicPartition added = new TopicPartition(topic, 8);
            List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, byte[]> encoded = codec.encode(
                        EventTestUtils.asEventMessage(new ParcelDispatched("new", i)), topic, "new", null
                );
                records.add(new ProducerRecord<>(topic, added.partition(), "new", encoded.value(), encoded.headers()));
            }
            KafkaTestBroker.produce(records);

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 8);
            assertThat(recorder.sequences.get(added)).containsExactly(0, 1, 2);
        }
    }

    @Nested
    class SegmentsAndReplays {

        @Test
        void splitAndMergeHandOverProgressWithoutLosingOrRepeatingEvents() {
            // given
            PartitionRecorder recorder = new PartitionRecorder();
            publish("a", 0, 20);
            start(kafkaSource(), recorder, 1);
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 20);

            // when
            assertThat(processor().splitSegment(0).orTimeout(30, TimeUnit.SECONDS).join()).isTrue();
            publish("b", 0, 20);
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() >= 40);
            await().atMost(Duration.ofSeconds(30))
                   .until(() -> processor().processingStatus().size() == 2);
            assertThat(processor().mergeSegment(0).orTimeout(30, TimeUnit.SECONDS).join()).isTrue();
            publish("c", 0, 20);

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() >= 60);
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(
                    () -> assertThat(recorder.handled).doesNotHaveDuplicates().hasSize(60)
            );
        }

        @Test
        void replaysFromAPointInTime() {
            // given
            PartitionRecorder recorder = new PartitionRecorder();
            Instant cutOff = Instant.now().minus(Duration.ofHours(1));
            publishAt("early", 5, cutOff.minusSeconds(60));
            publishAt("late", 5, cutOff.plusSeconds(60));
            start(kafkaSource(), recorder, 2);
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 10);

            // when
            processor().shutdown().orTimeout(30, TimeUnit.SECONDS).join();
            processor().resetTokens(source -> source.tokenAt(cutOff, null)).orTimeout(30, TimeUnit.SECONDS).join();
            processor().start().orTimeout(30, TimeUnit.SECONDS).join();

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 15);
            assertThat(recorder.handled.subList(10, 15)).allMatch(h -> h.startsWith("late-"));
        }
    }

    @Nested
    class CombinedSources {

        @Test
        void combinesKafkaWithAnotherSourceInOneProcessor() {
            // given
            PartitionRecorder recorder = new PartitionRecorder();
            AsyncInMemoryStreamableEventSource store = new AsyncInMemoryStreamableEventSource();
            MultiStreamableEventSource combined = MultiStreamableEventSource.combining("store", store)
                                                                            .and("kafka", kafkaSource())
                                                                            .comparingTimestamps();
            start(combined, recorder, 2);

            // when
            store.publishMessage(EventTestUtils.asEventMessage(new ParcelDispatched("stored", 0)));
            publish("streamed", 0, 1);

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 2);
            assertThat(recorder.handled).containsExactlyInAnyOrder("stored-0", "streamed-0");
        }
    }

    @Nested
    class SplittingAndMergingWhileEventsFlow {

        private static final int PARTITIONS = 3;
        private static final int EVENTS_PER_KEY_PER_ROUND = 50;
        private static final List<String> KEYS = List.of("k0", "k1", "k2", "k3", "k4", "k5");

        @BeforeEach
        void setUp() {
            topic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("interleaved"), PARTITIONS);
        }

        /**
         * Records, per partition, the offsets of the handled events in the order they were handled, and which
         * segments handled events.
         */
        public static class OffsetRecorder {

            final Map<TopicPartition, List<Long>> offsets = new ConcurrentHashMap<>();
            final Set<Integer> segments = ConcurrentHashMap.newKeySet();

            @KafkaPartitionSequencing
            @EventHandler
            public void on(ParcelDispatched event, ProcessingContext context) {
                TopicPartition partition = Objects.requireNonNull(context.getResource(KafkaResources.TOPIC_PARTITION));
                Long offset = Objects.requireNonNull(context.getResource(KafkaResources.OFFSET));
                offsets.computeIfAbsent(partition, p -> new CopyOnWriteArrayList<>()).add(offset);
                Segment segment = Objects.requireNonNull(context.getResource(Segment.RESOURCE_KEY));
                segments.add(segment.getSegmentId());
            }
        }

        /**
         * Publishes the given {@code round} of events: each key gets {@link #EVENTS_PER_KEY_PER_ROUND} events, and the
         * keys are interleaved, so that every partition receives the events of two keys alternately.
         */
        private void publishRound(int round) {
            List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
            int from = round * EVENTS_PER_KEY_PER_ROUND;
            for (int sequence = from; sequence < from + EVENTS_PER_KEY_PER_ROUND; sequence++) {
                for (int k = 0; k < KEYS.size(); k++) {
                    String key = KEYS.get(k);
                    ProducerRecord<String, byte[]> encoded = codec.encode(
                            EventTestUtils.asEventMessage(new ParcelDispatched(key, sequence)), topic, key, null
                    );
                    records.add(new ProducerRecord<>(topic, k % PARTITIONS, key, encoded.value(), encoded.headers()));
                }
            }
            KafkaTestBroker.produce(records);
        }

        private void split(int segment, int expectedSegments) {
            assertThat(processor().splitSegment(segment).orTimeout(30, TimeUnit.SECONDS).join()).isTrue();
            await().atMost(Duration.ofSeconds(30))
                   .until(() -> processor().processingStatus().size() == expectedSegments);
        }

        private void merge(int segment, int expectedSegments) {
            assertThat(processor().mergeSegment(segment).orTimeout(30, TimeUnit.SECONDS).join()).isTrue();
            await().atMost(Duration.ofSeconds(30))
                   .until(() -> processor().processingStatus().size() == expectedSegments);
        }

        @Test
        void keepsTheOrderOfEachPartitionWhenSegmentsAreSplitAndMergedWhileEventsFlow() {
            // given
            OffsetRecorder recorder = new OffsetRecorder();
            start(kafkaSource(), recorder, 2);
            publishRound(0);
            await().atMost(Duration.ofSeconds(60)).until(() -> !recorder.offsets.isEmpty());

            // when - each change of the segments happens while the events published just before are handled
            split(0, 3);
            publishRound(1);
            split(1, 4);
            publishRound(2);
            merge(0, 3);
            publishRound(3);
            merge(1, 2);
            publishRound(4);

            // then
            long perPartition = (long) KEYS.size() / PARTITIONS * 5 * EVENTS_PER_KEY_PER_ROUND;
            List<Long> allOffsets = LongStream.range(0, perPartition).boxed().toList();
            await().atMost(Duration.ofSeconds(60)).until(
                    () -> recorder.offsets.size() == PARTITIONS && recorder.offsets.values().stream().allMatch(
                            handled -> new HashSet<>(handled).size() == perPartition
                    )
            );
            // An event may be handled again after a segment change, but no event is handled for the first time
            // before an earlier event of the same partition.
            assertThat(recorder.offsets.values()).allSatisfy(
                    handled -> assertThat(new LinkedHashSet<>(handled)).containsExactlyElementsOf(allOffsets)
            );
            assertThat(recorder.segments).hasSizeGreaterThan(2);
        }
    }

    @Nested
    class ResumingCombinedSources {

        private static final List<String> FIRST_EVENTS =
                List.of("stored-0", "stored-1", "streamed-0", "streamed-1", "streamed-2");
        private static final List<String> NEW_EVENTS =
                List.of("stored-2", "stored-3", "streamed-3", "streamed-4", "streamed-5");

        private final InMemoryTokenStore tokenStore = new InMemoryTokenStore();
        private final AsyncInMemoryStreamableEventSource store = new AsyncInMemoryStreamableEventSource(false, false);

        private MultiStreamableEventSource combined() {
            return MultiStreamableEventSource.combining("store", store)
                                             .and("kafka", kafkaSource())
                                             .comparingTimestamps();
        }

        private void store(int from, int count) {
            for (int i = from; i < from + count; i++) {
                store.publishMessage(EventTestUtils.asEventMessage(new ParcelDispatched("stored", i)));
            }
        }

        /**
         * Runs the processor until it handled the first events of both sources, stops it, adds events to both
         * sources, and starts the processor again with the same token store.
         *
         * @return the recorder of the second run
         */
        private PartitionRecorder restartAfterTheFirstEvents(int segments) {
            PartitionRecorder firstRun = new PartitionRecorder();
            start(combined(), firstRun, segments, tokenStore);
            store(0, 2);
            publish("streamed", 0, 3);
            await().atMost(Duration.ofSeconds(60)).until(() -> firstRun.handled.size() == FIRST_EVENTS.size());
            configuration.shutdown();
            assertThat(firstRun.handled).containsExactlyInAnyOrderElementsOf(FIRST_EVENTS);

            store(2, 2);
            publish("streamed", 3, 3);
            PartitionRecorder secondRun = new PartitionRecorder();
            start(combined(), secondRun, segments, tokenStore);
            return secondRun;
        }

        @Test
        void resumesBothSourcesFromTheStoredTokenAfterARestart() {
            // when - a processor that handled the first events of both sources stops, both sources receive events
            // while it is down, and it starts with the stored token
            PartitionRecorder secondRun = restartAfterTheFirstEvents(1);

            // then - only the new events are handled
            await().atMost(Duration.ofSeconds(60)).until(() -> secondRun.handled.containsAll(NEW_EVENTS));
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(
                    () -> assertThat(secondRun.handled).containsExactlyInAnyOrderElementsOf(NEW_EVENTS)
            );
        }

        @Test
        void resumesSegmentsAtDifferentPositionsWithoutMissingEvents() {
            // when - a processor whose segments handled the events of different sources stops, and starts again
            PartitionRecorder secondRun = restartAfterTheFirstEvents(2);

            // then - every new event is handled once. The stream reopens at the lowest position of the segments, so a
            // segment that was further ahead in one source may handle some of the first events again.
            await().atMost(Duration.ofSeconds(60)).until(() -> secondRun.handled.containsAll(NEW_EVENTS));
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(secondRun.handled).filteredOn(NEW_EVENTS::contains)
                                             .containsExactlyInAnyOrderElementsOf(NEW_EVENTS);
                assertThat(secondRun.handled).filteredOn(handled -> !NEW_EVENTS.contains(handled))
                                             .doesNotHaveDuplicates()
                                             .isSubsetOf(FIRST_EVENTS);
            });
        }
    }

    @Nested
    class PublishingFromProcessorHandlers {

        private String outboundTopic;

        @BeforeEach
        void setUp() {
            outboundTopic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("outbound"), 1);
        }

        /**
         * Publishes every event to Kafka, then fails the first attempt as if the application crashed before the
         * processor stored its progress.
         */
        public static class FailingOncePublisher {

            private final KafkaEventSink sink;
            private final AtomicBoolean failed = new AtomicBoolean();

            FailingOncePublisher(KafkaEventSink sink) {
                this.sink = sink;
            }

            @EventHandler
            public void on(OrderShipped event, ProcessingContext context) {
                sink.publish(context, EventTestUtils.asEventMessage(new ParcelDispatched(event.orderId(), 0)))
                    .orTimeout(30, TimeUnit.SECONDS).join();
                if (failed.compareAndSet(false, true)) {
                    throw new IllegalStateException("crash before the token is stored");
                }
            }
        }

        /**
         * Publishes every event to Kafka. The first attempt fails after its Kafka transaction committed, as if the
         * application crashed before the processor stored its progress. The retry waits until it is allowed to
         * continue.
         */
        public static class FailingAfterTheKafkaCommitPublisher {

            private final KafkaEventSink sink;
            private final AtomicInteger attempts = new AtomicInteger();
            private final CompletableFuture<Void> retryAllowed = new CompletableFuture<>();

            FailingAfterTheKafkaCommitPublisher(KafkaEventSink sink) {
                this.sink = sink;
            }

            @EventHandler
            public void on(OrderShipped event, ProcessingContext context) {
                if (attempts.incrementAndGet() == 1) {
                    // The Kafka transaction commits while the context prepares to commit. The commit phase runs after
                    // that, and before the token store applies the processor's progress, after the commit.
                    context.runOnCommit(c -> {
                        throw new IllegalStateException("crash after the Kafka commit, before the token is stored");
                    });
                } else {
                    retryAllowed.orTimeout(60, TimeUnit.SECONDS).join();
                }
                sink.publish(context, EventTestUtils.asEventMessage(new ParcelDispatched(event.orderId(), 0)))
                    .orTimeout(30, TimeUnit.SECONDS).join();
            }
        }

        private KafkaEventSink sink(ConfirmationMode mode) {
            KafkaEventSink sink = new KafkaEventSink(
                    KafkaTestBroker.clientFactory(),
                    KafkaEventSinkConfiguration.create(codec, TopicResolver.fixed(outboundTopic))
                                               .withConfirmationMode(mode)
                                               .withTransactionalIdPrefix(outboundTopic + "-")
            );
            cleanup.add(() -> sink.shutdown().orTimeout(30, TimeUnit.SECONDS).join());
            return sink;
        }

        @Test
        void acknowledgedPublicationIsRepeatedWhenTheProcessorRetries() {
            // given
            AsyncInMemoryStreamableEventSource store = new AsyncInMemoryStreamableEventSource(false, false);
            start(store, new FailingOncePublisher(sink(ConfirmationMode.WAIT_FOR_ACK)), 1);

            // when
            store.publishMessage(EventTestUtils.asEventMessage(new OrderShipped("order-1")));

            // then - published at least once: the retry publishes the event again
            await().atMost(Duration.ofSeconds(60)).until(
                    () -> KafkaTestBroker.readAll(outboundTopic, "read_uncommitted", Duration.ofSeconds(1)).size() == 2
            );
        }

        @Test
        void transactionalPublicationOfTheFailedAttemptIsNeverVisible() {
            // given
            AsyncInMemoryStreamableEventSource store = new AsyncInMemoryStreamableEventSource(false, false);
            start(store, new FailingOncePublisher(sink(ConfirmationMode.TRANSACTIONAL)), 1);

            // when
            store.publishMessage(EventTestUtils.asEventMessage(new OrderShipped("order-1")));

            // then
            await().atMost(Duration.ofSeconds(60)).until(
                    () -> !KafkaTestBroker.readAll(outboundTopic, "read_committed", Duration.ofSeconds(1)).isEmpty()
            );
            List<ConsumerRecord<String, byte[]>> committed =
                    KafkaTestBroker.readAll(outboundTopic, "read_committed", Duration.ofSeconds(2));
            assertThat(committed).hasSize(1);
        }

        @Test
        void transactionalPublicationIsRepeatedWhenTheProcessorFailsAfterTheKafkaCommit() {
            // given
            AsyncInMemoryStreamableEventSource store = new AsyncInMemoryStreamableEventSource(false, false);
            FailingAfterTheKafkaCommitPublisher publisher =
                    new FailingAfterTheKafkaCommitPublisher(sink(ConfirmationMode.TRANSACTIONAL));
            start(store, publisher, 1);

            // when
            store.publishMessage(EventTestUtils.asEventMessage(new OrderShipped("order-1")));

            // then - the processor retries the event, and the failed attempt's transaction is committed
            await().atMost(Duration.ofSeconds(60)).until(() -> publisher.attempts.get() == 2);
            assertThat(KafkaTestBroker.readAll(outboundTopic, "read_committed", Duration.ofSeconds(2))).hasSize(1);

            // when
            publisher.retryAllowed.complete(null);

            // then - published at least once: the retry publishes the event again
            await().atMost(Duration.ofSeconds(60)).until(
                    () -> KafkaTestBroker.readAll(outboundTopic, "read_committed", Duration.ofSeconds(1)).size() == 2
            );
            assertThat(KafkaTestBroker.readAll(outboundTopic, "read_committed", Duration.ofSeconds(2)))
                    .hasSize(2)
                    .allSatisfy(record -> assertThat(new String(record.value(), StandardCharsets.UTF_8))
                            .contains("order-1"));
            assertThat(publisher.attempts).hasValue(2);
        }
    }
}
