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

import io.axoniq.framework.kafka.codec.CodecTestFixtures;
import io.axoniq.framework.kafka.codec.DefaultKafkaEventCodec;
import io.axoniq.framework.kafka.codec.UndecodableRecordPolicy;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.utils.KafkaTestBroker;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.axoniq.framework.kafka.codec.CodecTestFixtures.header;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class KafkaSubscribableEventSourceIT {

    private final DefaultKafkaEventCodec codec =
            CodecTestFixtures.defaultCodec();
    private final List<Runnable> cleanup = new ArrayList<>();
    private String topic;
    private String group;

    public record ParcelDispatched(String parcelId, int sequence) {

    }

    /**
     * Records every event it handles, running an optional action first, which may fail or block.
     */
    public static class Recorder {

        final List<String> handled = new CopyOnWriteArrayList<>();
        final Set<TopicPartition> partitions = ConcurrentHashMap.newKeySet();
        volatile Consumer<ParcelDispatched> action = e -> {
        };

        @EventHandler
        public void on(ParcelDispatched event, ProcessingContext context) {
            action.accept(event);
            handled.add(event.parcelId() + "-" + event.sequence());
            partitions.add(context.getResource(KafkaResources.TOPIC_PARTITION));
        }
    }

    @BeforeEach
    void setUp() {
        topic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("subscribable"), 4);
        group = topic + "-group";
    }

    @AfterEach
    void tearDown() {
        cleanup.reversed().forEach(Runnable::run);
    }

    private void start(Recorder recorder, KafkaSubscribableEventSourceConfiguration configuration) {
        KafkaSubscribableEventSource source = new KafkaSubscribableEventSource(
                KafkaTestBroker.clientFactory(), configuration, UnitOfWorkTestUtils.SIMPLE_FACTORY
        );
        var module = EventProcessorModule.subscribing("kafka-subscriber")
                                         .eventHandlingComponents(c -> c.autodetected("recorder", cfg -> recorder))
                                         .customized((cfg, c) -> c.eventSource(source));
        AxonConfiguration configured = MessagingConfigurer.create()
                                                          .eventProcessing(ep -> ep.subscribing(
                                                                  sp -> sp.processor(module)))
                                                          .build();
        configured.start();
        source.start();
        cleanup.add(() -> {
            source.shutdown().orTimeout(90, TimeUnit.SECONDS).join();
            configured.shutdown();
        });
    }

    private KafkaSubscribableEventSourceConfiguration configuration() {
        return KafkaSubscribableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic), group)
                                                        .withRetryBackoff(Duration.ofMillis(50),
                                                                          Duration.ofMillis(200));
    }

    private void publish(String key, int count) {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched(key, i));
            records.add(codec.encode(event, topic, key, null));
        }
        KafkaTestBroker.produce(records);
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
        return KafkaTestBroker.committedOffsets(group);
    }

    private long totalCommitted() {
        return committedOffsets().values().stream().mapToLong(OffsetAndMetadata::offset).sum();
    }

    private long membersWithPartitions() {
        return KafkaTestBroker.membersWithPartitions(group);
    }

    @Nested
    class Processing {

        @Test
        void processesEventsInOrderPerPartitionAndCommitsAfterProcessing() {
            // given
            Recorder recorder = new Recorder();
            start(recorder, configuration());

            // when
            publish("a", 10);
            publish("b", 10);

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 20);
            assertThat(recorder.handled.stream().filter(h -> h.startsWith("a-")).toList())
                    .containsExactly("a-0", "a-1", "a-2", "a-3", "a-4", "a-5", "a-6", "a-7", "a-8", "a-9");
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == 20);
        }

        @Test
        void eventsOfAbortedKafkaTransactionsAreSkipped() {
            // given
            Recorder recorder = new Recorder();
            start(recorder, configuration());
            Map<String, Object> properties = Map.of(
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaTestBroker.bootstrapServers(),
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, topic + "-aborted"
            );

            // when - two events are published in a Kafka transaction that aborts, and one is published afterwards
            try (KafkaProducer<String, byte[]> producer =
                         new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer())) {
                producer.initTransactions();
                producer.beginTransaction();
                for (int i = 0; i < 2; i++) {
                    producer.send(codec.encode(EventTestUtils.asEventMessage(new ParcelDispatched("aborted", i)),
                                               topic, "parcel", null));
                }
                producer.flush();
                producer.abortTransaction();
            }
            KafkaTestBroker.produce(List.of(codec.encode(
                    EventTestUtils.asEventMessage(new ParcelDispatched("committed", 0)), topic, "parcel", null
            )));

            // then - the event published after the aborted ones shares their partition, so once it is handled, the
            // aborted events would have been handled as well
            await().atMost(Duration.ofSeconds(60)).until(() -> !recorder.handled.isEmpty());
            assertThat(recorder.handled).containsExactly("committed-0");
        }

        @Test
        void newGroupStartsAtTheEndWhenConfiguredTo() {
            // given - events published before the group exists
            publish("before", 5);
            Recorder recorder = new Recorder();

            // when
            start(recorder, configuration().withInitialPosition(InitialPosition.LATEST));

            // then - events keep being published until the group handles one, as it starts at the end only once Kafka
            // has assigned it the partitions
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500)).until(() -> {
                publish("after", 1);
                return !recorder.handled.isEmpty();
            });
            assertThat(recorder.handled).allMatch(handled -> handled.startsWith("after-"));
        }

        @Test
        void twoInstancesShareThePartitions() {
            // given
            Recorder first = new Recorder();
            Recorder second = new Recorder();
            start(first, configuration());
            start(second, configuration());
            await().atMost(Duration.ofSeconds(60)).until(() -> membersWithPartitions() == 2);

            // when
            for (int k = 0; k < 20; k++) {
                publish("parcel-" + k, 2);
            }

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> first.handled.size() + second.handled.size() >= 40);
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                   .until(() -> first.handled.size() + second.handled.size() == 40);
            List<String> all = new ArrayList<>(first.handled);
            all.addAll(second.handled);
            assertThat(all).hasSize(40).doesNotHaveDuplicates();
            assertThat(first.handled).isNotEmpty();
            assertThat(second.handled).isNotEmpty();
        }

        @Test
        void handlerSlowerThanMaxPollIntervalIsNotEvicted() {
            // given
            Recorder recorder = new Recorder();
            recorder.action = event -> {
                if (event.sequence() == 0) {
                    sleep(4000);
                }
            };
            start(recorder, configuration().withConsumerProperties(Map.of(
                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1500,
                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000,
                    ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500
            )));

            // when
            publish("slow", 3);

            // then - processed once, without the rebalance an eviction would cause
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() >= 3);
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(
                    () -> assertThat(recorder.handled).containsExactly("slow-0", "slow-1", "slow-2")
            );
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == 3);
        }
    }

    @Nested
    class Failures {

        @Test
        void failedEventIsRetriedWithoutAdvancingTheOffset() {
            // given
            Recorder recorder = new Recorder();
            AtomicInteger attempts = new AtomicInteger();
            CompletableFuture<Void> allowSuccess = new CompletableFuture<>();
            recorder.action = event -> {
                if (event.sequence() == 1) {
                    attempts.incrementAndGet();
                    if (!allowSuccess.isDone()) {
                        throw new IllegalStateException("not yet");
                    }
                }
            };
            start(recorder, configuration());

            // when
            publish("retry", 3);

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> attempts.get() >= 3);
            assertThat(recorder.handled).containsExactly("retry-0");
            assertThat(totalCommitted()).isEqualTo(1L);

            // when
            allowSuccess.complete(null);

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> recorder.handled.size() == 3);
            assertThat(recorder.handled).containsExactly("retry-0", "retry-1", "retry-2");
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == 3);
        }

        @Test
        void skipPolicyContinuesWithTheNextRecord() {
            // given
            Recorder recorder = new Recorder();
            recorder.action = event -> {
                if (event.sequence() == 1) {
                    throw new IllegalStateException("poison");
                }
            };
            start(recorder, configuration().withFailedRecordPolicy(FailedRecordPolicy.SKIP));

            // when
            publish("skip", 3);

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> recorder.handled.size() == 2);
            assertThat(recorder.handled).containsExactly("skip-0", "skip-2");
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == 3);
        }

        @Test
        void deadLetterTopicPolicyMovesTheRecordAndContinues() {
            // given
            String deadLetterTopic = KafkaTestBroker.createTopic(topic + ".DLT", 1);
            Recorder recorder = new Recorder();
            recorder.action = event -> {
                if (event.sequence() == 1) {
                    throw new IllegalStateException("poison");
                }
            };
            start(recorder, configuration().withFailedRecordPolicy(FailedRecordPolicy.DEAD_LETTER_TOPIC));

            // when
            publish("dlt", 3);

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> recorder.handled.size() == 2);
            assertThat(recorder.handled).containsExactly("dlt-0", "dlt-2");
            List<ConsumerRecord<String, byte[]>> deadLetters =
                    KafkaTestBroker.readAll(deadLetterTopic, "read_uncommitted", Duration.ofSeconds(2));
            assertThat(deadLetters).singleElement().satisfies(record -> {
                assertThat(header(record.headers(), DeadLetterHeaders.EXCEPTION_MESSAGE)).isEqualTo("poison");
                assertThat(header(record.headers(), DeadLetterHeaders.ORIGINAL_TOPIC)).isEqualTo(topic);
                assertThat(header(record.headers(), DeadLetterHeaders.ORIGINAL_OFFSET)).isEqualTo("1");
                assertThat(codec.decode(record).orElseThrow().event().payloadAs(ParcelDispatched.class))
                        .isEqualTo(new ParcelDispatched("dlt", 1));
            });
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == 3);
        }

        private void publishMalformedRecord(String key) {
            ProducerRecord<String, byte[]> encoded = codec.encode(
                    EventTestUtils.asEventMessage(new ParcelDispatched(key, -1)), topic, key, null
            );
            CodecTestFixtures.makeTimestampUnreadable(encoded.headers());
            KafkaTestBroker.produce(List.of(encoded));
        }

        @Test
        void malformedRecordIsSkippedAndConsumptionContinues() {
            // given
            Recorder recorder = new Recorder();
            start(recorder, configuration());

            // when
            publishMalformedRecord("malformed");
            publish("malformed", 2);

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> recorder.handled.size() == 2);
            assertThat(recorder.handled).containsExactly("malformed-0", "malformed-1");
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == 3);
        }

        @Test
        void malformedRecordStopsItsPartitionUnderTheFailPolicyWhateverTheFailedRecordPolicy() {
            // given - failed records would be skipped, but the undecodable-record policy takes precedence
            Recorder recorder = new Recorder();
            start(recorder, configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL)
                                           .withFailedRecordPolicy(FailedRecordPolicy.SKIP));

            // when
            publishMalformedRecord("malformed");
            publish("malformed", 2);

            // then - the source commits up to the record, never past it, and processes nothing after it
            await().atMost(Duration.ofSeconds(30)).until(() -> committedOffsets().values().stream()
                                                                                 .anyMatch(o -> o.offset() == 0));
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                   .until(recorder.handled::isEmpty);
            assertThat(totalCommitted()).isZero();
        }
    }

    @Nested
    class Membership {

        @Test
        void rebalanceDuringProcessingRedeliversInsteadOfLosingEvents() {
            // given - the first instance blocks on its first event, longer than the revocation timeout
            Recorder first = new Recorder();
            CompletableFuture<Void> release = new CompletableFuture<>();
            AtomicInteger blocked = new AtomicInteger();
            first.action = event -> {
                if (blocked.getAndIncrement() == 0) {
                    release.orTimeout(30, TimeUnit.SECONDS).exceptionally(e -> null).join();
                }
            };
            KafkaSubscribableEventSourceConfiguration configuration =
                    configuration().withRevocationTimeout(Duration.ofMillis(500));
            start(first, configuration);
            for (int k = 0; k < 8; k++) {
                publish("parcel-" + k, 3);
            }
            await().atMost(Duration.ofSeconds(30)).until(() -> blocked.get() > 0);

            // when - a second instance joins, taking partitions away from the first
            Recorder second = new Recorder();
            start(second, configuration);
            // the broker delays the rebalance a few seconds, to let more members join
            await().atMost(Duration.ofSeconds(60)).until(() -> membersWithPartitions() == 2);
            release.complete(null);

            // then - every event is processed at least once
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                Set<String> all = ConcurrentHashMap.newKeySet();
                all.addAll(first.handled);
                all.addAll(second.handled);
                assertThat(all).hasSize(24);
            });
            // and processing of the revoked partition stopped after the event in progress, so that its new owner is the
            // only one processing the rest: just that event is handled twice
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                   .until(() -> first.handled.size() + second.handled.size() <= 25);
            List<String> all = new ArrayList<>(first.handled);
            all.addAll(second.handled);
            List<String> duplicates = new ArrayList<>(all);
            Set.copyOf(all).forEach(duplicates::remove);
            assertThat(duplicates).hasSizeLessThanOrEqualTo(1);
        }

        @Test
        void cancellingTheSubscriptionStopsConsumption() {
            // given
            KafkaSubscribableEventSource source = new KafkaSubscribableEventSource(
                    KafkaTestBroker.clientFactory(), configuration(), UnitOfWorkTestUtils.SIMPLE_FACTORY
            );
            List<EventMessage> received = new CopyOnWriteArrayList<>();
            var registration = source.subscribe((events, context) -> {
                received.addAll(events);
                return CompletableFuture.completedFuture(null);
            });
            source.start();
            cleanup.add(() -> source.shutdown().orTimeout(90, TimeUnit.SECONDS).join());
            publish("a", 1);
            await().atMost(Duration.ofSeconds(30)).until(() -> received.size() == 1);

            // when
            assertThat(registration.cancel()).isTrue();
            publish("a", 1);

            // then
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                   .untilAsserted(() -> assertThat(received).hasSize(1));
        }
    }

    @Nested
    class RetentionGaps {

        /**
         * Consumes the first {@code count} records of partition 0 in the test's consumer group, and stops, leaving the
         * group's committed offset of that partition at {@code count}.
         */
        private void consumeAndStop(int count) {
            KafkaSubscribableEventSource source = new KafkaSubscribableEventSource(
                    KafkaTestBroker.clientFactory(), configuration(), UnitOfWorkTestUtils.SIMPLE_FACTORY
            );
            List<EventMessage> received = new CopyOnWriteArrayList<>();
            source.subscribe((events, context) -> {
                received.addAll(events);
                return CompletableFuture.completedFuture(null);
            });
            source.start();
            await().atMost(Duration.ofSeconds(60)).until(() -> received.size() == count);
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == count);
            source.shutdown().orTimeout(90, TimeUnit.SECONDS).join();
        }

        private void publishToPartitionZero(int from, int count) {
            List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
            for (int i = from; i < from + count; i++) {
                ProducerRecord<String, byte[]> encoded = codec.encode(
                        EventTestUtils.asEventMessage(new ParcelDispatched("gap", i)), topic, "gap", null
                );
                records.add(new ProducerRecord<>(topic, 0, "gap", encoded.value(), encoded.headers()));
            }
            KafkaTestBroker.produce(records);
        }

        @Test
        void resumingGroupStopsAtARetentionGapByDefault() {
            // given - the group committed offset 3, and retention deleted the records before offset 5
            publishToPartitionZero(0, 3);
            consumeAndStop(3);
            publishToPartitionZero(3, 3);
            KafkaTestBroker.deleteRecordsBefore(new TopicPartition(topic, 0), 5);
            Recorder recorder = new Recorder();

            // when
            start(recorder, configuration());

            // then - nothing is processed or committed past the gap
            await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(recorder.handled).isEmpty();
                assertThat(totalCommitted()).isEqualTo(3L);
            });
        }

        @Test
        void resumingGroupContinuesFromTheEarliestRecordWhenConfiguredTo() {
            // given
            publishToPartitionZero(0, 3);
            consumeAndStop(3);
            publishToPartitionZero(3, 3);
            KafkaTestBroker.deleteRecordsBefore(new TopicPartition(topic, 0), 5);
            Recorder recorder = new Recorder();

            // when
            start(recorder, configuration().withRetentionGapPolicy(RetentionGapPolicy.RESET_TO_EARLIEST));

            // then
            await().atMost(Duration.ofSeconds(60)).until(() -> recorder.handled.size() == 1);
            assertThat(recorder.handled).containsExactly("gap-5");
            await().atMost(Duration.ofSeconds(30)).until(() -> totalCommitted() == 6);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
