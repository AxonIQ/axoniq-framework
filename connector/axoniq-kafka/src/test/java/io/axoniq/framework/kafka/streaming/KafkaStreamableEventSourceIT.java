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
import io.axoniq.framework.kafka.codec.UndecodableRecordException;
import io.axoniq.framework.kafka.codec.UndecodableRecordPolicy;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.utils.KafkaTestBroker;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.StreamingCondition;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class KafkaStreamableEventSourceIT {

    private final DefaultKafkaEventCodec codec =
            CodecTestFixtures.defaultCodec()
                    .withTagResolver(e -> Set.of(Tag.of("parcelId", e.payloadAs(ParcelDispatched.class).parcelId())));
    private final List<MessageStream<EventMessage>> openStreams = new ArrayList<>();
    private KafkaStreamableEventSource testSubject;
    private String topic;

    record ParcelDispatched(String parcelId, int sequence) {

    }

    @BeforeEach
    void setUp() {
        topic = KafkaTestBroker.createTopic(KafkaTestBroker.uniqueTopic("streamable"), 2);
        testSubject = source(KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic)));
    }

    @AfterEach
    void tearDown() {
        openStreams.forEach(MessageStream::close);
        testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();
    }

    private void replaceSource(KafkaStreamableEventSourceConfiguration configuration) {
        testSubject.shutdown().orTimeout(30, TimeUnit.SECONDS).join();
        testSubject = source(configuration);
    }

    private KafkaStreamableEventSource source(KafkaStreamableEventSourceConfiguration configuration) {
        return new KafkaStreamableEventSource(KafkaTestBroker.clientFactory(), configuration);
    }

    private void publish(String key, int count) {
        publish(topic, null, key, count);
    }

    private void publish(String topic, @Nullable Integer partition, String key, int count) {
        publish(topic, partition, key, count, null);
    }

    /**
     * Publishes {@code count} events with the given {@code key}, with the given record {@code timestamp}, or the
     * event's timestamp when {@code null}.
     */
    private void publish(String topic,
                         @Nullable Integer partition,
                         String key,
                         int count,
                         @Nullable Instant timestamp) {
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched(key, i));
            ProducerRecord<String, byte[]> encoded = codec.encode(event, topic, key, null);
            Long recordTimestamp = timestamp != null ? Long.valueOf(timestamp.toEpochMilli()) : encoded.timestamp();
            records.add(new ProducerRecord<>(topic, partition, recordTimestamp, key, encoded.value(),
                                             encoded.headers()));
        }
        KafkaTestBroker.produce(records);
    }

    private MessageStream<EventMessage> open(StreamingCondition condition) {
        MessageStream<EventMessage> stream = testSubject.open(condition, null);
        openStreams.add(stream);
        return stream;
    }

    private static List<MessageStream.Entry<EventMessage>> read(MessageStream<EventMessage> stream, int count) {
        List<MessageStream.Entry<EventMessage>> result = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            Optional<MessageStream.Entry<EventMessage>> next;
            while (result.size() < count && (next = stream.next()).isPresent()) {
                result.add(next.get());
            }
            return result.size() >= count;
        });
        return result;
    }

    private static ParcelDispatched payload(MessageStream.Entry<EventMessage> entry) {
        return Objects.requireNonNull(entry.message().payloadAs(ParcelDispatched.class));
    }

    private static KafkaTrackingToken token(MessageStream.Entry<EventMessage> entry) {
        return (KafkaTrackingToken) TrackingToken.fromContext(entry).orElseThrow();
    }

    @Nested
    class Reading {

        @Test
        void readsAllPartitionsInOrderPerPartitionWithResources() {
            // given
            publish("a", 5);
            publish("b", 5);

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(null)), 10);

            // then
            assertThat(entries).extracting(e -> payload(e).parcelId() + payload(e).sequence())
                               .containsSubsequence("a0", "a1", "a2", "a3", "a4")
                               .containsSubsequence("b0", "b1", "b2", "b3", "b4");
            MessageStream.Entry<EventMessage> first = entries.getFirst();
            TopicPartition partition = first.getResource(KafkaResources.TOPIC_PARTITION);
            assertThat(partition.topic()).isEqualTo(topic);
            assertThat(first.getResource(KafkaResources.OFFSET)).isZero();
            assertThat(first.getResource(KafkaResources.RECORD_KEY)).isIn("a", "b");
            assertThat(token(first).offsetOf(partition)).isEqualTo(1L);
        }

        @Test
        void tokensAreCumulativeAcrossPartitions() {
            // given
            publish("a", 3);
            publish("b", 3);

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(null)), 6);

            // then
            KafkaTrackingToken last = token(entries.getLast());
            assertThat(last.position()).hasValue(6L);
            for (int i = 1; i < entries.size(); i++) {
                assertThat(token(entries.get(i)).covers(token(entries.get(i - 1)))).isTrue();
            }
        }

        @Test
        void resumesFromTokenWithoutRereadingOrSkipping() {
            // given
            publish("a", 6);
            List<MessageStream.Entry<EventMessage>> firstRun = read(open(StreamingCondition.startingFrom(null)), 3);
            openStreams.getFirst().close();

            // when
            List<MessageStream.Entry<EventMessage>> secondRun =
                    read(open(StreamingCondition.startingFrom(token(firstRun.getLast()))), 3);

            // then
            assertThat(secondRun).extracting(e -> payload(e).sequence()).containsExactly(3, 4, 5);
        }

        @Test
        void resumesFromAxonFramework4TokenWithoutRereadingOrSkipping() {
            // given - Axon Framework 4 stored the offset of the last consumed record
            publish(topic, 0, "a", 6);
            LegacyKafkaTrackingToken legacy = new LegacyKafkaTrackingToken(Map.of(topic + "-0", 2L));

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(legacy)), 3);

            // then
            assertThat(entries).extracting(e -> payload(e).sequence()).containsExactly(3, 4, 5);
        }

        @Test
        void startsAtEndFromLatestToken() {
            // given
            publish("early", 3);
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(TrackingToken.LATEST));
            // none of the events published before arrive, which also gives the stream time to find the end
            await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).until(() -> stream.next().isEmpty());

            // when
            publish("late", 1);

            // then
            assertThat(read(stream, 1)).extracting(e -> payload(e).parcelId()).containsExactly("late");
        }

        @Test
        void filtersOnCriteriaWhileAdvancingTokens() {
            // given
            publish(topic, 0, "a", 2);
            publish(topic, 0, "b", 2);
            publish(topic, 0, "a", 1);

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.conditionFor(
                    TrackingToken.FIRST, EventCriteria.havingTags(Tag.of("parcelId", "a"))
            )), 3);

            // then
            assertThat(entries).extracting(e -> payload(e).parcelId()).containsOnly("a");
            assertThat(token(entries.getLast()).offsetOf(new TopicPartition(topic, 0))).isEqualTo(5L);
        }

        @Test
        void picksUpPartitionsAddedLater() {
            // given
            replaceSource(KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                                        .withMetadataRefreshInterval(
                                                                                Duration.ofMillis(200)));
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(null));

            // when
            KafkaTestBroker.increasePartitions(topic, 3);
            publish(topic, 2, "c", 2);

            // then
            assertThat(read(stream, 2)).allSatisfy(e -> assertThat(e.getResource(KafkaResources.TOPIC_PARTITION))
                    .isEqualTo(new TopicPartition(topic, 2)));
        }

        @Test
        void readsTheNamedTopicsThatExistAndPicksUpAMissingOneOnceCreated() {
            // given - one of the named topics does not exist yet
            String missing = KafkaTestBroker.uniqueTopic("streamable-missing");
            TopicSubscription subscription = TopicSubscription.topics(topic, missing);
            replaceSource(KafkaStreamableEventSourceConfiguration.create(codec, subscription)
                                                                        .withMetadataRefreshInterval(
                                                                                Duration.ofMillis(200)));
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(null));
            publish(topic, 0, "a", 1);
            assertThat(read(stream, 1)).singleElement().satisfies(
                    e -> assertThat(e.getResource(KafkaResources.TOPIC_PARTITION).topic()).isEqualTo(topic)
            );

            // when
            KafkaTestBroker.createTopic(missing, 1);
            publish(missing, 0, "b", 1);

            // then
            assertThat(read(stream, 1)).singleElement().satisfies(
                    e -> assertThat(e.getResource(KafkaResources.TOPIC_PARTITION).topic()).isEqualTo(missing)
            );
            assertThat(stream.error()).isEmpty();
        }

        @Test
        void patternSubscriptionPicksUpTopicsCreatedLater() {
            // given
            String prefix = KafkaTestBroker.uniqueTopic("pattern");
            replaceSource(KafkaStreamableEventSourceConfiguration.create(
                    codec, TopicSubscription.pattern(Pattern.compile(Pattern.quote(prefix) + "-.*"))
            ).withMetadataRefreshInterval(Duration.ofMillis(200)));
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(null));

            // when
            String created = KafkaTestBroker.createTopic(prefix + "-late", 1);
            publish(created, 0, "late", 2);

            // then
            assertThat(read(stream, 2)).allSatisfy(e -> assertThat(e.getResource(KafkaResources.TOPIC_PARTITION))
                    .isEqualTo(new TopicPartition(created, 0)));
        }

        @Test
        void appliesBackPressureWithoutLosingEvents() {
            // given
            replaceSource(KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                                        .withBufferSize(2));
            publish(topic, 0, "a", 50);
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(null));

            // when - read only after the polling thread had time to fill the small buffer
            await().pollDelay(Duration.ofMillis(500)).until(() -> true);
            List<MessageStream.Entry<EventMessage>> entries = read(stream, 50);

            // then
            assertThat(entries).extracting(e -> payload(e).sequence())
                               .containsExactlyElementsOf(IntStream.range(0, 50).boxed().toList());
        }
    }

    @Nested
    class UndecodableRecords {

        private void publishForeignRecord() {
            KafkaTestBroker.produce(List.of(new ProducerRecord<>(topic, 0, "x", "not an event".getBytes(
                    StandardCharsets.UTF_8))));
        }

        private void publishMalformedRecord() {
            ProducerRecord<String, byte[]> encoded = codec.encode(
                    EventTestUtils.asEventMessage(new ParcelDispatched("malformed", 0)), topic, "x", null
            );
            CodecTestFixtures.makeTimestampUnreadable(encoded.headers());
            KafkaTestBroker.produce(List.of(new ProducerRecord<>(topic, 0, "x", encoded.value(),
                                                                 encoded.headers())));
        }

        @Test
        void malformedRecordIsSkippedAndAdvancesTheToken() {
            // given
            publishMalformedRecord();
            publish(topic, 0, "a", 1);

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(null)), 1);

            // then
            assertThat(payload(entries.getFirst()).parcelId()).isEqualTo("a");
            assertThat(token(entries.getFirst()).offsetOf(new TopicPartition(topic, 0))).isEqualTo(2L);
        }

        @Test
        void malformedRecordFailsTheStreamUnderTheFailPolicy() {
            // given
            replaceSource(KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                                        .withUndecodableRecordPolicy(
                                                                                UndecodableRecordPolicy.FAIL));
            publishMalformedRecord();
            publish(topic, 0, "a", 1);

            // when
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(null));

            // then - the policy rejects the record, and nothing after it is delivered
            List<MessageStream.Entry<EventMessage>> delivered = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                stream.next().ifPresent(delivered::add);
                return stream.error().isPresent();
            });
            assertThat(delivered).isEmpty();
            assertThat(stream.error()).get(InstanceOfAssertFactories.THROWABLE)
                                      .isInstanceOf(UndecodableRecordException.class)
                                      .hasMessageContaining("offset [0] of partition [0]")
                                      .hasCauseInstanceOf(UndecodableRecordException.class);
        }

        @Test
        void skippedRecordsAdvanceTheToken() {
            // given
            publishForeignRecord();
            publish(topic, 0, "a", 1);

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(null)), 1);

            // then
            assertThat(token(entries.getFirst()).offsetOf(new TopicPartition(topic, 0))).isEqualTo(2L);
        }

        @Test
        void failPolicyFailsTheStream() {
            // given
            replaceSource(KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                                        .withUndecodableRecordPolicy(
                                                                                UndecodableRecordPolicy.FAIL));
            publishForeignRecord();

            // when
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(null));

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).containsInstanceOf(UndecodableRecordException.class);
        }
    }

    @Nested
    class RetentionGaps {

        private KafkaTrackingToken tokenBeforeDeletedRecords() {
            return tokenBeforeDeletedRecords(1L);
        }

        private KafkaTrackingToken tokenBeforeDeletedRecords(long offset) {
            publish(topic, 0, "a", 5);
            KafkaTestBroker.deleteRecordsBefore(new TopicPartition(topic, 0), 3);
            return KafkaTrackingToken.of(Map.of(new TopicPartition(topic, 0), offset));
        }

        @Test
        void failsTheStreamForATokenAtTheFirstOffsetOfThePartitionByDefault() {
            // given - the token has read nothing of the partition, and Kafka deleted its first records since
            KafkaTrackingToken token = tokenBeforeDeletedRecords(0L);

            // when
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(token));

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).containsInstanceOf(OffsetOutOfRangeException.class);
        }

        @Test
        void continuesFromEarliestForATokenAtTheFirstOffsetOfThePartitionWhenConfigured() {
            // given
            replaceSource(KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                                        .withRetentionGapPolicy(
                                                                                RetentionGapPolicy.RESET_TO_EARLIEST));
            KafkaTrackingToken token = tokenBeforeDeletedRecords(0L);

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(token)), 2);

            // then
            assertThat(entries).extracting(e -> payload(e).sequence()).containsExactly(3, 4);
        }

        @Test
        void readsAPartitionTheTokenDoesNotMentionFromItsEarliestAvailableRecord() {
            // given - the token mentions the other partition only
            publish(topic, 0, "a", 5);
            KafkaTestBroker.deleteRecordsBefore(new TopicPartition(topic, 0), 3);
            KafkaTrackingToken token = KafkaTrackingToken.of(Map.of(new TopicPartition(topic, 1), 0L));

            // when
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(token));
            List<MessageStream.Entry<EventMessage>> entries = read(stream, 2);

            // then
            assertThat(entries).extracting(e -> payload(e).sequence()).containsExactly(3, 4);
            assertThat(stream.error()).isEmpty();
        }

        @Test
        void failsTheStreamByDefault() {
            // given
            KafkaTrackingToken token = tokenBeforeDeletedRecords();

            // when
            MessageStream<EventMessage> stream = open(StreamingCondition.startingFrom(token));

            // then
            await().atMost(Duration.ofSeconds(30)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).containsInstanceOf(OffsetOutOfRangeException.class);
        }

        @Test
        void continuesFromEarliestWhenConfigured() {
            // given
            replaceSource(KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                                        .withRetentionGapPolicy(
                                                                                RetentionGapPolicy.RESET_TO_EARLIEST));
            KafkaTrackingToken token = tokenBeforeDeletedRecords();

            // when
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(token)), 2);

            // then
            assertThat(entries).extracting(e -> payload(e).sequence()).containsExactly(3, 4);
        }
    }

    @Nested
    class TokenSource {

        @Test
        void firstAndLatestTokensReflectTopicContents() {
            // given
            publish(topic, 0, "a", 3);
            publish(topic, 1, "b", 2);

            // when
            TrackingToken first = testSubject.firstToken(null).orTimeout(30, TimeUnit.SECONDS).join();
            TrackingToken latest = testSubject.latestToken(null).orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(first).isEqualTo(KafkaTrackingToken.empty());
            assertThat(latest).isEqualTo(KafkaTrackingToken.of(Map.of(new TopicPartition(topic, 0), 3L,
                                                                       new TopicPartition(topic, 1), 2L)));
        }

        @Test
        void tokenAtReplaysFromTheGivenTime() {
            // given
            Instant cutOff = Instant.now().minus(Duration.ofHours(1));
            publish(topic, 0, "a", 2, cutOff.minusSeconds(60));
            publish(topic, 0, "late", 2, cutOff.plusSeconds(60));

            // when
            TrackingToken at = testSubject.tokenAt(cutOff, null).orTimeout(30, TimeUnit.SECONDS).join();
            List<MessageStream.Entry<EventMessage>> entries = read(open(StreamingCondition.startingFrom(at)), 2);

            // then
            assertThat(entries).extracting(e -> payload(e).parcelId()).containsOnly("late");
            assertThat(((KafkaTrackingToken) at).offsetOf(new TopicPartition(topic, 1))).isZero();
        }

        @Test
        void tokenAtStartsAPartitionWithoutLaterRecordsAtItsEnd() {
            // given - partition 1 only holds records from before the cut-off
            Instant cutOff = Instant.now().minus(Duration.ofHours(1));
            publish(topic, 0, "a", 1, cutOff.plusSeconds(60));
            publish(topic, 1, "b", 3, cutOff.minusSeconds(60));

            // when
            TrackingToken at = testSubject.tokenAt(cutOff, null).orTimeout(30, TimeUnit.SECONDS).join();

            // then - Kafka has no offset for partition 1 at that time, so it starts at its end
            assertThat(((KafkaTrackingToken) at).offsetOf(new TopicPartition(topic, 0))).isZero();
            assertThat(((KafkaTrackingToken) at).offsetOf(new TopicPartition(topic, 1))).isEqualTo(3L);
        }

        @Test
        void latestTokenFollowsTheIsolationLevelOfTheSource() {
            // given - records of a transaction that is still open
            Map<String, Object> properties = Map.of(
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaTestBroker.bootstrapServers(),
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, topic + "-open"
            );
            try (KafkaProducer<String, byte[]> producer =
                         new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer())) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, 0, "open", "uncommitted".getBytes(StandardCharsets.UTF_8)));
                producer.send(new ProducerRecord<>(topic, 0, "open", "uncommitted".getBytes(StandardCharsets.UTF_8)));
                producer.flush();
                KafkaStreamableEventSource uncommitted = source(
                        KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(topic))
                                                              .withConsumerProperties(Map.of(
                                                                      ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                                                                      "read_uncommitted"
                                                              ))
                );

                // when
                TrackingToken committedLatest = testSubject.latestToken(null).orTimeout(30, TimeUnit.SECONDS).join();
                TrackingToken uncommittedLatest;
                try {
                    uncommittedLatest = uncommitted.latestToken(null).orTimeout(30, TimeUnit.SECONDS).join();
                } finally {
                    uncommitted.shutdown().orTimeout(30, TimeUnit.SECONDS).join();
                }

                // then
                TopicPartition partition = new TopicPartition(topic, 0);
                assertThat(((KafkaTrackingToken) committedLatest).offsetOf(partition)).isZero();
                assertThat(((KafkaTrackingToken) uncommittedLatest).offsetOf(partition)).isEqualTo(2L);
                producer.abortTransaction();
            }
        }

        @Test
        void tokenAtFutureTimeIsTheLatestToken() {
            // given
            publish(topic, 0, "a", 2);

            // when
            TrackingToken at = testSubject.tokenAt(Instant.now().plusSeconds(3600), null)
                                          .orTimeout(30, TimeUnit.SECONDS).join();

            // then
            assertThat(at).isEqualTo(testSubject.latestToken(null).orTimeout(30, TimeUnit.SECONDS).join());
        }
    }
}
