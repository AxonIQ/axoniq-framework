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
import io.axoniq.framework.kafka.codec.KafkaEventCodec;
import io.axoniq.framework.kafka.codec.UndecodableRecordException;
import io.axoniq.framework.kafka.codec.UndecodableRecordPolicy;
import io.axoniq.framework.kafka.common.KafkaResources;
import io.axoniq.framework.kafka.common.RetentionGapPolicy;
import io.axoniq.framework.kafka.common.TopicSubscription;
import io.axoniq.framework.kafka.utils.ConsumedRecords;
import io.axoniq.framework.kafka.utils.RecordingEntitlementManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class KafkaPollingStreamTest {

    private static final String TOPIC = "parcels";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    private final DefaultKafkaEventCodec codec = CodecTestFixtures.defaultCodec();
    private final TestConsumer consumer = new TestConsumer();
    private final List<KafkaPollingStream<byte[]>> streams = new ArrayList<>();
    private final List<MessageStream<EventMessage>> started = new ArrayList<>();

    record ParcelDispatched(String parcelId) {

    }

    /**
     * A {@link MockConsumer} whose metadata lookups can fail a number of times after the first one.
     */
    static class TestConsumer extends MockConsumer<String, byte[]> {

        final AtomicInteger lookups = new AtomicInteger();
        final AtomicInteger failingRefreshes = new AtomicInteger();

        // The constructor taking a String, which kafka-clients 4 prefers, does not exist in the 3.x line.
        @SuppressWarnings("deprecation")
        TestConsumer() {
            super(OffsetResetStrategy.NONE);
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
            if (lookups.getAndIncrement() > 0 && failingRefreshes.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new TimeoutException("metadata not available");
            }
            return super.listTopics(timeout);
        }
    }

    @BeforeEach
    void setUp() {
        consumer.updatePartitions(TOPIC, List.of(new PartitionInfo(TOPIC, 0, null, null, null),
                                                 new PartitionInfo(TOPIC, 1, null, null, null)));
        consumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
    }

    @AfterEach
    void tearDown() {
        started.forEach(MessageStream::close);
        // the polling threads end before the next test reuses the consumer settings
        streams.forEach(stream -> assertThat(stream.terminated()).succeedsWithin(Duration.ofSeconds(5)));
    }

    /**
     * Looks up the partitions of the subscribed topic through the test consumer, so that tests can make lookups fail.
     */
    private Set<TopicPartition> partitions(Duration timeout) {
        return consumer.listTopics(timeout).getOrDefault(TOPIC, List.of()).stream()
                       .map(info -> new TopicPartition(info.topic(), info.partition()))
                       .collect(Collectors.toSet());
    }

    private KafkaStreamableEventSourceConfiguration configuration() {
        return KafkaStreamableEventSourceConfiguration.create(codec, TopicSubscription.topics(TOPIC))
                                                      .withPollTimeout(Duration.ofMillis(10));
    }

    private MessageStream<EventMessage> open(KafkaStreamableEventSourceConfiguration configuration,
                                             KafkaTrackingToken token) {
        return open(configuration, codec, token);
    }

    private MessageStream<EventMessage> open(KafkaStreamableEventSourceConfiguration configuration,
                                             KafkaEventCodec<byte[]> streamCodec,
                                             KafkaTrackingToken token) {
        return open(configuration, streamCodec, token, EventCriteria.havingAnyTag(), new RecordingEntitlementManager());
    }

    private MessageStream<EventMessage> open(KafkaStreamableEventSourceConfiguration configuration,
                                             KafkaEventCodec<byte[]> streamCodec,
                                             KafkaTrackingToken token,
                                             EventCriteria criteria,
                                             RecordingEntitlementManager entitlementManager) {
        KafkaPollingStream<byte[]> stream = new KafkaPollingStream<>(
                consumer, this::partitions, configuration, streamCodec,
                new KafkaPollingStream.StartPosition(token, false),
                criteria, entitlementManager
        );
        MessageStream<EventMessage> result = stream.start();
        streams.add(stream);
        started.add(result);
        return result;
    }

    private ConsumerRecord<String, byte[]> record(TopicPartition partition, long offset) {
        ProducerRecord<String, byte[]> produced = codec.encode(
                EventTestUtils.asEventMessage(new ParcelDispatched("p-" + offset)), TOPIC, null, null
        );
        return ConsumedRecords.consumed(produced, partition.partition(), offset);
    }

    private static MessageStream.Entry<EventMessage> next(MessageStream<EventMessage> stream) {
        List<MessageStream.Entry<EventMessage>> found = new ArrayList<>();
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            stream.next().ifPresent(found::add);
            return !found.isEmpty();
        });
        return found.getFirst();
    }

    private static KafkaTrackingToken token(MessageStream.Entry<EventMessage> entry) {
        return (KafkaTrackingToken) TrackingToken.fromContext(entry).orElseThrow();
    }

    @Nested
    class StartingPositions {

        @Test
        void partitionTheTokenDoesNotMentionStartsAtItsEarliestAvailableRecord() {
            // given - retention removed the first records of a partition the token does not mention
            consumer.updateBeginningOffsets(Map.of(P0, 5L, P1, 0L));
            consumer.schedulePollTask(() -> consumer.addRecord(record(P0, 5)));

            // when
            MessageStream<EventMessage> stream = open(configuration(), KafkaTrackingToken.of(Map.of(P1, 0L)));

            // then
            assertThat(token(next(stream)).offsetOf(P0)).isEqualTo(6L);
            assertThat(stream.error()).isEmpty();
        }

        @Test
        void explicitOffsetZeroBeforeTheEarliestAvailableRecordIsARetentionGap() {
            // given - retention removed records that the token, at offset 0 of the partition, has not read
            consumer.updateBeginningOffsets(Map.of(P0, 5L, P1, 0L));
            consumer.schedulePollTask(() -> consumer.addRecord(record(P0, 5)));

            // when
            MessageStream<EventMessage> stream = open(configuration(), KafkaTrackingToken.of(Map.of(P0, 0L)));

            // then - the default retention gap policy fails the stream
            await().atMost(Duration.ofSeconds(5)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).get(InstanceOfAssertFactories.THROWABLE)
                                      .isInstanceOf(OffsetOutOfRangeException.class);
        }

        @Test
        void explicitOffsetZeroBeforeTheEarliestAvailableRecordContinuesThereWhenResetting() {
            // given
            consumer.updateBeginningOffsets(Map.of(P0, 5L, P1, 0L));
            consumer.schedulePollTask(() -> consumer.addRecord(record(P0, 5)));

            // when
            MessageStream<EventMessage> stream = open(
                    configuration().withRetentionGapPolicy(RetentionGapPolicy.RESET_TO_EARLIEST),
                    KafkaTrackingToken.of(Map.of(P0, 0L))
            );

            // then
            assertThat(token(next(stream)).offsetOf(P0)).isEqualTo(6L);
            assertThat(stream.error()).isEmpty();
        }
    }

    @Nested
    class RetentionGaps {

        @Test
        void resetToEarliestMovesTheTokenOfTheResetPartition() {
            // given - the token is behind the log start of partition 0
            consumer.updateBeginningOffsets(Map.of(P0, 5L, P1, 0L));
            consumer.schedulePollTask(() -> consumer.setPollException(
                    new OffsetOutOfRangeException("out of range", Map.of(P0, 2L))
            ));
            consumer.schedulePollTask(() -> consumer.addRecord(record(P1, 0)));

            // when
            MessageStream<EventMessage> stream = open(
                    configuration().withRetentionGapPolicy(RetentionGapPolicy.RESET_TO_EARLIEST),
                    KafkaTrackingToken.of(Map.of(P0, 2L))
            );

            // then - an event of another partition carries the position partition 0 continues from
            MessageStream.Entry<EventMessage> entry = next(stream);
            assertThat(token(entry).offsetOf(P1)).isEqualTo(1L);
            assertThat(token(entry).offsetOf(P0)).isEqualTo(5L);
        }

        @Test
        void positionBeyondTheEndOfThePartitionFailsTheStreamEvenWhenResetting() {
            // given - the topic holds fewer records than the token says were read
            consumer.schedulePollTask(() -> consumer.setPollException(
                    new OffsetOutOfRangeException("out of range", Map.of(P0, 100L))
            ));

            // when
            MessageStream<EventMessage> stream = open(
                    configuration().withRetentionGapPolicy(RetentionGapPolicy.RESET_TO_EARLIEST),
                    KafkaTrackingToken.of(Map.of(P0, 100L))
            );

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).get(InstanceOfAssertFactories.THROWABLE)
                                      .isInstanceOf(IllegalStateException.class)
                                      .hasMessageContaining("beyond the end");
        }
    }

    @Nested
    class UndecodableRecords {

        @Test
        void skipsARecordWhoseDecodingThrowsAnyRuntimeException() {
            // given
            consumer.schedulePollTask(() -> {
                consumer.addRecord(record(P0, 0));
                consumer.addRecord(record(P0, 1));
            });
            KafkaEventCodec<byte[]> failing =
                    CodecTestFixtures.failingAt(codec, 0, new IllegalArgumentException("unknown event type"));

            // when
            MessageStream<EventMessage> stream = open(configuration(), failing, KafkaTrackingToken.empty());

            // then
            MessageStream.Entry<EventMessage> entry = next(stream);
            assertThat(entry.getResource(KafkaResources.OFFSET)).isEqualTo(1L);
            assertThat(token(entry).offsetOf(P0)).isEqualTo(2L);
            assertThat(stream.error()).isEmpty();
        }

        @Test
        void failsTheStreamWhenDecodingThrowsUnderTheFailPolicy() {
            // given
            consumer.schedulePollTask(() -> consumer.addRecord(record(P0, 0)));
            KafkaEventCodec<byte[]> failing =
                    CodecTestFixtures.failingAt(codec, 0, new IllegalArgumentException("unknown event type"));

            // when
            MessageStream<EventMessage> stream = open(
                    configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL),
                    failing,
                    KafkaTrackingToken.empty()
            );

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).get(InstanceOfAssertFactories.THROWABLE)
                                      .isInstanceOf(UndecodableRecordException.class)
                                      .hasCauseInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void skipsARecordThatCannotBeDeserialized() {
            // given
            consumer.schedulePollTask(() -> {
                consumer.addRecord(record(P0, 1));
                consumer.setPollException(ConsumedRecords.undeserializable(P0, 0));
            });

            // when
            MessageStream<EventMessage> stream = open(configuration(), KafkaTrackingToken.empty());

            // then - the event after the record carries a token past both records
            MessageStream.Entry<EventMessage> entry = next(stream);
            assertThat(token(entry).offsetOf(P0)).isEqualTo(2L);
            assertThat(stream.error()).isEmpty();
        }
    }

    @Nested
    class Failures {

        @Test
        void transientMetadataRefreshFailureKeepsTheStreamOpen() {
            // given
            consumer.failingRefreshes.set(2);
            consumer.schedulePollTask(() -> {
            });
            consumer.schedulePollTask(() -> {
            });
            consumer.schedulePollTask(() -> consumer.addRecord(record(P0, 0)));

            // when
            MessageStream<EventMessage> stream = open(
                    configuration().withMetadataRefreshInterval(Duration.ofMillis(1)),
                    KafkaTrackingToken.empty()
            );

            // then
            next(stream);
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.failingRefreshes.get() == 0);
            assertThat(stream.error()).isEmpty();
        }

        @Test
        void errorWhileReadingFailsTheStream() {
            // given - a codec that depends on a class that is missing at runtime
            KafkaEventCodec<byte[]> broken = CodecTestFixtures.decodingWith(codec, record -> {
                throw new NoClassDefFoundError("io/cloudevents/CloudEvent");
            });
            consumer.schedulePollTask(() -> consumer.addRecord(record(P0, 0)));

            // when
            MessageStream<EventMessage> stream = open(configuration(), broken, KafkaTrackingToken.empty());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).containsInstanceOf(NoClassDefFoundError.class);
            await().atMost(Duration.ofSeconds(5)).until(consumer::closed);
        }
    }

    @Nested
    class BackPressure {

        @Test
        void pausesThePartitionsWhileTheBufferIsFullAndResumesThemOnceDrained() {
            // given - more records than the buffer of one event holds
            consumer.schedulePollTask(() -> {
                consumer.addRecord(record(P0, 0));
                consumer.addRecord(record(P0, 1));
                consumer.addRecord(record(P0, 2));
            });

            // when
            MessageStream<EventMessage> stream = open(configuration().withBufferSize(1), KafkaTrackingToken.empty());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.paused().containsAll(List.of(P0, P1)));

            // when
            List<Long> offsets = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                offsets.add(next(stream).getResource(KafkaResources.OFFSET));
            }

            // then
            assertThat(offsets).containsExactly(0L, 1L, 2L);
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.paused().isEmpty());
        }
    }

    @Nested
    class Entitlement {

        private final DefaultKafkaEventCodec taggingCodec = codec.withTagResolver(
                event -> Set.of(Tag.of("parcelId", event.payloadAs(ParcelDispatched.class).parcelId()))
        );

        private ConsumerRecord<String, byte[]> taggedRecord(long offset) {
            ProducerRecord<String, byte[]> produced = taggingCodec.encode(
                    EventTestUtils.asEventMessage(new ParcelDispatched("p-" + offset)), TOPIC, null, null
            );
            return ConsumedRecords.consumed(produced, P0.partition(), offset);
        }

        @Test
        void claimsDeliveredEventsOnly() {
            // given - a matching event, a record without event, and an event the criteria filter out
            RecordingEntitlementManager entitlementManager = new RecordingEntitlementManager();
            consumer.schedulePollTask(() -> {
                consumer.addRecord(taggedRecord(0));
                consumer.addRecord(ConsumedRecords.foreign(TOPIC, 0, 1, "x", "not an event".getBytes()));
                consumer.addRecord(taggedRecord(2));
                consumer.addRecord(taggedRecord(3));
            });
            EventCriteria criteria = EventCriteria.havingTags(Tag.of("parcelId", "p-0"))
                                                  .or().havingTags(Tag.of("parcelId", "p-3"));

            // when
            MessageStream<EventMessage> stream = open(configuration(), taggingCodec, KafkaTrackingToken.empty(),
                                                      criteria, entitlementManager);
            next(stream);
            next(stream);

            // then
            assertThat(entitlementManager.claims()).extracting(RecordingEntitlementManager.Claim::count)
                                                   .containsOnly(2);
        }
    }

    @Nested
    class StreamFailures {

        @Test
        void recordThatCannotBeDeserializedFailsTheStreamUnderTheFailPolicy() {
            // given
            consumer.schedulePollTask(() -> consumer.setPollException(ConsumedRecords.undeserializable(P0, 0)));

            // when
            MessageStream<EventMessage> stream = open(
                    configuration().withUndecodableRecordPolicy(UndecodableRecordPolicy.FAIL),
                    KafkaTrackingToken.empty()
            );

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).containsInstanceOf(UndecodableRecordException.class);
        }

        @Test
        void wakeUpWhileRunningFailsTheStream() {
            // given - woken up by something other than closing the stream
            consumer.schedulePollTask(() -> consumer.setPollException(new WakeupException()));

            // when
            MessageStream<EventMessage> stream = open(configuration(), KafkaTrackingToken.empty());

            // then
            await().atMost(Duration.ofSeconds(5)).until(() -> stream.next().isEmpty() && stream.error().isPresent());
            assertThat(stream.error()).containsInstanceOf(WakeupException.class);
        }
    }

    @Nested
    class Closing {

        @Test
        void closingTheStreamStopsThePollingThreadAndClosesTheConsumer() throws Exception {
            // given
            KafkaPollingStream<byte[]> stream = new KafkaPollingStream<>(
                    consumer, KafkaPollingStreamTest.this::partitions, configuration(), codec,
                    new KafkaPollingStream.StartPosition(KafkaTrackingToken.empty(), false),
                    EventCriteria.havingAnyTag(), new RecordingEntitlementManager()
            );
            MessageStream<EventMessage> started = stream.start();

            // when
            started.close();

            // then
            stream.terminated().get(5, TimeUnit.SECONDS);
            assertThat(consumer.closed()).isTrue();
        }
    }
}
