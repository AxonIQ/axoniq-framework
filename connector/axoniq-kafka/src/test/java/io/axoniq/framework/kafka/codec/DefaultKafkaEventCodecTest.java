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

package io.axoniq.framework.kafka.codec;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.kafka.codec.CodecTestFixtures.header;
import static io.axoniq.framework.kafka.utils.ConsumedRecords.consumed;
import static io.axoniq.framework.kafka.utils.ConsumedRecords.foreign;
import static org.assertj.core.api.Assertions.*;

class DefaultKafkaEventCodecTest {

    private static final String TOPIC = "parcels";

    private final EventConverter converter = new DelegatingEventConverter(new JacksonConverter());
    private final DefaultKafkaEventCodec testSubject = new DefaultKafkaEventCodec(converter);

    record ParcelDispatched(String parcelId, int weight) {

    }

    private static EventMessage parcelDispatched() {
        return EventTestUtils.asEventMessage(new ParcelDispatched("parcel-1", 12));
    }

    @Nested
    class Encode {

        @Test
        void writesEventIdentityAsReadableHeaders() {
            // given
            EventMessage event = parcelDispatched();

            // when
            ProducerRecord<String, byte[]> result = testSubject.encode(event, TOPIC, null, null);

            // then
            assertThat(result.topic()).isEqualTo(TOPIC);
            assertThat(header(result.headers(), "axon-id")).isEqualTo(event.identifier());
            assertThat(header(result.headers(), "axon-type")).isEqualTo(event.type().name());
            assertThat(header(result.headers(), "axon-version")).isEqualTo(event.type().version());
            assertThat(header(result.headers(), "axon-timestamp")).isEqualTo(event.timestamp().toString());
        }

        @Test
        void writesEachMetadataEntryAsHeader() {
            // given
            EventMessage event = parcelDispatched().andMetadata(Map.of("traceId", "t-1", "tenant", "acme"));

            // when
            ProducerRecord<String, byte[]> result = testSubject.encode(event, TOPIC, null, null);

            // then
            assertThat(header(result.headers(), "axon-metadata-traceId")).isEqualTo("t-1");
            assertThat(header(result.headers(), "axon-metadata-tenant")).isEqualTo("acme");
        }

        @Test
        void usesEventTimestampAsRecordTimestampAndLeavesPartitionToKafka() {
            // given
            EventMessage event = parcelDispatched();

            // when
            ProducerRecord<String, byte[]> result = testSubject.encode(event, TOPIC, "parcel-1", null);

            // then
            assertThat(result.timestamp()).isEqualTo(event.timestamp().toEpochMilli());
            assertThat(result.partition()).isNull();
            assertThat(result.key()).isEqualTo("parcel-1");
        }

        @Test
        void writesPayloadConvertedToBytes() {
            // given
            EventMessage event = parcelDispatched();

            // when
            ProducerRecord<String, byte[]> result = testSubject.encode(event, TOPIC, null, null);

            // then
            assertThat(new String(result.value(), StandardCharsets.UTF_8))
                    .contains("\"parcelId\":\"parcel-1\"")
                    .contains("\"weight\":12");
        }

        @Test
        void writesNoTagsWithoutTagResolver() {
            // when
            ProducerRecord<String, byte[]> result = testSubject.encode(parcelDispatched(), TOPIC, null, null);

            // then
            assertThat(result.headers()).noneMatch(h -> h.key().startsWith("axon-tag-"));
        }

        @Test
        void tagResolverReceivesTheProcessingContext() {
            // given
            AtomicReference<ProcessingContext> seen = new AtomicReference<>();
            DefaultKafkaEventCodec tagging = testSubject.withTagResolver(new TagResolver() {
                @Override
                public Set<Tag> resolve(EventMessage event) {
                    return Set.of();
                }

                @Override
                public Set<Tag> resolve(EventMessage event, @Nullable ProcessingContext context) {
                    seen.set(context);
                    return Set.of();
                }
            });
            ProcessingContext context = new StubProcessingContext();

            // when
            tagging.encode(parcelDispatched(), TOPIC, null, context);

            // then
            assertThat(seen.get()).isSameAs(context);
        }

        @Test
        void writesOneHeaderPerTagWithTagResolver() {
            // given
            DefaultKafkaEventCodec tagging = testSubject.withTagResolver(
                    e -> Set.of(Tag.of("parcelId", "parcel-1"), Tag.of("region", "eu"), Tag.of("region", "us"))
            );

            // when
            ProducerRecord<String, byte[]> result = tagging.encode(parcelDispatched(), TOPIC, null, null);

            // then
            assertThat(result.headers().headers("axon-tag-region"))
                    .extracting(h -> new String(h.value(), StandardCharsets.UTF_8))
                    .containsExactlyInAnyOrder("eu", "us");
            assertThat(header(result.headers(), "axon-tag-parcelId")).isEqualTo("parcel-1");
        }
    }

    @Nested
    class RoundTrip {

        @Test
        void decodesWhatWasEncoded() {
            // given
            EventMessage event = parcelDispatched().andMetadata(Map.of("traceId", "t-1"));
            ConsumerRecord<String, byte[]> record = consumed(testSubject.encode(event, TOPIC, "parcel-1", null), 3, 42);

            // when
            Optional<DecodedRecord> result = testSubject.decode(record);

            // then
            assertThat(result).isPresent();
            EventMessage decoded = result.get().event();
            assertThat(decoded.identifier()).isEqualTo(event.identifier());
            assertThat(decoded.type()).isEqualTo(event.type());
            assertThat(decoded.timestamp()).isEqualTo(event.timestamp());
            assertThat(decoded.metadata()).isEqualTo(event.metadata());
            assertThat(decoded.payloadAs(ParcelDispatched.class)).isEqualTo(new ParcelDispatched("parcel-1", 12));
        }

        @Test
        void keepsMetadataEntriesWithoutValue() {
            // given
            Map<String, @Nullable String> metadata = new HashMap<>();
            metadata.put("empty", null);
            EventMessage event = parcelDispatched().withMetadata(metadata);
            ConsumerRecord<String, byte[]> record = consumed(testSubject.encode(event, TOPIC, null, null), 0, 0);

            // when
            EventMessage decoded = testSubject.decode(record).orElseThrow().event();

            // then
            assertThat(decoded.metadata()).containsKey("empty");
            assertThat(decoded.metadata().get("empty")).isNull();
        }

        @Test
        void decodesTagsIntoResources() {
            // given
            DefaultKafkaEventCodec tagging = testSubject.withTagResolver(
                    e -> Set.of(Tag.of("parcelId", "parcel-1"), Tag.of("region", "eu"))
            );
            ConsumerRecord<String, byte[]> record =
                    consumed(tagging.encode(parcelDispatched(), TOPIC, null, null), 0, 0);

            // when - decoding does not need a tag resolver
            DecodedRecord result = testSubject.decode(record).orElseThrow();

            // then
            assertThat(Tag.fromContext(result.resources()))
                    .contains(Set.of(Tag.of("parcelId", "parcel-1"), Tag.of("region", "eu")));
        }

        @Test
        void ignoresATagHeaderWithoutValue() {
            // given
            ProducerRecord<String, byte[]> produced = testSubject.encode(parcelDispatched(), TOPIC, null, null);
            produced.headers().add(DefaultKafkaEventCodec.TAG_HEADER_PREFIX + "region", null);

            // when
            DecodedRecord result = testSubject.decode(consumed(produced, 0, 0)).orElseThrow();

            // then
            assertThat(Tag.fromContext(result.resources())).contains(Set.of());
        }

        @Test
        void decodesEmptyTagsWhenNoneWereWritten() {
            // given
            ConsumerRecord<String, byte[]> record =
                    consumed(testSubject.encode(parcelDispatched(), TOPIC, null, null), 0, 0);

            // when
            DecodedRecord result = testSubject.decode(record).orElseThrow();

            // then
            assertThat(Tag.fromContext(result.resources())).contains(Set.of());
        }

        @Test
        void decodesNullPayload() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("p", 1));
            ProducerRecord<String, byte[]> produced = testSubject.encode(event, TOPIC, null, null);
            ProducerRecord<String, byte[]> withoutValue = new ProducerRecord<>(
                    TOPIC, null, produced.timestamp(), null, null, produced.headers()
            );

            // when
            EventMessage decoded = testSubject.decode(consumed(withoutValue, 0, 0)).orElseThrow().event();

            // then
            assertThat(decoded.payload()).isNull();
        }
    }

    @Nested
    class Decode {

        @Test
        void returnsEmptyForRecordWithoutAxonHeaders() {
            // given
            ConsumerRecord<String, byte[]> record = foreign(TOPIC, 0, 7, "key", "{}".getBytes(StandardCharsets.UTF_8));

            // when
            Optional<DecodedRecord> result = testSubject.decode(record);

            // then
            assertThat(result).isEmpty();
        }

        @Test
        void returnsEmptyWhenTypeHeaderIsMissing() {
            // given
            RecordHeaders headers = new RecordHeaders();
            headers.add("axon-id", "id-1".getBytes(StandardCharsets.UTF_8));
            ConsumerRecord<String, byte[]> record = record(headers, 0L);

            // when / then
            assertThat(testSubject.decode(record)).isEmpty();
        }

        @Test
        void resolvesMissingVersionThroughEventTypeResolver() {
            // given
            RecordHeaders headers = minimalHeaders();
            DefaultKafkaEventCodec custom =
                    testSubject.withEventTypeResolver(EventTypeResolver.withDefaultVersion("9.9"));

            // when
            EventMessage decoded = custom.decode(record(headers, 0L)).orElseThrow().event();

            // then
            assertThat(decoded.type()).isEqualTo(new MessageType("com.example.ParcelDispatched", "9.9"));
        }

        @Test
        void fallsBackToRecordTimestampWithoutTimestampHeader() {
            // given
            Instant recordTime = Instant.parse("2026-10-10T12:00:00Z");

            // when
            EventMessage decoded = testSubject.decode(record(minimalHeaders(), recordTime.toEpochMilli()))
                                              .orElseThrow().event();

            // then
            assertThat(decoded.timestamp()).isEqualTo(recordTime);
            assertThat(decoded.metadata()).isEqualTo(Metadata.emptyInstance());
        }

        @Test
        void timestampsARecordWithoutAnyTimestampOnReading() {
            // given
            Instant before = Instant.now();

            // when
            EventMessage decoded = testSubject.decode(record(minimalHeaders(), ConsumerRecord.NO_TIMESTAMP))
                                              .orElseThrow().event();

            // then
            assertThat(decoded.timestamp()).isAfterOrEqualTo(before);
        }

        @Test
        void rejectsUnreadableTimestampHeader() {
            // given
            RecordHeaders headers = minimalHeaders();
            headers.add("axon-timestamp", "yesterday".getBytes(StandardCharsets.UTF_8));

            // when / then
            assertThatThrownBy(() -> testSubject.decode(record(headers, 0L)))
                    .isInstanceOf(UndecodableRecordException.class)
                    .hasMessageContaining("yesterday")
                    .hasMessageContaining(TOPIC);
        }

        private RecordHeaders minimalHeaders() {
            RecordHeaders headers = new RecordHeaders();
            headers.add("axon-id", "id-1".getBytes(StandardCharsets.UTF_8));
            headers.add("axon-type", "com.example.ParcelDispatched".getBytes(StandardCharsets.UTF_8));
            return headers;
        }

        private ConsumerRecord<String, byte[]> record(RecordHeaders headers, long timestamp) {
            return new ConsumerRecord<>(TOPIC, 0, 0, timestamp, TimestampType.CREATE_TIME, -1, -1,
                                        null, new byte[0], headers, Optional.empty());
        }
    }

    @Test
    void valueSerdeHandlesBytes() {
        // given
        byte[] value = {1, 2, 3};

        // when
        byte[] result = testSubject.valueSerde().deserializer()
                                   .deserialize(TOPIC, testSubject.valueSerde().serializer().serialize(TOPIC, value));

        // then
        assertThat(result).isEqualTo(value);
    }
}
