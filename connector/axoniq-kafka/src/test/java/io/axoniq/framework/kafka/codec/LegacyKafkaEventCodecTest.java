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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.kafka.utils.ConsumedRecords.consumed;
import static org.assertj.core.api.Assertions.*;

class LegacyKafkaEventCodecTest {

    private static final String TOPIC = "parcels";

    private final EventConverter converter = new DelegatingEventConverter(new JacksonConverter());
    private final LegacyKafkaEventCodec testSubject =
            new LegacyKafkaEventCodec(new DefaultKafkaEventCodec(converter), converter);

    record ParcelDispatched(String parcelId) {

    }

    private static byte[] text(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] number(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static @Nullable String textHeader(ProducerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * The headers the Axon Framework 4 extension writes for a domain event, as its HeaderUtils produced them: strings
     * as UTF-8, numbers and timestamps as 8-byte big-endian longs, metadata values as their string form.
     */
    private static RecordHeaders axonFramework4DomainEventHeaders(Instant timestamp) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("axon-metadata-traceId", text("trace-1"));
        headers.add("axon-message-id", text("event-1"));
        headers.add("axon-message-type", text("com.example.ParcelDispatched"));
        headers.add("axon-message-revision", text("2"));
        headers.add("axon-message-timestamp", number(timestamp.toEpochMilli()));
        headers.add("axon-message-aggregate-type", text("Parcel"));
        headers.add("axon-message-aggregate-id", text("parcel-1"));
        headers.add("axon-message-aggregate-seq", number(7));
        return headers;
    }

    private static ConsumerRecord<String, byte[]> record(RecordHeaders headers, byte[] value) {
        return new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, -1, -1, "parcel-1", value, headers,
                                    Optional.empty());
    }

    @Nested
    class ReadingAxonFramework4Records {

        @Test
        void decodesEventFromLegacyHeaders() {
            // given
            Instant timestamp = Instant.parse("2025-03-01T10:00:00Z");
            byte[] payload = text("{\"parcelId\":\"parcel-1\"}");

            // when
            DecodedRecord result = testSubject.decode(record(axonFramework4DomainEventHeaders(timestamp), payload))
                                              .orElseThrow();

            // then
            EventMessage event = result.event();
            assertThat(event.identifier()).isEqualTo("event-1");
            assertThat(event.type()).isEqualTo(new MessageType("com.example.ParcelDispatched", "2"));
            assertThat(event.timestamp()).isEqualTo(timestamp);
            assertThat(event.metadata()).containsEntry("traceId", "trace-1");
            assertThat(event.payloadAs(ParcelDispatched.class)).isEqualTo(new ParcelDispatched("parcel-1"));
        }

        @Test
        void exposesAggregateHeadersAsLegacyResources() {
            // when
            Context resources = testSubject.decode(record(axonFramework4DomainEventHeaders(Instant.now()), new byte[0]))
                                           .orElseThrow().resources();

            // then
            assertThat(resources.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isEqualTo("parcel-1");
            assertThat(resources.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isEqualTo(7L);
            assertThat(resources.getResource(LegacyResources.AGGREGATE_TYPE_KEY)).isEqualTo("Parcel");
        }

        @Test
        void missingRevisionResolvesToDefaultVersion() {
            // given
            RecordHeaders headers = new RecordHeaders();
            headers.add("axon-message-id", text("event-1"));
            headers.add("axon-message-type", text("com.example.ParcelDispatched"));

            // when
            EventMessage event = testSubject.decode(record(headers, new byte[0])).orElseThrow().event();

            // then
            assertThat(event.type().version()).isEqualTo("0.0.0");
        }

        @Test
        void recordsWithoutAnyAxonHeadersAreNotEvents() {
            // when / then
            assertThat(testSubject.decode(record(new RecordHeaders(), text("x")))).isEmpty();
        }

        @Test
        void rejectsMalformedTimestamp() {
            // given
            RecordHeaders headers = new RecordHeaders();
            headers.add("axon-message-id", text("event-1"));
            headers.add("axon-message-type", text("com.example.ParcelDispatched"));
            headers.add("axon-message-timestamp", text("yesterday"));

            // when / then
            assertThatThrownBy(() -> testSubject.decode(record(headers, new byte[0])))
                    .isInstanceOf(UndecodableRecordException.class);
        }
    }

    @Nested
    class ReadingMixedRecords {

        @Test
        void aggregateHeadersOnARecordInTheAxoniqFrameworkFormatBecomeLegacyResources() {
            // given - a record in the new format, which an Axon Framework 4 producer would not write, with aggregate
            // headers
            ProducerRecord<String, byte[]> produced = new DefaultKafkaEventCodec(converter).encode(
                    EventTestUtils.asEventMessage(new ParcelDispatched("parcel-1")), TOPIC, null, null
            );
            produced.headers().add(LegacyKafkaEventCodec.AGGREGATE_ID, text("parcel-1"));
            produced.headers().add(LegacyKafkaEventCodec.AGGREGATE_SEQ, number(7));

            // when
            Context resources = testSubject.decode(consumed(produced, 0, 0)).orElseThrow().resources();

            // then
            assertThat(resources.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isEqualTo("parcel-1");
            assertThat(resources.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isEqualTo(7L);
        }

        @Test
        void malformedAggregateSequenceIsIgnored() {
            // given
            RecordHeaders headers = axonFramework4DomainEventHeaders(Instant.now());
            headers.remove(LegacyKafkaEventCodec.AGGREGATE_SEQ);
            headers.add(LegacyKafkaEventCodec.AGGREGATE_SEQ, text("seven"));

            // when
            Context resources = testSubject.decode(record(headers, text("{}"))).orElseThrow().resources();

            // then - the event is still read, without aggregate information
            assertThat(resources.containsResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY)).isFalse();
            assertThat(resources.containsResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY)).isFalse();
        }

        @Test
        void resolvesTheTypeWithTheConfiguredEventTypeResolver() {
            // given
            LegacyKafkaEventCodec resolving = testSubject.withEventTypeResolver(
                    (name, version) -> new MessageType("renamed." + name, version == null ? "9.9" : version)
            );
            RecordHeaders headers = axonFramework4DomainEventHeaders(Instant.now());
            headers.remove(LegacyKafkaEventCodec.MESSAGE_REVISION);

            // when
            EventMessage event = resolving.decode(record(headers, text("{}"))).orElseThrow().event();

            // then
            assertThat(event.type().name()).startsWith("renamed.");
            assertThat(event.type().version()).isEqualTo("9.9");
        }
    }

    @Nested
    class Writing {

        @Test
        void writesNoAggregateHeadersForAnIdentifierWithoutSequence() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("parcel-1"));

            // when
            ProducerRecord<String, byte[]> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(
                    ctx -> CompletableFuture.completedFuture(testSubject.encode(
                            event, TOPIC, null, ctx.withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "parcel-1")
                    ))
            ).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(result.headers().lastHeader(LegacyKafkaEventCodec.AGGREGATE_ID)).isNull();
            assertThat(result.headers().lastHeader(LegacyKafkaEventCodec.AGGREGATE_SEQ)).isNull();
        }

        @Test
        void addsLegacyHeadersReadableByAxonFramework4() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("parcel-1"))
                                               .andMetadata(Map.of("traceId", "trace-1"));

            // when
            ProducerRecord<String, byte[]> result = testSubject.encode(event, TOPIC, "parcel-1", null);

            // then - Axon Framework 4 reads these with HeaderUtils.valueAsString / valueAsLong
            assertThat(textHeader(result, "axon-message-id")).isEqualTo(event.identifier());
            assertThat(textHeader(result, "axon-message-type")).isEqualTo(event.type().name());
            assertThat(textHeader(result, "axon-message-revision")).isEqualTo(event.type().version());
            assertThat(ByteBuffer.wrap(result.headers().lastHeader("axon-message-timestamp").value()).getLong())
                    .isEqualTo(event.timestamp().toEpochMilli());
            assertThat(textHeader(result, "axon-metadata-traceId")).isEqualTo("trace-1");
            // and the Axoniq Framework headers are still present
            assertThat(textHeader(result, "axon-id")).isEqualTo(event.identifier());
        }

        @Test
        void omitsTheRevisionOfAnEventWhoseVersionIsWhatAMissingRevisionReadsAs() {
            // given - an event that an Axon Framework 4 producer wrote without a revision, published again
            EventMessage event = new GenericEventMessage(new MessageType("com.example.ParcelDispatched", "0.0.0"),
                                                         new ParcelDispatched("parcel-1"));

            // when
            ProducerRecord<String, byte[]> result = testSubject.encode(event, TOPIC, "parcel-1", null);

            // then
            assertThat(result.headers().lastHeader("axon-message-revision")).isNull();
            assertThat(testSubject.decode(consumed(result, 0, 0)).orElseThrow().event().type())
                    .isEqualTo(event.type());
        }

        @Test
        void addsAggregateHeadersFromLegacyResources() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("parcel-1"));

            // when
            ProducerRecord<String, byte[]> result = UnitOfWorkTestUtils.aUnitOfWork().executeWithResult(ctx -> {
                var context = ctx.withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, "parcel-1")
                                 .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY, 3L)
                                 .withResource(LegacyResources.AGGREGATE_TYPE_KEY, "Parcel");
                return CompletableFuture.completedFuture(
                        testSubject.encode(event, TOPIC, null, context));
            }).orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(textHeader(result, "axon-message-aggregate-id")).isEqualTo("parcel-1");
            assertThat(ByteBuffer.wrap(result.headers().lastHeader("axon-message-aggregate-seq").value()).getLong())
                    .isEqualTo(3L);
            assertThat(textHeader(result, "axon-message-aggregate-type")).isEqualTo("Parcel");
        }

        @Test
        void roundTripPrefersTheAxoniqFrameworkFormat() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("parcel-1"));

            // when
            EventMessage decoded = testSubject.decode(consumed(testSubject.encode(event, TOPIC, null, null), 0, 0))
                                              .orElseThrow().event();

            // then
            assertThat(decoded.identifier()).isEqualTo(event.identifier());
            assertThat(decoded.type()).isEqualTo(event.type());
        }
    }
}
