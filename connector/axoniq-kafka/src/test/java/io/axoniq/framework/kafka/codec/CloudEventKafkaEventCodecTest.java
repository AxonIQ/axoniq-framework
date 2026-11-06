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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.kafka.KafkaMessageFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.axoniq.framework.kafka.utils.ConsumedRecords.consumed;
import static io.axoniq.framework.kafka.utils.ConsumedRecords.foreign;
import static org.assertj.core.api.Assertions.*;

class CloudEventKafkaEventCodecTest {

    private static final String TOPIC = "parcels";
    private static final URI SOURCE = URI.create("/parcel-service");

    private final EventConverter converter = new DelegatingEventConverter(new JacksonConverter());
    private final CloudEventKafkaEventCodec testSubject = new CloudEventKafkaEventCodec(converter, SOURCE);

    record ParcelDispatched(String parcelId) {

    }

    private CloudEvent written(EventMessage event) {
        return KafkaMessageFactory.createReader(consumed(testSubject.encode(event, TOPIC, "p-1", null), 0, 0))
                                  .toEvent();
    }

    @Nested
    class Writing {

        private Logger codecLogger;
        private ListAppender appender;

        @BeforeEach
        void attachAppender() {
            codecLogger = (Logger) LogManager.getLogger(CloudEventKafkaEventCodec.class);
            appender = new ListAppender("CloudEventKafkaEventCodec");
            appender.start();
            codecLogger.addAppender(appender);
        }

        @AfterEach
        void detachAppender() {
            codecLogger.removeAppender(appender);
            appender.stop();
        }

        @Test
        void writesACloudEventThatPlainCloudEventsReadersUnderstand() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("p-1"))
                                               .andMetadata(Map.of("traceid", "t-1", "Not-Valid", "dropped"));

            // when
            ProducerRecord<String, byte[]> record = testSubject.encode(event, TOPIC, "p-1", null);
            CloudEvent cloudEvent = KafkaMessageFactory.createReader(consumed(record, 0, 0)).toEvent();

            // then
            assertThat(cloudEvent.getId()).isEqualTo(event.identifier());
            assertThat(cloudEvent.getType()).isEqualTo(event.type().name());
            assertThat(cloudEvent.getSource()).isEqualTo(SOURCE);
            assertThat(cloudEvent.getTime().toInstant()).isEqualTo(event.timestamp());
            assertThat(cloudEvent.getDataContentType()).isEqualTo("application/json");
            assertThat(new String(cloudEvent.getData().toBytes(), StandardCharsets.UTF_8))
                    .contains("\"parcelId\":\"p-1\"");
            assertThat(cloudEvent.getExtension("axonversion")).isEqualTo(event.type().version());
            assertThat(cloudEvent.getExtension("traceid")).isEqualTo("t-1");
            assertThat(cloudEvent.getExtensionNames()).doesNotContain("Not-Valid", "not-valid");
            assertThat(record.key()).isEqualTo("p-1");
        }

        @Test
        void metadataNamedAfterCloudEventsAttributesDoesNotOverrideThem() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("p-1"))
                                               .andMetadata(Map.of("id", "forged", "source", "/elsewhere",
                                                                   "type", "Forged"));

            // when
            CloudEvent cloudEvent = written(event);
            EventMessage decoded = testSubject.decode(consumed(testSubject.encode(event, TOPIC, null, null), 0, 0))
                                              .orElseThrow().event();

            // then
            assertThat(cloudEvent.getId()).isEqualTo(event.identifier());
            assertThat(cloudEvent.getSource()).isEqualTo(SOURCE);
            assertThat(cloudEvent.getType()).isEqualTo(event.type().name());
            assertThat(decoded.identifier()).isEqualTo(event.identifier());
            assertThat(decoded.metadata()).doesNotContainKeys("id", "source", "type");
        }

        @Test
        void reportsEachDroppedMetadataKeyOnce() {
            // given
            EventMessage first = EventTestUtils.asEventMessage(new ParcelDispatched("p-1"))
                                               .andMetadata(Map.of("traceId", "t-1"));
            EventMessage second = EventTestUtils.asEventMessage(new ParcelDispatched("p-2"))
                                                .andMetadata(Map.of("traceId", "t-2"));

            // when
            CloudEvent cloudEvent = written(first);
            written(second);

            // then
            assertThat(cloudEvent.getExtensionNames()).doesNotContain("traceId", "traceid");
            assertThat(appender.getEvents()).singleElement().satisfies(logEvent -> {
                assertThat(logEvent.getLevel()).isEqualTo(Level.WARN);
                assertThat(logEvent.getMessage().getFormattedMessage()).contains("[traceId]");
            });
        }

        @Test
        void stopsReportingDroppedMetadataKeysAfterALimit() {
            // given - more distinct invalid keys than are reported
            int keys = CloudEventKafkaEventCodec.MAX_REPORTED_KEYS + 10;

            // when
            for (int i = 0; i < keys; i++) {
                written(EventTestUtils.asEventMessage(new ParcelDispatched("p-" + i))
                                      .andMetadata(Map.of("Key-" + i, "value")));
            }

            // then - one warning per reported key, and one saying that no further keys are reported
            assertThat(appender.getEvents()).hasSize(CloudEventKafkaEventCodec.MAX_REPORTED_KEYS + 1);
            assertThat(appender.getEvents().getLast().getMessage().getFormattedMessage())
                    .contains("further keys are not reported");
        }
    }

    @Nested
    class WritingOptions {

        @Test
        void writesTheConfiguredDataContentType() {
            // given
            CloudEventKafkaEventCodec xml = testSubject.withDataContentType("application/xml");

            // when
            CloudEvent cloudEvent = KafkaMessageFactory.createReader(consumed(
                    xml.encode(EventTestUtils.asEventMessage(new ParcelDispatched("p-1")), TOPIC, null, null), 0, 0
            )).toEvent();

            // then
            assertThat(cloudEvent.getDataContentType()).isEqualTo("application/xml");
        }

        @Test
        void writesNoDataForAnEventWithoutPayload() {
            // given - an event read from a record without value
            DefaultKafkaEventCodec defaultCodec = CodecTestFixtures.defaultCodec();
            ProducerRecord<String, byte[]> produced =
                    defaultCodec.encode(EventTestUtils.asEventMessage(new ParcelDispatched("p-1")), TOPIC, null, null);
            EventMessage withoutPayload = defaultCodec.decode(consumed(new ProducerRecord<>(
                    TOPIC, null, produced.timestamp(), null, null, produced.headers()
            ), 0, 0)).orElseThrow().event();

            // when
            CloudEvent cloudEvent = written(withoutPayload);

            // then
            assertThat(cloudEvent.getData()).isNull();
        }

        @Test
        void skipsMetadataEntriesWithoutValue() {
            // given
            Map<String, @Nullable String> metadata = new HashMap<>();
            metadata.put("traceid", null);
            metadata.put("tenant", "acme");
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("p-1")).withMetadata(metadata);

            // when
            CloudEvent cloudEvent = written(event);

            // then
            assertThat(cloudEvent.getExtensionNames()).contains("tenant").doesNotContain("traceid");
        }
    }

    @Nested
    class Reading {

        @Test
        void resolvesAMissingVersionWithTheConfiguredEventTypeResolver() {
            // given
            CloudEventKafkaEventCodec resolving = testSubject.withEventTypeResolver(
                    (name, version) -> new MessageType(name, version == null ? "9.9" : version)
            );
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                                                     .withId("ce-1")
                                                     .withSource(URI.create("/billing"))
                                                     .withType("com.example.InvoiceSent")
                                                     .build();
            ProducerRecord<String, byte[]> produced = KafkaMessageFactory.createWriter(TOPIC, "key")
                                                                         .writeBinary(cloudEvent);

            // when
            EventMessage event = resolving.decode(consumed(produced, 0, 0)).orElseThrow().event();

            // then
            assertThat(event.type()).isEqualTo(new MessageType("com.example.InvoiceSent", "9.9"));
        }

        @Test
        void cloudEventWithoutTimeTakesTheRecordTimestamp() {
            // given
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                                                     .withId("ce-1")
                                                     .withSource(URI.create("/billing"))
                                                     .withType("com.example.InvoiceSent")
                                                     .build();
            ProducerRecord<String, byte[]> produced = KafkaMessageFactory.createWriter(TOPIC, "key")
                                                                         .writeBinary(cloudEvent);
            ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                    TOPIC, 0, 0, 1_234L, TimestampType.CREATE_TIME, -1, -1, "key",
                    produced.value(), new RecordHeaders(produced.headers().toArray()), Optional.empty()
            );

            // when
            EventMessage event = testSubject.decode(record).orElseThrow().event();

            // then
            assertThat(event.timestamp()).isEqualTo(Instant.ofEpochMilli(1_234L));
        }

        @Test
        void readsCloudEventsWrittenByOtherSystems() {
            // given
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                                                     .withId("ce-1")
                                                     .withSource(URI.create("/billing"))
                                                     .withType("com.example.InvoiceSent")
                                                     .withTime(OffsetDateTime.parse("2026-10-10T12:00:00Z"))
                                                     .withData("application/json", "{\"invoice\":42}".getBytes())
                                                     .withExtension("tenant", "acme")
                                                     .build();
            ProducerRecord<String, byte[]> produced = KafkaMessageFactory.createWriter(TOPIC, "key")
                                                                         .writeBinary(cloudEvent);

            // when
            EventMessage event = testSubject.decode(consumed(produced, 0, 0)).orElseThrow().event();

            // then
            assertThat(event.identifier()).isEqualTo("ce-1");
            assertThat(event.type()).isEqualTo(new MessageType("com.example.InvoiceSent", "0.0.0"));
            assertThat(event.timestamp()).isEqualTo(OffsetDateTime.parse("2026-10-10T12:00:00Z").toInstant());
            assertThat(event.metadata()).containsEntry("tenant", "acme");
            assertThat(event.payloadAs(String.class)).isEqualTo("{\"invoice\":42}");
        }

        @Test
        void cloudEventWithoutTimeOrRecordTimestampIsTimestampedOnReading() {
            // given
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                                                     .withId("ce-1")
                                                     .withSource(URI.create("/billing"))
                                                     .withType("com.example.InvoiceSent")
                                                     .build();
            ProducerRecord<String, byte[]> produced = KafkaMessageFactory.createWriter(TOPIC, "key")
                                                                         .writeBinary(cloudEvent);
            ConsumerRecord<String, byte[]> withoutTimestamp = new ConsumerRecord<>(
                    TOPIC, 0, 0, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1, "key",
                    produced.value(), new RecordHeaders(produced.headers().toArray()), Optional.empty()
            );
            Instant before = Instant.now();

            // when
            EventMessage event = testSubject.decode(withoutTimestamp).orElseThrow().event();

            // then
            assertThat(event.timestamp()).isAfterOrEqualTo(before);
        }

        @Test
        void recordsThatAreNotCloudEventsAreNotEvents() {
            // given
            ConsumerRecord<String, byte[]> record = foreign(TOPIC, 0, 0, "k", "plain".getBytes());

            // when / then
            assertThat(testSubject.decode(record)).isEmpty();
        }

        @Test
        void malformedCloudEventIsRejected() {
            // given - a CloudEvents spec version, but no identifier, source or type
            ConsumerRecord<String, byte[]> record = foreign(TOPIC, 0, 7, "k", "{}".getBytes());
            record.headers().add("ce_specversion", "1.0".getBytes(StandardCharsets.UTF_8));

            // when / then
            assertThatThrownBy(() -> testSubject.decode(record))
                    .isInstanceOf(UndecodableRecordException.class)
                    .hasMessageContaining("[7]");
        }
    }

    @Nested
    class RoundTrip {

        @Test
        void roundTripKeepsTypeVersionAndPayload() {
            // given
            EventMessage event = EventTestUtils.asEventMessage(new ParcelDispatched("p-1"));

            // when
            EventMessage decoded = testSubject.decode(consumed(testSubject.encode(event, TOPIC, null, null), 0, 0))
                                              .orElseThrow().event();

            // then
            assertThat(decoded.type()).isEqualTo(event.type());
            assertThat(decoded.metadata()).doesNotContainKey("axonversion");
            assertThat(decoded.payloadAs(ParcelDispatched.class)).isEqualTo(new ParcelDispatched("p-1"));
        }
    }
}
