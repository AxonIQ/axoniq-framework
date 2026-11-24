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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.LegacyResources;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link KafkaEventCodec} that adds the record headers of Axon Framework 4's Kafka extension to the records of
 * another codec, and reads records that only carry those headers.
 * <p>
 * Use it while applications on Axon Framework 4 and on Axoniq Framework share topics:
 * <ul>
 *     <li><b>Writing</b>, records carry both formats. Next to the headers of the wrapped codec, every record gets
 *     {@value #MESSAGE_ID}, {@value #MESSAGE_TYPE}, {@value #MESSAGE_REVISION} and {@value #MESSAGE_TIMESTAMP} (epoch
 *     milliseconds as an 8-byte number). The revision header is left out when the event's version is the one that a
 *     missing revision reads as with the configured {@link EventTypeResolver}, as Axon Framework 4 wrote no revision
 *     for unrevisioned events. When the publishing context carries them as {@link LegacyResources}, records also get
 *     the aggregate headers {@value #AGGREGATE_ID}, {@value #AGGREGATE_SEQ} and {@value #AGGREGATE_TYPE}. Axon
 *     Framework 4 consumers resolve the payload class from the type header, so publish such events under a type
 *     whose name is the payload's class name, with a payload format their serializer reads.</li>
 *     <li><b>Reading</b>, records in the wrapped codec's format are decoded by it. Records that only carry Axon
 *     Framework 4 headers are decoded here: the type header and revision become the event's
 *     {@link org.axonframework.messaging.core.MessageType}, and the aggregate headers become {@link LegacyResources},
 *     so that sequencing per aggregate keeps working for events from Axon Framework 4 producers. The payload is
 *     converted by the configured converter when a handler asks for it, so it must be able to read the payload
 *     format of the Axon Framework 4 producer.</li>
 * </ul>
 * Metadata needs no translation: both formats write one {@value DefaultKafkaEventCodec#METADATA_HEADER_PREFIX}
 * header per entry.
 * <pre>{@code
 * KafkaEventCodec<byte[]> codec =
 *         new LegacyKafkaEventCodec(new DefaultKafkaEventCodec(eventConverter), eventConverter);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class LegacyKafkaEventCodec implements KafkaEventCodec<byte[]>, DescribableComponent {

    /**
     * The Axon Framework 4 header holding the event's identifier.
     */
    public static final String MESSAGE_ID = "axon-message-id";
    /**
     * The Axon Framework 4 header holding the payload type.
     */
    public static final String MESSAGE_TYPE = "axon-message-type";
    /**
     * The Axon Framework 4 header holding the payload revision.
     */
    public static final String MESSAGE_REVISION = "axon-message-revision";
    /**
     * The Axon Framework 4 header holding the event's timestamp, as epoch milliseconds in an 8-byte number.
     */
    public static final String MESSAGE_TIMESTAMP = "axon-message-timestamp";
    /**
     * The Axon Framework 4 header holding the identifier of the aggregate that published the event.
     */
    public static final String AGGREGATE_ID = "axon-message-aggregate-id";
    /**
     * The Axon Framework 4 header holding the sequence number of the event in its aggregate, as an 8-byte number.
     */
    public static final String AGGREGATE_SEQ = "axon-message-aggregate-seq";
    /**
     * The Axon Framework 4 header holding the type of the aggregate that published the event.
     */
    public static final String AGGREGATE_TYPE = "axon-message-aggregate-type";

    private final KafkaEventCodec<byte[]> delegate;
    private final EventConverter converter;
    private final EventTypeResolver eventTypeResolver;

    /**
     * Constructs a codec adding Axon Framework 4 headers to the records of the given {@code delegate}, and reading
     * records that only carry those headers.
     *
     * @param delegate  the codec for the Axoniq Framework format, typically a {@link DefaultKafkaEventCodec}
     * @param converter the converter that events decoded from the Axon Framework 4 format use to convert their
     *                  payload when a handler asks for it
     */
    public LegacyKafkaEventCodec(KafkaEventCodec<byte[]> delegate, EventConverter converter) {
        this(delegate, converter, EventTypeResolver.DEFAULT);
    }

    private LegacyKafkaEventCodec(KafkaEventCodec<byte[]> delegate,
                                  EventConverter converter,
                                  EventTypeResolver eventTypeResolver) {
        this.delegate = Objects.requireNonNull(delegate, "The delegate must not be null.");
        this.converter = Objects.requireNonNull(converter, "The converter must not be null.");
        this.eventTypeResolver = Objects.requireNonNull(eventTypeResolver, "The eventTypeResolver must not be null.");
    }

    /**
     * Returns a copy of this codec that resolves the type of events decoded from the Axon Framework 4 format with the
     * given {@code eventTypeResolver}. By default, a missing revision becomes version
     * {@value EventTypeResolver#MISSING_VERSION_DEFAULT}.
     *
     * @param eventTypeResolver the resolver building the type of a decoded event from its type and revision headers
     * @return a copy of this codec with the given {@code eventTypeResolver}
     */
    public LegacyKafkaEventCodec withEventTypeResolver(EventTypeResolver eventTypeResolver) {
        return new LegacyKafkaEventCodec(delegate, converter, eventTypeResolver);
    }

    @Override
    public ProducerRecord<String, byte[]> encode(EventMessage event,
                                                 String topic,
                                                 @Nullable String key,
                                                 @Nullable ProcessingContext context) {
        ProducerRecord<String, byte[]> record = delegate.encode(event, topic, key, context);
        Headers headers = record.headers();
        headers.add(MESSAGE_ID, text(event.identifier()));
        headers.add(MESSAGE_TYPE, text(event.type().name()));
        // Axon Framework 4 wrote no revision for unrevisioned events, and its upcasters match on that: an event whose
        // version is what a missing revision reads as, is written without one.
        String version = event.type().version();
        if (!version.equals(eventTypeResolver.resolve(event.type().name(), null).version())) {
            headers.add(MESSAGE_REVISION, text(version));
        }
        headers.add(MESSAGE_TIMESTAMP, number(event.timestamp().toEpochMilli()));
        if (context != null) {
            String aggregateId = context.getResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY);
            Long sequence = context.getResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY);
            String aggregateType = context.getResource(LegacyResources.AGGREGATE_TYPE_KEY);
            if (aggregateId != null && sequence != null) {
                headers.add(AGGREGATE_ID, text(aggregateId));
                headers.add(AGGREGATE_SEQ, number(sequence));
                if (aggregateType != null) {
                    headers.add(AGGREGATE_TYPE, text(aggregateType));
                }
            }
        }
        return record;
    }

    @Override
    public Optional<DecodedRecord> decode(ConsumerRecord<String, byte[]> record) {
        Optional<DecodedRecord> decoded = delegate.decode(record);
        if (decoded.isPresent()) {
            return decoded.map(d -> new DecodedRecord(d.event(), withAggregate(d.resources(), record.headers())));
        }
        Headers headers = record.headers();
        String identifier = textValue(headers, MESSAGE_ID);
        String type = textValue(headers, MESSAGE_TYPE);
        if (identifier == null || type == null) {
            return Optional.empty();
        }
        Long timestamp = numberValue(record, MESSAGE_TIMESTAMP);
        EventMessage event = new GenericEventMessage(
                identifier,
                eventTypeResolver.resolve(type, textValue(headers, MESSAGE_REVISION)),
                record.value(),
                metadata(headers),
                timestamp != null ? Instant.ofEpochMilli(timestamp) : DefaultKafkaEventCodec.recordTimestamp(record)
        ).withConverter(converter);
        Context resources = withAggregate(Tag.addToContext(Context.empty(), Set.of()), headers);
        return Optional.of(new DecodedRecord(event, resources));
    }

    private static Map<String, String> metadata(Headers headers) {
        Map<String, String> metadata = new HashMap<>();
        for (Header header : headers) {
            if (header.key().startsWith(DefaultKafkaEventCodec.METADATA_HEADER_PREFIX) && header.value() != null) {
                metadata.put(header.key().substring(DefaultKafkaEventCodec.METADATA_HEADER_PREFIX.length()),
                             new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        return metadata;
    }

    private static Context withAggregate(Context context, Headers headers) {
        String aggregateId = textValue(headers, AGGREGATE_ID);
        Header sequence = headers.lastHeader(AGGREGATE_SEQ);
        if (aggregateId == null || sequence == null || sequence.value() == null
                || sequence.value().length != Long.BYTES) {
            return context;
        }
        Context result = context.withResource(LegacyResources.AGGREGATE_IDENTIFIER_KEY, aggregateId)
                                .withResource(LegacyResources.AGGREGATE_SEQUENCE_NUMBER_KEY,
                                              ByteBuffer.wrap(sequence.value()).getLong());
        String aggregateType = textValue(headers, AGGREGATE_TYPE);
        return aggregateType == null ? result : result.withResource(LegacyResources.AGGREGATE_TYPE_KEY, aggregateType);
    }

    private static byte[] text(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] number(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static @Nullable String textValue(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static @Nullable Long numberValue(ConsumerRecord<String, byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        if (header == null || header.value() == null) {
            return null;
        }
        if (header.value().length != Long.BYTES) {
            throw new UndecodableRecordException(
                    "The record at offset [" + record.offset() + "] of partition [" + record.partition()
                            + "] on topic [" + record.topic() + "] has an unreadable [" + key + "] header."
            );
        }
        return ByteBuffer.wrap(header.value()).getLong();
    }

    @Override
    public Serde<byte[]> valueSerde() {
        return delegate.valueSerde();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("delegate", delegate);
        descriptor.describeProperty("converter", converter);
        descriptor.describeProperty("eventTypeResolver", eventTypeResolver);
    }
}
