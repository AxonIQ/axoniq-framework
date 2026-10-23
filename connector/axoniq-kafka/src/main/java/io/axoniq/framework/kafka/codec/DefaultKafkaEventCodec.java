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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The {@link KafkaEventCodec} implementing the Axoniq Framework wire format, with text keys and binary values.
 * <p>
 * The record value is the event's payload, converted to bytes by the configured {@link EventConverter}. Everything
 * else about the event travels in record headers. All header values are UTF-8 text, so that tools and consumers that
 * do not use Axon can read them:
 * <table>
 *     <caption>Record headers</caption>
 *     <tr><th>Header</th><th>Value</th></tr>
 *     <tr><td>{@value #ID_HEADER}</td><td>the event's {@link EventMessage#identifier() identifier}</td></tr>
 *     <tr><td>{@value #TYPE_HEADER}</td><td>the qualified name of the event's
 *     {@link EventMessage#type() type}</td></tr>
 *     <tr><td>{@value #VERSION_HEADER}</td><td>the version of the event's type</td></tr>
 *     <tr><td>{@value #TIMESTAMP_HEADER}</td><td>the event's {@link EventMessage#timestamp() timestamp}, in ISO-8601
 *     format</td></tr>
 *     <tr><td>{@value #METADATA_HEADER_PREFIX}{@code <key>}</td><td>one header per metadata entry</td></tr>
 *     <tr><td>{@value #TAG_HEADER_PREFIX}{@code <key>}</td><td>one header per {@link Tag tag}, when a
 *     {@link TagResolver} is configured</td></tr>
 * </table>
 * The record's own timestamp is set to the event's timestamp as well, so that consumers can look up positions in a
 * topic by time.
 * <p>
 * A record without the {@value #ID_HEADER} and {@value #TYPE_HEADER} headers carries no event in this format, and
 * {@link #decode(ConsumerRecord) decodes} to an empty result. Decoding does not convert the payload: the decoded event
 * holds the payload bytes and converts them with the configured {@link EventConverter} when a handler asks for the
 * payload in a specific type. The event of a record without the {@value #TIMESTAMP_HEADER} header gets the record's
 * own timestamp, or the current time when the record has none. A malformed {@value #TIMESTAMP_HEADER} header is
 * rejected with an {@link UndecodableRecordException}.
 * <p>
 * Example usage:
 * <pre>{@code
 * KafkaEventCodec<byte[]> codec = new DefaultKafkaEventCodec(eventConverter)
 *         .withTagResolver(new AnnotationBasedTagResolver());
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class DefaultKafkaEventCodec implements KafkaEventCodec<byte[]>, DescribableComponent {

    /**
     * The header holding the event's identifier.
     */
    public static final String ID_HEADER = "axon-id";
    /**
     * The header holding the qualified name of the event's type.
     */
    public static final String TYPE_HEADER = "axon-type";
    /**
     * The header holding the version of the event's type.
     */
    public static final String VERSION_HEADER = "axon-version";
    /**
     * The header holding the event's timestamp, in ISO-8601 format.
     */
    public static final String TIMESTAMP_HEADER = "axon-timestamp";
    /**
     * The prefix of the headers holding the event's metadata, followed by the metadata key.
     */
    public static final String METADATA_HEADER_PREFIX = "axon-metadata-";
    /**
     * The prefix of the headers holding the event's tags, followed by the tag key.
     */
    public static final String TAG_HEADER_PREFIX = "axon-tag-";

    private final EventConverter converter;
    private final EventTypeResolver eventTypeResolver;
    private final @Nullable TagResolver tagResolver;

    /**
     * Constructs a codec that converts payloads with the given {@code converter}.
     * <p>
     * The codec writes no tags, and resolves the type of a decoded event with {@link EventTypeResolver#DEFAULT}.
     *
     * @param converter the converter that converts event payloads to bytes when encoding, and that decoded events use
     *                  to convert their payload when a handler asks for it
     */
    public DefaultKafkaEventCodec(EventConverter converter) {
        this(converter, EventTypeResolver.DEFAULT, null);
    }

    private DefaultKafkaEventCodec(EventConverter converter,
                                   EventTypeResolver eventTypeResolver,
                                   @Nullable TagResolver tagResolver) {
        this.converter = Objects.requireNonNull(converter, "The converter must not be null.");
        this.eventTypeResolver = Objects.requireNonNull(eventTypeResolver, "The eventTypeResolver must not be null.");
        this.tagResolver = tagResolver;
    }

    /**
     * Returns a copy of this codec that writes the {@link Tag tags} the given {@code tagResolver} resolves for each
     * event, as {@value #TAG_HEADER_PREFIX}{@code <key>} headers.
     * <p>
     * Tags let consumers select events by tag, for example with the
     * {@link org.axonframework.messaging.eventstreaming.EventCriteria} of an event processor. Decoding reads tags
     * whether or not a resolver is configured.
     *
     * @param tagResolver the resolver of the tags to write for each event
     * @return a copy of this codec that writes the tags the given {@code tagResolver} resolves
     */
    public DefaultKafkaEventCodec withTagResolver(TagResolver tagResolver) {
        return new DefaultKafkaEventCodec(converter,
                                          eventTypeResolver,
                                          Objects.requireNonNull(tagResolver, "The tagResolver must not be null."));
    }

    /**
     * Returns a copy of this codec that resolves the type of decoded events with the given
     * {@code eventTypeResolver}.
     *
     * @param eventTypeResolver the resolver that builds the type of a decoded event from its {@value #TYPE_HEADER}
     *                          and {@value #VERSION_HEADER} headers
     * @return a copy of this codec that resolves the type of decoded events with the given {@code eventTypeResolver}
     */
    public DefaultKafkaEventCodec withEventTypeResolver(EventTypeResolver eventTypeResolver) {
        return new DefaultKafkaEventCodec(converter, eventTypeResolver, tagResolver);
    }

    @Override
    public ProducerRecord<String, byte[]> encode(EventMessage event,
                                                 String topic,
                                                 @Nullable String key,
                                                 @Nullable ProcessingContext context) {
        Objects.requireNonNull(event, "The event must not be null.");
        Objects.requireNonNull(topic, "The topic must not be null.");
        Headers headers = new RecordHeaders();
        headers.add(ID_HEADER, toBytes(event.identifier()));
        headers.add(TYPE_HEADER, toBytes(event.type().name()));
        headers.add(VERSION_HEADER, toBytes(event.type().version()));
        headers.add(TIMESTAMP_HEADER, toBytes(event.timestamp().toString()));
        event.metadata().forEach((metadataKey, value) -> headers.add(METADATA_HEADER_PREFIX + metadataKey,
                                                                    value == null ? null : toBytes(value)));
        if (tagResolver != null) {
            tagResolver.resolve(event, context)
                       .forEach(tag -> headers.add(TAG_HEADER_PREFIX + tag.key(), toBytes(tag.value())));
        }
        return new ProducerRecord<>(topic,
                                    null,
                                    event.timestamp().toEpochMilli(),
                                    key,
                                    event.payloadAs(byte[].class, converter),
                                    headers);
    }

    @Override
    public Optional<DecodedRecord> decode(ConsumerRecord<String, byte[]> record) {
        Objects.requireNonNull(record, "The record must not be null.");
        Headers headers = record.headers();
        String identifier = lastValue(headers, ID_HEADER);
        String type = lastValue(headers, TYPE_HEADER);
        if (identifier == null || type == null) {
            return Optional.empty();
        }
        Map<String, @Nullable String> metadata = new HashMap<>();
        Set<Tag> tags = new HashSet<>();
        for (Header header : headers) {
            if (header.key().startsWith(METADATA_HEADER_PREFIX)) {
                metadata.put(header.key().substring(METADATA_HEADER_PREFIX.length()), fromBytes(header.value()));
            } else if (header.key().startsWith(TAG_HEADER_PREFIX) && header.value() != null) {
                tags.add(Tag.of(header.key().substring(TAG_HEADER_PREFIX.length()), fromBytes(header.value())));
            }
        }
        //noinspection NullableProblems -- metadata values may be null, as Metadata allows
        EventMessage event = new GenericEventMessage(
                identifier,
                eventTypeResolver.resolve(type, lastValue(headers, VERSION_HEADER)),
                record.value(),
                metadata,
                timestampOf(record)
        ).withConverter(converter);
        return Optional.of(new DecodedRecord(event, Tag.addToContext(Context.empty(), Set.copyOf(tags))));
    }

    /**
     * Returns the timestamp of the given {@code record}, or the current time when the record has none.
     *
     * @param record the record whose timestamp to return
     * @return the record's timestamp, or the current time when the record has none
     */
    static Instant recordTimestamp(ConsumerRecord<?, ?> record) {
        return record.timestamp() >= 0 ? Instant.ofEpochMilli(record.timestamp()) : Instant.now();
    }

    private static Instant timestampOf(ConsumerRecord<String, byte[]> record) {
        String timestamp = lastValue(record.headers(), TIMESTAMP_HEADER);
        if (timestamp == null) {
            // Not written by this codec, but an event nonetheless: the record's own timestamp is the best estimate.
            return recordTimestamp(record);
        }
        try {
            return Instant.parse(timestamp);
        } catch (DateTimeParseException e) {
            throw new UndecodableRecordException(
                    "The record at offset [" + record.offset() + "] of partition [" + record.partition()
                            + "] on topic [" + record.topic() + "] has an unreadable [" + TIMESTAMP_HEADER
                            + "] header: [" + timestamp + "].",
                    e
            );
        }
    }

    private static @Nullable String lastValue(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header == null ? null : fromBytes(header.value());
    }

    private static byte[] toBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static @Nullable String fromBytes(byte @Nullable [] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public Serde<byte[]> valueSerde() {
        return Serdes.ByteArray();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("converter", converter);
        descriptor.describeProperty("eventTypeResolver", eventTypeResolver);
        if (tagResolver != null) {
            descriptor.describeProperty("tagResolver", tagResolver);
        }
    }
}
