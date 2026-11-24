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
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.kafka.KafkaMessageFactory;
import io.cloudevents.rw.CloudEventRWException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.eventsourcing.eventstore.EventTypeResolver;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * A {@link KafkaEventCodec} writing and reading events as <a href="https://cloudevents.io">CloudEvents</a>, in the
 * binary content mode of the CloudEvents Kafka protocol binding.
 * <p>
 * Use it for topics that systems outside the Axon ecosystem read or write in the CloudEvents format. An event maps to
 * a CloudEvent as follows:
 * <table>
 *     <caption>Mapping between events and CloudEvents</caption>
 *     <tr><th>Event</th><th>CloudEvent</th></tr>
 *     <tr><td>identifier</td><td>{@code id}</td></tr>
 *     <tr><td>qualified name of the type</td><td>{@code type}</td></tr>
 *     <tr><td>version of the type</td><td>the {@value #VERSION_EXTENSION} extension</td></tr>
 *     <tr><td>timestamp</td><td>{@code time}</td></tr>
 *     <tr><td>payload, converted to bytes</td><td>{@code data}, with the configured {@code datacontenttype}</td></tr>
 *     <tr><td>metadata</td><td>one extension per entry whose key is a valid extension name: lowercase letters and
 *     digits only, and not the name of a CloudEvents attribute, such as {@code id}, {@code type} or {@code source}.
 *     Other entries are not written; a warning names each such key once, for the first
 *     100 such keys.</td></tr>
 * </table>
 * The {@code source} attribute is the same for every event this codec writes, and given when the codec is created.
 * <p>
 * Reading, every CloudEvent decodes to an event, also when another system wrote it. A missing
 * {@value #VERSION_EXTENSION} extension resolves through the {@link EventTypeResolver}, and every extension other than
 * {@value #VERSION_EXTENSION} becomes a metadata entry. Records that are not CloudEvents decode to an empty result.
 * A record that carries a {@code ce_specversion} header, but cannot be read as a CloudEvent, is rejected with an
 * {@link UndecodableRecordException}.
 * <p>
 * The codec writes in the binary content mode. Records in the structured content mode, which carry the whole
 * CloudEvent in their value, with a content type such as {@code application/cloudevents+json} and no
 * {@code ce_specversion} header, are only read when a CloudEvents event format for that content type, such as the one
 * of {@code io.cloudevents:cloudevents-json-jackson}, is on the classpath. Without one, they decode to an empty
 * result, which the source's undecodable-record policy deals with.
 * <p>
 * The codec writes and reads no {@link Tag tags}, so the
 * {@link org.axonframework.messaging.eventstreaming.EventCriteria} of a processor cannot select events from a topic
 * in the CloudEvents format by tag.
 * <p>
 * This codec requires the {@code io.cloudevents:cloudevents-kafka} library on the classpath.
 * <pre>{@code
 * KafkaEventCodec<byte[]> codec = new CloudEventKafkaEventCodec(eventConverter, URI.create("/parcel-service"));
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public class CloudEventKafkaEventCodec implements KafkaEventCodec<byte[]>, DescribableComponent {

    /**
     * The CloudEvents extension holding the version of the event's type.
     */
    public static final String VERSION_EXTENSION = "axonversion";
    /**
     * The default content type of the data of written CloudEvents.
     */
    public static final String DEFAULT_DATA_CONTENT_TYPE = "application/json";

    /**
     * The number of distinct metadata keys that are reported when they cannot be written as an extension.
     */
    static final int MAX_REPORTED_KEYS = 100;

    private static final Logger logger = LoggerFactory.getLogger(CloudEventKafkaEventCodec.class);
    private static final Pattern EXTENSION_NAME = Pattern.compile("[a-z0-9]+");
    private static final String SPEC_VERSION_HEADER = "ce_specversion";
    private static final Set<String> RESERVED_NAMES = Set.of(
            "id", "source", "specversion", "type", "datacontenttype", "dataschema", "subject", "time", "data"
    );
    private final Set<String> droppedKeys = ConcurrentHashMap.newKeySet();

    private final EventConverter converter;
    private final URI source;
    private final String dataContentType;
    private final EventTypeResolver eventTypeResolver;

    /**
     * Constructs a codec that converts payloads with the given {@code converter}, and writes CloudEvents with the
     * given {@code source} and the {@value #DEFAULT_DATA_CONTENT_TYPE} content type.
     *
     * @param converter the converter that converts payloads to bytes when encoding, and that decoded events use to
     *                  convert their payload when a handler asks for it
     * @param source    the {@code source} attribute of the CloudEvents this codec writes
     */
    public CloudEventKafkaEventCodec(EventConverter converter, URI source) {
        this(converter, source, DEFAULT_DATA_CONTENT_TYPE, EventTypeResolver.DEFAULT);
    }

    private CloudEventKafkaEventCodec(EventConverter converter,
                                      URI source,
                                      String dataContentType,
                                      EventTypeResolver eventTypeResolver) {
        this.converter = Objects.requireNonNull(converter, "The converter must not be null.");
        this.source = Objects.requireNonNull(source, "The source must not be null.");
        this.dataContentType = Objects.requireNonNull(dataContentType, "The dataContentType must not be null.");
        this.eventTypeResolver = Objects.requireNonNull(eventTypeResolver, "The eventTypeResolver must not be null.");
    }

    /**
     * Returns a copy of this codec that writes the given {@code dataContentType}, which must match the format the
     * converter writes payloads in.
     *
     * @param dataContentType the {@code datacontenttype} attribute of the CloudEvents this codec writes
     * @return a copy of this codec with the given {@code dataContentType}
     */
    public CloudEventKafkaEventCodec withDataContentType(String dataContentType) {
        return new CloudEventKafkaEventCodec(converter, source, dataContentType, eventTypeResolver);
    }

    /**
     * Returns a copy of this codec that resolves the type of decoded events with the given
     * {@code eventTypeResolver}.
     *
     * @param eventTypeResolver the resolver that builds the type of a decoded event from its CloudEvents type and
     *                          {@value #VERSION_EXTENSION} extension
     * @return a copy of this codec with the given {@code eventTypeResolver}
     */
    public CloudEventKafkaEventCodec withEventTypeResolver(EventTypeResolver eventTypeResolver) {
        return new CloudEventKafkaEventCodec(converter, source, dataContentType, eventTypeResolver);
    }

    @Override
    public ProducerRecord<String, byte[]> encode(EventMessage event,
                                                 String topic,
                                                 @Nullable String key,
                                                 @Nullable ProcessingContext context) {
        Objects.requireNonNull(event, "The event must not be null.");
        CloudEventBuilder builder = CloudEventBuilder.v1()
                                                     .withId(event.identifier())
                                                     .withSource(source)
                                                     .withType(event.type().name())
                                                     .withTime(event.timestamp().atOffset(ZoneOffset.UTC))
                                                     .withExtension(VERSION_EXTENSION, event.type().version());
        byte[] payload = event.payloadAs(byte[].class, converter);
        if (payload != null) {
            builder.withData(dataContentType, payload);
        }
        event.metadata().forEach((metadataKey, value) -> {
            if (value == null || VERSION_EXTENSION.equals(metadataKey)) {
                return;
            }
            if (EXTENSION_NAME.matcher(metadataKey).matches() && !RESERVED_NAMES.contains(metadataKey)) {
                builder.withExtension(metadataKey, value);
            } else {
                reportDropped(metadataKey);
            }
        });
        return KafkaMessageFactory.createWriter(topic, null, event.timestamp().toEpochMilli(), key)
                                  .writeBinary(builder.build());
    }

    private void reportDropped(String metadataKey) {
        // Bounded, so that metadata with ever-changing keys cannot grow the set without limit.
        if (droppedKeys.size() >= MAX_REPORTED_KEYS || !droppedKeys.add(metadataKey)) {
            return;
        }
        logger.warn("Metadata entries with key [{}] are not written to CloudEvents: the key is not a valid CloudEvents "
                            + "extension name (lowercase letters and digits only), or it is the name of a CloudEvents "
                            + "attribute. This is reported once per key.", metadataKey);
        if (droppedKeys.size() == MAX_REPORTED_KEYS) {
            logger.warn("Reported {} metadata keys that are not written to CloudEvents; further keys are not "
                                + "reported.", MAX_REPORTED_KEYS);
        }
    }

    @Override
    public Optional<DecodedRecord> decode(ConsumerRecord<String, byte[]> record) {
        Objects.requireNonNull(record, "The record must not be null.");
        CloudEvent cloudEvent;
        try {
            cloudEvent = KafkaMessageFactory.createReader(record).toEvent();
        } catch (CloudEventRWException | IllegalStateException e) {
            if (record.headers().lastHeader(SPEC_VERSION_HEADER) == null) {
                return Optional.empty();
            }
            throw new UndecodableRecordException(
                    "The record at offset [" + record.offset() + "] of partition [" + record.partition()
                            + "] on topic [" + record.topic() + "] is a CloudEvent that cannot be read.", e
            );
        }
        Map<String, String> metadata = new HashMap<>();
        for (String extension : cloudEvent.getExtensionNames()) {
            Object value = cloudEvent.getExtension(extension);
            if (!VERSION_EXTENSION.equals(extension) && value != null) {
                metadata.put(extension, value.toString());
            }
        }
        Object version = cloudEvent.getExtension(VERSION_EXTENSION);
        CloudEventData data = cloudEvent.getData();
        Instant timestamp = cloudEvent.getTime() != null
                ? cloudEvent.getTime().toInstant()
                : record.timestamp() >= 0 ? Instant.ofEpochMilli(record.timestamp()) : Instant.now();
        EventMessage event = new GenericEventMessage(
                cloudEvent.getId(),
                eventTypeResolver.resolve(cloudEvent.getType(), version == null ? null : version.toString()),
                data == null ? null : data.toBytes(),
                metadata,
                timestamp
        ).withConverter(converter);
        return Optional.of(new DecodedRecord(event, Tag.addToContext(Context.empty(), Set.of())));
    }

    @Override
    public Serde<byte[]> valueSerde() {
        return Serdes.ByteArray();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("converter", converter);
        descriptor.describeProperty("source", source.toString());
        descriptor.describeProperty("dataContentType", dataContentType);
        descriptor.describeProperty("eventTypeResolver", eventTypeResolver);
    }
}
