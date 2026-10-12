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
import org.apache.kafka.common.serialization.Serde;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

/**
 * Translates an {@link EventMessage} to a Kafka record and back.
 * <p>
 * The codec is the format that the publishers and the consumers of a topic agree on. The
 * {@link io.axoniq.framework.kafka.publishing.KafkaEventSink} uses it to {@link #encode encode} the events it
 * publishes, and the {@link io.axoniq.framework.kafka.streaming.KafkaStreamableEventSource} and
 * {@link io.axoniq.framework.kafka.subscribing.KafkaSubscribableEventSource} use it to {@link #decode decode} the
 * records they consume. Every component reading from or writing to the same topic must use a compatible codec.
 * <p>
 * Record keys are strings. The value type {@code V} is up to the codec, and {@link #valueSerde()} provides the matching
 * Kafka serializer and deserializer.
 * <p>
 * {@link DefaultKafkaEventCodec} implements the Axoniq Framework wire format. Implement this interface to read or write
 * a different format, such as one that existing consumers of a topic already expect.
 *
 * @param <V> the type of record value this codec writes and reads
 * @author Allard Buijze
 * @since 5.5.0
 */
public interface KafkaEventCodec<V> {

    /**
     * Encodes the given {@code event} as a record for the given {@code topic}.
     * <p>
     * The returned record does not name a partition. Kafka's partitioner chooses one, based on the given {@code key}
     * when there is one.
     *
     * @param event   the event to encode
     * @param topic   the topic to publish the record to
     * @param key     the record key, which decides the partition, or {@code null} to publish without a key
     * @param context the context in which the event is published, or {@code null} when published outside of one
     * @return the record that represents the given {@code event}
     */
    ProducerRecord<String, V> encode(EventMessage event,
                                     String topic,
                                     @Nullable String key,
                                     @Nullable ProcessingContext context);

    /**
     * Decodes the given {@code record} into an event, along with the resources the record carries.
     * <p>
     * An empty result means the record does not carry an event in this codec's format, for example because another
     * application wrote it. The consuming component applies its {@link UndecodableRecordPolicy} to such records. It
     * applies the same policy when this method throws any {@link RuntimeException}.
     *
     * @param record the record to decode
     * @return the decoded event and its resources, or an empty {@link Optional} when the record does not carry an
     * event in this codec's format
     * @throws UndecodableRecordException if the record carries an event in this codec's format, but its contents
     *                                    cannot be read
     */
    Optional<DecodedRecord> decode(ConsumerRecord<String, V> record);

    /**
     * Returns the Kafka serializer and deserializer for the record values of this codec.
     * <p>
     * The Kafka clients the sink and sources create own the serializer and deserializer they are given, and close
     * them when they close.
     *
     * @return the serde for the record values of this codec
     */
    Serde<V> valueSerde();
}
