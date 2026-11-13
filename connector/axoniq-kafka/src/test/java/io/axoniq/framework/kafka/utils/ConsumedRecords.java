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

package io.axoniq.framework.kafka.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Builds {@link ConsumerRecord ConsumerRecords} as a consumer would receive them, without a broker.
 *
 * @author Allard Buijze
 */
public final class ConsumedRecords {

    private ConsumedRecords() {
    }

    /**
     * Returns the record a consumer receives for the given {@code produced} record, once written at the given
     * {@code partition} and {@code offset}.
     */
    public static <V> ConsumerRecord<String, V> consumed(ProducerRecord<String, V> produced,
                                                         int partition,
                                                         long offset) {
        long timestamp = produced.timestamp() == null ? ConsumerRecord.NO_TIMESTAMP : produced.timestamp();
        return new ConsumerRecord<>(produced.topic(), partition, offset, timestamp, TimestampType.CREATE_TIME,
                                    -1, -1, produced.key(), produced.value(),
                                    new RecordHeaders(produced.headers().toArray()), Optional.empty());
    }

    /**
     * Returns the exception a consumer raises when it cannot deserialize the value of the record at the given
     * {@code offset} of the given {@code partition}.
     */
    public static RecordDeserializationException undeserializable(TopicPartition partition, long offset) {
        return new RecordDeserializationException(DeserializationExceptionOrigin.VALUE, partition, offset,
                                                  ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                                                  ByteBuffer.allocate(0), ByteBuffer.allocate(0),
                                                  new RecordHeaders(), "bad record", null);
    }

    /**
     * Returns a record without any headers, as written by an application that does not use Axon.
     */
    public static <V> ConsumerRecord<String, V> foreign(String topic, int partition, long offset, String key, V value) {
        return new ConsumerRecord<>(topic, partition, offset, key, value);
    }
}
