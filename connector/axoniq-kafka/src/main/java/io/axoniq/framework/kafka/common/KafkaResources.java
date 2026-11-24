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

package io.axoniq.framework.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.Context.ResourceKey;

import java.time.Instant;
import java.util.Objects;

/**
 * The {@link ResourceKey ResourceKeys} that describe the Kafka record an event was consumed from.
 * <p>
 * The Kafka event sources add these resources to the context of every event they hand out. Handlers, interceptors and
 * sequencing policies read them from the {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}, for
 * example to process events per partition, or to log where a failing event came from:
 * <pre>{@code
 * @EventHandler
 * void on(ParcelDispatched event, ProcessingContext context) {
 *     TopicPartition partition = context.getResource(KafkaResources.TOPIC_PARTITION);
 *     Long offset = context.getResource(KafkaResources.OFFSET);
 *     // ...
 * }
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.5.0
 */
public final class KafkaResources {

    /**
     * The topic and partition of the record the event was consumed from.
     */
    public static final ResourceKey<TopicPartition> TOPIC_PARTITION = ResourceKey.withLabel("kafkaTopicPartition");

    /**
     * The offset of the record the event was consumed from, within its {@link #TOPIC_PARTITION}.
     * <p>
     * This is the offset of the record itself. The position to continue consuming from after this record is the next
     * offset, {@code offset + 1}.
     */
    public static final ResourceKey<Long> OFFSET = ResourceKey.withLabel("kafkaOffset");

    /**
     * The key of the record the event was consumed from. Absent when the record has no key.
     */
    public static final ResourceKey<String> RECORD_KEY = ResourceKey.withLabel("kafkaRecordKey");

    /**
     * The timestamp of the record the event was consumed from. Depending on the topic's configuration, this is the
     * time the producer created the record or the time the broker appended it. Absent when the record carries no
     * timestamp.
     */
    public static final ResourceKey<Instant> RECORD_TIMESTAMP = ResourceKey.withLabel("kafkaRecordTimestamp");

    private KafkaResources() {
        // utility class
    }

    /**
     * Adds the resources describing the given {@code record} to the given {@code context}: its
     * {@link #TOPIC_PARTITION topic and partition}, its {@link #OFFSET offset}, and its {@link #RECORD_KEY key} and
     * {@link #RECORD_TIMESTAMP timestamp} when the record has them.
     *
     * @param context the context to add the resources to
     * @param record  the record to describe
     * @return a copy of the given {@code context} with the resources describing the given {@code record} added
     */
    public static Context addToContext(Context context, ConsumerRecord<String, ?> record) {
        Objects.requireNonNull(context, "The context must not be null.");
        Objects.requireNonNull(record, "The record must not be null.");
        Context result = context.withResource(TOPIC_PARTITION, new TopicPartition(record.topic(), record.partition()))
                                .withResource(OFFSET, record.offset());
        if (record.key() != null) {
            result = result.withResource(RECORD_KEY, record.key());
        }
        if (record.timestamp() != ConsumerRecord.NO_TIMESTAMP) {
            result = result.withResource(RECORD_TIMESTAMP, Instant.ofEpochMilli(record.timestamp()));
        }
        return result;
    }
}
